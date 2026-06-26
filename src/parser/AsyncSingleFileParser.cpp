// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncSingleFileParser.h"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <cstring>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "engine/CallFixedSize.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/Log.h"

namespace qlever::parser {

namespace net = boost::asio;
using ec_t = boost::system::error_code;

// ============================================================================
// AsyncParserBase<InnerParser>
// ============================================================================
//
// Common base for `AsyncStreamingParser` and `AsyncParallelFileParser`. Stores
// the inner parser as a member (composition instead of inheritance), removing
// the multiple-inheritance conflict that would arise if the async parsers both
// inherited from `InnerParser` (which has `final` methods in `RdfParserBase`)
// and from `AsyncSingleFileParser`.
template <typename InnerParser>
class AsyncParserBase : public AsyncSingleFileParser {
 protected:
  InnerParser parser_;

 public:
  explicit AsyncParserBase(const EncodedIriManager* ev,
                           TripleComponent defaultGraphIri =
                               qlever::specialIds().at(DEFAULT_GRAPH_IRI))
      : parser_{ev, std::move(defaultGraphIri)} {}

  // Forward the `AsyncSingleFileParser` option accessors to the inner parser.
  TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() override {
    return parser_.integerOverflowBehavior();
  }
  bool& invalidLiteralsAreSkipped() override {
    return parser_.invalidLiteralsAreSkipped();
  }
};

// ============================================================================
// AsyncStreamingParser<InnerParser>
// ============================================================================
//
// One-thread-at-a-time streaming parser. Each call to `asyncGetNextBatch`
// returns the next ~PARSER_MIN_TRIPLES_AT_ONCE triples (or fewer at EOF). On
// a partial statement, the parser saves its state via `backupState()`, asks
// the byte source for more bytes, splices them into the buffer, and retries.
//
// `InnerParser` is a concrete parser type such as `TurtleParser<Tokenizer>`.
// It is held by composition in `AsyncParserBase<InnerParser>::parser_`, which
// means `AsyncStreamingParser` no longer inherits from `InnerParser` and
// therefore does not need `getLineImpl`/`getParsePosition` stubs or forwarding
// overrides for `integerOverflowBehavior`/`invalidLiteralsAreSkipped`.
template <typename InnerParser>
class AsyncStreamingParser final : public AsyncParserBase<InnerParser> {
  using Handler = AsyncSingleFileParser::Handler;
  using TripleBatch = AsyncSingleFileParser::TripleBatch;

 public:
  AsyncStreamingParser(std::unique_ptr<AsyncBlockSource> rawBuffer,
                       const EncodedIriManager* ev,
                       TripleComponent defaultGraphIri,
                       net::any_io_executor exec)
      : AsyncParserBase<InnerParser>{ev, std::move(defaultGraphIri)},
        strand_{net::make_strand(exec)} {
    this->parser_.clear();
    // The block source is given our strand as executor so that its
    // `asyncGetNextBlock` callbacks fire on `strand_` without a second
    // re-dispatch.
    fileBuffer_ = std::make_unique<AsyncEndRegexBlockSource>(
        strand_, std::move(rawBuffer), "([\\r\\n]+)");
  }

  void asyncGetNextBatch(Handler handler) override {
    net::post(strand_, [this, h = std::move(handler)]() mutable {
      if (this->parser_.isParserExhausted_) {
        h(nullptr, std::nullopt);
        return;
      }
      if (!firstBlockLoaded_) {
        loadFirstBlock(std::move(h));
        return;
      }
      runOneParseRound(std::move(h));
    });
  }

 private:
  struct BackupState {
    size_t numBlankNodes = 0;
    size_t numTriples = 0;
    const char* tokenizerPosition = nullptr;
    size_t tokenizerSize = 0;
  };

  // Snapshot the parts of the parser state that may need to be rewound when
  // a partial statement forces an asynchronous re-read.
  BackupState backupState() const {
    return {this->parser_.numBlankNodes_, this->parser_.triples_.size(),
            this->parser_.tok_.data().begin(),
            this->parser_.tok_.data().size()};
  }

  // Restore the parser to `b` and splice the freshly-arrived `nextBytes` at
  // the unparsed tail of `byteVec_`. Returns false (only) if the total buffer
  // exceeds `RDF_PARSER_MAX_TOTAL_BUFFER_SIZE` — caller raises in that case.
  bool spliceAndResetState(BackupState& b, ByteBlock nextBytes) {
    this->parser_.numBlankNodes_ = b.numBlankNodes;
    AD_CONTRACT_CHECK(this->parser_.triples_.size() >= b.numTriples);
    this->parser_.triples_.resize(b.numTriples);
    this->parser_.tok_.reset(b.tokenizerPosition, b.tokenizerSize);

    ByteBlock buf;
    numBytesBeforeCurrentBatch_ +=
        byteVec_.size() - this->parser_.tok_.data().size();
    buf.resize(this->parser_.tok_.data().size() + nextBytes.size());
    std::memcpy(buf.data(), this->parser_.tok_.data().begin(),
                this->parser_.tok_.data().size());
    std::memcpy(buf.data() + this->parser_.tok_.data().size(), nextBytes.data(),
                nextBytes.size());
    byteVec_ = std::move(buf);
    this->parser_.tok_.reset(byteVec_.data(), byteVec_.size());
    AD_LOG_TRACE << "Successfully decompressed next batch of "
                 << nextBytes.size() << " << bytes to parser\n";

    b = backupState();
    return byteVec_.size() <= RDF_PARSER_MAX_TOTAL_BUFFER_SIZE().getBytes();
  }

  // Initialize: read the first block of bytes and prime the tokenizer.
  // Called on `strand_`; the block source callback also fires on `strand_`.
  void loadFirstBlock(Handler h) {
    fileBuffer_->asyncGetNextBlock(
        [this, h = std::move(h)](std::exception_ptr ep,
                                 std::optional<ByteBlock> opt) mutable {
          if (ep) {
            h(ep, std::nullopt);
            return;
          }
          firstBlockLoaded_ = true;
          if (!opt) {
            AD_LOG_WARN << "The input stream for the turtle parser seems to "
                           "contain no data!\n";
            this->parser_.isParserExhausted_ = true;
            h(nullptr, std::nullopt);
            return;
          }
          byteVec_ = std::move(*opt);
          this->parser_.tok_.reset(byteVec_.data(), byteVec_.size());
          runOneParseRound(std::move(h));
        });
  }

  // One parse round: parse statements until we have ≥
  // PARSER_MIN_TRIPLES_AT_ONCE triples, EOF, or a partial statement that needs
  // more bytes. If a partial statement is hit, request more bytes from the byte
  // source and resume.
  void runOneParseRound(Handler h) {
    auto backup = backupState();
    while (this->parser_.triples_.size() < PARSER_MIN_TRIPLES_AT_ONCE &&
           !this->parser_.isParserExhausted_) {
      bool parsedStatement;
      std::optional<typename InnerParser::ParseException> ex;
      try {
        parsedStatement = this->parser_.statement();
      } catch (const typename InnerParser::ParseException& p) {
        parsedStatement = false;
        ex = p;
      }
      if (parsedStatement) continue;

      // Partial statement: fetch more bytes asynchronously and resume.
      fileBuffer_->asyncGetNextBlock([this, h = std::move(h), backup, ex](
                                         std::exception_ptr ep,
                                         std::optional<ByteBlock> opt) mutable {
        if (ep) {
          h(ep, std::nullopt);
          return;
        }
        if (!opt || opt->empty()) {
          // No more bytes. Surface the parse exception if any, else mark
          // exhausted and return the triples parsed so far.
          if (ex) {
            h(std::make_exception_ptr(*ex), std::nullopt);
            return;
          }
          this->parser_.tok_.skipWhitespaceAndComments();
          std::string_view unparsed = this->parser_.tok_.view();
          if (!unparsed.empty()) {
            AD_LOG_INFO << "Parsing of line has Failed, but parseInput is not "
                           "yet exhausted. Remaining bytes: "
                        << unparsed.size() << '\n';
            AD_LOG_INFO << "Logging first 1000 unparsed characters\n";
            AD_LOG_INFO << unparsed.substr(0, 1000) << std::endl;
          }
          this->parser_.isParserExhausted_ = true;
          deliverTriples(std::move(h));
          return;
        }
        BackupState b = backup;
        if (!spliceAndResetState(b, std::move(*opt))) {
          std::string_view unparsed = this->parser_.tok_.view();
          AD_LOG_ERROR << "Could not parse " << PARSER_MIN_TRIPLES_AT_ONCE
                       << " Within " << RDF_PARSER_MAX_TOTAL_BUFFER_SIZE()
                       << " of Turtle input\n";
          AD_LOG_INFO << "Logging first 1000 unparsed characters\n";
          AD_LOG_INFO << unparsed.substr(0, 1000) << std::endl;
          h(ex ? std::make_exception_ptr(*ex)
               : std::make_exception_ptr(std::runtime_error{
                     "Too many bytes parsed without finishing a turtle "
                     "statement"}),
            std::nullopt);
          return;
        }
        // Continue the same parse round with the extended buffer.
        runOneParseRoundFromState(std::move(h), b);
      });
      return;
    }
    deliverTriples(std::move(h));
  }

  // Same as `runOneParseRound`, but uses a caller-provided backup state — used
  // by the async fetch continuation to avoid re-creating the snapshot.
  void runOneParseRoundFromState(Handler h, BackupState backup) {
    while (this->parser_.triples_.size() < PARSER_MIN_TRIPLES_AT_ONCE &&
           !this->parser_.isParserExhausted_) {
      bool parsedStatement;
      std::optional<typename InnerParser::ParseException> ex;
      try {
        parsedStatement = this->parser_.statement();
      } catch (const typename InnerParser::ParseException& p) {
        parsedStatement = false;
        ex = p;
      }
      if (parsedStatement) {
        backup = backupState();
        continue;
      }
      // Re-enter the async fetch path with the latest snapshot.
      fileBuffer_->asyncGetNextBlock([this, h = std::move(h), backup, ex](
                                         std::exception_ptr ep,
                                         std::optional<ByteBlock> opt) mutable {
        if (ep) {
          h(ep, std::nullopt);
          return;
        }
        if (!opt || opt->empty()) {
          if (ex) {
            h(std::make_exception_ptr(*ex), std::nullopt);
            return;
          }
          this->parser_.tok_.skipWhitespaceAndComments();
          std::string_view unparsed = this->parser_.tok_.view();
          if (!unparsed.empty()) {
            AD_LOG_INFO << "Parsing of line has Failed, but parseInput is not "
                           "yet exhausted. Remaining bytes: "
                        << unparsed.size() << '\n';
            AD_LOG_INFO << "Logging first 1000 unparsed characters\n";
            AD_LOG_INFO << unparsed.substr(0, 1000) << std::endl;
          }
          this->parser_.isParserExhausted_ = true;
          deliverTriples(std::move(h));
          return;
        }
        BackupState b = backup;
        if (!spliceAndResetState(b, std::move(*opt))) {
          std::string_view unparsed = this->parser_.tok_.view();
          AD_LOG_ERROR << "Could not parse " << PARSER_MIN_TRIPLES_AT_ONCE
                       << " Within " << RDF_PARSER_MAX_TOTAL_BUFFER_SIZE()
                       << " of Turtle input\n";
          AD_LOG_INFO << "Logging first 1000 unparsed characters\n";
          AD_LOG_INFO << unparsed.substr(0, 1000) << std::endl;
          h(ex ? std::make_exception_ptr(*ex)
               : std::make_exception_ptr(std::runtime_error{
                     "Too many bytes parsed without finishing a turtle "
                     "statement"}),
            std::nullopt);
          return;
        }
        runOneParseRoundFromState(std::move(h), b);
      });
      return;
    }
    deliverTriples(std::move(h));
  }

  void deliverTriples(Handler h) {
    if (this->parser_.triples_.empty()) {
      h(nullptr, std::nullopt);
      return;
    }
    TripleBatch batch = std::move(this->parser_.triples_);
    this->parser_.triples_.clear();
    h(nullptr, std::move(batch));
  }

  net::strand<net::any_io_executor> strand_;
  std::unique_ptr<AsyncEndRegexBlockSource> fileBuffer_;
  ByteBlock byteVec_;
  size_t numBytesBeforeCurrentBatch_ = 0;
  bool firstBlockLoaded_ = false;
};

// ============================================================================
// AsyncParallelFileParser<InnerParser>
// ============================================================================
//
// Fan-out parser. A feeder coroutine (modeled as a callback chain on the
// feeder strand) pulls statement-aligned blocks from the byte source, and for
// each block grabs a token from an inflight-cap channel (capacity
// `NUM_PARALLEL_PARSER_THREADS`) and posts a parse task to `exec_`. Each parse
// task constructs an ephemeral `RdfStringParser<InnerParser>`, parses the block
// in isolation, pushes the result to an output channel (capacity
// `QUEUE_SIZE_AFTER_PARALLEL_PARSING`), releases the token, and increments a
// completed-batch counter on the feeder strand. When the byte source signals
// EOF and all dispatched parses have completed, the output channel is closed.
//
// Each external `asyncGetNextBatch` call is a single `async_receive` on the
// output channel.
template <typename InnerParser>
class AsyncParallelFileParser final : public AsyncParserBase<InnerParser> {
  using Handler = AsyncSingleFileParser::Handler;
  using TripleBatch = AsyncSingleFileParser::TripleBatch;

 public:
  AsyncParallelFileParser(std::unique_ptr<AsyncBlockSource> rawBuffer,
                          const EncodedIriManager* ev,
                          const TripleComponent& defaultGraphIri,
                          net::any_io_executor exec)
      : AsyncParserBase<InnerParser>{ev, defaultGraphIri},
        exec_{exec},
        defaultGraphIri_{defaultGraphIri} {
    this->parser_.clear();
    fileBuffer_ = std::make_unique<AsyncEndRegexBlockSource>(
        exec, std::move(rawBuffer), "\\.[\\t ]*([\\r\\n]+)");
  }

  void asyncGetNextBatch(Handler handler) override {
    // Lazily start the feeder on first call; pin the output channel and
    // tokens channel to `exec_` and the feeder strand to a strand on `exec_`.
    std::call_once(feederStarted_, [this] {
      feederStrand_ = std::make_unique<Strand>(net::make_strand(exec_));
      outputCh_ = std::make_unique<OutputCh>(
          exec_, /*max_buffer_size=*/QUEUE_SIZE_AFTER_PARALLEL_PARSING);
      tokenCh_ = std::make_unique<TokenCh>(
          exec_, /*max_buffer_size=*/NUM_PARALLEL_PARSER_THREADS);
      net::post(*feederStrand_, [this] { initFeeder(); });
    });
    outputCh_->async_receive(
        [h = std::move(handler)](ec_t ec, BatchOrEof v) mutable {
          if (ec) {
            // Channel closed without an explicit exception means EOF; we
            // distinguish by `v.error` being set.
            if (v.error) {
              h(v.error, std::nullopt);
              return;
            }
            h(nullptr, std::nullopt);
            return;
          }
          if (v.error) {
            h(v.error, std::nullopt);
            return;
          }
          if (!v.batch) {
            h(nullptr, std::nullopt);
            return;
          }
          h(nullptr, std::move(*v.batch));
        });
  }

  // Destroy the parser. If the feeder was ever started, waits for all async
  // work (including any in-flight `dispatchParse` tasks) to finish before
  // returning so that no callbacks reference `this` after destruction.
  ~AsyncParallelFileParser() override {
    cancel();
    // `feederStrand_` is non-null only if `asyncGetNextBatch` was called at
    // least once (lazy init inside `std::call_once`).
    if (feederStrand_) feederDoneFuture_.get();
  }

  void cancel() override {
    cancelled_ = true;
    if (outputCh_) outputCh_->close();
    if (tokenCh_) tokenCh_->close();
  }

 private:
  struct BatchOrEof {
    std::optional<TripleBatch> batch;
    std::exception_ptr error;
  };
  // Sends to `outputCh_` are serialized through `feederStrand_` (via
  // `trySendInOrder`), so a plain `channel` suffices here. We keep
  // `concurrent_channel` for safety in case any future path bypasses the
  // strand.
  using OutputCh =
      net::experimental::concurrent_channel<void(ec_t, BatchOrEof)>;
  // A token is a `bool` placeholder; we use `bool` so the channel signature
  // is the moral equivalent of "semaphore of unit tokens".
  using TokenCh = net::experimental::concurrent_channel<void(ec_t, bool)>;
  using Strand = net::strand<net::any_io_executor>;

  // Initialization runs once on the feeder strand. It parses the prefix
  // declarations synchronously from the first block(s), copies the resulting
  // headers to the master parser (so that error messages refer to them), and
  // then enters the main feeder loop.
  void initFeeder() {
    fillTokenBucket();
    readPrefixHeaderStep();
  }

  // Pre-populate the inflight-token channel with NUM_PARALLEL_PARSER_THREADS
  // tokens so the feeder can launch that many parses concurrently.
  void fillTokenBucket() {
    for (size_t i = 0; i < NUM_PARALLEL_PARSER_THREADS; ++i) {
      tokenCh_->try_send(ec_t{}, true);
    }
  }

  // Step 1 of initialization: read blocks until we have parsed all prefix
  // declarations. The residual bytes (which belong to the first real
  // statement) become the first batch of the feeder loop.
  //
  // `asyncGetNextBlock` fires its callback on the file buffer's strand, which
  // is a different strand from `feederStrand_`. The error path calls
  // `closeWithError`, which accesses `inflight_` and `pendingError_` —
  // state that must only be modified on `feederStrand_`. Only that path is
  // dispatched; the normal paths already post to `feederStrand_` explicitly
  // (`net::post(*feederStrand_, ...)`) and are therefore safe as-is.
  void readPrefixHeaderStep() {
    fileBuffer_->asyncGetNextBlock(
        [this](std::exception_ptr ep, std::optional<ByteBlock> opt) mutable {
          if (ep) {
            net::dispatch(*feederStrand_, [this, ep] { closeWithError(ep); });
            return;
          }
          if (!opt) {
            // Empty input: no prefix block to read. `finishFeeder` only
            // closes the channels (thread-safe) and fulfils the promise.
            AD_LOG_WARN << "Empty input to the TURTLE parser, is this what "
                           "you intended?"
                        << std::endl;
            finishFeeder();
            return;
          }
          // Use a transient string parser to consume prefix declarations.
          declarationParser_.setInputStream(std::move(*opt));
          while (declarationParser_.parseDirectiveManually()) {
          }
          auto remainder = declarationParser_.getUnparsedRemainder();
          if (remainder.empty()) {
            // Need more bytes to find the first non-directive content.
            net::post(*feederStrand_, [this] { readPrefixHeaderStep(); });
            return;
          }
          // Copy parsed prefixes / base IRI into the master parser so its
          // diagnostics see the same headers.
          InnerParser::copyHeaderFrom(declarationParser_, this->parser_);
          ByteBlock first;
          first.reserve(remainder.size());
          ql::ranges::copy(remainder, std::back_inserter(first));
          // Feeder strand now drives the main loop.
          net::post(*feederStrand_, [this, first = std::move(first)]() mutable {
            feederStep(std::move(first));
          });
        });
  }

  // The main feeder loop. `firstBatch` is the residual bytes from the prefix
  // initialization (used only on the first invocation; empty on subsequent
  // ones).
  void feederStep(ByteBlock firstBatch) {
    if (cancelled_) {
      // Don't call `finishFeeder()` until all in-flight parse tasks are done.
      if (inflight_ == 0) finishFeeder();
      return;
    }
    if (!firstBatch.empty()) {
      acquireTokenAndDispatch(std::move(firstBatch));
      return;
    }
    // The callback fires on the file buffer's strand.  The error and EOF
    // paths touch `pendingError_`, `inflight_`, and `eofSeen_`, which must
    // only be accessed on `feederStrand_`; dispatch those paths there.
    // The normal path calls `acquireTokenAndDispatch`, which internally uses
    // `bind_executor(*feederStrand_, …)` to run its work on the feeder strand
    // and is therefore safe to call from any thread.
    fileBuffer_->asyncGetNextBlock(
        [this](std::exception_ptr ep, std::optional<ByteBlock> opt) mutable {
          if (ep) {
            net::dispatch(*feederStrand_, [this, ep] { closeWithError(ep); });
            return;
          }
          if (!opt) {
            // EOF: no more blocks. Wait for inflight parses to complete and
            // then close the output channel.
            net::dispatch(*feederStrand_, [this] {
              eofSeen_ = true;
              maybeFinishFeeder();
            });
            return;
          }
          acquireTokenAndDispatch(std::move(*opt));
        });
  }

  // Acquire a token from the inflight semaphore, then post a parse task to
  // `exec_`. Each block is tagged with a monotonically increasing sequence
  // number. Results are buffered in `pendingResults_` and forwarded to
  // `outputCh_` strictly in sequence order by `trySendInOrder`, so the caller
  // always sees errors in the order they appear in the input.
  void acquireTokenAndDispatch(ByteBlock block) {
    tokenCh_->async_receive(net::bind_executor(
        *feederStrand_,
        [this, block = std::move(block)](ec_t ec, bool) mutable {
          if (ec) {
            // Token channel closed (cancellation). Only call `finishFeeder()`
            // when no parse tasks are in flight so they can still complete.
            if (inflight_ == 0) finishFeeder();
            return;
          }
          ++inflight_;
          size_t parsePosition = parsePositionCursor_;
          parsePositionCursor_ += block.size();
          size_t seqNum = nextDispatchSeq_++;
          dispatchParse(parsePosition, seqNum, std::move(block));
          // Immediately schedule the next feeder step so the inflight cap is
          // re-checked.
          net::post(*feederStrand_, [this] { feederStep(ByteBlock{}); });
        }));
  }

  void dispatchParse(size_t parsePosition, size_t seqNum, ByteBlock block) {
    net::post(exec_, [this, parsePosition, seqNum,
                      block = std::move(block)]() mutable {
      std::exception_ptr error;
      std::optional<TripleBatch> result;
      try {
        RdfStringParser<InnerParser> parser{&this->parser_.encodedIriManager(),
                                            defaultGraphIri_};
        InnerParser::copyHeaderFrom(this->parser_, parser);
        parser.useSimplifiedGrammar();
        parser.setPositionOffset(parsePosition);
        parser.setFileBlankNodePrefix(this->parser_.fileBlankNodePrefix_);
        parser.setInputStream(std::move(block));
        result = parser.parseAndReturnAllTriples();
      } catch (...) {
        error = std::current_exception();
      }
      // Dispatch result to `feederStrand_` for ordered delivery.
      // `trySendInOrder` flushes consecutive results to `outputCh_`.
      net::dispatch(
          *feederStrand_,
          [this, seqNum, r = BatchOrEof{std::move(result), error}]() mutable {
            pendingResults_.emplace(seqNum, std::move(r));
            trySendInOrder();
          });
    });
  }

  // Forward the next in-sequence result (if available) from `pendingResults_`
  // to `outputCh_`. Must be called on `feederStrand_`. After each successful
  // send the callback decrements `inflight_`, releases a token, and calls
  // `trySendInOrder` again to continue flushing.
  void trySendInOrder() {
    auto it = pendingResults_.find(nextSendSeq_);
    if (it == pendingResults_.end()) {
      // Next result is not yet ready; we will be called again when it arrives.
      if (inflight_ == 0) {
        if (pendingError_) {
          maybeCloseWithError();
        } else {
          maybeFinishFeeder();
        }
      }
      return;
    }
    auto result = std::move(it->second);
    pendingResults_.erase(it);
    ++nextSendSeq_;
    outputCh_->async_send(ec_t{}, std::move(result), [this](ec_t /*ec*/) {
      net::dispatch(*feederStrand_, [this] {
        --inflight_;
        if (cancelled_ && inflight_ == 0) {
          finishFeeder();
          return;
        }
        if (!cancelled_ && !pendingError_) {
          tokenCh_->try_send(ec_t{}, true);
        }
        trySendInOrder();
      });
    });
  }

  void maybeFinishFeeder() {
    if (eofSeen_ && inflight_ == 0) {
      finishFeeder();
    }
  }

  // Close channels and signal the destructor that all feeder work is done.
  // Guard with `feederDoneFlag_` so the promise is fulfilled exactly once.
  void finishFeeder() {
    if (outputCh_) outputCh_->close();
    if (tokenCh_) tokenCh_->close();
    std::call_once(feederDoneFlag_, [this] { feederDonePromise_.set_value(); });
  }

  // Send the pending error and close channels. Called on `feederStrand_`
  // once `inflight_ == 0` to guarantee all parse tasks have completed.
  void maybeCloseWithError() {
    if (inflight_ != 0) return;
    if (outputCh_) {
      outputCh_->async_send(ec_t{}, BatchOrEof{std::nullopt, pendingError_},
                            [this](ec_t) { finishFeeder(); });
    }
  }

  // Defer a file-read error: record it and wait for in-flight parse tasks to
  // drain (so they can still push their results), then close via
  // `maybeCloseWithError()`.
  void closeWithError(std::exception_ptr ep) {
    pendingError_ = ep;
    maybeCloseWithError();
  }

  net::any_io_executor exec_;
  TripleComponent defaultGraphIri_;
  std::unique_ptr<AsyncEndRegexBlockSource> fileBuffer_;
  RdfStringParser<InnerParser> declarationParser_{
      &this->parser_.encodedIriManager()};
  std::once_flag feederStarted_;
  std::unique_ptr<Strand> feederStrand_;
  std::unique_ptr<OutputCh> outputCh_;
  std::unique_ptr<TokenCh> tokenCh_;
  size_t inflight_ = 0;
  bool eofSeen_ = false;
  bool cancelled_ = false;
  size_t parsePositionCursor_ = 0;
  // Sequence counter for ordered delivery: each block dispatched in
  // `acquireTokenAndDispatch` gets the next value of `nextDispatchSeq_`.
  // `nextSendSeq_` tracks the next result to forward to `outputCh_`.
  // `pendingResults_` buffers completed results that arrived out of order.
  size_t nextDispatchSeq_ = 0;
  size_t nextSendSeq_ = 0;
  std::map<size_t, BatchOrEof> pendingResults_;
  // Set when the file reader signals an error; the error is forwarded to the
  // output channel only after all in-flight parse tasks finish (so their
  // results — which may include valid triples — are emitted first).
  std::exception_ptr pendingError_;
  // Fulfilled by `finishFeeder()` once all async work is done. The destructor
  // waits on this future to prevent callbacks from referencing `this` after
  // destruction.
  std::promise<void> feederDonePromise_;
  std::shared_future<void> feederDoneFuture_{feederDonePromise_.get_future()};
  std::once_flag feederDoneFlag_;
};

// ============================================================================
// Factory: dispatch over (parseInParallel, filetype) and return the right
// concrete `AsyncSingleFileParser`. Mirrors the dispatch logic of the old
// `makeSingleRdfParser`.
// ============================================================================
namespace {
template <typename TokenizerT>
std::unique_ptr<AsyncSingleFileParser> makeWithTokenizer(
    const qlever::InputFileSpecification& input, const EncodedIriManager* ev,
    ad_utility::MemorySize bufferSize, net::any_io_executor exec) {
  auto graph = [&input]() -> TripleComponent {
    if (input.defaultGraph_.has_value()) {
      return TripleComponent::Iri::fromIrirefWithoutBrackets(
          *input.defaultGraph_);
    }
    return qlever::specialIds().at(DEFAULT_GRAPH_IRI);
  };
  auto impl = ad_utility::ApplyAsValueIdentity{
      [&input, &bufferSize, &graph, ev, &exec](
          auto useParallel,
          auto isTurtleInput) -> std::unique_ptr<AsyncSingleFileParser> {
        using InnerParser =
            std::conditional_t<isTurtleInput == 1, TurtleParser<TokenizerT>,
                               NQuadParser<TokenizerT>>;
        using ParserT = std::conditional_t<useParallel == 1,
                                           AsyncParallelFileParser<InnerParser>,
                                           AsyncStreamingParser<InnerParser>>;
        return std::make_unique<ParserT>(
            input.getAsyncBlockSource(exec, bufferSize.getBytes()), ev, graph(),
            exec);
      }};
  return ad_utility::callFixedSize(
      std::array{input.parseInParallel_ ? 1 : 0,
                 input.filetype_ == qlever::Filetype::Turtle ? 1 : 0},
      impl);
}
}  // namespace

std::unique_ptr<AsyncSingleFileParser> makeAsyncSingleFileParser(
    const qlever::InputFileSpecification& input, const EncodedIriManager* ev,
    ad_utility::MemorySize bufferSize, net::any_io_executor exec) {
  return makeWithTokenizer<Tokenizer>(input, ev, bufferSize, exec);
}

// ____________________________________________________________________________
template <typename InnerParser>
std::unique_ptr<AsyncSingleFileParser> makeStreamingParser(
    std::unique_ptr<AsyncBlockSource> rawBuffer, const EncodedIriManager* ev,
    TripleComponent defaultGraph, net::any_io_executor exec) {
  return std::make_unique<AsyncStreamingParser<InnerParser>>(
      std::move(rawBuffer), ev, std::move(defaultGraph), exec);
}

template <typename InnerParser>
std::unique_ptr<AsyncSingleFileParser> makeParallelFileParser(
    std::unique_ptr<AsyncBlockSource> rawBuffer, const EncodedIriManager* ev,
    const TripleComponent& defaultGraph, net::any_io_executor exec) {
  return std::make_unique<AsyncParallelFileParser<InnerParser>>(
      std::move(rawBuffer), ev, defaultGraph, exec);
}

// Explicit instantiations of the factory templates so the symbols are
// linkable from outside this translation unit.
template std::unique_ptr<AsyncSingleFileParser>
makeStreamingParser<TurtleParser<Tokenizer>>(std::unique_ptr<AsyncBlockSource>,
                                             const EncodedIriManager*,
                                             TripleComponent,
                                             net::any_io_executor);
template std::unique_ptr<AsyncSingleFileParser>
makeStreamingParser<TurtleParser<TokenizerCtre>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    TripleComponent, net::any_io_executor);
template std::unique_ptr<AsyncSingleFileParser>
makeStreamingParser<NQuadParser<Tokenizer>>(std::unique_ptr<AsyncBlockSource>,
                                            const EncodedIriManager*,
                                            TripleComponent,
                                            net::any_io_executor);
template std::unique_ptr<AsyncSingleFileParser>
makeStreamingParser<NQuadParser<TokenizerCtre>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    TripleComponent, net::any_io_executor);

template std::unique_ptr<AsyncSingleFileParser>
makeParallelFileParser<TurtleParser<Tokenizer>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    const TripleComponent&, net::any_io_executor);
template std::unique_ptr<AsyncSingleFileParser>
makeParallelFileParser<TurtleParser<TokenizerCtre>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    const TripleComponent&, net::any_io_executor);
template std::unique_ptr<AsyncSingleFileParser>
makeParallelFileParser<NQuadParser<Tokenizer>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    const TripleComponent&, net::any_io_executor);
template std::unique_ptr<AsyncSingleFileParser>
makeParallelFileParser<NQuadParser<TokenizerCtre>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    const TripleComponent&, net::any_io_executor);

// Explicit instantiations.
template class AsyncStreamingParser<TurtleParser<Tokenizer>>;
template class AsyncStreamingParser<TurtleParser<TokenizerCtre>>;
template class AsyncStreamingParser<NQuadParser<Tokenizer>>;
template class AsyncStreamingParser<NQuadParser<TokenizerCtre>>;
template class AsyncParallelFileParser<TurtleParser<Tokenizer>>;
template class AsyncParallelFileParser<TurtleParser<TokenizerCtre>>;
template class AsyncParallelFileParser<NQuadParser<Tokenizer>>;
template class AsyncParallelFileParser<NQuadParser<TokenizerCtre>>;

}  // namespace qlever::parser
