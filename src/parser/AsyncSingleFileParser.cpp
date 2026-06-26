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
// AsyncStreamingParser<T>
// ============================================================================
//
// One-thread-at-a-time streaming parser. Each call to `asyncGetNextBatch`
// returns the next ~PARSER_MIN_TRIPLES_AT_ONCE triples (or fewer at EOF). On
// a partial statement, the parser saves its state via `backupState()`, asks
// the byte source for more bytes, splices them into the buffer, and retries.
template <typename T>
class AsyncStreamingParser final : public T, public AsyncSingleFileParser {
 public:
  AsyncStreamingParser(std::unique_ptr<AsyncBlockSource> rawBuffer,
                       const EncodedIriManager* ev,
                       TripleComponent defaultGraphIri =
                           qlever::specialIds().at(DEFAULT_GRAPH_IRI))
      : T{ev, std::move(defaultGraphIri)} {
    this->clear();
    fileBuffer_ = std::make_unique<AsyncEndRegexBlockSource>(
        std::move(rawBuffer), "([\\r\\n]+)");
  }

  void asyncGetNextBatch(net::any_io_executor exec, Handler handler) override {
    net::post(exec, [this, exec, h = std::move(handler)]() mutable {
      if (this->isParserExhausted_) {
        h(nullptr, std::nullopt);
        return;
      }
      if (!firstBlockLoaded_) {
        loadFirstBlock(exec, std::move(h));
        return;
      }
      runOneParseRound(exec, std::move(h));
    });
  }

  // These sync methods are not used in the async streaming path.
  bool getLineImpl(TurtleTriple*) override { AD_FAIL(); }
  size_t getParsePosition() const override { return 0; }

  // Forward `AsyncSingleFileParser`'s pure virtual accessors to `T`'s
  // implementations (in `RdfParserBase`). Without explicit forwarding, clang
  // does not merge the two vtable slots even though the signatures are
  // identical.
  TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() override {
    return T::integerOverflowBehavior();
  }
  bool& invalidLiteralsAreSkipped() override {
    return T::invalidLiteralsAreSkipped();
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
    return {this->numBlankNodes_, this->triples_.size(),
            this->tok_.data().begin(), this->tok_.data().size()};
  }

  // Restore the parser to `b` and splice the freshly-arrived `nextBytes` at
  // the unparsed tail of `byteVec_`. Returns false (only) if the total buffer
  // exceeds `RDF_PARSER_MAX_TOTAL_BUFFER_SIZE` — caller raises in that case.
  bool spliceAndResetState(BackupState& b, ByteBlock nextBytes) {
    this->numBlankNodes_ = b.numBlankNodes;
    AD_CONTRACT_CHECK(this->triples_.size() >= b.numTriples);
    this->triples_.resize(b.numTriples);
    this->tok_.reset(b.tokenizerPosition, b.tokenizerSize);

    ByteBlock buf;
    numBytesBeforeCurrentBatch_ += byteVec_.size() - this->tok_.data().size();
    buf.resize(this->tok_.data().size() + nextBytes.size());
    std::memcpy(buf.data(), this->tok_.data().begin(),
                this->tok_.data().size());
    std::memcpy(buf.data() + this->tok_.data().size(), nextBytes.data(),
                nextBytes.size());
    byteVec_ = std::move(buf);
    this->tok_.reset(byteVec_.data(), byteVec_.size());
    AD_LOG_TRACE << "Successfully decompressed next batch of "
                 << nextBytes.size() << " << bytes to parser\n";

    b = backupState();
    return byteVec_.size() <= RDF_PARSER_MAX_TOTAL_BUFFER_SIZE().getBytes();
  }

  // Initialize: read the first block of bytes and prime the tokenizer.
  void loadFirstBlock(net::any_io_executor exec, Handler h) {
    fileBuffer_->asyncGetNextBlock(
        exec, [this, exec, h = std::move(h)](
                  std::exception_ptr ep, std::optional<ByteBlock> opt) mutable {
          if (ep) {
            h(ep, std::nullopt);
            return;
          }
          firstBlockLoaded_ = true;
          if (!opt) {
            AD_LOG_WARN << "The input stream for the turtle parser seems to "
                           "contain no data!\n";
            this->isParserExhausted_ = true;
            h(nullptr, std::nullopt);
            return;
          }
          byteVec_ = std::move(*opt);
          this->tok_.reset(byteVec_.data(), byteVec_.size());
          runOneParseRound(exec, std::move(h));
        });
  }

  // One parse round: parse statements until we have ≥
  // PARSER_MIN_TRIPLES_AT_ONCE triples, EOF, or a partial statement that needs
  // more bytes. If a partial statement is hit, request more bytes from the byte
  // source and resume.
  void runOneParseRound(net::any_io_executor exec, Handler h) {
    auto backup = backupState();
    while (this->triples_.size() < PARSER_MIN_TRIPLES_AT_ONCE &&
           !this->isParserExhausted_) {
      bool parsedStatement;
      std::optional<typename T::ParseException> ex;
      try {
        parsedStatement = T::statement();
      } catch (const typename T::ParseException& p) {
        parsedStatement = false;
        ex = p;
      }
      if (parsedStatement) continue;

      // Partial statement: fetch more bytes asynchronously and resume.
      fileBuffer_->asyncGetNextBlock(
          exec,
          [this, exec, h = std::move(h), backup, ex](
              std::exception_ptr ep, std::optional<ByteBlock> opt) mutable {
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
              this->tok_.skipWhitespaceAndComments();
              std::string_view unparsed = this->tok_.view();
              if (!unparsed.empty()) {
                AD_LOG_INFO
                    << "Parsing of line has Failed, but parseInput is not "
                       "yet exhausted. Remaining bytes: "
                    << unparsed.size() << '\n';
                AD_LOG_INFO << "Logging first 1000 unparsed characters\n";
                AD_LOG_INFO << unparsed.substr(0, 1000) << std::endl;
              }
              this->isParserExhausted_ = true;
              deliverTriples(std::move(h));
              return;
            }
            BackupState b = backup;
            if (!spliceAndResetState(b, std::move(*opt))) {
              std::string_view unparsed = this->tok_.view();
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
            runOneParseRoundFromState(exec, std::move(h), b);
          });
      return;
    }
    deliverTriples(std::move(h));
  }

  // Same as `runOneParseRound`, but uses a caller-provided backup state — used
  // by the async fetch continuation to avoid re-creating the snapshot.
  void runOneParseRoundFromState(net::any_io_executor exec, Handler h,
                                 BackupState backup) {
    while (this->triples_.size() < PARSER_MIN_TRIPLES_AT_ONCE &&
           !this->isParserExhausted_) {
      bool parsedStatement;
      std::optional<typename T::ParseException> ex;
      try {
        parsedStatement = T::statement();
      } catch (const typename T::ParseException& p) {
        parsedStatement = false;
        ex = p;
      }
      if (parsedStatement) {
        backup = backupState();
        continue;
      }
      // Re-enter the async fetch path with the latest snapshot.
      fileBuffer_->asyncGetNextBlock(
          exec,
          [this, exec, h = std::move(h), backup, ex](
              std::exception_ptr ep, std::optional<ByteBlock> opt) mutable {
            if (ep) {
              h(ep, std::nullopt);
              return;
            }
            if (!opt || opt->empty()) {
              if (ex) {
                h(std::make_exception_ptr(*ex), std::nullopt);
                return;
              }
              this->tok_.skipWhitespaceAndComments();
              std::string_view unparsed = this->tok_.view();
              if (!unparsed.empty()) {
                AD_LOG_INFO
                    << "Parsing of line has Failed, but parseInput is not "
                       "yet exhausted. Remaining bytes: "
                    << unparsed.size() << '\n';
                AD_LOG_INFO << "Logging first 1000 unparsed characters\n";
                AD_LOG_INFO << unparsed.substr(0, 1000) << std::endl;
              }
              this->isParserExhausted_ = true;
              deliverTriples(std::move(h));
              return;
            }
            BackupState b = backup;
            if (!spliceAndResetState(b, std::move(*opt))) {
              std::string_view unparsed = this->tok_.view();
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
            runOneParseRoundFromState(exec, std::move(h), b);
          });
      return;
    }
    deliverTriples(std::move(h));
  }

  void deliverTriples(Handler h) {
    if (this->triples_.empty()) {
      h(nullptr, std::nullopt);
      return;
    }
    TripleBatch batch = std::move(this->triples_);
    this->triples_.clear();
    h(nullptr, std::move(batch));
  }

  std::unique_ptr<AsyncEndRegexBlockSource> fileBuffer_;
  ByteBlock byteVec_;
  size_t numBytesBeforeCurrentBatch_ = 0;
  bool firstBlockLoaded_ = false;
};

// ============================================================================
// AsyncParallelFileParser<T>
// ============================================================================
//
// Fan-out parser. A feeder coroutine (modeled as a callback chain on the
// feeder strand) pulls statement-aligned blocks from the byte source, and for
// each block grabs a token from an inflight-cap channel (capacity
// `NUM_PARALLEL_PARSER_THREADS`) and posts a parse task to `exec`. Each parse
// task constructs an ephemeral `RdfStringParser<T>`, parses the block in
// isolation, pushes the result to an output channel (capacity
// `QUEUE_SIZE_AFTER_PARALLEL_PARSING`), releases the token, and increments a
// completed-batch counter on the feeder strand. When the byte source signals
// EOF and all dispatched parses have completed, the output channel is closed.
//
// Each external `asyncGetNextBatch` call is a single `async_receive` on the
// output channel.
template <typename T>
class AsyncParallelFileParser final : public T, public AsyncSingleFileParser {
 public:
  AsyncParallelFileParser(std::unique_ptr<AsyncBlockSource> rawBuffer,
                          const EncodedIriManager* ev,
                          const TripleComponent& defaultGraphIri =
                              qlever::specialIds().at(DEFAULT_GRAPH_IRI))
      : T{ev, defaultGraphIri}, defaultGraphIri_{defaultGraphIri} {
    this->clear();
    fileBuffer_ = std::make_unique<AsyncEndRegexBlockSource>(
        std::move(rawBuffer), "\\.[\\t ]*([\\r\\n]+)");
  }

  void asyncGetNextBatch(net::any_io_executor exec, Handler handler) override {
    // Lazily start the feeder on first call; pin the output channel and
    // tokens channel to `exec` and the feeder strand to a strand on `exec`.
    std::call_once(feederStarted_, [this, exec] {
      feederStrand_ = std::make_unique<Strand>(net::make_strand(exec));
      outputCh_ = std::make_unique<OutputCh>(
          exec, /*max_buffer_size=*/QUEUE_SIZE_AFTER_PARALLEL_PARSING);
      tokenCh_ = std::make_unique<TokenCh>(
          exec, /*max_buffer_size=*/NUM_PARALLEL_PARSER_THREADS);
      net::post(*feederStrand_, [this, exec] { initFeeder(exec); });
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

  ~AsyncParallelFileParser() override { cancel(); }

  // These sync methods are not used in the async parallel path.
  bool getLineImpl(TurtleTriple*) override { AD_FAIL(); }
  size_t getParsePosition() const override { return 0; }

  // Forward `AsyncSingleFileParser`'s pure virtual accessors to `T`'s
  // implementations (in `RdfParserBase`). Same rationale as in
  // `AsyncStreamingParser<T>`.
  TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() override {
    return T::integerOverflowBehavior();
  }
  bool& invalidLiteralsAreSkipped() override {
    return T::invalidLiteralsAreSkipped();
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
  // Thread-safe channel (real per-channel mutex): `dispatchParse` sends to
  // `outputCh_` concurrently from multiple parse tasks on the pool, so a plain
  // `channel` (which uses a `null_mutex`) would race and corrupt its queues.
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
  void initFeeder(net::any_io_executor exec) {
    fillTokenBucket();
    readPrefixHeaderStep(exec);
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
  void readPrefixHeaderStep(net::any_io_executor exec) {
    fileBuffer_->asyncGetNextBlock(
        exec, [this, exec](std::exception_ptr ep,
                           std::optional<ByteBlock> opt) mutable {
          if (ep) {
            closeWithError(ep);
            return;
          }
          if (!opt) {
            // Empty input: no prefix block to read.
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
            net::post(*feederStrand_,
                      [this, exec] { readPrefixHeaderStep(exec); });
            return;
          }
          // Copy parsed prefixes / base IRI into the master parser so its
          // diagnostics see the same headers.
          T::copyHeaderFrom(declarationParser_, *this);
          ByteBlock first;
          first.reserve(remainder.size());
          ql::ranges::copy(remainder, std::back_inserter(first));
          // Feeder strand now drives the main loop.
          net::post(*feederStrand_,
                    [this, exec, first = std::move(first)]() mutable {
                      feederStep(exec, std::move(first));
                    });
        });
  }

  // The main feeder loop. `firstBatch` is the residual bytes from the prefix
  // initialization (used only on the first invocation; empty on subsequent
  // ones).
  void feederStep(net::any_io_executor exec, ByteBlock firstBatch) {
    if (cancelled_) {
      finishFeeder();
      return;
    }
    if (!firstBatch.empty()) {
      acquireTokenAndDispatch(exec, std::move(firstBatch));
      return;
    }
    fileBuffer_->asyncGetNextBlock(
        exec, [this, exec](std::exception_ptr ep,
                           std::optional<ByteBlock> opt) mutable {
          if (ep) {
            closeWithError(ep);
            return;
          }
          if (!opt) {
            // EOF: no more blocks. Wait for inflight parses to complete and
            // then close the output channel.
            eofSeen_ = true;
            maybeFinishFeeder();
            return;
          }
          acquireTokenAndDispatch(exec, std::move(*opt));
        });
  }

  // Acquire a token from the inflight semaphore, then post a parse task to
  // `exec`. The parse task pushes its result to the output channel, releases
  // the token, and on the feeder strand triggers the next feeder step or
  // finalization.
  void acquireTokenAndDispatch(net::any_io_executor exec, ByteBlock block) {
    tokenCh_->async_receive(net::bind_executor(
        *feederStrand_,
        [this, exec, block = std::move(block)](ec_t ec, bool) mutable {
          if (ec) {
            // Channel closed (cancellation).
            finishFeeder();
            return;
          }
          ++inflight_;
          size_t parsePosition = parsePositionCursor_;
          parsePositionCursor_ += block.size();
          dispatchParse(exec, parsePosition, std::move(block));
          // Immediately schedule the next feeder step so the inflight cap is
          // re-checked.
          net::post(*feederStrand_,
                    [this, exec] { feederStep(exec, ByteBlock{}); });
        }));
  }

  void dispatchParse(net::any_io_executor exec, size_t parsePosition,
                     ByteBlock block) {
    net::post(exec, [this, parsePosition, block = std::move(block)]() mutable {
      std::exception_ptr error;
      std::optional<TripleBatch> result;
      try {
        RdfStringParser<T> parser{&this->encodedIriManager(), defaultGraphIri_};
        T::copyHeaderFrom(*this, parser);
        parser.useSimplifiedGrammar();
        parser.setPositionOffset(parsePosition);
        parser.setFileBlankNodePrefix(this->fileBlankNodePrefix_);
        parser.setInputStream(std::move(block));
        result = parser.parseAndReturnAllTriples();
      } catch (...) {
        error = std::current_exception();
      }
      // Always push *something* (batch or error or EOF marker) so consumers
      // see the failure even if more blocks were queued before the throw.
      outputCh_->async_send(ec_t{}, BatchOrEof{std::move(result), error},
                            [this](ec_t /*ec*/) {
                              // After a successful send (or after the channel
                              // closed, which means we're tearing down),
                              // release the token and tick the feeder strand.
                              net::dispatch(*feederStrand_, [this] {
                                --inflight_;
                                tokenCh_->try_send(ec_t{}, true);
                                maybeFinishFeeder();
                              });
                            });
    });
  }

  void maybeFinishFeeder() {
    if (eofSeen_ && inflight_ == 0) {
      finishFeeder();
    }
  }

  void finishFeeder() {
    if (outputCh_) outputCh_->close();
    if (tokenCh_) tokenCh_->close();
  }

  void closeWithError(std::exception_ptr ep) {
    if (outputCh_) {
      outputCh_->async_send(ec_t{}, BatchOrEof{std::nullopt, ep},
                            [this](ec_t) { finishFeeder(); });
    }
  }

  std::unique_ptr<AsyncEndRegexBlockSource> fileBuffer_;
  TripleComponent defaultGraphIri_;
  RdfStringParser<T> declarationParser_{&this->encodedIriManager()};
  std::once_flag feederStarted_;
  std::unique_ptr<Strand> feederStrand_;
  std::unique_ptr<OutputCh> outputCh_;
  std::unique_ptr<TokenCh> tokenCh_;
  size_t inflight_ = 0;
  bool eofSeen_ = false;
  bool cancelled_ = false;
  size_t parsePositionCursor_ = 0;
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
    ad_utility::MemorySize bufferSize) {
  auto graph = [&input]() -> TripleComponent {
    if (input.defaultGraph_.has_value()) {
      return TripleComponent::Iri::fromIrirefWithoutBrackets(
          *input.defaultGraph_);
    }
    return qlever::specialIds().at(DEFAULT_GRAPH_IRI);
  };
  auto impl = ad_utility::ApplyAsValueIdentity{
      [&input, &bufferSize, &graph, ev](
          auto useParallel,
          auto isTurtleInput) -> std::unique_ptr<AsyncSingleFileParser> {
        using InnerParser =
            std::conditional_t<isTurtleInput == 1, TurtleParser<TokenizerT>,
                               NQuadParser<TokenizerT>>;
        using ParserT = std::conditional_t<useParallel == 1,
                                           AsyncParallelFileParser<InnerParser>,
                                           AsyncStreamingParser<InnerParser>>;
        return std::make_unique<ParserT>(
            input.getAsyncBlockSource(bufferSize.getBytes()), ev, graph());
      }};
  return ad_utility::callFixedSize(
      std::array{input.parseInParallel_ ? 1 : 0,
                 input.filetype_ == qlever::Filetype::Turtle ? 1 : 0},
      impl);
}
}  // namespace

std::unique_ptr<AsyncSingleFileParser> makeAsyncSingleFileParser(
    const qlever::InputFileSpecification& input, const EncodedIriManager* ev,
    ad_utility::MemorySize bufferSize) {
  return makeWithTokenizer<Tokenizer>(input, ev, bufferSize);
}

// ____________________________________________________________________________
template <typename InnerParser>
std::unique_ptr<AsyncSingleFileParser> makeStreamingParser(
    std::unique_ptr<AsyncBlockSource> rawBuffer, const EncodedIriManager* ev,
    TripleComponent defaultGraph) {
  return std::make_unique<AsyncStreamingParser<InnerParser>>(
      std::move(rawBuffer), ev, std::move(defaultGraph));
}

template <typename InnerParser>
std::unique_ptr<AsyncSingleFileParser> makeParallelFileParser(
    std::unique_ptr<AsyncBlockSource> rawBuffer, const EncodedIriManager* ev,
    const TripleComponent& defaultGraph) {
  return std::make_unique<AsyncParallelFileParser<InnerParser>>(
      std::move(rawBuffer), ev, defaultGraph);
}

// Explicit instantiations of the factory templates so the symbols are
// linkable from outside this translation unit.
template std::unique_ptr<AsyncSingleFileParser>
makeStreamingParser<TurtleParser<Tokenizer>>(std::unique_ptr<AsyncBlockSource>,
                                             const EncodedIriManager*,
                                             TripleComponent);
template std::unique_ptr<AsyncSingleFileParser> makeStreamingParser<
    TurtleParser<TokenizerCtre>>(std::unique_ptr<AsyncBlockSource>,
                                 const EncodedIriManager*, TripleComponent);
template std::unique_ptr<AsyncSingleFileParser>
makeStreamingParser<NQuadParser<Tokenizer>>(std::unique_ptr<AsyncBlockSource>,
                                            const EncodedIriManager*,
                                            TripleComponent);
template std::unique_ptr<AsyncSingleFileParser> makeStreamingParser<
    NQuadParser<TokenizerCtre>>(std::unique_ptr<AsyncBlockSource>,
                                const EncodedIriManager*, TripleComponent);

template std::unique_ptr<AsyncSingleFileParser> makeParallelFileParser<
    TurtleParser<Tokenizer>>(std::unique_ptr<AsyncBlockSource>,
                             const EncodedIriManager*, const TripleComponent&);
template std::unique_ptr<AsyncSingleFileParser>
makeParallelFileParser<TurtleParser<TokenizerCtre>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    const TripleComponent&);
template std::unique_ptr<AsyncSingleFileParser> makeParallelFileParser<
    NQuadParser<Tokenizer>>(std::unique_ptr<AsyncBlockSource>,
                            const EncodedIriManager*, const TripleComponent&);
template std::unique_ptr<AsyncSingleFileParser>
makeParallelFileParser<NQuadParser<TokenizerCtre>>(
    std::unique_ptr<AsyncBlockSource>, const EncodedIriManager*,
    const TripleComponent&);

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
