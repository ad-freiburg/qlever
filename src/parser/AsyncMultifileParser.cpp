// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/AsyncMultifileParser.h"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <memory>
#include <utility>

#include "index/ConstantsIndexBuilding.h"

namespace qlever::parser {

namespace net = boost::asio;
using ec_t = boost::system::error_code;

// Internal pImpl. The orchestrator owns its own per-file state (file specs +
// in-flight per-file parser objects) and an output channel that consumers
// poll via `asyncGetNextBatch`.
struct AsyncMultifileParser::Impl {
  std::vector<qlever::InputFileSpecification> files_;
  const EncodedIriManager* encodedIriManager_;
  ad_utility::MemorySize bufferSize_;
  TurtleParserIntegerOverflowBehavior overflowBehavior_ =
      TurtleParserIntegerOverflowBehavior::Error;
  bool invalidLiteralsAreSkipped_ = false;

  struct BatchOrError {
    std::optional<TripleBatch> batch;
    std::exception_ptr error;
  };
  using OutputCh = net::experimental::channel<void(ec_t, BatchOrError)>;
  using TokenCh = net::experimental::channel<void(ec_t, bool)>;
  using Strand = net::strand<net::any_io_executor>;

  std::once_flag started_;
  std::unique_ptr<Strand> dispatcherStrand_;
  std::unique_ptr<OutputCh> outputCh_;
  std::unique_ptr<TokenCh> tokenCh_;

  // Number of files currently being parsed (or queued waiting to be parsed).
  // Used to close the output channel after the last file has emitted EOF.
  size_t filesRemaining_ = 0;
  bool dispatcherDone_ = false;

  Impl(std::vector<qlever::InputFileSpecification> files,
       const EncodedIriManager* ev, ad_utility::MemorySize bufferSize)
      : files_{std::move(files)},
        encodedIriManager_{ev},
        bufferSize_{bufferSize} {
    filesRemaining_ = files_.size();
  }

  void ensureStarted(net::any_io_executor exec) {
    std::call_once(started_, [this, exec] {
      dispatcherStrand_ = std::make_unique<Strand>(net::make_strand(exec));
      outputCh_ = std::make_unique<OutputCh>(exec, /*max_buffer_size=*/10);
      tokenCh_ = std::make_unique<TokenCh>(
          exec, /*max_buffer_size=*/NUM_PARALLEL_PARSER_THREADS);
      for (size_t i = 0; i < NUM_PARALLEL_PARSER_THREADS; ++i) {
        tokenCh_->try_send(ec_t{}, true);
      }
      net::post(*dispatcherStrand_,
                [this, exec] { dispatchNextFile(exec, /*idx=*/0); });
    });
  }

  // Walk the file list and, for each, acquire a token before constructing and
  // pumping the per-file `AsyncSingleFileParser`. Runs on the dispatcher
  // strand.
  void dispatchNextFile(net::any_io_executor exec, size_t idx) {
    if (idx >= files_.size()) {
      dispatcherDone_ = true;
      maybeFinish();
      return;
    }
    tokenCh_->async_receive(net::bind_executor(
        *dispatcherStrand_, [this, exec, idx](ec_t ec, bool) mutable {
          if (ec) {
            // Channel closed during shutdown.
            return;
          }
          startFile(exec, idx);
          dispatchNextFile(exec, idx + 1);
        }));
  }

  // Construct the per-file parser and arm its first `asyncGetNextBatch`. On
  // each batch arrival, push to the output channel and re-arm; on EOF release
  // the file token and consider the orchestrator's overall completion.
  void startFile(net::any_io_executor exec, size_t idx) {
    try {
      auto parser = makeAsyncSingleFileParser(files_[idx], encodedIriManager_,
                                              bufferSize_);
      // Propagate the parser-wide configuration set by the caller before the
      // file is actually parsed. The per-file parser inherits these settings
      // via its `RdfParserBase` portion.
      parser->integerOverflowBehavior() = overflowBehavior_;
      parser->invalidLiteralsAreSkipped() = invalidLiteralsAreSkipped_;
      auto parserPtr =
          std::shared_ptr<AsyncSingleFileParser>{std::move(parser)};
      pumpFile(exec, parserPtr);
    } catch (...) {
      // Construction failure: emit the exception through the output channel
      // and free the slot.
      auto ep = std::current_exception();
      outputCh_->async_send(
          ec_t{}, BatchOrError{std::nullopt, ep},
          net::bind_executor(*dispatcherStrand_, [this](ec_t) {
            releaseFileSlot();
            maybeFinish();
          }));
    }
  }

  void pumpFile(net::any_io_executor exec,
                std::shared_ptr<AsyncSingleFileParser> parser) {
    parser->asyncGetNextBatch(
        exec, [this, exec, parser](std::exception_ptr ep,
                                   std::optional<TripleBatch> opt) mutable {
          if (ep) {
            outputCh_->async_send(
                ec_t{}, BatchOrError{std::nullopt, ep},
                net::bind_executor(*dispatcherStrand_, [this](ec_t) {
                  releaseFileSlot();
                  maybeFinish();
                }));
            return;
          }
          if (!opt) {
            // This file is done. Release the slot.
            net::dispatch(*dispatcherStrand_, [this] {
              releaseFileSlot();
              maybeFinish();
            });
            return;
          }
          outputCh_->async_send(ec_t{}, BatchOrError{std::move(*opt), nullptr},
                                [this, exec, parser](ec_t sendEc) mutable {
                                  if (sendEc) {
                                    // Output channel closed mid-flight.
                                    net::dispatch(*dispatcherStrand_, [this] {
                                      releaseFileSlot();
                                      maybeFinish();
                                    });
                                    return;
                                  }
                                  pumpFile(exec, parser);
                                });
        });
  }

  void releaseFileSlot() {
    --filesRemaining_;
    tokenCh_->try_send(ec_t{}, true);
  }

  void maybeFinish() {
    if (dispatcherDone_ && filesRemaining_ == 0) {
      if (outputCh_) outputCh_->close();
      if (tokenCh_) tokenCh_->close();
    }
  }
};

// ____________________________________________________________________________
AsyncMultifileParser::AsyncMultifileParser(
    std::vector<qlever::InputFileSpecification> files,
    const EncodedIriManager* encodedIriManager,
    ad_utility::MemorySize bufferSize)
    : impl_{std::make_unique<Impl>(std::move(files), encodedIriManager,
                                   bufferSize)} {}

// ____________________________________________________________________________
AsyncMultifileParser::~AsyncMultifileParser() { cancel(); }

// ____________________________________________________________________________
void AsyncMultifileParser::cancel() {
  if (impl_->outputCh_) impl_->outputCh_->close();
  if (impl_->tokenCh_) impl_->tokenCh_->close();
}

// ____________________________________________________________________________
TurtleParserIntegerOverflowBehavior&
AsyncMultifileParser::integerOverflowBehavior() {
  return impl_->overflowBehavior_;
}

// ____________________________________________________________________________
bool& AsyncMultifileParser::invalidLiteralsAreSkipped() {
  return impl_->invalidLiteralsAreSkipped_;
}

// ____________________________________________________________________________
void AsyncMultifileParser::asyncGetNextBatch(net::any_io_executor exec,
                                             Handler handler) {
  impl_->ensureStarted(exec);
  impl_->outputCh_->async_receive(
      [h = std::move(handler)](ec_t ec, Impl::BatchOrError v) mutable {
        if (ec) {
          // Channel closed: EOF.
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

}  // namespace qlever::parser
