// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_RDFASYNCPARALLELPARSER_H
#define QLEVER_SRC_PARSER_RDFASYNCPARALLELPARSER_H

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

#include "global/SpecialIds.h"
#include "index/InputFileSpecification.h"
#include "parser/AsyncBlockSource.h"
#include "parser/AsyncParserDriver.h"
#include "parser/RdfParser.h"
#include "util/AsyncResourcePool.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

// An RDF parser that, like `RdfParallelParser`, parses a single input file by
// splitting it into batches and parsing those batches in parallel, but
// schedules all of its work via `boost::asio` on an externally-provided
// executor instead of owning its own threads. It is not an `RdfParserBase`,
// because its interface is asynchronous; `RdfParallelParserViaAsync` below
// wraps it in the synchronous `RdfParserBase` interface.
//
// Unlike `RdfParallelParser`, this class does not prefetch or buffer batches
// on its own behalf: parallelism comes entirely from the caller keeping
// several calls to `asyncGetBatch()` in flight at once. Fetching the next
// block is serialized (at most one fetch is in flight at any time, as required
// by `AsyncBlockSource`), while parsing of different blocks happens in
// parallel on `executor_`.
//
// Once any batch fails to parse, `errorWasEncountered_` is set and:
//   - the failing call's completion handler receives the exception, and
//   - every subsequent `asyncGetBatch()` call completes with
//     `(nullptr, nullopt)` to trigger early stopping in the caller.
//
// An instance of this class must outlive all in-flight `asyncGetBatch()` calls.
// Because it owns no threads, its destructor cannot wait for pending work.
template <typename Parser>
class RdfAsyncParallelParser {
 private:
  boost::asio::any_io_executor executor_;

  // The state that this parser shares with all of its workers, in particular
  // the header of the input file.
  RdfParallelParsingState<Parser> state_;

  // Owns the file-reading and block-splitting logic. It cuts the input into
  // blocks at statement boundaries; concurrent `asyncGetBatch()` calls parse
  // different blocks in parallel.
  qlever::parser::AsyncStatementBoundaryBlockSource blockSource_;

  // A semaphore with a single permit that serializes the fetches from
  // `blockSource_`, which requires that at most one call to
  // `asyncGetNextBlock` is in flight at any time. A concurrent
  // `asyncGetBatch()` call that arrives while a fetch is in flight suspends
  // here instead of blocking its thread.
  ad_utility::AsyncResourcePool<void> blockFetchPermit_;

  // Set to true by the first `asyncGetBatch()` call that encounters an error.
  // All subsequent calls complete with `(nullptr, nullopt)` instead of
  // propagating further exceptions, so that the caller's pipeline stops
  // cleanly.
  std::atomic_bool errorWasEncountered_{false};

 public:
  // Construct a parser that reads from `spec` and schedules all of its work
  // on `executor`. As in `RdfParallelParser`, the constructor eagerly parses
  // the leading declarations and stores the remainder of the first block for
  // the first call to `asyncGetBatch()`.
  RdfAsyncParallelParser(const boost::asio::any_io_executor& executor,
                         const qlever::InputFileSpecification& spec,
                         ad_utility::MemorySize blocksize,
                         const EncodedIriManager* encodedIriManager,
                         const TripleComponent& defaultGraphIri =
                             qlever::specialIds().at(DEFAULT_GRAPH_IRI));

  // Asynchronously fetch and parse the next batch of triples. Accept any
  // Asio completion token. The completion signature is
  // `void(std::exception_ptr, std::optional<std::vector<TurtleTriple>>)`: a
  // null `exception_ptr` with a non-null optional signals success; a non-null
  // `exception_ptr` signals that this batch failed (see the class comment for
  // what happens to subsequent calls); a null `exception_ptr` with `nullopt`
  // means EOF or that an earlier batch already failed.
  template <typename CompletionToken>
  auto asyncGetBatch(CompletionToken&& token) {
    namespace net = boost::asio;
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr,
                                    std::optional<std::vector<TurtleTriple>>)>(
        [this](auto handler) {
          auto ex = net::get_associated_executor(handler, executor_);
          // Factory: dispatch `(ep, batch)` to `ex` and invoke `handler`.
          auto dispatchResult =
              [ex, h = std::move(handler)](
                  std::exception_ptr ep,
                  std::optional<std::vector<TurtleTriple>> batch) mutable {
                net::dispatch(ex, [h = std::move(h), ep,
                                   batch = std::move(batch)]() mutable {
                  std::move(h)(ep, std::move(batch));
                });
              };
          // If a prior error was encountered, signal clean EOF to stop the
          // caller's pipeline without re-throwing.
          if (errorWasEncountered_.load()) {
            dispatchResult(nullptr, std::nullopt);
            return;
          }
          // The first caller gets to parse the remainder that was left over by
          // the parsing of the header.
          if (auto remainder = state_.takeRemainderFromInitialization()) {
            net::post(executor_,
                      [this, remainder = std::move(remainder),
                       dispatchResult = std::move(dispatchResult)]() mutable {
                        handleBlockAndDispatch(nullptr, std::move(remainder),
                                               dispatchResult);
                      });
            return;
          }
          // General case: take the single fetch permit, fetch the next block
          // asynchronously, and then parse it. `handleBlockAndDispatch`
          // factors out the parse-and-dispatch logic so it can be shared with
          // the initial-batch path above. The lambdas are defined here (rather
          // than in a struct) so they can call the private
          // `handleBlockAndDispatch()` via the captured `this`.
          blockFetchPermit_.asyncAcquire(net::bind_executor(
              executor_,
              [this, dispatchResult = std::move(dispatchResult)](
                  const boost::system::error_code& errorCode,
                  ad_utility::AsyncResourcePool<void>::Handle permit) mutable {
                // Nothing ever cancels `blockFetchPermit_`, so acquiring a
                // permit cannot fail.
                AD_CORRECTNESS_CHECK(!errorCode && permit.isValid());
                blockSource_.asyncGetNextBlock(net::bind_executor(
                    executor_, [this, permit = std::move(permit),
                                dispatchResult = std::move(dispatchResult)](
                                   std::exception_ptr fetchEptr,
                                   std::optional<qlever::parser::ByteBlock>
                                       block) mutable {
                      // Let the next waiting call fetch its block while this
                      // call parses. This is safe: `blockSource_` has already
                      // updated all of its state by the time this handler
                      // runs.
                      permit.release();
                      handleBlockAndDispatch(fetchEptr, std::move(block),
                                             dispatchResult);
                    }));
              }));
        },
        token);
  }

 private:
  // Handle the result of a block fetch: parse `block` if successful and
  // dispatch the result (or the error) via `dispatch`. Set
  // `errorWasEncountered_` on the first error so that subsequent calls
  // return `(nullptr, nullopt)` early, stopping the caller's pipeline.
  template <typename DispatchFn>
  void handleBlockAndDispatch(std::exception_ptr fetchEptr,
                              std::optional<qlever::parser::ByteBlock> block,
                              DispatchFn& dispatch) {
    if (fetchEptr) {
      if (!errorWasEncountered_.exchange(true)) {
        dispatch(fetchEptr, std::nullopt);
      } else {
        dispatch(nullptr, std::nullopt);
      }
      return;
    }
    if (!block.has_value()) {
      dispatch(nullptr, std::nullopt);
      return;
    }
    try {
      dispatch(nullptr, state_.parseBatch(std::move(*block)));
    } catch (...) {
      if (!errorWasEncountered_.exchange(true)) {
        dispatch(std::current_exception(), std::nullopt);
      } else {
        dispatch(nullptr, std::nullopt);
      }
    }
  }
};

// The `RdfAsyncParallelParser` driven by its own thread pool, which makes it a
// drop-in replacement for `RdfParallelParser`. The only purpose of this class
// is to provide the constructor of `RdfParallelParser`; everything else is
// inherited from `AsyncParserDriver`.
template <typename Parser>
class RdfParallelParserViaAsync
    : public AsyncParserDriver<RdfAsyncParallelParser<Parser>> {
 public:
  // Construct a parser that reads from `spec` on an internally-managed thread
  // pool. The interface is identical to that of `RdfParallelParser`.
  RdfParallelParserViaAsync(const qlever::InputFileSpecification& spec,
                            ad_utility::MemorySize blocksize,
                            const EncodedIriManager* ev,
                            const TripleComponent& defaultGraphIri =
                                qlever::specialIds().at(DEFAULT_GRAPH_IRI))
      : AsyncParserDriver<RdfAsyncParallelParser<Parser>>{
            ev, spec, blocksize, ev, defaultGraphIri} {}

  // Overload that accepts and ignores a `sleepTimeForTesting` parameter so that
  // tests can instantiate this class and `RdfParallelParser` with the same
  // constructor arguments (see `RdfParserTest.stopParsingOnOutsideFailure`).
  RdfParallelParserViaAsync(
      const qlever::InputFileSpecification& spec,
      ad_utility::MemorySize blocksize, const EncodedIriManager* ev,
      const TripleComponent& defaultGraphIri,
      [[maybe_unused]] std::chrono::milliseconds sleepTimeForTesting)
      : RdfParallelParserViaAsync{spec, blocksize, ev, defaultGraphIri} {}
};

#endif  // QLEVER_SRC_PARSER_RDFASYNCPARALLELPARSER_H
