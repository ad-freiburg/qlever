// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_RDFASYNCPARALLELPARSER_H
#define QLEVER_SRC_PARSER_RDFASYNCPARALLELPARSER_H

#include <atomic>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

#include "backports/asio.h"
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
// The constructor never blocks: it only schedules the parsing of the header
// (see `RdfParallelParsingState::parseHeader`), which runs asynchronously on
// `executor_` and holds the single permit of `blockFetchPermit_` for its whole
// duration. Every `asyncGetBatch()` call has to acquire that permit before it
// may touch the input, so the calls automatically suspend (without blocking a
// thread) until the header has been dealt with. An error during the parsing of
// the header is stored in `initializationError_` and reported to the first
// `asyncGetBatch()` call.
//
// Once any batch fails to parse, `errorWasEncountered_` is set and:
//   - the failing call's completion handler receives the exception, and
//   - every subsequent `asyncGetBatch()` call completes with
//     `(nullptr, nullopt)` to trigger early stopping in the caller.
//
// An instance of this class must outlive all of its in-flight asynchronous
// operations (the `asyncGetBatch()` calls as well as the parsing of the header
// that the constructor schedules). Because it owns no threads, its destructor
// cannot wait for pending work.
template <typename Parser>
class RdfAsyncParallelParser {
 private:
  // A handle for the single permit of `blockFetchPermit_` below.
  using Permit = ad_utility::AsyncResourcePool<void>::Handle;

  ql::any_io_executor executor_;

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
  // here instead of blocking its thread. The permit is also held by the
  // asynchronous parsing of the header, see the class comment above.
  //
  // NOTE: A `strand` would not be enough here. It serializes the *execution*
  // of handlers, whereas `AsyncBlockSource` requires that at most one
  // operation is *outstanding*: initiating the next fetch is only allowed once
  // the previous fetch's completion handler has run. Two initiations posted to
  // a strand would still overlap, because `asyncGetNextBlock` returns as soon
  // as it has initiated. Hence the single permit, which is held for the whole
  // duration of a fetch, and which suspends a waiting caller instead of
  // blocking its thread.
  ad_utility::AsyncResourcePool<void> blockFetchPermit_;

  // The error (if any) that the asynchronous parsing of the header ran into.
  // It is reported to the first `asyncGetBatch()` call. It is only written and
  // read while the permit of `blockFetchPermit_` is held and hence needs no
  // further synchronization.
  std::exception_ptr initializationError_;

  // Set to true by the first `asyncGetBatch()` call that encounters an error.
  // All subsequent calls complete with `(nullptr, nullopt)` instead of
  // propagating further exceptions, so that the caller's pipeline stops
  // cleanly.
  std::atomic_bool errorWasEncountered_{false};

 public:
  // Construct a parser that reads from `spec` and schedules all of its work
  // on `executor`. The constructor does not block; it only schedules the
  // parsing of the leading declarations, see the class comment above.
  RdfAsyncParallelParser(const ql::any_io_executor& executor,
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
          // Acquire the single permit before anything else. This serializes
          // the fetching of the blocks and at the same time waits for the
          // asynchronous parsing of the header to be finished, see the class
          // comment above.
          blockFetchPermit_.asyncAcquire(net::bind_executor(
              executor_, [this, dispatchResult = std::move(dispatchResult)](
                             const boost::system::error_code& errorCode,
                             Permit permit) mutable {
                // Nothing ever cancels `blockFetchPermit_`, so acquiring a
                // permit cannot fail.
                AD_CORRECTNESS_CHECK(!errorCode && permit.isValid());
                getBatchWithPermit(std::move(permit),
                                   std::move(dispatchResult));
              }));
        },
        token);
  }

 private:
  // Schedule the parsing of the header, and hence the initialization of
  // `state_`, on `executor_`. The permit of `blockFetchPermit_` is acquired
  // first and held until the header is complete, so that no `asyncGetBatch()`
  // call can interleave with (or overtake) this. The operation is detached,
  // its result is communicated via `state_` and `initializationError_`.
  void parseHeaderAsync();

  // Fetch the next block and feed it to `state_.parseHeaderStep()`, repeating
  // until the header is complete. `permit` is the permit that
  // `parseHeaderAsync` acquired and is released once the header is done (or
  // has failed).
  void continueParsingHeader(Permit permit);

  // Get the next batch, given that the single permit of `blockFetchPermit_` is
  // held and that the header has been parsed. The permit is released as soon
  // as the next block has been fetched, so that the (expensive) parsing of
  // that block overlaps with the fetching of the following block.
  template <typename DispatchFn>
  void getBatchWithPermit(Permit permit, DispatchFn dispatchResult) {
    namespace net = boost::asio;
    // The parsing of the header failed, report that error.
    if (initializationError_) {
      permit.release();
      dispatchError(initializationError_, dispatchResult);
      return;
    }
    // A previous batch failed, signal a clean EOF to stop the caller's
    // pipeline without re-throwing.
    if (errorWasEncountered_.load()) {
      permit.release();
      dispatchResult(nullptr, std::nullopt);
      return;
    }
    // The first caller gets to parse the remainder that was left over by the
    // parsing of the header.
    if (auto remainder = state_.takeRemainderFromInitialization()) {
      permit.release();
      handleBlockAndDispatch(nullptr, std::move(remainder), dispatchResult);
      return;
    }
    // General case: fetch the next block asynchronously, then parse it.
    blockSource_.asyncGetNextBlock(net::bind_executor(
        executor_, [this, permit = std::move(permit),
                    dispatchResult = std::move(dispatchResult)](
                       std::exception_ptr fetchEptr,
                       std::optional<qlever::parser::ByteBlock> block) mutable {
          // Let the next waiting call fetch its block while this call parses
          // the block it just got. This is safe: `blockSource_` has already
          // updated all of its state by the time this handler runs.
          permit.release();
          handleBlockAndDispatch(fetchEptr, std::move(block), dispatchResult);
        }));
  }

  // Dispatch `error` to the caller, but only if it is the first error that any
  // caller has encountered. All subsequent callers get `(nullptr, nullopt)`
  // instead, which stops their pipeline cleanly.
  template <typename DispatchFn>
  void dispatchError(std::exception_ptr error, DispatchFn& dispatch) {
    if (!errorWasEncountered_.exchange(true)) {
      dispatch(error, std::nullopt);
    } else {
      dispatch(nullptr, std::nullopt);
    }
  }

  // Handle the result of a block fetch: parse `block` if successful and
  // dispatch the result (or the error, see `dispatchError`) via `dispatch`.
  template <typename DispatchFn>
  void handleBlockAndDispatch(std::exception_ptr fetchEptr,
                              std::optional<qlever::parser::ByteBlock> block,
                              DispatchFn& dispatch) {
    if (fetchEptr) {
      dispatchError(fetchEptr, dispatch);
      return;
    }
    if (!block.has_value()) {
      dispatch(nullptr, std::nullopt);
      return;
    }
    try {
      dispatch(nullptr, state_.parseBatch(std::move(*block)));
    } catch (...) {
      dispatchError(std::current_exception(), dispatch);
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
