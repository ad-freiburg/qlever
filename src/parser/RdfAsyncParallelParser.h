// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_RDFASYNCPARALLELPARSER_H
#define QLEVER_SRC_PARSER_RDFASYNCPARALLELPARSER_H

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
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
#include "util/Forward.h"
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
// The constructor does nothing but open the input; in particular it neither
// blocks nor starts any asynchronous operation. The leading declarations of
// the input (the "header", see `RdfParallelParsingState`) are parsed by the
// first `asyncGetBatch()` call, which holds the single permit of
// `blockFetchPermit_` while doing so. Every `asyncGetBatch()` call has to
// acquire that permit before it may touch the input, so the calls that arrive
// in the meantime automatically suspend (without blocking a thread) until the
// header has been dealt with. An error during the parsing of the header is
// stored in `initializationError_`, because without a header not a single
// batch can be parsed.
//
// Once the header or any batch fails to parse, `errorWasEncountered_` is set
// and:
//   - the failing call completes with that exception, and
//   - every subsequent `asyncGetBatch()` call completes with `nullopt` to
//     trigger early stopping in the caller.
//
// An instance of this class must outlive all of its in-flight
// `asyncGetBatch()` calls. Because it owns no threads, its destructor cannot
// wait for pending work.
//
// NOTE: This class is implemented using C++20 coroutines, and
// `ad_utility::AsyncResourcePool` requires Boost 1.80 or newer. It is
// therefore excluded from the `REDUCED_FEATURE_SET_FOR_CPP17` build, see
// `src/parser/CMakeLists.txt`.
template <typename Parser>
class RdfAsyncParallelParser {
 public:
  // The result of a single `asyncGetBatch()` call: the triples of the next
  // batch, or `nullopt` at the end of the input (see `asyncGetBatch` below).
  using OptionalTriples = std::optional<std::vector<TurtleTriple>>;

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
  // here instead of blocking its thread. The permit is also held for the whole
  // duration of the parsing of the header, see the class comment above.
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

  // True once the parsing of the header has been attempted, such that only the
  // first `asyncGetBatch()` call does it. Like `initializationError_` below,
  // this is only accessed while the permit of `blockFetchPermit_` is held and
  // hence needs no further synchronization.
  bool headerWasParsed_ = false;

  // The error (if any) that the parsing of the header ran into. Every
  // `asyncGetBatch()` call reports it, see the class comment above.
  std::exception_ptr initializationError_;

  // Set to true by the first `asyncGetBatch()` call that encounters an error.
  // All subsequent calls complete with `nullopt` instead of propagating
  // further exceptions, so that the caller's pipeline stops cleanly.
  std::atomic_bool errorWasEncountered_{false};

 public:
  // Construct a parser that reads from `spec` and schedules all of its work
  // on `executor`. The constructor does not block and starts no asynchronous
  // operation, see the class comment above.
  RdfAsyncParallelParser(const ql::any_io_executor& executor,
                         const qlever::InputFileSpecification& spec,
                         ad_utility::MemorySize blocksize,
                         const EncodedIriManager* encodedIriManager,
                         const TripleComponent& defaultGraphIri =
                             qlever::specialIds().at(DEFAULT_GRAPH_IRI));

  // Asynchronously fetch and parse the next batch of triples. Accept any
  // Asio completion token. The completion signature is that of
  // `boost::asio::co_spawn`ing `getBatchCoroutine()`, namely
  // `void(std::exception_ptr, OptionalTriples)`: a null `exception_ptr`
  // together with a non-null optional signals success; a non-null
  // `exception_ptr` signals that this batch failed (see the class comment for
  // what happens to subsequent calls); a null `exception_ptr` together with
  // `nullopt` means the end of the input, or that an earlier batch already
  // failed.
  template <typename CompletionToken>
  auto asyncGetBatch(CompletionToken&& token) {
    return boost::asio::co_spawn(executor_, getBatchCoroutine(), AD_FWD(token));
  }

 private:
  // The implementation of `asyncGetBatch` above, which simply `co_spawn`s this
  // coroutine: parse the header if this is the first call, then fetch the next
  // block and parse it into triples. Throw on a parse error, and return
  // `nullopt` at the end of the input.
  boost::asio::awaitable<OptionalTriples> getBatchCoroutine();
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
