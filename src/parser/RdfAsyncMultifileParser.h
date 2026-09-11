// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_RDFASYNCMULTIFILEPARSER_H
#define QLEVER_SRC_PARSER_RDFASYNCMULTIFILEPARSER_H

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "backports/asio.h"
#include "index/InputFileSpecification.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "parser/AsyncParserDriver.h"
#include "parser/AsyncRdfParserBase.h"
#include "parser/RdfParser.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Synchronized.h"

// The asynchronous counterpart of `RdfMultifileParser`: parses several input
// files, each specified by an `InputFileSpecification`, and delivers their
// triples through the `AsyncRdfParserBase` interface (see there for the
// documentation of `asyncGetBatch`). Unlike `RdfMultifileParser`, this class
// owns no threads of its own; all of its work -- including that of the
// per-file parsers -- is scheduled on the executor that is passed to the
// constructor, and the parallelism comes entirely from the caller keeping
// several `asyncGetBatch()` calls in flight at once. The main use case is the
// first pass of the index building, which a follow-up PR moves onto a single
// thread pool using this class (in the `REDUCED_FEATURE_SET_FOR_CPP17` build,
// which has no coroutines, `AsyncSerialParserAdapter` around
// `RdfMultifileParser` serves as the fallback).
//
// Lifetime: an instance of this class must outlive all of its in-flight
// `asyncGetBatch()` calls. Because it owns no threads, its destructor cannot
// wait for pending work.
//
// Scheduling policy: files are opened lazily, one at a time, as calls arrive
// that need a new file; the constructor itself opens nothing and starts no
// asynchronous operation. Each open file is parsed by its own
// `AsyncRdfParserBase`: an `RdfAsyncParallelParser` for a file with
// `parseInParallel_ == true`, or an `AsyncSerialParserAdapter` around an
// `RdfStreamParser` otherwise. A file parsed by `RdfAsyncParallelParser`
// supports arbitrarily many concurrent `asyncGetBatch()` calls, whereas a
// serial file is only ever parsed by one call at a time; a further
// concurrent call for that same file is simply queued by the file's own
// `AsyncSerialParserAdapter` (on a strand, without blocking a thread) instead
// of being handed to a different file. Note that even for a serial file, the
// *mapping* of the resulting batches performed by the caller still runs in
// parallel across calls for different files; only the actual byte-level
// parsing of one serial file is single-threaded. Concretely, every
// `asyncGetBatch()` call
//   1. picks an open file that currently has no call in flight at all,
//   2. failing that, opens the next unopened file, if there is one,
//   3. failing that (no idle file and no unopened files left), concludes that
//      every input is exhausted if there is no open file either,
//   4. and otherwise picks the open file with the fewest calls in flight,
//      preferring parallel files, which are the ones that can actually make
//      progress on several calls at once.
// This scheme never opens more files than there are concurrent callers. Note
// that an idle file is preferred over a busy parallel one (step 1) even though
// the latter accepts concurrent calls: the block *fetching* of a single
// `RdfAsyncParallelParser` is serialized, so piling all calls onto one such
// file would make reading from that one file the bottleneck.
//
// Error semantics: once any per-file parser reports an error, that error is
// propagated to exactly the `asyncGetBatch()` call that encountered it, and
// all subsequent `asyncGetBatch()` calls of this instance (regardless of
// which file they would otherwise have picked) complete with `nullopt` to
// trigger clean early stopping in the caller, exactly like
// `RdfAsyncParallelParser`.
class RdfAsyncMultifileParser : public AsyncRdfParserBase {
 public:
  // The result of a single `asyncGetBatch()` call, inherited from
  // `AsyncRdfParserBase`.
  using AsyncRdfParserBase::OptionalTriples;

 private:
  // The per-file bookkeeping needed by the scheduling policy in the class
  // comment above.
  struct OpenFile {
    std::unique_ptr<AsyncRdfParserBase> parser_;
    // `true` if `parser_` accepts arbitrarily many concurrent
    // `asyncGetBatch()` calls (a parallel file), `false` if it is a serial
    // file that may only be called one at a time.
    bool supportsConcurrentCalls_;
    // Number of `asyncGetBatch()` calls currently in flight on `parser_`.
    size_t numCallsInFlight_ = 0;
  };

  // All the state of the scheduling policy that is shared between concurrent
  // `asyncGetBatch()` calls, and hence may only be accessed while the lock of
  // `fileState_` is held.
  struct FileState {
    // The input files that have not been opened yet. `get()` is only called
    // when a new file is actually wanted, and returns `std::nullopt` from then
    // on (both implementations of `InputRangeTypeErased` keep doing so once
    // exhausted), so no separate "no files left" flag is needed.
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files_;
    // The currently open files, in the order in which they were opened, which
    // is the tie-break of the scheduling policy above. A `shared_ptr`, because
    // a file may be removed from this list (by whichever caller happens to see
    // its end of input) while other callers still have calls in flight on it,
    // and the per-file parser must outlive those calls.
    std::vector<std::shared_ptr<OpenFile>> openFiles_;
  };

  const EncodedIriManager* encodedIriManager_;
  ad_utility::MemorySize bufferSize_;
  bool useRelaxedParsing_;

  // Only ever locked exclusively, hence a plain `std::mutex`.
  ad_utility::Synchronized<FileState, std::mutex> fileState_;

  // Set once any per-file parser reports an error. All subsequent
  // `asyncGetBatch()` calls then complete with `nullopt` instead of
  // propagating further exceptions, see the class comment above.
  std::atomic_bool errorWasEncountered_{false};

 public:
  // Construct a parser that reads the files produced by `files` and schedules
  // all of its work (including that of the per-file parsers) on `executor`.
  // The constructor does not block and starts no asynchronous operation;
  // files are opened lazily by `asyncGetBatch()` calls, see the class comment
  // above. If `useRelaxedParsing` is `true`, the faster `TokenizerCtre` is
  // used for all files instead of the standard-compliant `Tokenizer` (see the
  // comment on `TurtleParser` in `RdfParser.h` for the limitations of the
  // relaxed mode).
  RdfAsyncMultifileParser(
      const ql::any_io_executor& executor,
      ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
      const EncodedIriManager* encodedIriManager,
      ad_utility::MemorySize bufferSize = DEFAULT_PARSER_BUFFER_SIZE,
      bool useRelaxedParsing = false);

 protected:
  // Implement `AsyncRdfParserBase::asyncGetBatchImpl` by `co_spawn`ing
  // `getBatchCoroutine()` on `executor()`. The completion signature of that
  // coroutine (`void(std::exception_ptr, OptionalTriples)`) matches `Handler`
  // exactly, so `handler` itself is a valid completion token for `co_spawn`.
  void asyncGetBatchImpl(Handler handler) override;

 private:
  // The implementation of `asyncGetBatchImpl`: pick a file according to the
  // scheduling policy in the class comment, delegate to its
  // `asyncGetBatch()`, and retry with a (possibly different) file if that
  // file turns out to be already exhausted. Return `nullopt` once every file
  // is exhausted or an error was previously encountered. See the class
  // comment for the exact error semantics.
  boost::asio::awaitable<OptionalTriples> getBatchCoroutine();

  // Pick the next file to call according to the scheduling policy from the
  // class comment above (opening a new one if necessary and incrementing its
  // `numCallsInFlight_`), or return `nullptr` if there is currently no file
  // to call because all inputs are exhausted. Every picked file must later be
  // given back via `releaseFile`.
  std::shared_ptr<OpenFile> pickFile();

  // Give back a file that was obtained from `pickFile`: decrement its
  // `numCallsInFlight_` and, if `wasExhausted` is `true`, remove it from the
  // list of open files.
  void releaseFile(const std::shared_ptr<OpenFile>& file, bool wasExhausted);

  // Construct the per-file `AsyncRdfParserBase` for `spec`, choosing the
  // concrete parser type based on `spec.parseInParallel_` and
  // `spec.filetype_`, and the tokenizer based on `useRelaxedParsing_`. This is
  // cheap for both parser types (neither of them parses or even reads anything
  // in its constructor, see `AsyncSerialParserAdapter`), which is what allows
  // `pickFile` to call it while holding the lock.
  std::unique_ptr<AsyncRdfParserBase> makeFileParser(
      const qlever::InputFileSpecification& spec) const;
};

// The `RdfAsyncMultifileParser` driven by its own thread pool, which makes it a
// drop-in replacement for `RdfMultifileParser`. The only purpose of this class
// is to provide the constructor of `RdfMultifileParser`; everything else is
// inherited from `AsyncParserDriver`.
class RdfMultifileParserViaAsync
    : public AsyncParserDriver<RdfAsyncMultifileParser> {
 public:
  // Construct a parser that reads the files produced by `files` on an
  // internally-managed thread pool. The interface is identical to that of
  // `RdfMultifileParser`.
  RdfMultifileParserViaAsync(
      ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
      const EncodedIriManager* encodedIriManager,
      ad_utility::MemorySize bufferSize = DEFAULT_PARSER_BUFFER_SIZE,
      bool useRelaxedParsing = false)
      : AsyncParserDriver<RdfAsyncMultifileParser>{
            encodedIriManager, std::move(files), encodedIriManager, bufferSize,
            useRelaxedParsing} {}
};

#endif  // QLEVER_SRC_PARSER_RDFASYNCMULTIFILEPARSER_H
