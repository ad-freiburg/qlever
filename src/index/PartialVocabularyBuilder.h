// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_PARTIALVOCABULARYBUILDER_H
#define QLEVER_SRC_INDEX_PARTIALVOCABULARYBUILDER_H

#include <atomic>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "index/IndexBuilderTypes.h"
#include "parser/AsyncRdfParserBase.h"
#include "util/GlobalExecutor.h"
#include "util/Log.h"
#include "util/PostAndGetFuture.h"
#include "util/ProgressBar.h"
#include "util/Synchronized.h"

// The building blocks of the first pass of the index building (see
// `IndexImpl::buildPartialVocabularies`): a single `boost::asio::io_context`
// (owned by `runTaskChains` below) is driven by several "task chains", each of
// which repeatedly asks the asynchronous RDF parser for a batch of triples,
// maps the triples to local IDs and writes a partial vocabulary (together with
// the corresponding ID triples) whenever enough triples have been collected.
// The parser runs on the same `io_context`, so that there is exactly one
// compute resource for the whole first pass. The threads that drive the
// `io_context` are donated by the global thread pool (see
// `util/GlobalExecutor.h`), so that the first pass and the other phases of the
// index build share a single set of threads.
//
// The classes are templated on the `Index` type (`IndexImpl` in production)
// so that the pipeline can be unit-tested in isolation with a mock index. An
// `Index` must provide:
// - `ProcessedTriple processTriple(TurtleTriple&&)`, see `mapTripleToIds` in
//   `IndexBuilderTypes.h`.
// - `void writePartialVocabulary(size_t partialVocabIdx,
//   const ItemMapAndBuffer& items, std::vector<IdRow>& localIds)`, which
//   writes the partial vocabulary with the given index and its triples
//   (`IdRow` is defined in `IndexBuilderTypes.h`). `localIds` is only passed
//   by reference so that its memory can be reused; its contents are
//   unspecified afterwards. The function must not hold on to anything from
//   `items` after it returns. It is called concurrently from several task
//   chains, but never twice for the same `partialVocabIdx`.
namespace qlever::partialVocabularyBuilder {

// Shared state for all the task chains of a single first pass. An aggregate:
// the caller initializes the first four members, the remaining ones are the
// progress bar, the counters and the flags that the chains update while they
// run and that the caller reads once `runTaskChains` has returned. Must
// outlive every task chain, which is guaranteed by `runTaskChains` below,
// because the chains are local to that function.
template <typename Index>
struct FirstPassSharedState {
  Index* index_;
  // The comparator for the `ItemMapManager`s of the task chains.
  const TripleComponentComparator* comparator_;
  size_t linesPerPartial_;

  // Show progress and statistics for the number of parsed input triples. The
  // total number of triples is not known in advance, and the task chains
  // report their progress concurrently.
  ad_utility::ConcurrentProgressBar progressBar_{"Triples parsed: ",
                                                 std::nullopt};

  // The number of `ql:has-word` triples that were created by all task chains
  // (see `mapTripleToIds`). Each chain counts its own share locally and adds
  // it here exactly once, when it ends. Stays zero unless the index is
  // configured to create such triples. Only used for logging.
  std::atomic<size_t> numHasWordTriples_ = 0;

  // The shared counter for the indices of the partial vocabularies. Each task
  // chain claims the next free index whenever it has to write a partial
  // vocabulary, together with the corresponding triples (see
  // `BuildPartialVocabulariesResult`).
  std::atomic<size_t> nextPartialVocabIdx_ = 0;

  // The total number of triples (including the internal ones added by QLever)
  // that were written by all task chains. Only used for logging.
  std::atomic<size_t> numTriples_ = 0;

  // Set to `true` as soon as any task chain has failed. Every chain checks
  // this flag at the start of handling its next batch and, if it is set, ends
  // without doing further work or scheduling another step.
  std::atomic<bool> stopRequested_ = false;

  // The first error that was reported by any task chain (see `reportError`),
  // or a null `exception_ptr` if no chain has failed. Rethrown by
  // `runTaskChains` after all chains have ended.
  ad_utility::Synchronized<std::exception_ptr, std::mutex> firstError_{};

  // Record `ep` as `firstError_` (unless an error has already been recorded)
  // and set `stopRequested_`. Thread-safe; may be called concurrently by
  // several task chains.
  void reportError(std::exception_ptr ep) {
    firstError_.withWriteLock([&ep](std::exception_ptr& firstError) {
      if (!firstError) {
        firstError = std::move(ep);
      }
    });
    stopRequested_.store(true);
  }
};

// A single task chain of the first pass. This is *not* a thread: after
// construction, `start()` schedules the first step as a `boost::asio`
// completion handler on `executor_`, and every subsequent step is likewise
// scheduled via `boost::asio::post` (never called inline from within a
// handler), so that the call stack never grows with the number of processed
// batches. A chain contributes its share of the work simply by keeping one
// call to `AsyncRdfParserBase::asyncGetBatch` in flight at a time; since the
// parser supports concurrent calls, several chains together parse and map
// triples in parallel.
//
// Each chain builds its own sequence of partial vocabularies: it owns a
// private `ItemMapManager` (so no synchronization with the other chains is
// needed while mapping triples to local IDs) and a private buffer of the
// resulting `IdRow`s. Once `linesPerPartial_` triples have been collected (or
// the input is exhausted), the chain atomically claims the next free partial
// vocabulary index from the shared counter, writes the vocabulary and the
// corresponding ID triples under that index and, if there is more input,
// resets its `ItemMapManager` for the next partial vocabulary.
//
// Error handling: if any step of a chain throws, the exception is recorded in
// the shared state (see `FirstPassSharedState::reportError`) and the chain
// ends without scheduling another step. Every other chain notices
// `stopRequested_` at the start of its own next step and likewise ends.
template <typename Index>
class PartialVocabularyTaskChain {
 private:
  FirstPassSharedState<Index>& shared_;
  AsyncRdfParserBase& parser_;
  // The executor on which all the steps of this chain run. The completion
  // handlers of the `asyncGetBatch` calls are explicitly bound to it (see
  // `step`), so that the chain does not depend on the executor of the parser.
  ql::any_io_executor executor_;
  // The `ItemMapManager` and buffered local-ID triples of the partial
  // vocabulary that is currently being built by this chain; both are reset to
  // an empty state, but keep their memory, every time a partial vocabulary is
  // written (see `startNewPartialVocabulary`).
  ItemMapManager itemMap_;
  std::vector<IdRow> localTriples_;
  size_t numInputTriples_ = 0;
  // The number of `ql:has-word` triples that this chain has created (see
  // `mapTripleToIds`). Counted locally and added to the shared counter in
  // `finish`, so that the chains do not contend for a single atomic.
  size_t numHasWordTriples_ = 0;

 public:
  PartialVocabularyTaskChain(FirstPassSharedState<Index>& shared,
                             AsyncRdfParserBase& parser,
                             ql::any_io_executor executor)
      : shared_{shared},
        parser_{parser},
        executor_{std::move(executor)},
        itemMap_{0, shared.comparator_} {
    startNewPartialVocabulary();
  }

  // Schedule the first step of this chain. Must be called exactly once, after
  // all task chains have been constructed and while the `io_context` behind
  // `executor_` is still being run (see `runTaskChains`). This is `noexcept`
  // because `runTaskChains` starts the chains one after the other: if starting
  // a later chain threw (which only `bad_alloc` from `post` could do), the
  // earlier chains would already be running while `runTaskChains` unwinds and
  // destroys them.
  void start() noexcept { postNextStep(); }

 private:
  // Reset `itemMap_`, the triple buffer and the input-triple counter for a
  // fresh partial vocabulary. Both keep their memory, so they only have to grow
  // for the first partial vocabulary of this chain.
  void startNewPartialVocabulary() {
    itemMap_.clear();
    localTriples_.clear();
    numInputTriples_ = 0;
  }

  // Claim the next free partial vocabulary index from the shared counter and
  // write the current (non-empty) partial vocabulary and its triples under
  // that index. Both files are exclusively owned by this chain, so no further
  // synchronization is needed. The item map and the vector of triples stay with
  // this chain, which reuses their memory for the next partial vocabulary (see
  // `startNewPartialVocabulary`). The write is synchronous, so nothing refers
  // to the item map's strings once it returns.
  void writeCurrentPartialVocabulary() {
    size_t partialVocabIdx = shared_.nextPartialVocabIdx_.fetch_add(1);
    shared_.numTriples_.fetch_add(localTriples_.size());
    shared_.index_->writePartialVocabulary(partialVocabIdx, itemMap_.map_,
                                           localTriples_);
  }

  // Schedule the next step of this chain on `executor_`.
  void postNextStep() {
    boost::asio::post(executor_, [this] { step(); });
  }

  // A single step of this chain: ask the parser for the next batch of triples
  // and, once it arrives, handle it on `executor_`. The completion handler
  // contains the control flow of the asynchronous loop and the error handling,
  // the actual work is done in `handleBatch` and `finish`.
  void step() {
    parser_.asyncGetBatch(boost::asio::bind_executor(
        executor_, [this](std::exception_ptr ep,
                          std::optional<std::vector<TurtleTriple>> batch) {
          try {
            if (ep) {
              std::rethrow_exception(ep);
            }
            if (shared_.stopRequested_.load()) {
              // Another chain has failed; end this chain without further work.
              return;
            }
            if (!batch.has_value()) {
              // End of input for this chain.
              finish();
              return;
            }
            handleBatch(std::move(batch).value());
            // This is the `continue` of the asynchronous loop: schedule the
            // next step of this chain.
            postNextStep();
          } catch (...) {
            shared_.reportError(std::current_exception());
            // End this chain: do not schedule another step.
          }
        }));
  }

  // Map the triples in `batch` to local IDs, report the progress and, if the
  // current partial vocabulary is full, write it and start a new one.
  void handleBatch(std::vector<TurtleTriple> batch) {
    for (auto& triple : batch) {
      mapTripleToIds(std::move(triple), itemMap_, shared_.index_, localTriples_,
                     numHasWordTriples_);
    }
    numInputTriples_ += batch.size();
    shared_.progressBar_.add(batch.size());
    if (auto update = shared_.progressBar_.update()) {
      AD_LOG_INFO << update->getProgressString() << std::flush;
    }
    if (numInputTriples_ >= shared_.linesPerPartial_) {
      writeCurrentPartialVocabulary();
      startNewPartialVocabulary();
    }
  }

  // Handle the end of the input for this chain: write the current partial
  // vocabulary, unless this chain never received a single triple for it, and
  // add this chain's count of `ql:has-word` triples to the shared counter.
  void finish() {
    if (!localTriples_.empty()) {
      writeCurrentPartialVocabulary();
    }
    shared_.numHasWordTriples_.fetch_add(numHasWordTriples_);
  }
};

// Run `numThreads` task chains and block until all of them have ended, i.e.
// until the parser has delivered the end of its input to every chain or a
// chain has failed. In the latter case, rethrow the first recorded error. The
// parser is created by `makeParser`, which is called with the executor of this
// function's `io_context` (see below) and must return a
// `std::unique_ptr<AsyncRdfParserBase>` that schedules all of its work on that
// executor. Afterwards, `shared.nextPartialVocabIdx_` is the number of partial
// vocabularies that were written (each index below it was claimed by exactly
// one chain) and `shared.numTriples_` the total number of triples that were
// written.
//
// The actual computing power comes from the global thread pool (see
// `util/GlobalExecutor.h`): this function donates `numThreads` of its threads
// to a local `io_context` that it owns. The local `io_context` is needed
// because the chains and the parser have to be destroyed only when no
// asynchronous operation of theirs is left, and the global pool (which is
// never joined, and which other phases may be using at the same time) cannot
// provide that guarantee per phase. Running the local `io_context` until it
// runs out of work is the exact equivalent of joining a thread pool of this
// function's own; it is part of the same "hacky for now" story as the global
// pool itself.
//
// NOTE: The futures of the donated threads are awaited on the calling thread,
// which therefore must not be one of the threads of the global thread pool
// (that thread would block while holding a thread that the donated runners may
// be waiting for).
template <typename Index, typename MakeParser>
void runTaskChains(FirstPassSharedState<Index>& shared, size_t numThreads,
                   MakeParser makeParser) {
  // `ioContext` is declared before `parser` and `chains`, so that these (whose
  // asynchronous operations are scheduled on `ioContext`) are destroyed first,
  // in reverse declaration order.
  boost::asio::io_context ioContext;
  // Keep the donated runners (see below) from returning immediately: they are
  // started before the chains, at which point the `io_context` has no work yet.
  auto workGuard = boost::asio::make_work_guard(ioContext);
  std::unique_ptr<AsyncRdfParserBase> parser =
      std::move(makeParser)(ql::any_io_executor{ioContext.get_executor()});
  // The chains are owned here, outside of the `io_context`, and are kept alive
  // until all the runners have returned. This is simpler than passing
  // `shared_ptr`s to the chains through every asynchronous step.
  std::vector<std::unique_ptr<PartialVocabularyTaskChain<Index>>> chains;
  chains.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    chains.push_back(std::make_unique<PartialVocabularyTaskChain<Index>>(
        shared, *parser, ql::any_io_executor{ioContext.get_executor()}));
  }

  // Donate `numThreads` threads of the global thread pool to `ioContext`.
  //
  // NOTE: It is harmless to donate more runners than the global pool has
  // threads: a runner that only starts when the work is already done simply
  // finds an empty (and stopped) `io_context` and returns immediately.
  //
  // NOTE: A completion handler that throws would make `run()` exit before the
  // work is done, so the runner keeps running the `io_context` until it
  // returns normally. This should never happen (the chains catch all
  // exceptions of their own steps, see `PartialVocabularyTaskChain::step`),
  // but if it does, the error is reported like any other error of a chain,
  // instead of terminating the process (which is what a `boost::asio::
  // thread_pool` would do).
  auto globalExecutor = ad_utility::globalExecutor();
  std::vector<std::future<void>> runners;
  runners.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    runners.push_back(
        ad_utility::postAndGetFuture(globalExecutor, [&ioContext, &shared]() {
          while (true) {
            try {
              ioContext.run();
              return;
            } catch (...) {
              shared.reportError(std::current_exception());
            }
          }
        }));
  }

  for (auto& chain : chains) {
    chain->start();
  }
  // All the chains are started, so from now on the `io_context` runs out of
  // work exactly when the first pass is done.
  workGuard.reset();

  // Block until every donated runner has returned, which happens only when the
  // `io_context` has no work left, i.e. when every task chain has ended: no
  // more `asyncGetBatch` calls are in flight and no more steps are queued. As
  // the parser also runs on `ioContext`, this means that no asynchronous
  // operation at all is left, and the chains, the parser and the `io_context`
  // can safely be destroyed. This is the exact replacement of the
  // `boost::asio::thread_pool::join()` that this function used before.
  //
  // NOTE: Every runner is awaited, also when an earlier one has already
  // reported an error, because `ioContext` must outlive all of them.
  for (auto& runner : runners) {
    runner.get();
  }

  std::exception_ptr firstError = *shared.firstError_.wlock();
  if (firstError) {
    std::rethrow_exception(firstError);
  }
}

}  // namespace qlever::partialVocabularyBuilder

#endif  // QLEVER_SRC_INDEX_PARTIALVOCABULARYBUILDER_H
