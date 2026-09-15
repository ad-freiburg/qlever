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
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "backports/asio.h"
#include "index/IndexBuilderTypes.h"
#include "parser/AsyncRdfParserBase.h"
#include "util/Log.h"
#include "util/ProgressBar.h"
#include "util/Synchronized.h"

// The building blocks of the first pass of the index building (see
// `IndexImpl::buildPartialVocabularies`): a single `boost::asio::thread_pool`
// (owned by `runTaskChains` below) is driven by several "task chains", each of
// which repeatedly asks the asynchronous RDF parser for a batch of triples,
// maps the triples to local IDs and writes a partial vocabulary (together with
// the corresponding ID triples) whenever enough triples have been collected.
// The parser runs on the same thread pool, so that there is exactly one
// compute resource for the whole first pass.
//
// The classes are templated on the `Index` type (`IndexImpl` in production)
// so that the pipeline can be unit-tested in isolation with a mock index. An
// `Index` must provide:
// - `ProcessedTriple processTriple(TurtleTriple&&)`, see `mapTripleToIds` in
//   `IndexBuilderTypes.h`.
// - `void writePartialVocabulary(size_t partialVocabIdx, ItemMapAndBuffer
//   items, std::vector<IdRow> localIds)`, which writes the partial vocabulary
//   with the given index and its triples (`IdRow` is defined in
//   `IndexBuilderTypes.h`). It is called concurrently from several task
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
  ItemAlloc itemAlloc_;
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
// starts a fresh `ItemMapManager` for the next partial vocabulary.
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
  // vocabulary that is currently being built by this chain; re-created with a
  // fresh, empty state every time a partial vocabulary is written (see
  // `writeCurrentPartialVocabulary`). `ItemMapManager` is
  // not movable, hence the `optional`.
  std::optional<ItemMapManager> itemMap_;
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
      : shared_{shared}, parser_{parser}, executor_{std::move(executor)} {
    startNewPartialVocabulary();
  }

  // Schedule the first step of this chain. Must be called exactly once, after
  // all task chains have been constructed and before the thread pool behind
  // `executor_` is joined. This is `noexcept` because `runTaskChains` starts
  // the chains one after the other: if starting a later chain threw (which
  // only `bad_alloc` from `post` could do), the earlier chains would already
  // be running on the pool while `runTaskChains` unwinds and destroys them.
  void start() noexcept { postNextStep(); }

 private:
  // (Re-)initialize `itemMap_` for a fresh partial vocabulary and clear the
  // triple buffer and the input-triple counter. The number of entries that
  // are reserved for the item map is somewhat arbitrary: half the number of
  // triples per partial vocabulary was empirically better than larger values.
  // Note that `reserve` on a hash map has to assume the worst case (many
  // collisions), so it allocates considerably more than the requested number
  // of entries. The memory allocation overhead of the first pass should be
  // systematically analyzed anyway.
  void startNewPartialVocabulary() {
    itemMap_.emplace(0, shared_.comparator_, shared_.itemAlloc_);
    itemMap_->map_.map_.reserve(shared_.linesPerPartial_ / 2);
    localTriples_.clear();
    numInputTriples_ = 0;
  }

  // Claim the next free partial vocabulary index from the shared counter and
  // write the current (non-empty) partial vocabulary and its triples under
  // that index. Both files are exclusively owned by this chain, so no further
  // synchronization is needed.
  void writeCurrentPartialVocabulary() {
    size_t partialVocabIdx = shared_.nextPartialVocabIdx_.fetch_add(1);
    shared_.numTriples_.fetch_add(localTriples_.size());
    shared_.index_->writePartialVocabulary(partialVocabIdx,
                                           std::move(*itemMap_).moveMap(),
                                           std::move(localTriples_));
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
      mapTripleToIds(std::move(triple), itemMap_.value(), shared_.index_,
                     localTriples_, numHasWordTriples_);
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

// Run `numThreads` task chains on a thread pool with `numThreads` threads,
// which is created (and destroyed) by this function, and block until all of
// them have ended, i.e. until the parser has delivered the end of its input to
// every chain or a chain has failed. In the latter case, rethrow the first
// recorded error. The parser is created by `makeParser`, which is called with
// the executor of the thread pool and must return a
// `std::unique_ptr<AsyncRdfParserBase>` that schedules all of its work on that
// executor. Afterwards, `shared.nextPartialVocabIdx_` is the number of partial
// vocabularies that were written (each index below it was claimed by exactly
// one chain) and `shared.numTriples_` the total number of triples that were
// written.
template <typename Index, typename MakeParser>
void runTaskChains(FirstPassSharedState<Index>& shared, size_t numThreads,
                   MakeParser makeParser) {
  // `pool` is declared before `parser` and `chains`, so that these (whose
  // asynchronous operations are scheduled on `pool`) are destroyed first, in
  // reverse declaration order.
  boost::asio::thread_pool pool{numThreads};
  std::unique_ptr<AsyncRdfParserBase> parser =
      std::move(makeParser)(pool.get_executor());
  // The chains are owned here, outside of the thread pool, and are kept alive
  // until `join()` has returned. This is simpler than passing `shared_ptr`s to
  // the chains through every asynchronous step.
  std::vector<std::unique_ptr<PartialVocabularyTaskChain<Index>>> chains;
  chains.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    chains.push_back(std::make_unique<PartialVocabularyTaskChain<Index>>(
        shared, *parser, pool.get_executor()));
  }
  for (auto& chain : chains) {
    chain->start();
  }
  // Block until every task chain has ended, i.e. no more `asyncGetBatch`
  // calls are in flight and no more steps are queued. As the parser also runs
  // on `pool`, this means that no asynchronous operation at all is left.
  pool.join();

  std::exception_ptr firstError = *shared.firstError_.wlock();
  if (firstError) {
    std::rethrow_exception(firstError);
  }
}

}  // namespace qlever::partialVocabularyBuilder

#endif  // QLEVER_SRC_INDEX_PARTIALVOCABULARYBUILDER_H
