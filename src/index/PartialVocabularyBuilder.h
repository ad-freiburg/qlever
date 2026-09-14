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
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "index/IndexBuilderTypes.h"
#include "parser/AsyncRdfParserBase.h"
#include "util/Log.h"
#include "util/ProgressBar.h"

// The building blocks of the first pass of the index building (see
// `IndexImpl::buildPartialVocabularies`): a single `boost::asio::thread_pool`
// is driven by several "task chains", each of which repeatedly asks the
// asynchronous RDF parser for a batch of triples, maps the triples to local IDs
// and writes a partial vocabulary (together with the corresponding ID triples)
// whenever enough triples have been collected.
//
// The classes are templated on the `Index` type (`IndexImpl` in production)
// so that the pipeline can be unit-tested in isolation with a mock index. An
// `Index` must provide:
// - `ProcessedTriple processTriple(TurtleTriple&&)`, see `mapTripleToIds` in
//   `IndexBuilderTypes.h`.
// - `void writePartialVocabulary(size_t partialVocabIdx, ItemMapAndBuffer
//   items, std::vector<IdRow> localIds)`, which writes the partial vocabulary
//   with the given index and its triples. It is called concurrently from
//   several task chains, but never twice for the same index.
namespace qlever::partialVocabularyBuilder {

// A row with the components already mapped to IDs. NOTE: Deliberately not
// named `IdTriple`, which is a class with a similar purpose defined in
// `index/IdTriple.h`.
using IdRow = std::array<Id, NumColumnsIndexBuilding>;

// Shared, mostly read-only state for all the task chains of a single first
// pass. Must outlive every task chain, which is guaranteed by `runTaskChains`
// below, because `boost::asio::thread_pool::join()` only returns once every
// chain has ended (no more calls in flight and no more steps queued).
template <typename Index>
struct FirstPassSharedState {
  Index* index_;
  AsyncRdfParserBase* parser_;
  boost::asio::thread_pool* pool_;
  // The comparator for the `ItemMapManager`s of the task chains.
  const TripleComponentComparator* comparator_;
  ItemAlloc itemAlloc_;
  size_t linesPerPartial_;
  size_t numThreads_;
  ad_utility::ConcurrentProgressBar* progressBar_;
  // May be `nullptr` if no `ql:has-word` triples are created.
  std::atomic<size_t>* numHasWordTriples_;

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
  // Guard `firstError_`.
  std::mutex errorMutex_;
  std::exception_ptr firstError_;

  // `ItemAlloc` (a `ql::pmr::polymorphic_allocator`) is copyable but not
  // copy-assignable, and `std::mutex`/`std::atomic` are neither, so this
  // constructor (rather than member-by-member assignment after default
  // construction) is used to set up all the members in one go.
  FirstPassSharedState(Index* index, AsyncRdfParserBase* parser,
                       boost::asio::thread_pool* pool,
                       const TripleComponentComparator* comparator,
                       ItemAlloc itemAlloc, size_t linesPerPartial,
                       size_t numThreads,
                       ad_utility::ConcurrentProgressBar* progressBar,
                       std::atomic<size_t>* numHasWordTriples)
      : index_{index},
        parser_{parser},
        pool_{pool},
        comparator_{comparator},
        itemAlloc_{itemAlloc},
        linesPerPartial_{linesPerPartial},
        numThreads_{numThreads},
        progressBar_{progressBar},
        numHasWordTriples_{numHasWordTriples} {}

  // Record `ep` as `firstError_` (unless an error has already been recorded)
  // and set `stopRequested_`. Thread-safe; may be called concurrently by
  // several task chains.
  void reportError(std::exception_ptr ep) {
    std::lock_guard l{errorMutex_};
    if (!firstError_) {
      firstError_ = ep;
    }
    stopRequested_.store(true, std::memory_order_relaxed);
  }
};

// A single task chain of the first pass. This is *not* a thread: after
// construction, `start()` schedules the first step as a `boost::asio`
// completion handler on the shared thread pool, and every subsequent step is
// likewise scheduled via `boost::asio::post` (never called inline from within
// a handler), so that the call stack never grows with the number of processed
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
  // The `ItemMapManager` and buffered local-ID triples of the partial
  // vocabulary that is currently being built by this chain; re-created with a
  // fresh, empty state every time a partial vocabulary is written (see
  // `writeCurrentPartialVocabulary`). `ItemMapManager` is `alignas(256)` and
  // not movable, hence the `optional`.
  std::optional<ItemMapManager> itemMap_;
  std::vector<IdRow> localTriples_;
  size_t numInputTriples_ = 0;

 public:
  explicit PartialVocabularyTaskChain(FirstPassSharedState<Index>& shared)
      : shared_{shared} {
    startNewPartialVocabulary();
  }

  // Schedule the first step of this chain. Must be called exactly once, after
  // all task chains have been constructed and before `shared_.pool_->join()`.
  void start() { postNextStep(); }

 private:
  // (Re-)initialize `itemMap_` for a fresh partial vocabulary and clear the
  // triple buffer and the input-triple counter. Reserve space for the
  // expected number of distinct words, which is a heuristic of five words per
  // triple, divided among the `numThreads_` task chains.
  void startNewPartialVocabulary() {
    itemMap_.emplace(0, shared_.comparator_, shared_.itemAlloc_);
    itemMap_->map_.map_.reserve(5 * shared_.linesPerPartial_ /
                                shared_.numThreads_);
    localTriples_.clear();
    numInputTriples_ = 0;
  }

  // Claim the next free partial vocabulary index from the shared counter and
  // write the current (non-empty) partial vocabulary and its triples under
  // that index. Both files are exclusively owned by this chain, so no further
  // synchronization is needed.
  void writeCurrentPartialVocabulary() {
    size_t partialVocabIdx = shared_.nextPartialVocabIdx_.fetch_add(1);
    shared_.numTriples_.fetch_add(localTriples_.size(),
                                  std::memory_order_relaxed);
    shared_.index_->writePartialVocabulary(partialVocabIdx,
                                           std::move(*itemMap_).moveMap(),
                                           std::move(localTriples_));
  }

  // Schedule the next step of this chain on the shared thread pool.
  void postNextStep() {
    boost::asio::post(*shared_.pool_, [this] { step(); });
  }

  // A single step of this chain: ask the parser for the next batch of
  // triples and, once it arrives, handle it in `handleBatch`.
  void step() {
    shared_.parser_->asyncGetBatch(
        [this](std::exception_ptr ep,
               std::optional<std::vector<TurtleTriple>> batch) {
          handleBatch(std::move(ep), std::move(batch));
        });
  }

  // Handle the result of one `asyncGetBatch` call: map the triples in `batch`
  // to local IDs, write and (re-)start partial vocabularies as needed, and
  // either schedule the next step or end this chain (by not scheduling one).
  void handleBatch(std::exception_ptr ep,
                   std::optional<std::vector<TurtleTriple>> batch) {
    try {
      if (ep) {
        std::rethrow_exception(ep);
      }
      if (shared_.stopRequested_.load(std::memory_order_relaxed)) {
        // Another chain has failed; end this chain without further work.
        return;
      }
      if (!batch.has_value()) {
        // End of input for this chain. Write the current partial vocabulary,
        // unless this chain never received a single batch for it.
        if (!localTriples_.empty()) {
          writeCurrentPartialVocabulary();
        }
        return;
      }
      for (auto& triple : batch.value()) {
        auto ids = mapTripleToIds(std::move(triple), itemMap_.value(),
                                  shared_.index_, shared_.numHasWordTriples_);
        localTriples_.insert(localTriples_.end(), ids.begin(), ids.end());
      }
      numInputTriples_ += batch->size();
      shared_.progressBar_->add(batch->size());
      if (auto update = shared_.progressBar_->update()) {
        AD_LOG_INFO << update->getProgressString() << std::flush;
      }
      if (numInputTriples_ >= shared_.linesPerPartial_) {
        writeCurrentPartialVocabulary();
        startNewPartialVocabulary();
      }
      postNextStep();
    } catch (...) {
      shared_.reportError(std::current_exception());
      // End this chain: do not schedule another step.
    }
  }
};

// Run `shared.numThreads_` task chains on `shared.pool_` and block until all
// of them have ended, i.e. until the parser has delivered the end of its
// input to every chain or a chain has failed. In the latter case, rethrow the
// first recorded error. Afterwards, `shared.nextPartialVocabIdx_` is the
// number of partial vocabularies that were written (each index below it was
// claimed by exactly one chain) and `shared.numTriples_` the total number of
// triples that were written.
template <typename Index>
void runTaskChains(FirstPassSharedState<Index>& shared) {
  // The chains are owned here, outside of the thread pool, and are kept alive
  // until `join()` has returned. This is simpler than passing `shared_ptr`s to
  // the chains through every asynchronous step.
  std::vector<std::unique_ptr<PartialVocabularyTaskChain<Index>>> chains;
  chains.reserve(shared.numThreads_);
  for (size_t i = 0; i < shared.numThreads_; ++i) {
    chains.push_back(
        std::make_unique<PartialVocabularyTaskChain<Index>>(shared));
  }
  for (auto& chain : chains) {
    chain->start();
  }
  // Block until every task chain has ended, i.e. no more `asyncGetBatch`
  // calls are in flight and no more steps are queued.
  shared.pool_->join();

  if (shared.firstError_) {
    std::rethrow_exception(shared.firstError_);
  }
}

}  // namespace qlever::partialVocabularyBuilder

#endif  // QLEVER_SRC_INDEX_PARTIALVOCABULARYBUILDER_H
