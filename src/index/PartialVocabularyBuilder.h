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

#include <algorithm>
#include <atomic>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cstdint>
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
#include "util/MemorySize/MemorySize.h"
#include "util/ProgressBar.h"
#include "util/Synchronized.h"

// The building blocks of the first pass of the index building (see
// `IndexImpl::buildPartialVocabularies`): a single `boost::asio::thread_pool`
// (owned by `runTaskChains` below) is driven by several "task chains", each of
// which repeatedly asks the asynchronous RDF parser for a batch of triples,
// maps the triples to local IDs and writes a partial vocabulary (together with
// the corresponding ID triples) whenever its memory budget is exhausted (see
// `FirstPassMemoryBudget`).
// The parser runs on the same thread pool, so that there is exactly one
// compute resource for the whole first pass.
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

// The memory that the parser of the first pass is assumed to hold per thread,
// in units of its buffer size: one block of raw input that is being parsed,
// plus the parsed triples of a batch, which are typically larger than the raw
// input from which they were parsed.
constexpr inline size_t NUM_PARSER_BUFFERS_PER_THREAD = 3;

// The minimal memory budget of a single task chain (see
// `computeMemoryPerTaskChain`). If the memory limit is too small to give each
// chain at least this much, the budget is raised to this value and the memory
// limit is (slightly) exceeded.
constexpr inline ad_utility::MemorySize MIN_MEMORY_PER_TASK_CHAIN =
    ad_utility::MemorySize::megabytes(64);

// The maximal number of words in a single partial vocabulary. The vocabulary
// merger stores the index of a word in its partial vocabulary in 32 bits (see
// `SegmentIdMapEntry`). A partial vocabulary is only written after a complete
// batch, so this limit leaves plenty of room below `2^32` for the words of
// that batch. In practice, the memory budget is reached long before.
constexpr inline size_t MAX_WORDS_PER_PARTIAL_VOCABULARY = size_t{1} << 31;

// The memory that `writePartialVocabulary` (see `IndexImpl`) additionally
// allocates per word of the partial vocabulary while it is written: an
// `ItemVecEntry` and an entry of the hash map that maps the local IDs to the
// IDs in the sorted partial vocabulary (see `createInternalMapping`). The hash
// map is reserved for the number of words, which (because of the maximal load
// factor of the Abseil hash maps and the rounding of the capacity to a power
// of two) yields up to two slots per word, each with one byte of metadata.
constexpr inline size_t WRITE_OVERHEAD_PER_WORD =
    sizeof(ItemVecEntry) + 2 * (sizeof(std::pair<uint64_t, uint64_t>) + 1);

// Return the memory budget of each of the `numThreads` task chains of the
// first pass, such that all of them together with the parser (see
// `NUM_PARSER_BUFFERS_PER_THREAD`) fit into `memoryLimit`. If the result would
// be smaller than `MIN_MEMORY_PER_TASK_CHAIN`, log a warning and return that
// minimum instead, so that the memory limit is slightly exceeded.
inline ad_utility::MemorySize computeMemoryPerTaskChain(
    ad_utility::MemorySize memoryLimit, ad_utility::MemorySize parserBufferSize,
    size_t numThreads) {
  AD_CONTRACT_CHECK(numThreads > 0);
  size_t parserBytes =
      numThreads * NUM_PARSER_BUFFERS_PER_THREAD * parserBufferSize.getBytes();
  size_t limitBytes = memoryLimit.getBytes();
  size_t bytesPerChain =
      limitBytes > parserBytes ? (limitBytes - parserBytes) / numThreads : 0;
  if (bytesPerChain >= MIN_MEMORY_PER_TASK_CHAIN.getBytes()) {
    return ad_utility::MemorySize::bytes(bytesPerChain);
  }
  AD_LOG_WARN << "The memory limit for the index build ("
              << memoryLimit.asString()
              << ") is too small for parsing the input and creating the "
                 "partial vocabularies with "
              << numThreads << " threads (the parser alone is assumed to use "
              << ad_utility::MemorySize::bytes(parserBytes).asString()
              << "). Each thread now uses "
              << MIN_MEMORY_PER_TASK_CHAIN.asString()
              << " for its partial vocabulary, so the memory limit will be "
                 "exceeded. Consider increasing the memory limit or reducing "
                 "the number of threads"
              << std::endl;
  return MIN_MEMORY_PER_TASK_CHAIN;
}

// The limits for the size of a single partial vocabulary of the first pass.
struct FirstPassMemoryBudget {
  // The memory that a single task chain may use for its current partial
  // vocabulary (see `PartialVocabularyTaskChain::isPartialVocabularyFull`).
  ad_utility::MemorySize memoryPerTaskChain_;
  // If set, a task chain additionally writes its partial vocabulary as soon as
  // it contains at least this many input triples. This is only used by tests,
  // which need many partial vocabularies with only a few triples.
  std::optional<size_t> maxTriplesPerPartialVocabularyOnlyForTesting_ =
      std::nullopt;
};

// Shared state for all the task chains of a single first pass. An aggregate:
// the caller initializes the first three members, the remaining ones are the
// progress bar, the counters and the flags that the chains update while they
// run and that the caller reads once `runTaskChains` has returned. Must
// outlive every task chain, which is guaranteed by `runTaskChains` below,
// because the chains are local to that function.
template <typename Index>
struct FirstPassSharedState {
  Index* index_;
  // The comparator for the `ItemMapManager`s of the task chains.
  const TripleComponentComparator* comparator_;
  FirstPassMemoryBudget budget_;

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
// resulting `IdRow`s. Once the memory of the current partial vocabulary has
// (almost) reached the budget of the chain (see `isPartialVocabularyFull`), or
// the input is exhausted, the chain atomically claims the next free partial
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
  // The storage for the batches of this chain. It is handed to the parser with
  // every `asyncGetBatch` call and comes back with the parsed triples (see
  // `AsyncRdfParserBase::asyncGetBatch`), so that this chain grows its buffer
  // only once instead of once per batch. As a chain only ever has a single
  // call in flight, one buffer per chain suffices.
  std::vector<TurtleTriple> batchBuffer_;
  // The number of input triples of the current partial vocabulary. Only needed
  // for `FirstPassMemoryBudget::maxTriplesPerPartialVocabularyOnlyForTesting_`.
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
  // all task chains have been constructed and before the thread pool behind
  // `executor_` is joined. This is `noexcept` because `runTaskChains` starts
  // the chains one after the other: if starting a later chain threw (which
  // only `bad_alloc` from `post` could do), the earlier chains would already
  // be running on the pool while `runTaskChains` unwinds and destroys them.
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
    parser_.asyncGetBatch(
        std::move(batchBuffer_),
        boost::asio::bind_executor(
            executor_, [this](std::exception_ptr ep,
                              std::optional<std::vector<TurtleTriple>> batch) {
              try {
                if (ep) {
                  std::rethrow_exception(ep);
                }
                if (shared_.stopRequested_.load()) {
                  // Another chain has failed; end this chain without further
                  // work.
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
  // current partial vocabulary is full, write it and start a new one. Keep the
  // storage of `batch` for the next call, see `batchBuffer_`.
  void handleBatch(std::vector<TurtleTriple> batch) {
    size_t numRowsBefore = localTriples_.size();
    size_t numWordsBefore = numWords();
    for (auto& triple : batch) {
      mapTripleToIds(std::move(triple), itemMap_, shared_.index_, localTriples_,
                     numHasWordTriples_);
    }
    numInputTriples_ += batch.size();
    shared_.progressBar_.add(batch.size());
    batchBuffer_ = std::move(batch);
    batchBuffer_.clear();
    if (auto update = shared_.progressBar_.update()) {
      AD_LOG_INFO << update->getProgressString() << std::flush;
    }
    if (isPartialVocabularyFull(localTriples_.size() - numRowsBefore,
                                numWords() - numWordsBefore)) {
      writeCurrentPartialVocabulary();
      startNewPartialVocabulary();
    }
  }

  // Return the number of distinct words in the current partial vocabulary.
  size_t numWords() const { return itemMap_.map_.map_.size(); }

  // Return the number of bytes that the current partial vocabulary of this
  // chain uses: the hash map and the strings of the `ItemMapManager` (which
  // are counted exactly, see `ItemMapAndBuffer`), the buffered `IdRow`s, and
  // the memory that `writePartialVocabulary` will additionally need for
  // writing it (see `WRITE_OVERHEAD_PER_WORD`).
  size_t memoryOfPartialVocabulary() const {
    const auto& map = itemMap_.map_;
    return map.numBytesAllocated() + localTriples_.capacity() * sizeof(IdRow) +
           numWords() * WRITE_OVERHEAD_PER_WORD;
  }

  // Return the number of bytes that the current partial vocabulary will
  // additionally need if the next batch adds as many rows and words as the
  // last one (`numNewRows` and `numNewWords`). This is only nonzero if the
  // next batch makes the vector of `IdRow`s or the hash map reallocate, in
  // which case the new storage (twice as large as the old one) is needed in
  // addition to the old one while the elements are moved. The strings of the
  // next batch are not accounted for, so a partial vocabulary might exceed the
  // budget by at most the strings of a single batch (rounded up to whole
  // blocks of the `MonotonicBuffer`).
  size_t memoryOfNextReallocation(size_t numNewRows, size_t numNewWords) const {
    size_t result = 0;
    size_t capacity = localTriples_.capacity();
    if (localTriples_.size() + numNewRows > capacity) {
      result += std::max(2 * capacity, localTriples_.size() + numNewRows) *
                sizeof(IdRow);
    }
    // An Abseil hash map grows to roughly twice its capacity as soon as its
    // size exceeds `7/8` of its capacity. Every slot of the map additionally
    // has one byte of metadata.
    const auto& map = itemMap_.map_.map_;
    if ((map.size() + numNewWords) * 8 > map.capacity() * 7) {
      result += 2 * (map.capacity() + 1) * (sizeof(ItemMapValue) + 1);
    }
    return result;
  }

  // Return true if the current partial vocabulary has to be written before
  // the next batch is handled, because the next batch (if it adds as many
  // rows and words as the last one, given by `numNewRows` and `numNewWords`)
  // could exceed the memory budget of this chain (see `FirstPassMemoryBudget`)
  // or the maximal number of words (see `MAX_WORDS_PER_PARTIAL_VOCABULARY`).
  // As this is only checked after a batch has been handled, every partial
  // vocabulary contains at least one batch, no matter how small the budget
  // is.
  bool isPartialVocabularyFull(size_t numNewRows, size_t numNewWords) const {
    const auto& budget = shared_.budget_;
    if (budget.maxTriplesPerPartialVocabularyOnlyForTesting_.has_value() &&
        numInputTriples_ >=
            budget.maxTriplesPerPartialVocabularyOnlyForTesting_.value()) {
      return true;
    }
    if (numWords() + numNewWords > MAX_WORDS_PER_PARTIAL_VOCABULARY) {
      return true;
    }
    return memoryOfPartialVocabulary() +
               memoryOfNextReallocation(numNewRows, numNewWords) >=
           budget.memoryPerTaskChain_.getBytes();
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
