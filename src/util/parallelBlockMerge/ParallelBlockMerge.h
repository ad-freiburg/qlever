// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/OutputSinkPolicy.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"
#include "util/parallelBlockMerge/SchedulerPolicy.h"

// An STXXL-style parallel k-way merge. The input is a set of presorted runs,
// each of which is split into blocks. The blocks may live compressed on disk;
// only their element count and their first and last key have to be available
// without I/O. This allows to split the global output range into chunks that
// can be merged completely independently and therefore in parallel.
//
// This header contains the merging logic itself; it is the header that users of
// this library include. The policies that the merge is built on (input,
// scheduler, and sink) as well as its options live in the sibling headers of
// this directory.
namespace ad_utility::parallelBlockMerge {

// ___________________________________________________________________________
// The splitters.
// ___________________________________________________________________________

// Compute the chunk boundaries by a weighted quantile over the block metadata
// only (no I/O at all): every block contributes its `lastKey`, weighted by its
// number of elements. Return a strictly increasing (wrt `comparator`) vector of
// at most `numChunks - 1` keys. Chunk `i` then covers the half-open value range
// `[result[i - 1], result[i])`, where `result[-1]` is minus infinity and
// `result[numChunks - 1]` is plus infinity.
//
// NOTE: The result may well be shorter than `numChunks - 1`, in particular it
// is empty if all keys are equal. In that case the merge simply consists of
// fewer chunks, and there is nothing that could be done about it, because all
// elements with the same key have to end up in the same chunk.
CPP_template(typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>)
    std::vector<typename Input::Key> computeSplitters(
        const Input& input, const Comparator& comparator, size_t numChunks) {
  using Key = typename Input::Key;
  std::vector<Key> result;
  if (numChunks <= 1) {
    return result;
  }

  // Collect the `lastKey` of every (non-empty) block together with the number
  // of elements in that block, which is the weight of the key. Also compute the
  // smallest key of the whole input, which is needed below.
  std::vector<std::pair<Key, size_t>> keysAndWeights;
  std::optional<Key> smallestKey;
  size_t totalNumElements = 0;
  size_t numRuns = input.numRuns();
  for (size_t run = 0; run < numRuns; ++run) {
    size_t numBlocks = input.numBlocks(run);
    for (size_t block = 0; block < numBlocks; ++block) {
      size_t numElements = input.numElementsInBlock(run, block);
      if (numElements == 0) {
        continue;
      }
      const Key& firstKey = input.firstKey(run, block);
      if (!smallestKey.has_value() || comparator(firstKey, *smallestKey)) {
        smallestKey = firstKey;
      }
      keysAndWeights.emplace_back(input.lastKey(run, block), numElements);
      totalNumElements += numElements;
    }
  }
  if (keysAndWeights.empty()) {
    return result;
  }

  ql::ranges::sort(keysAndWeights,
                   [&comparator](const std::pair<Key, size_t>& a,
                                 const std::pair<Key, size_t>& b) {
                     return comparator(a.first, b.first);
                   });

  // Replace the weights by their prefix sums, such that `keysAndWeights[i]`
  // holds the number of elements that are (approximately) not greater than
  // `keysAndWeights[i].first`.
  size_t prefixSum = 0;
  for (auto& keyAndWeight : keysAndWeights) {
    prefixSum += keyAndWeight.second;
    keyAndWeight.second = prefixSum;
  }

  // Walk the target quantiles. As the targets are increasing, a single linear
  // scan over the (sorted) keys suffices.
  size_t idx = 0;
  for (size_t i = 1; i < numChunks; ++i) {
    // NOTE: The target is at least `1`, because a target of `0` would always
    // yield the smallest key and therefore an empty first chunk.
    size_t target = std::max<size_t>(1, totalNumElements * i / numChunks);
    while (idx < keysAndWeights.size() && keysAndWeights[idx].second < target) {
      ++idx;
    }
    if (idx == keysAndWeights.size()) {
      break;
    }
    const Key& candidate = keysAndWeights[idx].first;
    // Only keep the candidate if it makes the result strictly increasing. The
    // smallest key of the input acts as the (virtual) predecessor of the first
    // splitter, because a splitter that is not greater than that key would
    // yield an empty first chunk.
    const Key& previous = result.empty() ? *smallestKey : result.back();
    if (comparator(previous, candidate)) {
      result.push_back(candidate);
    }
  }
  return result;
}

namespace detail {

// Return the total number of elements of all runs of the `input`. This only
// uses the metadata and therefore performs no I/O.
CPP_template(typename Input)(requires BlockedRunsInput<Input>) size_t
    totalNumElements(const Input& input) {
  size_t result = 0;
  size_t numRuns = input.numRuns();
  for (size_t run = 0; run < numRuns; ++run) {
    size_t numBlocks = input.numBlocks(run);
    for (size_t block = 0; block < numBlocks; ++block) {
      result += input.numElementsInBlock(run, block);
    }
  }
  return result;
}

// Return the first index in `[0, numIndices)` for which the monotone
// `predicate` (that is, once it is `true` it stays `true`) holds, or
// `numIndices` if there is no such index.
template <typename Predicate>
size_t firstIndexWhere(size_t numIndices, const Predicate& predicate) {
  size_t low = 0;
  size_t high = numIndices;
  while (low < high) {
    size_t mid = low + (high - low) / 2;
    if (predicate(mid)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  return low;
}

// Return the number of elements of the `block`.
template <typename Block>
size_t blockSize(const Block& block) {
  return static_cast<size_t>(ql::ranges::distance(block));
}

// Merge that part of the runs of a `BlockedRunsInput` that lies in the
// half-open key range `[lo, hi)` and yield the result as a sequence of output
// blocks (see `nextBlock()`). An empty `lo`/`hi` means minus/plus infinity.
//
// The blocks of the input are read lazily and one at a time per run, so the
// memory that a single `ChunkMerger` requires is one input block per run plus
// a single output block.
//
// The `Comparator` has to be able to compare two elements, two keys, as well as
// an element with a key (in both orders).
//
// If `moveElements` is `true`, then the elements are moved out of the input
// blocks into the output blocks.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class ChunkMerger {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  // The lazy cursor over that part of a single run that lies in `[lo, hi)`. The
  // current element is `begin(block_)[idx_]`, and the cursor is exhausted if
  // `idx_ == end_` and there is no further block to read.
  struct Cursor {
    size_t run_;
    size_t firstBlock_;
    size_t nextBlock_;
    size_t endBlock_;
    Block block_;
    size_t idx_ = 0;
    size_t end_ = 0;

    // NOTE: The `block` is only passed in because `Block` need not be default
    // constructible; it is always the result of `makeEmptyBlock()`.
    Cursor(size_t run, size_t firstBlock, size_t endBlock, Block block)
        : run_{run},
          firstBlock_{firstBlock},
          nextBlock_{firstBlock},
          endBlock_{endBlock},
          block_{std::move(block)} {}
  };

  const Input* input_;
  const Comparator* comparator_;
  MergeOptions options_;
  std::optional<Key> lo_;
  std::optional<Key> hi_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  std::vector<Cursor> cursors_;
  // The min-heap over the cursors, see `heapComparator()`.
  std::vector<Cursor*> heap_;
  bool isInitialized_ = false;

 public:
  // Construct from the `input` and the `comparator` (both of which have to
  // outlive the `ChunkMerger`), the `options`, the (inclusive) lower and the
  // (exclusive) upper bound of the chunk, and an optional `cancellationHandle`.
  ChunkMerger(const Input& input, const Comparator& comparator,
              MergeOptions options, std::optional<Key> lo,
              std::optional<Key> hi,
              ad_utility::SharedCancellationHandle cancellationHandle)
      : input_{&input},
        comparator_{&comparator},
        options_{std::move(options)},
        lo_{std::move(lo)},
        hi_{std::move(hi)},
        cancellationHandle_{std::move(cancellationHandle)} {}

  // The merger holds pointers into itself (the `heap_` points into
  // `cursors_`), so it must neither be copied nor moved.
  ChunkMerger(const ChunkMerger&) = delete;
  ChunkMerger& operator=(const ChunkMerger&) = delete;
  ChunkMerger(ChunkMerger&&) = delete;
  ChunkMerger& operator=(ChunkMerger&&) = delete;

  // Return the next output block, or `std::nullopt` if the chunk is exhausted.
  // The returned block is never empty. This is the only function that performs
  // I/O and it must not be called concurrently for the same `ChunkMerger`.
  std::optional<Block> nextBlock() {
    if (!isInitialized_) {
      initialize();
      isInitialized_ = true;
    }
    if (heap_.empty()) {
      return std::nullopt;
    }
    auto block = input_->makeEmptyBlock();
    size_t numElements = 0;
    MemorySize memory = MemorySize::bytes(0);
    auto comparator = heapComparator();
    while (!heap_.empty()) {
      ql::ranges::pop_heap(heap_, comparator);
      Cursor* cursor = heap_.back();
      auto&& element = ql::ranges::begin(cursor->block_)[cursor->idx_];
      memory += input_->memorySizeOfElement(element);
      if constexpr (moveElements) {
        input_->appendToBlock(block, std::move(element));
      } else {
        input_->appendToBlock(block, element);
      }
      ++numElements;
      ++cursor->idx_;
      // NOTE: `advance` may replace `cursor->block_`, which invalidates
      // `element`. This is fine, because the element was already appended.
      if (advance(*cursor)) {
        ql::ranges::push_heap(heap_, comparator);
      } else {
        heap_.pop_back();
      }
      if (numElements >= options_.outputBlockSize ||
          memory >= options_.maxOutputBlockMemory) {
        break;
      }
    }
    if (cancellationHandle_ != nullptr) {
      cancellationHandle_->throwIfCancelled();
    }
    return block;
  }

 private:
  // Return the comparator of the `heap_`. Its arguments are reversed, such that
  // the max-heap of the standard library acts as a min-heap. The result is
  // deterministic for a fixed configuration in any case; only with
  // `MergeOptions::stableTieBreaking` are ties additionally broken by the index
  // of the run, which makes the tie order independent of the number of chunks.
  //
  // NOTE: The default (unstable) path calls the `comparator` exactly once per
  // heap comparison. The stable path needs a second call whenever the first one
  // returns `false`, which for the expensive comparators of the index build
  // (a full ICU collation, for example) is a substantial cost. The flag is
  // therefore captured once here and not read per comparison.
  auto heapComparator() const {
    return [comparator = comparator_, isStable = options_.stableTieBreaking](
               const Cursor* a, const Cursor* b) {
      const auto& elA = ql::ranges::begin(a->block_)[a->idx_];
      const auto& elB = ql::ranges::begin(b->block_)[b->idx_];
      if (!isStable) {
        return (*comparator)(elB, elA);
      }
      if ((*comparator)(elB, elA)) {
        return true;
      }
      if ((*comparator)(elA, elB)) {
        return false;
      }
      return b->run_ < a->run_;
    };
  }

  // Set up the cursors of all runs that contribute to this chunk, and build the
  // initial heap.
  void initialize() {
    size_t numRuns = input_->numRuns();
    cursors_.reserve(numRuns);
    for (size_t run = 0; run < numRuns; ++run) {
      size_t numBlocks = input_->numBlocks(run);
      // The first block that may contain an element that is not smaller than
      // `lo`.
      size_t startBlock = 0;
      if (lo_.has_value()) {
        startBlock = firstIndexWhere(numBlocks, [this, run](size_t block) {
          return !(*comparator_)(input_->lastKey(run, block), lo_.value());
        });
      }
      // The first block all of whose elements are not smaller than `hi`, that
      // is the (exclusive) end of the range of blocks.
      //
      // NOTE: The predicate is deliberately `!comparator(firstKey, hi)` and not
      // `comparator(hi, firstKey)`. The latter would be off by one and would
      // read one superfluous block whenever `firstKey(block) == hi`.
      size_t endBlock = numBlocks;
      if (hi_.has_value()) {
        endBlock = firstIndexWhere(numBlocks, [this, run](size_t block) {
          return !(*comparator_)(input_->firstKey(run, block), hi_.value());
        });
      }
      if (startBlock >= endBlock) {
        continue;
      }
      cursors_.emplace_back(run, startBlock, endBlock,
                            input_->makeEmptyBlock());
    }
    heap_.reserve(cursors_.size());
    for (auto& cursor : cursors_) {
      if (advance(cursor)) {
        heap_.push_back(&cursor);
      }
    }
    ql::ranges::make_heap(heap_, heapComparator());
  }

  // Make sure that the `cursor` points to a valid element, reading further
  // blocks if necessary. Return `false` if the `cursor` is exhausted.
  bool advance(Cursor& cursor) {
    while (cursor.idx_ == cursor.end_) {
      if (cursor.nextBlock_ == cursor.endBlock_) {
        return false;
      }
      size_t block = cursor.nextBlock_;
      ++cursor.nextBlock_;
      cursor.block_ = input_->readBlock(cursor.run_, block);
      cursor.idx_ = 0;
      cursor.end_ = blockSize(cursor.block_);
      // Only the very first block of the chunk can contain elements that are
      // smaller than `lo`, and only the very last one can contain elements that
      // are not smaller than `hi`.
      if (block == cursor.firstBlock_ && lo_.has_value()) {
        cursor.idx_ = static_cast<size_t>(
            ql::ranges::lower_bound(cursor.block_, lo_.value(), *comparator_) -
            ql::ranges::begin(cursor.block_));
      }
      if (block + 1 == cursor.endBlock_ && hi_.has_value()) {
        cursor.end_ = static_cast<size_t>(
            ql::ranges::lower_bound(cursor.block_, hi_.value(), *comparator_) -
            ql::ranges::begin(cursor.block_));
      }
      AD_CORRECTNESS_CHECK(cursor.idx_ <= cursor.end_);
    }
    return true;
  }
};

// The state of a purely serial merge, exposed as a lazy range of blocks. It
// owns the input and the comparator, because the `ChunkMerger` only refers to
// them by pointer.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class SerialMergeState
    : public ad_utility::InputRangeFromGet<typename Input::Block> {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  Input input_;
  Comparator comparator_;
  std::optional<ChunkMerger<moveElements, Input, Comparator>> merger_;

 public:
  // ________________________________________________________________________
  SerialMergeState(Input input, Comparator comparator, MergeOptions options,
                   ad_utility::SharedCancellationHandle cancellationHandle)
      : input_{std::move(input)}, comparator_{std::move(comparator)} {
    merger_.emplace(input_, comparator_, std::move(options), std::nullopt,
                    std::nullopt, std::move(cancellationHandle));
  }

  // The `merger_` points to the other members, so this class must neither be
  // copied nor moved.
  SerialMergeState(const SerialMergeState&) = delete;
  SerialMergeState& operator=(const SerialMergeState&) = delete;
  SerialMergeState(SerialMergeState&&) = delete;
  SerialMergeState& operator=(SerialMergeState&&) = delete;

  // ________________________________________________________________________
  std::optional<Block> get() override { return merger_->nextBlock(); }
};

// The state of a parallel merge, exposed as a lazy range of blocks. It owns
// *everything* that the concurrently running chunk tasks refer to (the input,
// the comparator, the sink, and the bookkeeping of the tasks), because
// `InOrderBlockSink::blocks()` only holds a raw pointer to the sink. Its
// destructor aborts the sink and then waits for all in-flight tasks, so that a
// consumer that abandons the range early cannot cause a use-after-free.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class ParallelMergeState
    : public ad_utility::InputRangeFromGet<typename Input::Block> {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  Input input_;
  Comparator comparator_;
  MergeOptions options_;
  SharedMergeScheduler scheduler_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  // The splitters, `splitters_.size() + 1` is the number of chunks.
  std::vector<Key> splitters_;
  size_t numChunks_;
  size_t maxInFlight_;
  // NOTE: `blocks_` holds a raw pointer to `sink_`, so `sink_` has to be
  // declared (and therefore constructed) first.
  InOrderBlockSink<Block> sink_;
  ad_utility::InputRangeTypeErased<Block> blocks_;
  // Set by the destructor and by a failing chunk, so that the running tasks
  // stop as soon as possible.
  std::atomic<bool> stopRequested_{false};

  // The bookkeeping of the chunk tasks. NOTE: Two distinct counters are
  // required. `numChunksInFlight_` drives the dispatch policy and is decreased
  // as soon as a chunk is complete, such that the completing worker can
  // immediately dispatch the next chunk. `numActiveTasks_` in contrast counts
  // the tasks that may still touch this object at all (a task stays active
  // while it dispatches its successor) and is what the destructor waits for.
  std::mutex mutex_;
  std::condition_variable allTasksFinished_;
  size_t nextChunkToDispatch_ = 0;
  size_t numChunksInFlight_ = 0;
  size_t numActiveTasks_ = 0;

 public:
  // ________________________________________________________________________
  ParallelMergeState(Input input, Comparator comparator, MergeOptions options,
                     SharedMergeScheduler scheduler,
                     ad_utility::SharedCancellationHandle cancellationHandle,
                     std::vector<Key> splitters, size_t maxInFlight)
      : input_{std::move(input)},
        comparator_{std::move(comparator)},
        options_{std::move(options)},
        scheduler_{std::move(scheduler)},
        cancellationHandle_{std::move(cancellationHandle)},
        splitters_{std::move(splitters)},
        numChunks_{splitters_.size() + 1},
        maxInFlight_{maxInFlight},
        sink_{options_.bufferedBlocksPerChunk},
        blocks_{sink_.blocks()} {
    // The dispatch policy is what makes the merge deadlock-free, see
    // `InOrderBlockSink`. In particular, more than one chunk has to be in
    // flight, because otherwise a single chunk could fill its buffer and then
    // block forever.
    AD_CORRECTNESS_CHECK(maxInFlight_ > 1);
    AD_CORRECTNESS_CHECK(maxInFlight_ <= scheduler_->maxParallelism());
    AD_CORRECTNESS_CHECK(maxInFlight_ <= numChunks_);
    sink_.setNumChunks(numChunks_);
  }

  // The tasks refer to this object, so it must neither be copied nor moved.
  ParallelMergeState(const ParallelMergeState&) = delete;
  ParallelMergeState& operator=(const ParallelMergeState&) = delete;
  ParallelMergeState(ParallelMergeState&&) = delete;
  ParallelMergeState& operator=(ParallelMergeState&&) = delete;

  // Abort the sink (which unblocks all producers) and then wait for all
  // in-flight tasks, before any of the members they refer to is destroyed.
  ~ParallelMergeState() override {
    stopRequested_.store(true);
    sink_.abort();
    std::unique_lock lock{mutex_};
    allTasksFinished_.wait(lock, [this] { return numActiveTasks_ == 0; });
  }

  // Return the next block in the global order, or `std::nullopt` if the merge
  // is exhausted. Rethrow an exception from one of the chunk tasks.
  std::optional<Block> get() override {
    dispatchChunks();
    return blocks_.get();
  }

 private:
  // Dispatch chunks in strictly increasing order of their index, until
  // `maxInFlight_` of them are in flight. This is called from the consuming
  // thread (at the beginning of every `get()`) as well as from a worker that
  // has just completed a chunk. The strictly increasing order is preserved in
  // either case, because `nextChunkToDispatch_` is guarded by `mutex_`.
  void dispatchChunks() {
    while (true) {
      std::unique_lock lock{mutex_};
      if (stopRequested_.load() || nextChunkToDispatch_ >= numChunks_ ||
          numChunksInFlight_ >= maxInFlight_) {
        return;
      }
      size_t chunkIndex = nextChunkToDispatch_;
      ++nextChunkToDispatch_;
      ++numChunksInFlight_;
      ++numActiveTasks_;
      // NOTE: The lock must not be held while scheduling, because a scheduler
      // may run the task inline.
      lock.unlock();
      try {
        scheduler_->schedule([this, chunkIndex] { runChunk(chunkIndex); });
      } catch (...) {
        // The task was never started, so undo the bookkeeping. Otherwise the
        // destructor would wait for a task that does not exist. As always, the
        // notification has to happen while the lock is still held.
        std::unique_lock undoLock{mutex_};
        --numChunksInFlight_;
        --numActiveTasks_;
        allTasksFinished_.notify_all();
        undoLock.unlock();
        throw;
      }
    }
  }

  // Merge a single chunk and push its blocks to the sink. This never throws
  // (`TaskQueue::push` would call `std::terminate`), all exceptions are
  // forwarded to the consumer via `InOrderBlockSink::pushException`.
  void runChunk(size_t chunkIndex) noexcept {
    ad_utility::terminateIfThrows(
        [this, chunkIndex] {
          try {
            std::optional<Key> lo;
            std::optional<Key> hi;
            if (chunkIndex > 0) {
              lo = splitters_.at(chunkIndex - 1);
            }
            if (chunkIndex + 1 < numChunks_) {
              hi = splitters_.at(chunkIndex);
            }
            ChunkMerger<moveElements, Input, Comparator> merger{
                input_,        comparator_,   options_,
                std::move(lo), std::move(hi), cancellationHandle_};
            while (!stopRequested_.load()) {
              auto block = merger.nextBlock();
              if (!block.has_value()) {
                break;
              }
              sink_(chunkIndex, std::move(block.value()));
            }
          } catch (...) {
            stopRequested_.store(true);
            sink_.pushException(std::current_exception());
          }
          // NOTE: `finishChunk` has to be called on every path, because the
          // consumer would hang otherwise.
          sink_.finishChunk(chunkIndex);
          {
            std::unique_lock lock{mutex_};
            AD_CORRECTNESS_CHECK(numChunksInFlight_ > 0);
            --numChunksInFlight_;
          }
          // Top up the pipeline from the completing worker. This is required
          // for correctness and not only for throughput: a chunk that yields no
          // output block at all leaves the consumer parked inside
          // `InOrderBlockSink::popNextBlock`, where it cannot dispatch
          // anything, so the successors of that chunk would never be
          // dispatched and the merge would hang.
          //
          // NOTE: This relies on `MergeScheduler::schedule` not running the
          // task in the calling thread, because otherwise the dispatching would
          // recurse once per chunk. The constructor checks that the scheduler
          // offers a parallelism greater than one, which excludes the only
          // inline scheduler that exists (`InlineMergeScheduler`).
          try {
            dispatchChunks();
          } catch (...) {
            stopRequested_.store(true);
            sink_.pushException(std::current_exception());
          }
          std::unique_lock lock{mutex_};
          AD_CORRECTNESS_CHECK(numActiveTasks_ > 0);
          --numActiveTasks_;
          // NOTE: The notification has to happen while the lock is still
          // held. Otherwise the waiting destructor could return and destroy
          // `allTasksFinished_` while this thread is still inside
          // `notify_all`, which is a use-after-free.
          allTasksFinished_.notify_all();
        },
        "Merging a single chunk in `parallelBlockMergeToRange` failed.");
  }
};
}  // namespace detail

// ___________________________________________________________________________
// The public entry point.
// ___________________________________________________________________________

// Merge the presorted runs of `input` according to `comparator` and return the
// merged elements as a lazy range of blocks in globally sorted order. The
// result is deterministic for a fixed configuration (the same `options` and the
// same `MergeScheduler::maxParallelism()` always yield the same order, also for
// elements that the `comparator` considers equal). Set
// `MergeOptions::stableTieBreaking` to additionally make the order of the tied
// elements independent of the number of chunks; see there for the cost.
//
// The `comparator` has to be able to compare two elements, two keys, as well as
// an element with a key (in both orders). If `moveElements` is `true`, then the
// elements are moved out of the input blocks.
//
// The merge is performed serially in the calling thread if the input is small
// (see `MergeOptions::serialNumRunsThreshold` and
// `MergeOptions::serialNumElementsThreshold`), if the `scheduler` offers no
// parallelism, or if the input cannot be split into more than one chunk.
//
// NOTE: The returned range owns everything that the concurrently running tasks
// refer to, so it is safe (and cheap) to destroy it before it is exhausted.
CPP_template(bool moveElements, typename Input,
             typename Comparator)(requires BlockedRunsInput<Input>) ad_utility::
    InputRangeTypeErased<typename Input::Block> parallelBlockMergeToRange(
        Input input, Comparator comparator, MergeOptions options = {},
        SharedMergeScheduler scheduler = defaultMergeScheduler(),
        ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
  using Block = typename Input::Block;
  using Key = typename Input::Key;
  using SerialState = detail::SerialMergeState<moveElements, Input, Comparator>;
  using ParallelState =
      detail::ParallelMergeState<moveElements, Input, Comparator>;
  using Result = ad_utility::InputRangeTypeErased<Block>;

  if (scheduler == nullptr) {
    scheduler = defaultMergeScheduler();
  }
  const size_t maxParallelism = scheduler->maxParallelism();

  bool isSerial =
      input.numRuns() <= options.serialNumRunsThreshold || maxParallelism <= 1;
  if (!isSerial) {
    isSerial =
        detail::totalNumElements(input) <= options.serialNumElementsThreshold;
  }

  std::vector<Key> splitters;
  size_t maxInFlight = 0;
  if (!isSerial) {
    splitters = computeSplitters(
        input, comparator, maxParallelism * options.targetChunksPerThread);
    size_t numChunks = splitters.size() + 1;
    size_t requested = options.maxInFlightChunks == 0
                           ? maxParallelism
                           : options.maxInFlightChunks;
    maxInFlight = std::min({requested, maxParallelism, numChunks});
    // A single chunk (or a single in-flight chunk) cannot be parallelized, and
    // would even deadlock, because the producer of the only chunk would block
    // on a full buffer.
    isSerial = maxInFlight <= 1;
  }

  if (isSerial) {
    return Result{std::make_unique<SerialState>(
        std::move(input), std::move(comparator), std::move(options),
        std::move(cancellationHandle))};
  }
  return Result{std::make_unique<ParallelState>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(scheduler), std::move(cancellationHandle), std::move(splitters),
      maxInFlight)};
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELBLOCKMERGE_H
