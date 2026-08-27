// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_H

#include <absl/functional/any_invocable.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"
#include "util/TaskQueue.h"

// An STXXL-style parallel k-way merge. The input is a set of presorted runs,
// each of which is split into blocks. The blocks may live compressed on disk;
// only their element count and their first and last key have to be available
// without I/O. This allows to split the global output range into chunks that
// can be merged completely independently and therefore in parallel.
//
// This header only contains the *policies* (input, scheduler, and sink) as well
// as the options of the merge. The merge algorithm itself is built on top of
// them.
namespace ad_utility::parallelBlockMerge {

// The default number of elements in a single output block of the merge.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE = 100'000;

// The default upper bound for the memory that a single output block of the
// merge may occupy. An output block is finished as soon as either this limit or
// `DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE` is reached.
constexpr inline MemorySize DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY =
    MemorySize::megabytes(16);

// The default number of chunks that are created per available thread. Values
// greater than one lead to a finer granularity, which in turn improves the load
// balancing if the individual chunks require different amounts of work.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD = 4;

// The default number of input elements below which the merge is performed
// serially. For small inputs the overhead of setting up the parallel merge
// dominates the actual merging.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD =
    100'000;

// ___________________________________________________________________________
// The input policy.
// ___________________________________________________________________________

// The requirements of the `BlockedRunsInput` concept below, see there for the
// documentation.
template <typename T>
CPP_requires(
    BlockedRunsInput_,
    requires(const T& t, size_t run, size_t block, typename T::Block& out,
             ql::ranges::range_reference_t<typename T::Block> el)(
        // The number of presorted runs.
        ql::concepts::convertible_to<decltype(t.numRuns()), size_t>,
        // The number of blocks of a single run.
        ql::concepts::convertible_to<decltype(t.numBlocks(run)), size_t>,
        // The number of elements in a single block, available without I/O.
        ql::concepts::convertible_to<decltype(t.numElementsInBlock(run, block)),
                                     size_t>,
        // The first and the last key of a single block, available without I/O.
        ql::concepts::same_as<decltype(t.firstKey(run, block)),
                              const typename T::Key&>,
        ql::concepts::same_as<decltype(t.lastKey(run, block)),
                              const typename T::Key&>,
        // Materialize a single block. This is the only operation that performs
        // I/O, and it has to be thread-safe.
        ql::concepts::same_as<decltype(t.readBlock(run, block)),
                              typename T::Block>,
        // Create an empty block, and append a single element to a block.
        ql::concepts::same_as<decltype(t.makeEmptyBlock()), typename T::Block>,
        t.appendToBlock(out, el),
        // The memory that a single element occupies.
        ql::concepts::convertible_to<decltype(t.memorySizeOfElement(el)),
                                     MemorySize>));

namespace detail {
// Extract `T::Block` if it exists, and `void` otherwise. This is needed so that
// the `BlockedRunsInput` concept below is a hard `false` (instead of a
// compilation error) for types without a nested `Block` type. Note that this
// requires a class template (and not simply a `CPP_requires` clause), because
// in C++17 mode the concepts are emulated via variable templates, for which
// SFINAE does not apply to the template arguments.
template <typename T, typename = void>
struct BlockTypeOrVoid {
  using type = void;
};

// ___________________________________________________________________________
template <typename T>
struct BlockTypeOrVoid<T, std::void_t<typename T::Block>> {
  using type = typename T::Block;
};

// ___________________________________________________________________________
template <typename T>
using BlockTypeOrVoidT = typename BlockTypeOrVoid<T>::type;
}  // namespace detail

// The input policy of the parallel merge. It abstracts a set of presorted runs
// (`numRuns()` many), each of which is split into blocks (`numBlocks(run)` many
// for the run with the given index). The elements of the blocks are ordered
// according to a key of type `T::Key`, and the concatenation of all blocks of a
// single run is sorted with respect to that key.
//
// The crucial property of this policy is that the number of elements
// (`numElementsInBlock`) as well as the first and the last key
// (`firstKey`/`lastKey`) of every block are available *without* performing any
// I/O. They typically come from cheap in-memory metadata. It is exactly this
// property that allows the blocks themselves to live compressed on disk: the
// merge can compute the boundaries of the independent chunks from the metadata
// alone and only then read (via the thread-safe `readBlock`) those blocks that
// a given chunk actually needs.
//
// The remaining member functions describe how the *output* blocks are built:
// `makeEmptyBlock()` creates a fresh (empty) block, `appendToBlock(block, el)`
// appends a single element to it, and `memorySizeOfElement(el)` reports how
// much memory that element occupies, so that the memory limit of an output
// block can be honored.
//
// All member functions must be `const` and thread-safe, because the merge calls
// them concurrently from all worker threads.
template <typename T>
CPP_concept BlockedRunsInput =
    ql::ranges::random_access_range<detail::BlockTypeOrVoidT<T>> &&
    CPP_requires_ref(BlockedRunsInput_, T);

// ___________________________________________________________________________
// The scheduler policy.
// ___________________________________________________________________________

// The runtime interface via which the parallel merge obtains its parallelism.
// It is deliberately a runtime (and not a compile-time) policy, so that the
// merge does not have to be templated on the way its tasks are executed and so
// that a scheduler can be shared between several concurrent merges.
class MergeScheduler {
 public:
  virtual ~MergeScheduler() = default;

  // Schedule the `task` for execution. The task may run in any thread
  // (including the calling one) and at any time after this call.
  //
  // IMPORTANT: The `task` must NEVER throw an exception. The implementations
  // below forward to `TaskQueue::push`, which calls `std::terminate` if the
  // task throws. It is the responsibility of the *core* of the merge to wrap
  // every task body in a `try`/`catch` and to forward any exception to the sink
  // via `pushException`.
  virtual void schedule(absl::AnyInvocable<void()> task) = 0;

  // Return the number of tasks that can run concurrently. The core uses this to
  // bound the number of chunks that are in flight at the same time, which is
  // essential for the deadlock-freedom of `InOrderBlockSink` (see there).
  virtual size_t maxParallelism() const = 0;
};

// The natural way of passing a scheduler around, because a single scheduler is
// typically shared by several merges.
using SharedMergeScheduler = std::shared_ptr<MergeScheduler>;

// A `MergeScheduler` that owns a `TaskQueue` with a dedicated thread pool.
class TaskQueueMergeScheduler : public MergeScheduler {
 private:
  // NOTE: The order of these members matters, `numThreads_` is used to
  // initialize `queue_`.
  size_t numThreads_;
  TaskQueue<false> queue_;

 public:
  // Construct from the number of threads and the name of the queue (the latter
  // is only used for logging). A value of `0` for `numThreads` means "as many
  // threads as the hardware offers".
  //
  // NOTE: The maximal size of the queue is `2 * numThreads`, such that
  // `schedule` never blocks as long as at most `numThreads` tasks are in
  // flight.
  explicit TaskQueueMergeScheduler(size_t numThreads = 0,
                                   std::string name = "parallelBlockMerge")
      : numThreads_{numThreads == 0
                        ? std::max<size_t>(1,
                                           std::thread::hardware_concurrency())
                        : numThreads},
        queue_{2 * numThreads_, numThreads_, std::move(name)} {}

  // See `MergeScheduler::schedule`. The `task` must never throw.
  void schedule(absl::AnyInvocable<void()> task) override {
    queue_.push(std::move(task));
  }

  // ________________________________________________________________________
  size_t maxParallelism() const override { return numThreads_; }
};

// A `MergeScheduler` that uses an externally owned `TaskQueue`. Use this to
// share a single thread pool between the merge and other tasks.
class BorrowedTaskQueueMergeScheduler : public MergeScheduler {
 private:
  TaskQueue<false>* queue_;
  size_t maxParallelism_;

 public:
  // Construct from the borrowed `queue` (which has to outlive this scheduler
  // and every merge that uses it) and the number of tasks that this scheduler
  // may keep in flight.
  //
  // NOTE: The maximal size of the borrowed queue has to be at least
  // `maxParallelism`, because otherwise `schedule` might block although the
  // scheduled tasks have not yet been started, which in turn could deadlock the
  // merge.
  BorrowedTaskQueueMergeScheduler(TaskQueue<false>& queue,
                                  size_t maxParallelism)
      : queue_{&queue}, maxParallelism_{maxParallelism} {
    AD_CONTRACT_CHECK(maxParallelism > 0);
    AD_CONTRACT_CHECK(queue.maxQueueSize() >= maxParallelism);
  }

  // See `MergeScheduler::schedule`. The `task` must never throw.
  void schedule(absl::AnyInvocable<void()> task) override {
    queue_->push(std::move(task));
  }

  // ________________________________________________________________________
  size_t maxParallelism() const override { return maxParallelism_; }
};

// A `MergeScheduler` that runs every task immediately in the calling thread.
// Use this for serial fast paths and for deterministic tests.
class InlineMergeScheduler : public MergeScheduler {
 public:
  // Run the `task` immediately. The `task` must never throw (see
  // `MergeScheduler::schedule`); note that in contrast to the queue-based
  // schedulers an exception would propagate to the caller here, but the core
  // must not rely on that difference.
  void schedule(absl::AnyInvocable<void()> task) override { task(); }

  // ________________________________________________________________________
  size_t maxParallelism() const override { return 1; }
};

// Return the process-wide default scheduler, which is a
// `TaskQueueMergeScheduler` with one thread per hardware thread. It is created
// lazily on the first call and then shared by all merges that do not specify a
// scheduler of their own.
inline SharedMergeScheduler defaultMergeScheduler() {
  static const SharedMergeScheduler scheduler =
      std::make_shared<TaskQueueMergeScheduler>();
  return scheduler;
}

// ___________________________________________________________________________
// The options of the merge.
// ___________________________________________________________________________

// The tuning knobs of the parallel merge. All of them have sensible defaults,
// so that a caller typically only has to set the values it actually cares
// about.
struct MergeOptions {
  // Emit an output block as soon as it contains that many elements.
  size_t outputBlockSize = DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE;

  // Emit an output block as soon as it occupies that much memory (according to
  // `BlockedRunsInput::memorySizeOfElement`), even if it contains fewer than
  // `outputBlockSize` elements.
  MemorySize maxOutputBlockMemory = DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY;

  // Aim for that many independent chunks per thread. Larger values improve the
  // load balancing at the cost of a larger scheduling overhead.
  size_t targetChunksPerThread = DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD;

  // Never keep more than that many chunks in flight at the same time. The value
  // `0` means "as many as the scheduler offers", that is
  // `MergeScheduler::maxParallelism()`.
  size_t maxInFlightChunks = 0;

  // Merge serially (that is, without involving the scheduler at all) if the
  // input has at most that many runs.
  size_t serialNumRunsThreshold = 2;

  // Merge serially if the input has at most that many elements in total.
  size_t serialNumElementsThreshold =
      DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD;

  // Buffer at most that many finished output blocks per chunk before the
  // producing worker of that chunk is blocked. This is the back-pressure that
  // bounds the memory consumption of the merge.
  size_t bufferedBlocksPerChunk = 2;

  // Break ties (that is, elements that the comparator considers equal) by the
  // index of the run, which makes the tie order identical for every number of
  // chunks, at the cost of up to twice as many calls to the comparator. Leave
  // this off unless you actually need that property, because the result is
  // deterministic for a fixed configuration either way.
  bool stableTieBreaking = false;
};

// ___________________________________________________________________________
// The sink policy.
// ___________________________________________________________________________

// The requirements of the `BlockSink` concept below, see there for the
// documentation.
template <typename T, typename Block>
CPP_requires(BlockSink_, requires(T& sink, size_t numChunks, size_t chunkIndex,
                                  Block block, std::exception_ptr exception)(
                             sink.setNumChunks(numChunks),
                             sink(chunkIndex, std::move(block)),
                             sink.finishChunk(chunkIndex),
                             sink.pushException(std::move(exception))));

// The output policy of the parallel merge. The merge first announces the total
// number of chunks via `setNumChunks`, and then, for every chunk, calls
// `operator()` once per finished output block of that chunk and finally
// `finishChunk` exactly once. If a worker encounters an exception, it forwards
// it via `pushException`.
//
// `operator()`, `finishChunk`, and `pushException` are called concurrently from
// all worker threads and therefore all have to be thread-safe. `setNumChunks`
// is called exactly once, before any of the other functions.
template <typename T, typename Block>
CPP_concept BlockSink = CPP_requires_ref(BlockSink_, T, Block);

// A `BlockSink` that turns the concurrently produced blocks back into a single
// sequential range in which the blocks of chunk `0` come first, then those of
// chunk `1`, and so on. Within a chunk, the blocks appear in the order in which
// they were pushed. It buffers at most `maxBufferedBlocksPerChunk` blocks per
// chunk; a producer that exceeds this limit is blocked until the consumer has
// caught up.
//
// DEADLOCK-FREEDOM: The consumer always drains the lowest chunk that has not
// yet been fully consumed, so a producer of a *higher* chunk may well block on
// a full buffer. This is intended back-pressure and it never deadlocks, because
// the core dispatches the chunks in strictly increasing order of their index
// and keeps at most `MergeScheduler::maxParallelism()` of them in flight. Hence
// the lowest not-yet-finished chunk always owns a worker thread of its own and
// therefore always makes progress; once it is finished, the consumer moves on
// to the next chunk and thereby unblocks its producer, and so on.
//
// NOTE: The class is neither copyable nor movable, because the producers and
// the consumer refer to it by reference.
template <typename Block>
class InOrderBlockSink {
 private:
  // The state of a single chunk.
  struct PerChunk {
    std::queue<Block> blocks_{};
    bool finished_ = false;
  };

  size_t maxBufferedBlocksPerChunk_;
  std::mutex mutex_;
  std::condition_variable consumerCanProceed_;
  std::condition_variable producerCanProceed_;
  std::vector<PerChunk> chunks_;
  size_t numChunks_ = 0;
  // NOTE: This flag is required in addition to `numChunks_`, because the
  // consumer may already start to pull blocks before `setNumChunks` was called,
  // and because a merge with zero chunks is legal.
  bool numChunksIsSet_ = false;
  size_t nextChunkToRead_ = 0;
  bool aborted_ = false;
  std::exception_ptr exception_;

 public:
  using value_type = Block;

  // Construct from the maximal number of blocks that are buffered per chunk.
  // The value has to be at least one.
  explicit InOrderBlockSink(size_t maxBufferedBlocksPerChunk = 2)
      : maxBufferedBlocksPerChunk_{maxBufferedBlocksPerChunk} {
    AD_CONTRACT_CHECK(maxBufferedBlocksPerChunk > 0);
  }

  // The sink is referred to by reference from several threads, so it must not
  // be copied or moved.
  InOrderBlockSink(const InOrderBlockSink&) = delete;
  InOrderBlockSink& operator=(const InOrderBlockSink&) = delete;
  InOrderBlockSink(InOrderBlockSink&&) = delete;
  InOrderBlockSink& operator=(InOrderBlockSink&&) = delete;

  // Abort, so that no producer is left blocked when this sink goes away.
  ~InOrderBlockSink() { abort(); }

  // Announce the total number of chunks. Call this exactly once, and before any
  // call to `operator()` or `finishChunk`.
  void setNumChunks(size_t numChunks) {
    std::unique_lock lock{mutex_};
    AD_CONTRACT_CHECK(!numChunksIsSet_);
    numChunks_ = numChunks;
    numChunksIsSet_ = true;
    chunks_ = std::vector<PerChunk>(numChunks);
    lock.unlock();
    consumerCanProceed_.notify_all();
  }

  // Push a finished output `block` of the chunk with the given `chunkIndex`.
  // Block while the buffer of that chunk is full, unless the sink was aborted
  // or an exception was pushed, in which case the `block` is silently dropped.
  // Thread-safe.
  void operator()(size_t chunkIndex, Block block) {
    std::unique_lock lock{mutex_};
    AD_CONTRACT_CHECK(numChunksIsSet_);
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    auto& chunk = chunks_[chunkIndex];
    producerCanProceed_.wait(lock, [this, &chunk] {
      return chunk.blocks_.size() < maxBufferedBlocksPerChunk_ || aborted_ ||
             exception_ != nullptr;
    });
    if (aborted_ || exception_ != nullptr) {
      return;
    }
    chunk.blocks_.push(std::move(block));
    lock.unlock();
    consumerCanProceed_.notify_all();
  }

  // Announce that no further blocks will be pushed for the chunk with the given
  // `chunkIndex`. Call this exactly once per chunk. Thread-safe.
  void finishChunk(size_t chunkIndex) {
    std::unique_lock lock{mutex_};
    AD_CONTRACT_CHECK(numChunksIsSet_);
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    chunks_[chunkIndex].finished_ = true;
    lock.unlock();
    consumerCanProceed_.notify_all();
  }

  // Forward an `exception` to the consumer, which will rethrow it. Only the
  // first pushed exception is stored, all later ones are ignored. Pushing an
  // exception also unblocks all producers. Thread-safe.
  void pushException(std::exception_ptr exception) noexcept {
    ad_utility::terminateIfThrows(
        [this, &exception] {
          std::unique_lock lock{mutex_};
          if (exception_ == nullptr) {
            exception_ = std::move(exception);
          }
          lock.unlock();
          consumerCanProceed_.notify_all();
          producerCanProceed_.notify_all();
        },
        "Locking or unlocking a mutex in `InOrderBlockSink::pushException` "
        "failed.");
  }

  // Abort the sink from the consuming side. All blocked producers are unblocked
  // and all further blocks are dropped, and the consumer stops yielding blocks.
  // This is called by the destructor, so that a consumer that stops iterating
  // early (or that exits via an exception) never leaves a producer behind.
  // Thread-safe.
  void abort() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          std::unique_lock lock{mutex_};
          aborted_ = true;
          lock.unlock();
          consumerCanProceed_.notify_all();
          producerCanProceed_.notify_all();
        },
        "Locking or unlocking a mutex in `InOrderBlockSink::abort` failed.");
  }

  // Return a lazy range that yields all blocks of chunk `0`, then all blocks of
  // chunk `1`, and so on. Iterating over the range blocks until the next block
  // is available. If an exception was pushed, it is rethrown from the range.
  // Call this at most once, and only from a single (consuming) thread.
  ad_utility::InputRangeTypeErased<Block> blocks() {
    struct BlockRange : public ad_utility::InputRangeFromGet<Block> {
      InOrderBlockSink* sink_;
      explicit BlockRange(InOrderBlockSink* sink) : sink_{sink} {}
      std::optional<Block> get() override { return sink_->popNextBlock(); }
    };
    return ad_utility::InputRangeTypeErased<Block>{
        std::make_unique<BlockRange>(this)};
  }

 private:
  // Return the next block in the global order, or `std::nullopt` if all chunks
  // are exhausted or the sink was aborted. Rethrow a pushed exception. Block
  // until one of these conditions holds.
  std::optional<Block> popNextBlock() {
    std::unique_lock lock{mutex_};
    while (true) {
      if (exception_ != nullptr) {
        auto exception = exception_;
        lock.unlock();
        std::rethrow_exception(exception);
      }
      if (aborted_) {
        return std::nullopt;
      }
      if (numChunksIsSet_ && nextChunkToRead_ >= numChunks_) {
        return std::nullopt;
      }
      if (numChunksIsSet_) {
        auto& chunk = chunks_[nextChunkToRead_];
        if (!chunk.blocks_.empty()) {
          Block block = std::move(chunk.blocks_.front());
          chunk.blocks_.pop();
          lock.unlock();
          producerCanProceed_.notify_all();
          return block;
        }
        if (chunk.finished_) {
          ++nextChunkToRead_;
          continue;
        }
      }
      consumerCanProceed_.wait(lock);
    }
  }
};

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

// ___________________________________________________________________________
// An in-memory input policy.
// ___________________________________________________________________________

// Expose a set of sorted random-access ranges as runs of fixed-size virtual
// blocks. The first and last key of every virtual block are directly available
// from the underlying range, so no I/O and no stored metadata are needed. Use
// this to run the parallel merge on in-memory data, and in tests.
CPP_template(typename Range)(
    requires ql::ranges::random_access_range<Range>) class VectorRunsInput {
 public:
  using value_type = ql::ranges::range_value_t<Range>;
  using Key = value_type;
  using Block = std::vector<value_type>;

 private:
  std::vector<Range> runs_;
  size_t virtualBlockSize_;
  // If `true`, then `readBlock` moves the elements out of the underlying
  // ranges. This only has an effect if the elements of the ranges are mutable
  // (for example if `Range` is a `subrange` or a `span`), and it has to match
  // the `moveElements` argument of `parallelBlockMergeToRange`.
  bool moveElements_;

 public:
  // Construct from the `runs` (each of which has to be sorted) and the number
  // of elements in a single virtual block.
  explicit VectorRunsInput(std::vector<Range> runs, size_t virtualBlockSize,
                           bool moveElements = false)
      : runs_{std::move(runs)},
        virtualBlockSize_{virtualBlockSize},
        moveElements_{moveElements} {
    AD_CONTRACT_CHECK(virtualBlockSize > 0);
  }

  // ________________________________________________________________________
  size_t numRuns() const { return runs_.size(); }

  // ________________________________________________________________________
  size_t numBlocks(size_t run) const {
    size_t numElements = runSize(run);
    return (numElements + virtualBlockSize_ - 1) / virtualBlockSize_;
  }

  // ________________________________________________________________________
  size_t numElementsInBlock(size_t run, size_t block) const {
    size_t begin = block * virtualBlockSize_;
    return std::min(virtualBlockSize_, runSize(run) - begin);
  }

  // ________________________________________________________________________
  const Key& firstKey(size_t run, size_t block) const {
    return ql::ranges::begin(runs_[run])[block * virtualBlockSize_];
  }

  // ________________________________________________________________________
  const Key& lastKey(size_t run, size_t block) const {
    size_t begin = block * virtualBlockSize_;
    return ql::ranges::begin(
        runs_[run])[begin + numElementsInBlock(run, block) - 1];
  }

  // ________________________________________________________________________
  Block readBlock(size_t run, size_t block) const {
    size_t begin = block * virtualBlockSize_;
    size_t numElements = numElementsInBlock(run, block);
    Block result;
    result.reserve(numElements);
    auto it = ql::ranges::begin(runs_[run]) + begin;
    for (size_t i = 0; i < numElements; ++i, ++it) {
      if (moveElements_) {
        result.push_back(std::move(*it));
      } else {
        result.push_back(*it);
      }
    }
    return result;
  }

  // ________________________________________________________________________
  Block makeEmptyBlock() const { return Block{}; }

  // ________________________________________________________________________
  template <typename T>
  void appendToBlock(Block& block, T&& element) const {
    block.push_back(std::forward<T>(element));
  }

  // ________________________________________________________________________
  MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return MemorySize::bytes(sizeof(value_type));
  }

 private:
  // Return the number of elements of the run with the given index.
  size_t runSize(size_t run) const {
    return static_cast<size_t>(ql::ranges::distance(runs_[run]));
  }
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_H
