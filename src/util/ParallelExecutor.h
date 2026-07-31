//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
#define QLEVER_SRC_UTIL_PARALLELEXECUTOR_H

#include <algorithm>
#include <future>
#include <thread>
#include <type_traits>
#include <vector>

#include "util/Exception.h"
#include "util/TaskQueue.h"
#include "util/jthread.h"

namespace ad_utility {
// Run the given tasks in parallel and wait for their completion. This function
// will spawn a new thread for each task. If one of the tasks throws an
// exception, this exception will be rethrown in the main thread. If multiple
// tasks throw exceptions, only the first one will be rethrown.
inline void runTasksInParallel(
    std::vector<std::packaged_task<void()>>&& tasks) {
  std::vector<std::future<void>> futures;
  futures.reserve(tasks.size());
  std::vector<JThread> threads;
  threads.reserve(tasks.size());
  for (auto& task : tasks) {
    futures.push_back(task.get_future());
    threads.push_back(JThread{std::move(task)});
  }
  // Wait for completion.
  for (auto& future : futures) {
    future.get();
  }
}

// Split the range `[0, numElements)` into consecutive chunks of `chunkSize`
// elements (the last chunk may be smaller), call `computeChunk(begin, end)` for
// each of the chunks, and return the merged results of those calls. The chunks
// are processed by a `TaskQueue` with one thread per hardware thread. The
// partial results are merged via `result.mergeWith(partialResult)`, in the
// order of the chunks. The result type (the return type of `computeChunk`) has
// to be default-constructible and movable, and to provide such a `mergeWith`
// function.
//
// NOTE: The `chunkSize` should be small enough that each thread typically
// processes several chunks (this balances the load, especially when the chunks
// require different amounts of work), but large enough that the per-chunk
// overhead (one partial result that has to be merged) is negligible. If the
// range holds at most `chunkSize` elements, then `computeChunk` is called
// exactly once, in the calling thread, and its result is returned directly. No
// thread is spawned in that case, and no merging takes place. This makes the
// function cheap for small inputs, where the cost of spawning threads would
// dominate the actual work. For an empty range, `computeChunk` is not called at
// all, and a default-constructed result is returned.
//
// If `computeChunk` throws for one of the chunks, that exception is rethrown in
// the calling thread once all chunks have run to completion. If multiple chunks
// throw, only the first one (in the order of the chunks) is rethrown.
template <typename ChunkFunction>
auto computeInParallelChunks(size_t numElements, size_t chunkSize,
                             const ChunkFunction& computeChunk) {
  using Result = std::invoke_result_t<const ChunkFunction&, size_t, size_t>;
  AD_CONTRACT_CHECK(chunkSize > 0);
  if (numElements == 0) {
    return Result{};
  }
  if (numElements <= chunkSize) {
    return computeChunk(0, numElements);
  }
  size_t numThreads = std::max(1u, std::thread::hardware_concurrency());
  std::vector<std::future<Result>> futures;
  // The number of chunks is typically much larger than the number of threads,
  // which is exactly what a `TaskQueue` is for. Note: Its destructor waits for
  // all the chunks to complete, also if the merging below throws.
  TaskQueue<false> queue{numThreads, numThreads, "computeInParallelChunks"};
  for (size_t begin = 0; begin < numElements; begin += chunkSize) {
    futures.push_back(
        queue.submit([&computeChunk, begin,
                      end = std::min(begin + chunkSize, numElements)]() {
          return computeChunk(begin, end);
        }));
  }
  // Merge the partial results while the remaining chunks are still being
  // computed. This way at most a few of them are alive at the same time. Note:
  // `future.get()` rethrows an exception that `computeChunk` has thrown.
  Result result{};
  for (auto& future : futures) {
    result.mergeWith(future.get());
  }
  return result;
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
