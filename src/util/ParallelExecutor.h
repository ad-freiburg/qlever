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
#include <vector>

#include "util/Exception.h"
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

// Split the range `[0, numElements)` into consecutive chunks and call
// `body(chunkResult, begin, end)` for each of the chunks in parallel. The
// per-chunk results are then merged into `result` via
// `result.mergeWith(chunkResult)` (in the order of the chunks). `Result` has to
// be default-constructible and to provide such a `mergeWith` function.
//
// Each chunk holds at least `minChunkSize` elements, and there is at most one
// chunk per available hardware thread. In particular, if the range holds less
// than `2 * minChunkSize` elements, then `body` is called exactly once,
// directly on `result` and in the calling thread. No thread is spawned in that
// case, and no merging takes place. This makes the function cheap for small
// inputs, where the cost of spawning threads would dominate the actual work.
//
// If `body` throws for one of the chunks, that exception is rethrown in the
// calling thread after all chunks have run to completion (see
// `runTasksInParallel`).
template <typename Result, typename Body>
void computeInParallelChunks(size_t numElements, size_t minChunkSize,
                             Result& result, const Body& body) {
  AD_CONTRACT_CHECK(minChunkSize > 0);
  if (numElements == 0) {
    return;
  }
  auto ceilDiv = [](size_t a, size_t b) { return (a + b - 1) / b; };
  size_t maxNumChunks = std::max(1u, std::thread::hardware_concurrency());
  // Rounding down guarantees that each chunk holds at least `minChunkSize`
  // elements (the remainder is distributed over the chunks).
  size_t numChunks =
      std::min(maxNumChunks, std::max<size_t>(1, numElements / minChunkSize));
  size_t chunkSize = ceilDiv(numElements, numChunks);
  // Note: Rounding up the chunk size can make the last chunks superfluous (for
  // example, 10 elements in 6 chunks of 2), so the number of chunks has to be
  // recomputed. This guarantees that no chunk is empty, and it can only
  // decrease the number of chunks, so the minimal chunk size still holds.
  numChunks = ceilDiv(numElements, chunkSize);
  if (numChunks == 1) {
    body(result, 0, numElements);
    return;
  }
  std::vector<Result> chunkResults(numChunks);
  std::vector<std::packaged_task<void()>> tasks;
  tasks.reserve(numChunks);
  for (size_t i = 0; i < numChunks; ++i) {
    size_t begin = i * chunkSize;
    size_t end = std::min(begin + chunkSize, numElements);
    tasks.emplace_back([&chunkResults, &body, i, begin, end]() {
      body(chunkResults[i], begin, end);
    });
  }
  runTasksInParallel(std::move(tasks));
  for (const auto& chunkResult : chunkResults) {
    result.mergeWith(chunkResult);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
