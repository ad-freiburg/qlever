//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
#define QLEVER_SRC_UTIL_PARALLELEXECUTOR_H

#include <algorithm>
#include <atomic>
#include <future>
#include <range/v3/algorithm/fold_left.hpp>
#include <thread>
#include <type_traits>
#include <vector>

#include "backports/concepts.h"
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

namespace detail {
// The decayed type of the first argument of the callable `T`. Works for
// function pointers and for class types (in particular lambdas) with a single
// `operator()` that is neither overloaded nor templated. For all other types
// the member `type` is absent, so that this can be used in a SFINAE context.
template <typename T, typename = void>
struct FirstArgument {};

template <typename R, typename First, typename... Rest>
struct FirstArgument<R (*)(First, Rest...), void> {
  using type = std::decay_t<First>;
  static constexpr bool isLvalueReference = std::is_lvalue_reference_v<First>;
};

template <typename C, typename R, typename First, typename... Rest>
struct FirstArgument<R (C::*)(First, Rest...), void> {
  using type = std::decay_t<First>;
  static constexpr bool isLvalueReference = std::is_lvalue_reference_v<First>;
};

template <typename C, typename R, typename First, typename... Rest>
struct FirstArgument<R (C::*)(First, Rest...) const, void> {
  using type = std::decay_t<First>;
  static constexpr bool isLvalueReference = std::is_lvalue_reference_v<First>;
};

// For a class type (in particular a lambda), look at its `operator()`.
template <typename T>
struct FirstArgument<T, std::void_t<decltype(&T::operator())>>
    : FirstArgument<decltype(&T::operator()), void> {};

template <typename T>
using FirstArgumentT = typename FirstArgument<T>::type;
}  // namespace detail

// Split the range `[0, numElements)` into consecutive chunks of `chunkSize`
// elements (the last chunk may be smaller), fold all of them into a single
// `Result`, and return it. Each of `numThreads` worker threads (`0`, the
// default, means one thread per hardware thread) keeps one `Result` of its own
// and calls `computeChunk(result, begin, end)` for each of the chunks it gets.
// The chunks are handed out to the threads dynamically. At the end, the
// per-thread results are combined via `result.mergeWith(otherResult)`. The
// `Result` (which is deduced from the first parameter of `computeChunk`) has to
// be default-constructible and movable, and to provide such a `mergeWith`
// function. Note that `computeChunk` is called concurrently from several
// threads, so it has to be const-invocable (which in particular rules out a
// `mutable` lambda).
//
// NOTE: There is one `Result` per THREAD, not one per chunk. The `chunkSize`
// hence only controls the granularity of the load balancing, and not the number
// of merges: it should be small enough that each thread typically processes
// several chunks (which balances the load, especially when the chunks require
// different amounts of work), but large enough that the per-chunk overhead (one
// atomic increment) is negligible. Since the chunks are handed out dynamically,
// the order in which they are folded into the result is unspecified, and
// `mergeWith` therefore has to be associative and commutative.
//
// If the range holds at most `chunkSize` elements, then `computeChunk` is
// called exactly once, in the calling thread. No thread is spawned in that
// case, and no merging takes place. This makes the function cheap for small
// inputs, where the cost of spawning threads would dominate the actual work.
// For an empty range, `computeChunk` is not called at all, and a
// default-constructed result is returned.
//
// If `computeChunk` throws for one of the chunks, the thread that ran that
// chunk stops (the chunks it has not taken yet are then processed by the other
// threads), and the exception is rethrown in the calling thread once all
// threads have finished. If several threads throw, it is unspecified which of
// the exceptions is rethrown.
CPP_template(typename ChunkFunction)(
    requires ql::concepts::invocable<
        const ChunkFunction&, detail::FirstArgumentT<ChunkFunction>&, size_t,
        size_t>) auto computeInParallelChunks(size_t numElements,
                                              size_t chunkSize,
                                              const ChunkFunction& computeChunk,
                                              size_t numThreads = 0) {
  // The `Result` is the type of the first argument of `computeChunk`.
  using Result = detail::FirstArgumentT<ChunkFunction>;
  // Guard against a subtle bug: if `computeChunk` took its first parameter by
  // value, each call would write into a discarded copy and the final result
  // would be silently empty.
  static_assert(detail::FirstArgument<ChunkFunction>::isLvalueReference,
                "The first parameter of `computeChunk` (the result that it "
                "writes to) must be an lvalue reference");
  AD_CONTRACT_CHECK(chunkSize > 0);
  if (numElements == 0) {
    return Result{};
  }
  if (numElements <= chunkSize) {
    Result result{};
    computeChunk(result, 0, numElements);
    return result;
  }
  if (numThreads == 0) {
    numThreads = std::max(1u, std::thread::hardware_concurrency());
  }
  size_t numChunks = (numElements + chunkSize - 1) / chunkSize;
  // Threads beyond the number of chunks would have nothing to do.
  numThreads = std::min(numThreads, numChunks);
  // The index of the next chunk that has not been handed out yet. Taking the
  // chunks from this counter hands them out dynamically, which balances the
  // load also when the chunks require different amounts of work.
  std::atomic<size_t> nextChunk = 0;
  // Fold all the chunks that a single thread gets into one partial result.
  auto computeChunks = [&computeChunk, &nextChunk, numChunks, chunkSize,
                        numElements]() {
    Result partialResult{};
    for (size_t chunk = nextChunk++; chunk < numChunks; chunk = nextChunk++) {
      size_t begin = chunk * chunkSize;
      computeChunk(partialResult, begin,
                   std::min(begin + chunkSize, numElements));
    }
    return partialResult;
  };
  // Run exactly one of these tasks per thread, so that there is one partial
  // result per THREAD and not one per chunk. Note: The destructor of the
  // `TaskQueue` waits for all the tasks to complete, also if one of them
  // throws.
  TaskQueue<false> queue{numThreads, numThreads, "computeInParallelChunks"};
  std::vector<std::future<Result>> futures;
  futures.reserve(numThreads);
  for (size_t i = 0; i < numThreads; ++i) {
    futures.push_back(queue.submit(computeChunks));
  }
  // Merge the partial results into the first of them. Note: Using the first
  // one (instead of a default-constructed `Result`) as the initial value of the
  // fold lets the merging reuse the capacity it has already grown to, which
  // makes a big difference for `Result` types that are expensive to grow, like
  // a hash map. Note: `future.get()` rethrows an exception that `computeChunk`
  // has thrown.
  return ::ranges::fold_left(futures.begin() + 1, futures.end(),
                             futures.front().get(),
                             [](Result result, std::future<Result>& future) {
                               result.mergeWith(future.get());
                               return result;
                             });
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
