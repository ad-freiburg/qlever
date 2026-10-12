//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
#define QLEVER_SRC_UTIL_PARALLELEXECUTOR_H

#include <algorithm>
#include <atomic>
#include <boost/asio/post.hpp>
#include <condition_variable>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <range/v3/algorithm/fold_left.hpp>
#include <thread>
#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/TaskQueue.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

namespace ad_utility {
// Run the given tasks in parallel and wait for their completion. This function
// will spawn a new thread for each task. If `R` (the result type of the tasks)
// is not `void`, the results are returned in a `std::vector<R>`, in the same
// order as the corresponding tasks. If one of the tasks throws an exception,
// this exception will be rethrown in the main thread, after all the other
// tasks have completed. If multiple tasks throw exceptions, only the first one
// will be rethrown.
template <typename R>
auto runTasksInParallel(std::vector<std::packaged_task<R()>>&& tasks)
    -> std::conditional_t<std::is_void_v<R>, void, std::vector<R>> {
  std::vector<std::future<R>> futures;
  futures.reserve(tasks.size());
  std::vector<JThread> threads;
  threads.reserve(tasks.size());
  for (auto& task : tasks) {
    futures.push_back(task.get_future());
    threads.push_back(JThread{std::move(task)});
  }
  if constexpr (std::is_void_v<R>) {
    // Wait for completion.
    for (auto& future : futures) {
      future.get();
    }
  } else {
    std::vector<R> results;
    results.reserve(futures.size());
    for (auto& future : futures) {
      results.push_back(future.get());
    }
    return results;
  }
}

// `detail::FirstArgument`/`FirstArgumentT` (the decayed type of the first
// argument of a callable) live in `util/TypeTraits.h`, as they are also used
// by `ad_utility::visitIf`.

// Split the range `[0, numElements)` into consecutive chunks of `chunkSize`
// elements (the last chunk may be smaller), fold all of them into a single
// `Result`, and return it. Each of `numThreads` worker threads (`0`, the
// default, means the number of logical cores) keeps one `Result` of its own
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

namespace detail {
// The state that the caller of `runIndexedTasksOnExecutor` (see below) shares
// with the helpers that it posts to the executor.
struct RunIndexedTasksState {
  // The index of the next task that has not been claimed yet.
  std::atomic<size_t> nextTaskIdx_ = 0;
  // All the members below are protected by the `mutex_`.
  std::mutex mutex_;
  std::condition_variable helperFinished_;
  // The number of helpers that are currently running tasks.
  size_t numActiveHelpers_ = 0;
  // Set by the caller as soon as it has stopped running tasks itself. A helper
  // that starts after that returns right away.
  bool isClosed_ = false;
  std::exception_ptr firstException_;
};

// Claim and run tasks of the `state` until there are none left. Never throw:
// after a task has thrown, store the exception in the `state` and stop handing
// out the remaining tasks.
template <typename RunTask>
void claimAndRunIndexedTasks(RunIndexedTasksState& state, size_t numTasks,
                             const RunTask& runTask) {
  try {
    while (true) {
      size_t taskIdx = state.nextTaskIdx_.fetch_add(1);
      if (taskIdx >= numTasks) {
        return;
      }
      runTask(taskIdx);
    }
  } catch (...) {
    state.nextTaskIdx_.store(numTasks);
    std::lock_guard lock{state.mutex_};
    if (!state.firstException_) {
      state.firstException_ = std::current_exception();
    }
  }
}
}  // namespace detail

// Run the `numTasks` tasks `runTask(0), ..., runTask(numTasks - 1)` on the
// `executor`, which is expected to have `numThreads` threads (this only bounds
// the number of helpers that are posted to it, see below). Return only when
// all of the tasks are done, rethrowing the first exception that any of them
// has thrown. After a task has thrown, the tasks that have not been started
// yet are skipped. The tasks are started in the order of their indices. Note
// that `runTask` is called concurrently from several threads.
//
// NOTE: The calling thread doesn't only wait for the executor, but also runs
// tasks itself, and it waits only for the helpers on the executor that have
// actually started. That way this function completes even if none of the
// threads of the executor ever becomes available, so that (unlike the blocking
// functions of `TaskQueueOnExecutor`) it can safely be called from a thread
// that the threads of the executor are (indirectly) waiting for, and from the
// threads of the executor themselves.
template <typename RunTask>
void runIndexedTasksOnExecutor(const ql::any_io_executor& executor,
                               size_t numThreads, size_t numTasks,
                               const RunTask& runTask) {
  if (numTasks == 0) {
    return;
  }
  // NOTE: The `state` is shared with the helpers, because a helper may only be
  // started after this function has returned. Such a helper then sees that the
  // `state` is closed and returns without touching the `runTask`.
  auto state = std::make_shared<detail::RunIndexedTasksState>();
  auto helper = [state, numTasks, &runTask]() {
    {
      std::lock_guard lock{state->mutex_};
      if (state->isClosed_) {
        return;
      }
      ++state->numActiveHelpers_;
    }
    detail::claimAndRunIndexedTasks(*state, numTasks, runTask);
    {
      std::lock_guard lock{state->mutex_};
      --state->numActiveHelpers_;
    }
    state->helperFinished_.notify_all();
  };
  size_t numHelpers = std::min(numTasks, std::max(numThreads, size_t{1})) - 1;
  for ([[maybe_unused]] size_t i : ql::views::iota(size_t{0}, numHelpers)) {
    boost::asio::post(executor, helper);
  }
  detail::claimAndRunIndexedTasks(*state, numTasks, runTask);

  // All the tasks have been claimed, so wait for the helpers that are still
  // running one of them, and keep all the other ones from starting.
  std::unique_lock lock{state->mutex_};
  state->isClosed_ = true;
  state->helperFinished_.wait(
      lock, [&state]() { return state->numActiveHelpers_ == 0; });
  if (state->firstException_) {
    std::rethrow_exception(state->firstException_);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PARALLELEXECUTOR_H
