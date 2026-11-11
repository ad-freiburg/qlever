// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_EXECUTORSTATISTICS_H
#define QLEVER_SRC_UTIL_EXECUTORSTATISTICS_H

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>

#include "backports/asio.h"
#include "backports/three_way_comparison.h"
#include "util/Exception.h"
#include "util/Executor.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/asio/execution.hpp>
#include <boost/asio/prefer.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/require.hpp>
#include <concepts>
#include <functional>
#include <type_traits>

#include "util/Forward.h"
#endif

// This file measures how well the tasks on a thread pool are scheduled, in
// terms of three properties:
//
// 1. Locality: consecutive blocks of one index scan run on the same thread, so
//    that their data stays in the caches of one core. It is measured by the CPU
//    time per decompressed byte and by the number of thread changes.
// 2. Fairness: no task waits much longer for a thread than the others. It is
//    measured by the time between the posting and the start of a task.
// 3. Work conservation: no thread of the pool does nothing while there is work
//    for it. It is measured by the time that tasks hold a thread without using
//    a CPU (for example, because they wait for the disk), and by the time that
//    threads are idle while tasks are waiting.
//
// The measurements are sums of times and counts, from which the consumers
// (the query event log, the resource-usage log, and the metrics endpoint)
// compute the actual ratios.
namespace ad_utility {

// The sums over a set of tasks, for example, all tasks of one query. All times
// are in nanoseconds.
struct TaskStatistics {
  uint64_t numTasks_ = 0;
  // The time between the posting of a task and its start. This includes the
  // time during which a task is held back by a `ConcurrencyLimitedExecutor`.
  uint64_t waitTime_ = 0;
  // The time between the start and the end of a task, by the wall clock and by
  // the CPU clock of the thread. The difference is the time during which the
  // task held a thread without using a CPU.
  uint64_t wallTime_ = 0;
  uint64_t cpuTime_ = 0;
  // The time that consumers were blocked because the result of a task was not
  // yet available (see `AsyncTransformView::get`).
  uint64_t resultWaitTime_ = 0;
  // The blocks of index scans that were read by the tasks, their size after
  // decompression, the CPU time of the tasks that read them, and how many of
  // them were read by another thread than the previous block of the same scan.
  uint64_t numScanBlocks_ = 0;
  uint64_t numScanBytes_ = 0;
  uint64_t scanCpuTime_ = 0;
  uint64_t numScanThreadChanges_ = 0;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(TaskStatistics, numTasks_,
                                              waitTime_, wallTime_, cpuTime_,
                                              resultWaitTime_, numScanBlocks_,
                                              numScanBytes_, scanCpuTime_,
                                              numScanThreadChanges_)
};

// Convert a time from a `TaskStatistics` to milliseconds (rounded to full
// microseconds, so that it is written with at most three decimals) or seconds.
inline double nanosecondsToMilliseconds(uint64_t nanoseconds) {
  return std::round(static_cast<double>(nanoseconds) * 1e-3) * 1e-3;
}
inline double nanosecondsToSeconds(uint64_t nanoseconds) {
  return static_cast<double>(nanoseconds) * 1e-9;
}

// A `TaskStatistics` to which several threads can add concurrently.
class SharedTaskStatistics {
  using Counter = std::atomic<uint64_t>;
  Counter numTasks_ = 0;
  Counter waitTime_ = 0;
  Counter wallTime_ = 0;
  Counter cpuTime_ = 0;
  Counter resultWaitTime_ = 0;
  Counter numScanBlocks_ = 0;
  Counter numScanBytes_ = 0;
  Counter scanCpuTime_ = 0;
  Counter numScanThreadChanges_ = 0;

 public:
  // Add the `statistics` of one or more tasks.
  void add(const TaskStatistics& statistics);

  // Add time that a consumer waited for the result of a task.
  void addResultWaitTime(std::chrono::nanoseconds time) {
    resultWaitTime_.fetch_add(time.count(), std::memory_order_relaxed);
  }

  // Return the current sums.
  //
  // NOTE: The counters are read one after the other, so if tasks finish
  // concurrently, the sums are not from exactly the same point in time.
  TaskStatistics snapshot() const;
};

// The kinds of work that are distinguished in the statistics of a thread pool.
enum class TaskClass { Query, IndexRebuild, Other };
inline constexpr size_t numTaskClasses = 3;
std::string_view toString(TaskClass taskClass);

// The statistics of all the tasks that are run on one thread pool via an
// `ExecutorWithStatistics`.
class ThreadPoolStatistics {
 public:
  using Clock = std::chrono::steady_clock;

  // A consistent copy of the statistics.
  struct Snapshot {
    size_t numThreads_ = 0;
    std::array<TaskStatistics, numTaskClasses> perClass_;
    // The time (in nanoseconds, summed over the threads) during which a thread
    // was not running a task, although a task was waiting to be started.
    uint64_t idleWhileWaitingTime_ = 0;

    const TaskStatistics& ofClass(TaskClass taskClass) const {
      return perClass_.at(static_cast<size_t>(taskClass));
    }
    // The sum over all classes.
    TaskStatistics total() const;
    // The time (in nanoseconds, summed over the threads) during which a thread
    // was running a task and used a CPU, respectively did not use a CPU.
    uint64_t computingTime() const;
    uint64_t blockedTime() const;
  };

 private:
  size_t numThreads_;
  std::array<SharedTaskStatistics, numTaskClasses> perClass_;

  // The state from which the `idleWhileWaitingTime_` is computed. It changes
  // with every posted, started, and finished task, and all of it is protected
  // by the `mutex_`.
  mutable std::mutex mutex_;
  mutable size_t numWaiting_ = 0;
  mutable size_t numRunning_ = 0;
  mutable Clock::time_point lastChange_;
  mutable uint64_t idleWhileWaitingTime_ = 0;

 public:
  // Construct for a thread pool with the given number of threads, which has to
  // be at least one.
  explicit ThreadPoolStatistics(size_t numThreads,
                                Clock::time_point now = Clock::now());

  size_t numThreads() const { return numThreads_; }

  // The statistics of the tasks of the given class.
  SharedTaskStatistics& ofClass(TaskClass taskClass) {
    return perClass_.at(static_cast<size_t>(taskClass));
  }

  // Record that a task was posted, started, or has finished at time `now`.
  // Each task has to go through these three events in this order.
  void taskWasPosted(Clock::time_point now);
  void taskWasStarted(Clock::time_point now);
  void taskHasFinished(Clock::time_point now);

  // Return the statistics up to the time `now`.
  Snapshot snapshot(Clock::time_point now = Clock::now()) const;

 private:
  // Add the time since the last change of `numWaiting_` or `numRunning_` to the
  // `idleWhileWaitingTime_`. The `mutex_` has to be held.
  void advanceTo(Clock::time_point now) const;
};

// The CPU time that the calling thread has used so far, or zero on platforms
// that do not provide it.
std::chrono::nanoseconds threadCpuTime();

// Record that the task which currently runs on the calling thread via an
// `ExecutorWithStatistics` has read a block of an index scan with `numBytes`
// bytes after decompression. The `threadOfPreviousBlock` has to be shared by
// all blocks of the same scan and initialized with a default-constructed id.
// If the calling thread is not running such a task, nothing is recorded.
void recordScanBlockOfCurrentTask(
    size_t numBytes, std::atomic<std::thread::id>& threadOfPreviousBlock);

namespace detail {
// What the task that currently runs on a thread has recorded via
// `recordScanBlockOfCurrentTask`.
struct ScanBlocksOfTask {
  uint64_t numBlocks_ = 0;
  uint64_t numBytes_ = 0;
  uint64_t numThreadChanges_ = 0;
};

// Measures one task of an `ExecutorWithStatistics`. The constructor is called
// when the task is posted, and `run` when the task is run.
class TaskMeasurement {
  std::shared_ptr<ThreadPoolStatistics> pool_;
  TaskClass taskClass_;
  // Additional statistics to which the task is added, may be `nullptr`.
  std::shared_ptr<SharedTaskStatistics> additional_;
  ThreadPoolStatistics::Clock::time_point timeOfPost_;

 public:
  TaskMeasurement(std::shared_ptr<ThreadPoolStatistics> pool,
                  TaskClass taskClass,
                  std::shared_ptr<SharedTaskStatistics> additional);

  // Mark the start of the task and return a guard that marks its end when it
  // is destroyed. The task has to run on the calling thread while the guard
  // exists.
  class Running {
    const TaskMeasurement* measurement_;
    ThreadPoolStatistics::Clock::time_point timeOfStart_;
    std::chrono::nanoseconds cpuTimeAtStart_;
    ScanBlocksOfTask scanBlocks_;
    // The task that was running on this thread before, which is the case if an
    // executor runs a task inline from within another task.
    ScanBlocksOfTask* previous_;

   public:
    explicit Running(const TaskMeasurement* measurement);
    ~Running();
    Running(const Running&) = delete;
    Running& operator=(const Running&) = delete;
  };
};
}  // namespace detail

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
namespace detail {
// The properties that `ql::any_io_executor` supports for the respective
// operation (the same as for the `ConcurrencyLimitedExecutor`).
template <typename Property>
concept QueryablePropertyOfExecutor =
    boost::asio::can_query_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept RequirablePropertyOfExecutor =
    boost::asio::can_require_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept PreferablePropertyOfExecutor =
    boost::asio::can_prefer_v<const ql::any_io_executor&, Property>;
template <typename Function>
concept FunctionForExecutor =
    std::invocable<std::decay_t<Function>&> &&
    std::constructible_from<std::decay_t<Function>, Function> &&
    std::move_constructible<std::decay_t<Function>>;
}  // namespace detail

// An executor that runs its function objects on an underlying executor and
// adds the measurements of each of them to the statistics of a thread pool
// (under the given `TaskClass`) and optionally to additional statistics, for
// example those of a single query.
//
// NOTE: The wait time of a task starts when the task is passed to this
// executor. To include the time during which a task is held back by a
// `ConcurrencyLimitedExecutor`, this executor has to be the outer one of the
// two. Use `withStatisticsFor` below to get this right.
class ExecutorWithStatistics {
  ql::any_io_executor executor_;
  std::shared_ptr<ThreadPoolStatistics> pool_;
  TaskClass taskClass_;
  std::shared_ptr<SharedTaskStatistics> additional_;

 public:
  // The `executor` must not be empty and `pool` must not be `nullptr`, whereas
  // `additional` may be `nullptr`.
  ExecutorWithStatistics(
      ql::any_io_executor executor, std::shared_ptr<ThreadPoolStatistics> pool,
      TaskClass taskClass,
      std::shared_ptr<SharedTaskStatistics> additional = nullptr)
      : executor_{std::move(executor)},
        pool_{std::move(pool)},
        taskClass_{taskClass},
        additional_{std::move(additional)} {
    AD_CONTRACT_CHECK(static_cast<bool>(executor_));
    AD_CONTRACT_CHECK(pool_ != nullptr);
  }

  bool operator==(const ExecutorWithStatistics&) const = default;

  const ql::any_io_executor& underlyingExecutor() const { return executor_; }
  const std::shared_ptr<ThreadPoolStatistics>& pool() const { return pool_; }

  // Add time that a consumer waited for the result of a task of this executor.
  void addResultWaitTime(std::chrono::nanoseconds time) const {
    pool_->ofClass(taskClass_).addResultWaitTime(time);
    if (additional_ != nullptr) {
      additional_->addResultWaitTime(time);
    }
  }

  // All properties are forwarded to the underlying executor.
  template <detail::QueryablePropertyOfExecutor Property>
  decltype(auto) query(const Property& property) const {
    return boost::asio::query(executor_, property);
  }

  template <detail::RequirablePropertyOfExecutor Property>
  ExecutorWithStatistics require(const Property& property) const {
    return {boost::asio::require(executor_, property), pool_, taskClass_,
            additional_};
  }

  template <detail::PreferablePropertyOfExecutor Property>
  ExecutorWithStatistics prefer(const Property& property) const {
    return {boost::asio::prefer(executor_, property), pool_, taskClass_,
            additional_};
  }

  // Run `function` on the underlying executor and measure it.
  template <detail::FunctionForExecutor Function>
  void execute(Function&& function) const {
    executor_.execute([function = AD_FWD(function),
                       measurement = detail::TaskMeasurement{
                           pool_, taskClass_, additional_}]() mutable {
      detail::TaskMeasurement::Running running{&measurement};
      std::invoke(function);
    });
  }
};
#endif

// Return an executor that runs its tasks on `executor` and adds them to the
// statistics `pool` under the class `TaskClass::Other`.
//
// NOTE: In the reduced C++17 build, the `executor` is returned unchanged, so
// nothing is measured there (like for `makeConcurrencyLimitedExecutor`).
qlever::Executor makeExecutorWithStatistics(
    qlever::Executor executor, std::shared_ptr<ThreadPoolStatistics> pool);

// Add time that a consumer waited for the result of a task on the `executor` to
// the statistics of the `executor`. If it has none, nothing is recorded.
void addResultWaitTime(const qlever::Executor& executor,
                       std::chrono::nanoseconds time);

// Return `wrap(e)`, where `e` is the `executor` without its statistics. If the
// `executor` is an `ExecutorWithStatistics`, the tasks on the result are added
// to the same thread pool statistics, but under the given `taskClass` and with
// the given `additional` statistics. Otherwise, nothing is measured.
//
// NOTE: This is the way to combine the statistics with another wrapper like the
// `ConcurrencyLimitedExecutor`, such that each task is measured exactly once
// and its wait time includes the time spent in the other wrapper.
template <typename Wrap>
qlever::Executor withStatisticsFor(
    const qlever::Executor& executor, TaskClass taskClass,
    std::shared_ptr<SharedTaskStatistics> additional, const Wrap& wrap) {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  if (const auto* withStatistics = executor.target<ExecutorWithStatistics>()) {
    return ExecutorWithStatistics{wrap(withStatistics->underlyingExecutor()),
                                  withStatistics->pool(), taskClass,
                                  std::move(additional)};
  }
#else
  (void)taskClass;
  (void)additional;
#endif
  return wrap(executor);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_EXECUTORSTATISTICS_H
