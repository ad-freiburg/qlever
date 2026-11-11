// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ExecutorStatistics.h"

#include <algorithm>
#include <ctime>

namespace ad_utility {

namespace {
// The scan blocks of the task that currently runs on this thread via an
// `ExecutorWithStatistics`, or `nullptr` if there is no such task.
thread_local constinit detail::ScanBlocksOfTask* scanBlocksOfCurrentTask =
    nullptr;
}  // namespace

// _____________________________________________________________________________
void SharedTaskStatistics::add(const TaskStatistics& statistics) {
  auto add = [](Counter& counter, uint64_t value) {
    if (value != 0) {
      counter.fetch_add(value, std::memory_order_relaxed);
    }
  };
  add(numTasks_, statistics.numTasks_);
  add(waitTime_, statistics.waitTime_);
  add(wallTime_, statistics.wallTime_);
  add(cpuTime_, statistics.cpuTime_);
  add(resultWaitTime_, statistics.resultWaitTime_);
  add(numScanBlocks_, statistics.numScanBlocks_);
  add(numScanBytes_, statistics.numScanBytes_);
  add(scanCpuTime_, statistics.scanCpuTime_);
  add(numScanThreadChanges_, statistics.numScanThreadChanges_);
}

// _____________________________________________________________________________
TaskStatistics SharedTaskStatistics::snapshot() const {
  auto get = [](const Counter& counter) {
    return counter.load(std::memory_order_relaxed);
  };
  TaskStatistics result;
  result.numTasks_ = get(numTasks_);
  result.waitTime_ = get(waitTime_);
  result.wallTime_ = get(wallTime_);
  result.cpuTime_ = get(cpuTime_);
  result.resultWaitTime_ = get(resultWaitTime_);
  result.numScanBlocks_ = get(numScanBlocks_);
  result.numScanBytes_ = get(numScanBytes_);
  result.scanCpuTime_ = get(scanCpuTime_);
  result.numScanThreadChanges_ = get(numScanThreadChanges_);
  return result;
}

// _____________________________________________________________________________
std::string_view toString(TaskClass taskClass) {
  switch (taskClass) {
    case TaskClass::Query:
      return "query";
    case TaskClass::IndexRebuild:
      return "index_rebuild";
    case TaskClass::Other:
      return "other";
  }
  AD_FAIL();
}

// _____________________________________________________________________________
TaskStatistics ThreadPoolStatistics::Snapshot::total() const {
  TaskStatistics result;
  for (const TaskStatistics& s : perClass_) {
    result.numTasks_ += s.numTasks_;
    result.waitTime_ += s.waitTime_;
    result.wallTime_ += s.wallTime_;
    result.cpuTime_ += s.cpuTime_;
    result.resultWaitTime_ += s.resultWaitTime_;
    result.numScanBlocks_ += s.numScanBlocks_;
    result.numScanBytes_ += s.numScanBytes_;
    result.scanCpuTime_ += s.scanCpuTime_;
    result.numScanThreadChanges_ += s.numScanThreadChanges_;
  }
  return result;
}

// _____________________________________________________________________________
uint64_t ThreadPoolStatistics::Snapshot::computingTime() const {
  return total().cpuTime_;
}

// _____________________________________________________________________________
uint64_t ThreadPoolStatistics::Snapshot::blockedTime() const {
  // NOTE: The two clocks are read one after the other, so for a task that
  // never leaves the CPU, the CPU time can be slightly larger than the wall
  // time.
  uint64_t result = 0;
  for (const TaskStatistics& s : perClass_) {
    result += s.wallTime_ - std::min(s.wallTime_, s.cpuTime_);
  }
  return result;
}

// _____________________________________________________________________________
ThreadPoolStatistics::ThreadPoolStatistics(size_t numThreads,
                                           Clock::time_point now)
    : numThreads_{numThreads}, lastChange_{now} {
  AD_CONTRACT_CHECK(numThreads > 0);
}

// _____________________________________________________________________________
void ThreadPoolStatistics::advanceTo(Clock::time_point now) const {
  // The events come from different threads, each of which reads the clock
  // before it takes the `mutex_`, so `now` can be slightly in the past.
  if (now <= lastChange_) {
    return;
  }
  size_t numIdle = numThreads_ - std::min(numThreads_, numRunning_);
  uint64_t elapsed = std::chrono::nanoseconds{now - lastChange_}.count();
  idleWhileWaitingTime_ += std::min(numIdle, numWaiting_) * elapsed;
  lastChange_ = now;
}

// _____________________________________________________________________________
void ThreadPoolStatistics::taskWasPosted(Clock::time_point now) {
  std::lock_guard lock{mutex_};
  advanceTo(now);
  ++numWaiting_;
}

// _____________________________________________________________________________
void ThreadPoolStatistics::taskWasStarted(Clock::time_point now) {
  std::lock_guard lock{mutex_};
  advanceTo(now);
  AD_CORRECTNESS_CHECK(numWaiting_ > 0);
  --numWaiting_;
  ++numRunning_;
}

// _____________________________________________________________________________
void ThreadPoolStatistics::taskHasFinished(Clock::time_point now) {
  std::lock_guard lock{mutex_};
  advanceTo(now);
  AD_CORRECTNESS_CHECK(numRunning_ > 0);
  --numRunning_;
}

// _____________________________________________________________________________
auto ThreadPoolStatistics::snapshot(Clock::time_point now) const -> Snapshot {
  Snapshot result;
  result.numThreads_ = numThreads_;
  for (size_t i = 0; i < numTaskClasses; ++i) {
    result.perClass_[i] = perClass_[i].snapshot();
  }
  std::lock_guard lock{mutex_};
  advanceTo(now);
  result.idleWhileWaitingTime_ = idleWhileWaitingTime_;
  return result;
}

// _____________________________________________________________________________
std::chrono::nanoseconds threadCpuTime() {
#if defined(__linux__) || defined(__APPLE__)
  timespec time;
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &time) != 0) {
    return std::chrono::nanoseconds{0};
  }
  return std::chrono::seconds{time.tv_sec} +
         std::chrono::nanoseconds{time.tv_nsec};
#else
  return std::chrono::nanoseconds{0};
#endif
}

// _____________________________________________________________________________
void recordScanBlockOfCurrentTask(
    size_t numBytes, std::atomic<std::thread::id>& threadOfPreviousBlock) {
  auto* scanBlocks = scanBlocksOfCurrentTask;
  if (scanBlocks == nullptr) {
    return;
  }
  ++scanBlocks->numBlocks_;
  scanBlocks->numBytes_ += numBytes;
  // NOTE: The blocks of a scan are read concurrently, so the "previous" block
  // is the one that was recorded last, which is not necessarily the previous
  // one in the order of the scan.
  auto thisThread = std::this_thread::get_id();
  auto previousThread = threadOfPreviousBlock.exchange(thisThread);
  if (previousThread != std::thread::id{} && previousThread != thisThread) {
    ++scanBlocks->numThreadChanges_;
  }
}

namespace detail {
// _____________________________________________________________________________
TaskMeasurement::TaskMeasurement(
    std::shared_ptr<ThreadPoolStatistics> pool, TaskClass taskClass,
    std::shared_ptr<SharedTaskStatistics> additional)
    : pool_{std::move(pool)},
      taskClass_{taskClass},
      additional_{std::move(additional)},
      timeOfPost_{ThreadPoolStatistics::Clock::now()} {
  pool_->taskWasPosted(timeOfPost_);
}

// _____________________________________________________________________________
TaskMeasurement::Running::Running(const TaskMeasurement* measurement)
    : measurement_{measurement},
      timeOfStart_{ThreadPoolStatistics::Clock::now()},
      cpuTimeAtStart_{threadCpuTime()},
      previous_{std::exchange(scanBlocksOfCurrentTask, &scanBlocks_)} {
  measurement_->pool_->taskWasStarted(timeOfStart_);
}

// _____________________________________________________________________________
TaskMeasurement::Running::~Running() {
  scanBlocksOfCurrentTask = previous_;
  auto cpuTime = threadCpuTime() - cpuTimeAtStart_;
  auto timeOfEnd = ThreadPoolStatistics::Clock::now();
  measurement_->pool_->taskHasFinished(timeOfEnd);

  auto toCount = [](auto duration) {
    return static_cast<uint64_t>(
        std::max<int64_t>(0, std::chrono::nanoseconds{duration}.count()));
  };
  TaskStatistics statistics;
  statistics.numTasks_ = 1;
  statistics.waitTime_ = toCount(timeOfStart_ - measurement_->timeOfPost_);
  statistics.wallTime_ = toCount(timeOfEnd - timeOfStart_);
  statistics.cpuTime_ = toCount(cpuTime);
  statistics.numScanBlocks_ = scanBlocks_.numBlocks_;
  statistics.numScanBytes_ = scanBlocks_.numBytes_;
  statistics.numScanThreadChanges_ = scanBlocks_.numThreadChanges_;
  // The CPU time of a task counts as scan time if the task has read a block.
  statistics.scanCpuTime_ =
      scanBlocks_.numBlocks_ > 0 ? statistics.cpuTime_ : 0;

  measurement_->pool_->ofClass(measurement_->taskClass_).add(statistics);
  if (measurement_->additional_ != nullptr) {
    measurement_->additional_->add(statistics);
  }
}
}  // namespace detail

// _____________________________________________________________________________
qlever::Executor makeExecutorWithStatistics(
    qlever::Executor executor, std::shared_ptr<ThreadPoolStatistics> pool) {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  return ExecutorWithStatistics{std::move(executor), std::move(pool),
                                TaskClass::Other};
#else
  AD_CONTRACT_CHECK(static_cast<bool>(executor));
  AD_CONTRACT_CHECK(pool != nullptr);
  return executor;
#endif
}

// _____________________________________________________________________________
void addResultWaitTime(const qlever::Executor& executor,
                       std::chrono::nanoseconds time) {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  if (const auto* withStatistics = executor.target<ExecutorWithStatistics>()) {
    withStatistics->addResultWaitTime(time);
  }
#else
  (void)executor;
  (void)time;
#endif
}

}  // namespace ad_utility
