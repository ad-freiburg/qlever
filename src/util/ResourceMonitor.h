// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RESOURCEMONITOR_H
#define QLEVER_SRC_UTIL_RESOURCEMONITOR_H

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <istream>
#include <mutex>
#include <optional>
#include <string>

#include "backports/filesystem.h"
#include "util/jthread.h"

namespace ad_utility {

namespace resource_monitor {

// Current resident set size (RSS) of this process in bytes.
std::optional<uint64_t> currentRssBytes();

#if defined(__linux__)
// RSS in bytes from `/proc/self/statm` stream (its 2nd field is the
// resident size in pages), or `std::nullopt` if the content is malformed.
std::optional<uint64_t> rssBytesFromStatm(std::istream& statm);
#endif

// Total CPU time (user + system) used by this process so far, in seconds.
std::optional<double> cpuTimeSeconds();

// Turns successive cumulative CPU-time readings into CPU usage as a percentage
// of one core. Stateful: each `update` is the baseline for the next.
class CpuPercentTracker {
 public:
  explicit CpuPercentTracker(std::optional<double> initialCpuSeconds)
      : lastCpuSeconds_{initialCpuSeconds} {}

  // `std::nullopt` when usage cannot be computed yet: no reading this tick,
  // no baseline, or no time elapsed since the baseline.
  std::optional<double> update(std::optional<double> cpuSeconds,
                               double elapsed);

 private:
  std::optional<double> lastCpuSeconds_;
  double lastElapsed_ = 0.0;
};

// One TSV row; a missing `rss` or `cpuPercent` becomes an empty cell.
std::string formatTsvRow(double elapsed, int64_t timestampMs,
                         std::optional<uint64_t> rss,
                         std::optional<double> cpuPercent);

// The two OS readers, as swappable function objects (see
// `ResourceMonitor::setReadersForTesting`).
using RssReader = absl::AnyInvocable<std::optional<uint64_t>()>;
using CpuReader = absl::AnyInvocable<std::optional<double>()>;

}  // namespace resource_monitor

// Samples the RSS and CPU usage of this process on a background thread
// and appends one TSV row (`elapsed_s`, `timestamp_ms`, `rss`,
// `cpu_percent`) per interval; failed readings become empty cells. The
// destructor stops the sampling thread and closes the file.
class ResourceMonitor {
 public:
  // `Truncate` starts a fresh file per run (index builds); `Append`
  // accumulates rows across runs (server restarts).
  enum class Mode { Truncate, Append };

  ResourceMonitor() = default;
  ~ResourceMonitor();

  ResourceMonitor(const ResourceMonitor&) = delete;
  ResourceMonitor& operator=(const ResourceMonitor&) = delete;

  // Open the TSV at `path` (header written unless appending to a
  // non-empty file) and start sampling. An unopenable file only warns
  // and disables monitoring. Throws if called more than once or if
  // `interval` is not positive.
  void start(const ql::filesystem::path& path, Mode mode,
             std::chrono::milliseconds interval);

  // Test-only: swap the OS readers before `start`, e.g. a throwing reader to
  // exercise the sampler's error handling.
  void setReadersForTesting(resource_monitor::RssReader rssReader,
                            resource_monitor::CpuReader cpuReader);

 private:
  // Body of the sampling thread.
  void runLoop(std::chrono::milliseconds interval);

  // Declaration order is load-bearing: `sampler_` must be destroyed
  // (i.e. joined) first, while the members it uses are still alive.
  std::ofstream stream_;
  resource_monitor::RssReader rssReader_ = resource_monitor::currentRssBytes;
  resource_monitor::CpuReader cpuReader_ = resource_monitor::cpuTimeSeconds;
  std::atomic<bool> started_{false};
  std::mutex mutex_;
  std::condition_variable stopCondition_;
  bool stopRequested_ = false;
  JThread sampler_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RESOURCEMONITOR_H
