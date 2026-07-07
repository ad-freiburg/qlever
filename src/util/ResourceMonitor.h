// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RESOURCEMONITOR_H
#define QLEVER_SRC_UTIL_RESOURCEMONITOR_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>

#include "util/jthread.h"

namespace ad_utility {

namespace resource_monitor {

// Current resident set size (RSS) of this process in bytes.
std::optional<uint64_t> currentRssBytes();

// Total CPU time (user + system) used by this process so far, in seconds.
std::optional<double> cpuTimeSeconds();

}  // namespace resource_monitor

// Samples the RSS and CPU usage of this process on a background thread
// and appends one TSV row (`elapsed_s`, `timestamp_ms`, `rss`,
// `cpu_percent`) per interval; failed probes become empty cells. The
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
  // non-empty file) and start sampling. Throws if called more than
  // once, if the file cannot be opened, or if `interval` is not
  // positive.
  void start(const std::filesystem::path& path, Mode mode,
             std::chrono::milliseconds interval = std::chrono::seconds{1});

 private:
  // Body of the sampling thread.
  void runLoop(std::chrono::milliseconds interval);

  // Declaration order is load-bearing: `sampler_` must be destroyed
  // (i.e. joined) first, while the members it uses are still alive.
  std::ofstream stream_;
  std::atomic<bool> started_{false};
  std::mutex mutex_;
  std::condition_variable stopCondition_;
  bool stopRequested_ = false;
  JThread sampler_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RESOURCEMONITOR_H
