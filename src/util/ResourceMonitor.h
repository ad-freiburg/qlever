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

// The columns of the resource-usage TSV. `rss` is the resident set size of
// the process in bytes; `cpu_percent` is its CPU usage as a percentage of
// one core; `io_stall_percent` is the percentage of wall time in which at
// least one task (system-wide) was stalled on I/O; `read_bytes` and
// `write_bytes` are the cumulative bytes this process has read from and
// written to the storage layer; `cached_bytes` is the current size of the
// OS page cache (system-wide). The three I/O columns and `cached_bytes` are
// only available on Linux and stay empty elsewhere.
constexpr std::string_view tsvHeader =
    "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tio_stall_percent"
    "\tread_bytes\twrite_bytes\tcached_bytes";

// Cumulative I/O counters of this process: bytes actually read from and
// written to the storage layer (not the page cache).
struct IoBytes {
  uint64_t readBytes_;
  uint64_t writeBytes_;
};

// Current resident set size (RSS) of this process in bytes.
std::optional<uint64_t> currentRssBytes();

#if defined(__linux__)
// RSS in bytes from `/proc/self/statm` stream (its 2nd field is the
// resident size in pages), or `std::nullopt` if the content is malformed.
std::optional<uint64_t> rssBytesFromStatm(std::istream& statm);
#endif

// Total CPU time (user + system) used by this process so far, in seconds.
std::optional<double> cpuTimeSeconds();

// Cumulative time (in seconds) in which at least one task, system-wide, was
// stalled waiting for I/O, from the pressure stall information (PSI). Linux
// only (and only if the kernel has PSI enabled); `std::nullopt` otherwise.
std::optional<double> ioStallSeconds();

// Cumulative storage-layer I/O of this process. Linux only.
std::optional<IoBytes> currentIoBytes();

// Current size of the OS page cache in bytes (system-wide). Linux only.
std::optional<uint64_t> currentCachedBytes();

#if defined(__linux__)
// The parsers behind the three readers above, on streams with the format of
// `/proc/pressure/io`, `/proc/self/io`, and `/proc/meminfo` respectively;
// `std::nullopt` if the content is malformed.
std::optional<double> ioStallSecondsFromPressure(std::istream& pressure);
std::optional<IoBytes> ioBytesFromProcIo(std::istream& io);
std::optional<uint64_t> cachedBytesFromMeminfo(std::istream& meminfo);
#endif

// Turns successive cumulative time readings (CPU seconds, or I/O stall
// seconds) into a usage percentage over the elapsed wall time. Stateful:
// each `update` is the baseline for the next.
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

// One TSV row (in the order of `tsvHeader`); a missing reading becomes an
// empty cell (`ioBytes` covers the two cells `read_bytes` and `write_bytes`).
std::string formatTsvRow(double elapsed, int64_t timestampMs,
                         std::optional<uint64_t> rss,
                         std::optional<double> cpuPercent,
                         std::optional<double> ioStallPercent,
                         std::optional<IoBytes> ioBytes,
                         std::optional<uint64_t> cachedBytes);

// The two OS readers, as swappable function objects (see
// `ResourceMonitor::setReadersForTesting`).
using RssReader = absl::AnyInvocable<std::optional<uint64_t>()>;
using CpuReader = absl::AnyInvocable<std::optional<double>()>;

}  // namespace resource_monitor

// Samples resource usage of this process (and some system-wide I/O and
// page-cache statistics, see `resource_monitor::tsvHeader`) on a background
// thread and appends one TSV row per interval; failed readings become empty
// cells. The destructor stops the sampling thread and closes the file.
class ResourceMonitor {
 public:
  // `Truncate` starts a fresh file per run (index builds); `Append`
  // accumulates rows across runs (server restarts). When appending to a file
  // whose header does not match the current format (older QLever versions
  // wrote fewer columns), the old file is renamed to `<path>.old` and a
  // fresh file is started, so that every file is internally consistent.
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
