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
#include <string_view>

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

// Turns successive readings of a cumulative "seconds spent doing X" counter
// into a percentage of the elapsed wall time. Stateful: each `update` is the
// baseline for the next. Used for both `cpu_percent` and `io_stall_percent`.
class SecondsToPercentTracker {
 public:
  explicit SecondsToPercentTracker(std::optional<double> initialSeconds)
      : lastSeconds_{initialSeconds} {}

  // `std::nullopt` when usage cannot be computed yet: no reading this tick,
  // no baseline, or no time elapsed since the baseline.
  std::optional<double> update(std::optional<double> seconds, double elapsed);

 private:
  std::optional<double> lastSeconds_;
  double lastElapsed_ = 0.0;
};

// Cumulative bytes this process has read from and written to disk.
struct DiskIoBytes {
  uint64_t readBytes_;
  uint64_t writeBytes_;
};

// Cumulative disk bytes of this process, or `std::nullopt` if unavailable.
std::optional<DiskIoBytes> currentDiskIoBytes();

#if defined(__linux__)
// Disk bytes from a `/proc/self/io` stream, read by the `read_bytes:` and
// `write_bytes:` keys, never by position.
std::optional<DiskIoBytes> diskIoBytesFromProcIo(std::istream& procIo);
#endif

// Cumulative seconds during which at least one task was stalled on I/O, or
// `std::nullopt` if unavailable. Linux-only as not supported elsewhere.
std::optional<double> ioStallSeconds();

#if defined(__linux__)
// Stall seconds from a `/proc/pressure/io` stream: the `total=` microseconds
// on the `some` line, scaled to seconds.
std::optional<double> ioStallSecondsFromPressure(std::istream& pressure);
#endif

// One sampled row of the resource-usage log
struct Sample {
  double elapsedSeconds_;
  int64_t timestampMs_;
  std::optional<uint64_t> rssBytes_;
  std::optional<double> cpuPercent_;
  std::optional<DiskIoBytes> diskIoBytes_;
  std::optional<double> ioStallPercent_;
};

// The column names `formatTsvRow` produces values for, without the trailing
// newline. `start` also compares it against an existing file's first line to
// notice that the format has changed since that file was written.
inline constexpr std::string_view tsvHeader =
    "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tread_bytes\twrite_bytes\t"
    "io_stall_percent";

// One TSV row; a missing field becomes an empty cell.
std::string formatTsvRow(const Sample& sample);

// The OS readers, as swappable function objects (see
// `ResourceMonitor::setReadersForTesting`).
using RssReader = absl::AnyInvocable<std::optional<uint64_t>()>;
using CpuReader = absl::AnyInvocable<std::optional<double>()>;
using DiskIoReader = absl::AnyInvocable<std::optional<DiskIoBytes>()>;
using IoStallReader = absl::AnyInvocable<std::optional<double>()>;

// Which OS readers to override in `ResourceMonitor::setReadersForTesting`.
// An unset member leaves the real reader in place.
struct ReaderOverrides {
  RssReader rssReader_;
  CpuReader cpuReader_;
  DiskIoReader diskIoReader_;
  IoStallReader ioStallReader_;
};

}  // namespace resource_monitor

// Samples the RSS, CPU usage, and disk IO of this process, plus system-wide IO
// stall on a background thread and appends one TSV row (`elapsed_s`,
// `timestamp_ms`, `rss`, `cpu_percent`, `read_bytes`, `write_bytes`,
// `io_stall_percent`) per interval; failed readings become empty cells. The
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
  void setReadersForTesting(resource_monitor::ReaderOverrides readerOverrides);

 private:
  // Body of the sampling thread.
  void runLoop(std::chrono::milliseconds interval);

  // Declaration order is load-bearing: `sampler_` must be destroyed
  // (i.e. joined) first, while the members it uses are still alive.
  std::ofstream stream_;
  resource_monitor::RssReader rssReader_ = resource_monitor::currentRssBytes;
  resource_monitor::CpuReader cpuReader_ = resource_monitor::cpuTimeSeconds;
  resource_monitor::DiskIoReader diskIoReader_ =
      resource_monitor::currentDiskIoBytes;
  resource_monitor::IoStallReader ioStallReader_ =
      resource_monitor::ioStallSeconds;

  std::atomic<bool> started_{false};
  std::mutex mutex_;
  std::condition_variable stopCondition_;
  bool stopRequested_ = false;
  JThread sampler_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RESOURCEMONITOR_H
