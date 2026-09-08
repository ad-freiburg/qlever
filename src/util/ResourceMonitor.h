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
#include <memory>
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

// Turns successive readings of a cumulative counter into its rate of change per
// second of elapsed wall time. Stateful: each `update` is the baseline for the
// next. Used for `cpu_percent`, `io_stall_percent` and disk I/O rates.
class RateTracker {
 public:
  explicit RateTracker(std::optional<double> initialValue)
      : lastValue_{initialValue} {}

  // `std::nullopt` when usage cannot be computed yet: no reading this tick,
  // no baseline, or no time elapsed since the baseline.
  std::optional<double> update(std::optional<double> value, double elapsed);

 private:
  std::optional<double> lastValue_;
  double lastElapsed_ = 0.0;
};

// Holds the cumulative number of bytes that this process has read from and
// written to disk, as the OS reports them. A counter is empty if the OS did
// not report it.
struct DiskIoBytes {
  std::optional<uint64_t> numBytesRead_;
  std::optional<uint64_t> numBytesWritten_;
};

// Returns the cumulative disk bytes of this process. Both counters are empty
// on platforms where the OS does not expose them.
DiskIoBytes currentDiskIoBytes();

#if defined(__linux__)
// Parses the cumulative disk bytes of this process from a `/proc/self/io`
// stream. The values are taken from the lines with the `read_bytes:` and
// `write_bytes:` keys. A counter whose key does not appear in the stream stays
// empty.
DiskIoBytes diskIoBytesFromProcIo(std::istream& procIo);
#endif

// Returns the cumulative number of seconds during which at least one task was
// stalled on I/O. The result is `std::nullopt` on platforms other than Linux,
// which do not expose this figure.
std::optional<double> ioStallSeconds();

#if defined(__linux__)
// Parses the I/O stall seconds from a `/proc/pressure/io` stream. The value is
// the `total=` field on the `some` line, which the kernel reports in
// microseconds and this function converts to seconds.
std::optional<double> ioStallSecondsFromPressure(std::istream& pressure);
#endif

// One sampled row of the resource-usage log
struct Sample {
  double elapsedSeconds_;
  int64_t timestampMs_;
  std::optional<uint64_t> rssBytes_;
  std::optional<double> cpuPercent_;
  std::optional<double> bytesReadPerSecond_;
  std::optional<double> bytesWrittenPerSecond_;
  std::optional<double> ioStallPercent_;
  // The number of the index rebuild that was running when this row was
  // sampled. It is empty, and not 0, when no rebuild was running.
  std::optional<uint64_t> rebuildId_;
};

// The column names `formatTsvRow` produces values for, without the trailing
// newline. `start` also compares it against an existing file's first line to
// notice that the format has changed since that file was written.
inline constexpr std::string_view tsvHeader =
    "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tread_bytes_per_s\t"
    "write_bytes_per_s\tio_stall_percent\trebuild_id";

// One TSV row; a missing field becomes an empty cell.
std::string formatTsvRow(const Sample& sample);

// Moves the log at `path` aside if its first line is not `tsvHeader`, so that
// rows of two TSV formats do not land in one file. Returns true if a fresh
// header has to be written. If the move fails, the function still returns
// true, and the file keeps its old rows followed by a second header.
bool rotateLogIfHeaderOutdated(const ql::filesystem::path& path);

// The OS readers, as swappable function objects (see
// `ResourceMonitor::setReadersForTesting`).
using RssReader = absl::AnyInvocable<std::optional<uint64_t>()>;
using CpuReader = absl::AnyInvocable<std::optional<double>()>;
using DiskIoReader = absl::AnyInvocable<DiskIoBytes()>;
using IoStallReader = absl::AnyInvocable<std::optional<double>()>;

// The readers the sampler calls each tick. Each starts out as the real OS
// reader, so a test can replace one and leave the rest real.
struct Readers {
  RssReader rssReader_ = currentRssBytes;
  CpuReader cpuReader_ = cpuTimeSeconds;
  DiskIoReader diskIoReader_ = currentDiskIoBytes;
  IoStallReader ioStallReader_ = ioStallSeconds;
};

}  // namespace resource_monitor

// Holds the number of the index rebuild that is running. The sampler reads it
// once per tick and writes it to the `rebuild_id` column. The operating system
// knows nothing about rebuilds, so a rebuild has to report itself here.
//
// Rebuilds are numbered from 1. The numbering starts over in every new server
// process. The rebuild marks its start and end, and the sampler reads the
// number from another thread. That is why both counters are atomic.
//
// This class only observes. Making sure that two rebuilds never run at the
// same time is the job of the `Server`.
class RebuildIdTracker {
 public:
  // Marks the start of a rebuild and gives it the next number.
  void markStart() { currentId_.store(numRebuildsStarted_.fetch_add(1) + 1); }

  // Marks the end of a rebuild. It does not matter how the rebuild ended.
  void markEnd() { currentId_.store(0); }

  // Returns the number of the running rebuild, or nothing if none is running.
  [[nodiscard]] std::optional<uint64_t> currentId() const {
    auto id = currentId_.load();
    return id == 0 ? std::nullopt : std::optional(id);
  }

 private:
  // Counts the rebuilds that have started so far. The next rebuild takes its
  // number from here.
  std::atomic<uint64_t> numRebuildsStarted_{0};

  // The number of the rebuild that is running. It is 0 when none is running.
  std::atomic<uint64_t> currentId_{0};
};

// Samples the RSS, CPU usage, and disk IO rate of this process, plus
// system-wide IO stall on a background thread and appends one TSV row
// (`elapsed_s`, `timestamp_ms`, `rss`, `cpu_percent`, `read_bytes_per_s`,
// `write_bytes_per_s`, `io_stall_percent`, `rebuild_id`) per interval; failed
// readings become empty cells. The destructor stops the sampling thread and
// closes the file. Sampling is designed for intervals on the order of a
// second, as set by the `--resource-usage-interval-s` option. One tick reads
// a few small OS counters, which is negligible at such intervals.
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

  // Returns the tracker whose number the sampler writes to the `rebuild_id`
  // column. The server takes this handle and marks its rebuilds on it. An
  // index build marks nothing, so there the column stays empty.
  std::shared_ptr<RebuildIdTracker> rebuildIdTracker() const;

  // Test-only: swaps the OS readers before `start`, for example a throwing
  // reader that exercises the sampler's error handling. The readers that a
  // test does not set keep the defaults from `Readers`, which are the real OS
  // readers, so no reader is ever empty.
  void setReadersForTesting(resource_monitor::Readers readers);

 private:
  // Body of the sampling thread.
  void runLoop(std::chrono::milliseconds interval);

  // Declaration order is load-bearing: `sampler_` must be destroyed
  // (i.e. joined) first, while the members it uses are still alive.
  std::ofstream stream_;
  resource_monitor::Readers readers_;
  // The sampler reads the rebuild number from here every tick, and the server
  // writes to the same object. A `shared_ptr` keeps it alive no matter which of
  // the two is destroyed first.
  std::shared_ptr<RebuildIdTracker> rebuildIdTracker_ =
      std::make_shared<RebuildIdTracker>();
  std::atomic<bool> started_{false};
  std::mutex mutex_;
  std::condition_variable stopCondition_;
  bool stopRequested_ = false;
  JThread sampler_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RESOURCEMONITOR_H
