// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ResourceMonitor.h"

#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <string_view>
#include <type_traits>

#include "backports/StartsWithAndEndsWith.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"

// The readings use platform-specific APIs.
// `getrusage` (CPU time) is shared by both linux and macOS.
#if defined(__APPLE__)
#include <libproc.h>
#include <mach/mach.h>
#include <sys/resource.h>
#include <unistd.h>
#elif defined(__linux__)
#include <sys/resource.h>
#include <unistd.h>

#include <fstream>
#endif

namespace ad_utility::resource_monitor {

// _____________________________________________________________________________
std::optional<uint64_t> currentRssBytes() {
#if defined(__APPLE__)
  // `task_info` serves many query flavors through one generic buffer
  // pointer, hence the `reinterpret_cast`; `count` tells the kernel how
  // much room the struct has.
  mach_task_basic_info info;
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
    return std::nullopt;
  }
  return info.resident_size;
#elif defined(__linux__)
  std::ifstream statm{"/proc/self/statm"};
  return rssBytesFromStatm(statm);
#else
  return std::nullopt;
#endif
}

#if defined(__linux__)
// _____________________________________________________________________________
std::optional<uint64_t> rssBytesFromStatm(std::istream& statm) {
  // Field 2 of `/proc/self/statm` is the resident size in pages, so scale by
  // the machine's page size.
  uint64_t totalPages;
  uint64_t residentPages;
  if (!(statm >> totalPages >> residentPages)) {
    return std::nullopt;
  }
  return residentPages * static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
}
#endif

// _____________________________________________________________________________
std::optional<double> cpuTimeSeconds() {
#if defined(__APPLE__) || defined(__linux__)
  rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) != 0) {
    return std::nullopt;
  }
  // User time = running our code, system time = kernel work on our
  // behalf (I/O etc.); their sum is what counts as "CPU used".
  auto toSeconds = [](const timeval& time) {
    return static_cast<double>(time.tv_sec) +
           static_cast<double>(time.tv_usec) * 1e-6;
  };
  return toSeconds(usage.ru_utime) + toSeconds(usage.ru_stime);
#else
  return std::nullopt;
#endif
}

// _____________________________________________________________________________
std::optional<double> SecondsToPercentTracker::update(
    std::optional<double> seconds, double elapsed) {
  // Keep the old baseline on a failed reading, so the next value averages
  // over the whole gap rather than jumping.
  if (!seconds.has_value()) {
    return std::nullopt;
  }
  std::optional<double> percent;
  if (lastSeconds_.has_value() && elapsed > lastElapsed_) {
    percent = (seconds.value() - lastSeconds_.value()) /
              (elapsed - lastElapsed_) * 100.0;
  }
  lastSeconds_ = seconds;
  lastElapsed_ = elapsed;
  return percent;
}

// _____________________________________________________________________________
std::optional<DiskIoBytes> currentDiskIoBytes() {
#if defined(__APPLE__)
  // Not `getrusage`'s `ru_inblock`/`ru_oublock`: macOS leaves those at zero.
  rusage_info_current info;
  if (proc_pid_rusage(getpid(), RUSAGE_INFO_CURRENT,
                      reinterpret_cast<rusage_info_t*>(&info)) != 0) {
    return std::nullopt;
  }
  return DiskIoBytes{.readBytes_ = info.ri_diskio_bytesread,
                     .writeBytes_ = info.ri_diskio_byteswritten};
#elif defined(__linux__)
  std::ifstream procIo{"/proc/self/io"};
  return diskIoBytesFromProcIo(procIo);
#else
  return std::nullopt;
#endif
}

#if defined(__linux__)
// _____________________________________________________________________________
std::optional<DiskIoBytes> diskIoBytesFromProcIo(std::istream& procIo) {
  // `/proc/self/io` is a list of `key: value` lines whose set has grown
  // across kernel versions, so scan for the two keys instead of counting
  // lines.
  std::optional<uint64_t> readBytes;
  std::optional<uint64_t> writeBytes;
  std::string key;
  uint64_t value;

  while (procIo >> key >> value) {
    if (key == "read_bytes:") {
      readBytes = value;
    } else if (key == "write_bytes:") {
      writeBytes = value;
    }
  }

  if (!readBytes.has_value() || !writeBytes.has_value()) {
    return std::nullopt;
  }
  return DiskIoBytes{.readBytes_ = readBytes.value(),
                     .writeBytes_ = writeBytes.value()};
}
#endif

// _____________________________________________________________________________
std::optional<double> ioStallSeconds() {
#if defined(__linux__)
  std::ifstream pressure{"/proc/pressure/io"};
  return ioStallSecondsFromPressure(pressure);
#else
  return std::nullopt;
#endif
}

#if defined(__linux__)
// _____________________________________________________________________________
std::optional<double> ioStallSecondsFromPressure(std::istream& pressure) {
  // The relevant line is `some avg10=... avg60=... avg300=... total=<n>`
  // Read `some` (at least one task stalled), not `full` (every
  // runnable task stalled), which undercounts a busy server.
  constexpr std::string_view totalPrefix = "total=";
  std::string line;
  while (std::getline(pressure, line)) {
    if (!ql::starts_with(line, "some ")) continue;
    for (std::string_view token :
         absl::StrSplit(line, ' ', absl::SkipEmpty())) {
      if (!ql::starts_with(token, totalPrefix)) continue;
      token.remove_prefix(totalPrefix.size());
      uint64_t microseconds{};
      const char* end = token.data() + token.size();
      auto [ptr, ec] = std::from_chars(token.data(), end, microseconds);
      if (ec != std::errc() || ptr != end) {
        return std::nullopt;
      }
      return static_cast<double>(microseconds) * 1e-6;
    }
    return std::nullopt;  // A `some` line with no `total=` token.
  }
  return std::nullopt;  // No file, or no `some` line.
}
#endif

namespace {
// One TSV cell: an integer, a double, or either wrapped in an `optional`,
// where a missing value becomes an empty cell.
// _____________________________________________________________________________
template <typename T>
std::string formatCell(const T& value) {
  if constexpr (similarToInstantiation<T, std::optional>) {
    return value.has_value() ? formatCell(value.value()) : std::string{};
  } else if constexpr (std::is_floating_point_v<T>) {
    return absl::StrFormat("%.1f", value);
  } else {
    static_assert(std::is_integral_v<T>,
                  "A TSV cell must be an integer, a double or an optional of "
                  "one of those.");
    return std::to_string(value);
  }
}
}  // namespace

// _____________________________________________________________________________
std::string formatTsvRow(const Sample& sample) {
  std::string readBytes;
  std::string writeBytes;
  if (sample.diskIoBytes_.has_value()) {
    readBytes = formatCell(sample.diskIoBytes_->readBytes_);
    writeBytes = formatCell(sample.diskIoBytes_->writeBytes_);
  }

  const std::array tsvCells{formatCell(sample.elapsedSeconds_),
                            formatCell(sample.timestampMs_),
                            formatCell(sample.rssBytes_),
                            formatCell(sample.cpuPercent_),
                            std::move(readBytes),
                            std::move(writeBytes),
                            formatCell(sample.ioStallPercent_)};

  return absl::StrJoin(tsvCells, "\t") + "\n";
}

}  // namespace ad_utility::resource_monitor

namespace ad_utility {

// _____________________________________________________________________________
ResourceMonitor::~ResourceMonitor() {
  {
    std::lock_guard lock{mutex_};
    stopRequested_ = true;
  }
  stopCondition_.notify_all();
}

// _____________________________________________________________________________
void ResourceMonitor::start(const ql::filesystem::path& path, Mode mode,
                            std::chrono::milliseconds interval) {
  AD_CONTRACT_CHECK(!started_.exchange(true),
                    "ResourceMonitor::start may only be called once.");
  AD_CONTRACT_CHECK(interval > std::chrono::milliseconds{0},
                    "The resource-usage sampling interval must be positive.");
#if defined(__APPLE__) || defined(__linux__)
  // Monitoring is optional: on an unwritable file, warn and let QLever run
  // on rather than aborting the process.
  namespace fs = ql::filesystem;
  // Decide about the header before opening: truncating destroys the
  // old file size. A missing file or failed stat also gets a header.
  ql::error_code ec;
  const auto oldSize = fs::file_size(path, ec);
  const bool writeHeader = mode == Mode::Truncate || ec || oldSize == 0;
  const auto openMode =
      mode == Mode::Truncate ? std::ios::trunc : std::ios::app;
  stream_.open(path, std::ios::out | openMode);
  if (!stream_.is_open()) {
    AD_LOG_WARN << "ResourceMonitor: failed to open the output file; "
                   "continuing without a resource-usage log."
                << std::endl;
    return;
  }
  if (writeHeader) {
    stream_ << "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tread_bytes\t"
               "write_bytes\tio_stall_percent\n"
            << std::flush;
  }
  // Spawn last: the thread uses the stream right away. An exception
  // escaping a thread would terminate the process, so catch everything.
  sampler_ = JThread{[this, interval] {
    try {
      runLoop(interval);
    } catch (const std::exception& e) {
      AD_LOG_ERROR << "ResourceMonitor: sampling stopped: " << e.what()
                   << std::endl;
    } catch (...) {
      AD_LOG_ERROR << "ResourceMonitor: sampling stopped: unknown error"
                   << std::endl;
    }
  }};
#else
  // No implementation for this platform, and monitoring is optional: skip
  // it and let QLever run normally rather than failing.
  (void)path;
  (void)mode;
  AD_LOG_WARN << "ResourceMonitor: not supported on this platform; "
                 "continuing without a resource-usage log."
              << std::endl;
#endif
}

// _____________________________________________________________________________
void ResourceMonitor::setReadersForTesting(
    resource_monitor::ReaderOverrides readerOverrides) {
  AD_CONTRACT_CHECK(!started_,
                    "The readers must be swapped before `start` is called, "
                    "otherwise this would race the sampling thread.");
  if (readerOverrides.rssReader_) {
    rssReader_ = std::move(readerOverrides.rssReader_);
  }
  if (readerOverrides.cpuReader_) {
    cpuReader_ = std::move(readerOverrides.cpuReader_);
  }
  if (readerOverrides.diskIoReader_) {
    diskIoReader_ = std::move(readerOverrides.diskIoReader_);
  }
  if (readerOverrides.ioStallReader_) {
    ioStallReader_ = std::move(readerOverrides.ioStallReader_);
  }
}

// _____________________________________________________________________________
void ResourceMonitor::runLoop(std::chrono::milliseconds interval) {
  const Timer timer{Timer::Started};
  resource_monitor::SecondsToPercentTracker cpuTracker{cpuReader_()};
  resource_monitor::SecondsToPercentTracker ioStallTracker{ioStallReader_()};

  // Absolute deadlines keep the ticks on a steady grid, no matter how
  // long each sample takes.
  auto deadline = std::chrono::steady_clock::now() + interval;
  std::unique_lock lock{mutex_};
  while (!stopRequested_) {
    if (stopCondition_.wait_until(lock, deadline,
                                  [this] { return stopRequested_; })) {
      break;  // Woken by the destructor, not the timeout.
    }
    deadline += interval;
    const double elapsed = Timer::toSeconds(timer.value());

    // A stall is a fraction of one timeline, so it cannot exceed 100%; the
    // reading and the elapsed clock are taken at slightly different instants,
    // so a tick can still compute a hair over.
    std::optional<double> ioStallPercent{
        ioStallTracker.update(ioStallReader_(), elapsed)};
    if (ioStallPercent.has_value()) {
      ioStallPercent = std::clamp(ioStallPercent.value(), 0.0, 100.0);
    }
    stream_ << resource_monitor::formatTsvRow(
        {.elapsedSeconds_ = elapsed,
         .timestampMs_ = epochMillis(std::chrono::system_clock::now()),
         .rssBytes_ = rssReader_(),
         .cpuPercent_ = cpuTracker.update(cpuReader_(), elapsed),
         .diskIoBytes_ = diskIoReader_(),
         .ioStallPercent_ = ioStallPercent});
    stream_.flush();
    if (stream_.fail()) {
      AD_LOG_WARN << "ResourceMonitor: writing to the output file failed; "
                     "stopping the resource-usage log."
                  << std::endl;
      break;
    }
  }
}

}  // namespace ad_utility
