// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ResourceMonitor.h"

#include <absl/strings/str_format.h>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"

// The readings use platform-specific APIs.
// `getrusage` (CPU time) is shared by both linux and macOS.
#if defined(__APPLE__)
#include <mach/mach.h>
#include <sys/resource.h>
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
std::optional<double> CpuPercentTracker::update(
    std::optional<double> cpuSeconds, double elapsed) {
  // Keep the old baseline on a failed reading, so the next value averages
  // over the whole gap rather than jumping.
  if (!cpuSeconds.has_value()) {
    return std::nullopt;
  }
  std::optional<double> percent;
  if (lastCpuSeconds_.has_value() && elapsed > lastElapsed_) {
    percent =
        (*cpuSeconds - *lastCpuSeconds_) / (elapsed - lastElapsed_) * 100.0;
  }
  lastCpuSeconds_ = cpuSeconds;
  lastElapsed_ = elapsed;
  return percent;
}

// _____________________________________________________________________________
std::string formatTsvRow(double elapsed, int64_t timestampMs,
                         std::optional<uint64_t> rss,
                         std::optional<double> cpuPercent) {
  return absl::StrFormat("%.1f\t%d\t%s\t%s\n", elapsed, timestampMs,
                         rss.has_value() ? std::to_string(rss.value()) : "",
                         cpuPercent.has_value()
                             ? absl::StrFormat("%.1f", cpuPercent.value())
                             : "");
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
  auto oldSize = fs::file_size(path, ec);
  bool writeHeader = mode == Mode::Truncate || ec || oldSize == 0;
  auto openMode = mode == Mode::Truncate ? std::ios::trunc : std::ios::app;
  stream_.open(path, std::ios::out | openMode);
  if (!stream_.is_open()) {
    AD_LOG_WARN << "ResourceMonitor: failed to open the output file; "
                   "continuing without a resource-usage log."
                << std::endl;
    return;
  }
  if (writeHeader) {
    stream_ << "elapsed_s\ttimestamp_ms\trss\tcpu_percent\n" << std::flush;
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
    resource_monitor::RssReader rssReader,
    resource_monitor::CpuReader cpuReader) {
  AD_CONTRACT_CHECK(!started_,
                    "The readers must be swapped before `start` is called, "
                    "otherwise this would race the sampling thread.");
  rssReader_ = std::move(rssReader);
  cpuReader_ = std::move(cpuReader);
}

// _____________________________________________________________________________
void ResourceMonitor::runLoop(std::chrono::milliseconds interval) {
  Timer timer{Timer::Started};
  resource_monitor::CpuPercentTracker cpuTracker{cpuReader_()};

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
    double elapsed = Timer::toSeconds(timer.value());
    auto rss = rssReader_();
    auto cpuPercent = cpuTracker.update(cpuReader_(), elapsed);
    stream_ << resource_monitor::formatTsvRow(
        elapsed, epochMillis(std::chrono::system_clock::now()), rss,
        cpuPercent);
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
