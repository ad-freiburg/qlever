// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ResourceMonitor.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <sys/resource.h>

#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"

#ifdef __APPLE__
#include <mach/mach.h>
#else
#include <unistd.h>

#include <fstream>
#endif

namespace ad_utility::resource_monitor {

// _____________________________________________________________________________
std::optional<uint64_t> currentRssBytes() {
#ifdef __APPLE__
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
#else
  // Field 2 of `/proc/self/statm` is the resident size, counted in
  // pages, so multiply by the machine's page size.
  std::ifstream statm{"/proc/self/statm"};
  uint64_t totalPages, residentPages;
  if (!(statm >> totalPages >> residentPages)) {
    return std::nullopt;
  }
  return residentPages * static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
#endif
}

// _____________________________________________________________________________
std::optional<double> cpuTimeSeconds() {
  struct rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) != 0) {
    return std::nullopt;
  }
  // User time = running our code, system time = kernel work on our
  // behalf (I/O etc.); their sum is what counts as "CPU used".
  auto toSeconds = [](const timeval& time) {
    return static_cast<double>(time.tv_sec) + time.tv_usec * 1e-6;
  };
  return toSeconds(usage.ru_utime) + toSeconds(usage.ru_stime);
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
  // `sampler_` (declared last) is destroyed first and joins the thread.
}

// _____________________________________________________________________________
void ResourceMonitor::start(const std::filesystem::path& path, Mode mode,
                            std::chrono::milliseconds interval) {
  AD_CONTRACT_CHECK(interval > std::chrono::milliseconds{0},
                    "ResourceMonitor: the sampling interval must be positive.");
  AD_CONTRACT_CHECK(!started_.exchange(true),
                    "ResourceMonitor::start may only be called once.");
  namespace fs = std::filesystem;
  // Decide about the header before opening: truncating destroys the
  // old file size. A missing file or failed stat also gets a header.
  std::error_code ec;
  auto oldSize = fs::file_size(path, ec);
  bool writeHeader = mode == Mode::Truncate || ec || oldSize == 0;
  auto openMode = mode == Mode::Truncate ? std::ios::trunc : std::ios::app;
  stream_.open(path, std::ios::out | openMode);
  AD_CONTRACT_CHECK(stream_.is_open(),
                    "ResourceMonitor: failed to open output file.");
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
}

// _____________________________________________________________________________
void ResourceMonitor::runLoop(std::chrono::milliseconds interval) {
  Timer timer{Timer::Started};
  // Baseline for the CPU deltas; empty until the first successful probe.
  std::optional<double> lastCpuSeconds = resource_monitor::cpuTimeSeconds();
  double lastElapsed = 0.0;
  bool warnedWriteFailure = false;

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
    auto rss = resource_monitor::currentRssBytes();
    auto cpuSeconds = resource_monitor::cpuTimeSeconds();

    // CPU usage = CPU time consumed per wall-clock time, as a percent of
    // one core. Baselines only advance on success, so after a failed
    // probe the next value averages over the whole gap.
    std::string cpuColumn;
    if (cpuSeconds.has_value()) {
      if (lastCpuSeconds.has_value() && elapsed > lastElapsed) {
        double percent =
            (*cpuSeconds - *lastCpuSeconds) / (elapsed - lastElapsed) * 100.0;
        cpuColumn = absl::StrFormat("%.1f", percent);
      }
      lastCpuSeconds = cpuSeconds;
      lastElapsed = elapsed;
    }
    stream_ << absl::StrFormat("%.1f\t%d\t%s\t%s\n", elapsed,
                               epochMillis(std::chrono::system_clock::now()),
                               rss.has_value() ? absl::StrCat(*rss) : "",
                               cpuColumn);
    stream_.flush();
    // A failed stream drops all further writes; say so once.
    if (stream_.fail() && !warnedWriteFailure) {
      AD_LOG_WARN << "ResourceMonitor: writing to the output file failed"
                  << std::endl;
      warnedWriteFailure = true;
    }
  }
}

}  // namespace ad_utility
