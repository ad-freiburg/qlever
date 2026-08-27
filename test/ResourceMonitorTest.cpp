// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

// For `sysconf` in the statm test below.
#if defined(__linux__)
#include <unistd.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "./util/FileTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "backports/filesystem.h"
#include "util/ResourceMonitor.h"

namespace {
namespace fs = ql::filesystem;
using ad_utility::ResourceMonitor;
using ad_utility::resource_monitor::CpuPercentTracker;
using ad_utility::resource_monitor::cpuTimeSeconds;
using ad_utility::resource_monitor::currentRssBytes;
using ad_utility::resource_monitor::formatTsvRow;
using ad_utility::testing::readLines;
}  // namespace

// _____________________________________________________________________________
TEST(ResourceMonitor, ReadsCurrentMemoryAndCpuUsage) {
#if defined(__APPLE__) || defined(__linux__)
  // Both readings are implemented here, so each returns a value: the
  // running process always has some resident memory and has spent a
  // non-negative amount of CPU time.
  auto rss = currentRssBytes();
  ASSERT_TRUE(rss.has_value());
  EXPECT_GT(rss.value(), 0u);

  auto cpu = cpuTimeSeconds();
  ASSERT_TRUE(cpu.has_value());
  EXPECT_GE(cpu.value(), 0.0);
#else
  // No implementation elsewhere: both readings come back empty.
  EXPECT_FALSE(currentRssBytes().has_value());
  EXPECT_FALSE(cpuTimeSeconds().has_value());
#endif
}

#if defined(__linux__)
// _____________________________________________________________________________
TEST(ResourceMonitor, RssBytesFromStatmScalesSecondFieldAndRejectsGarbage) {
  using ad_utility::resource_monitor::rssBytesFromStatm;
  // `/proc/self/statm` lists total pages, then the resident pages we want,
  // which are scaled to bytes by the machine's page size.
  std::istringstream valid{"100 42 7 0 0 0 0"};
  auto bytes = rssBytesFromStatm(valid);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_EQ(bytes.value(), 42u * sysconf(_SC_PAGESIZE));

  std::istringstream garbage{"not a number"};
  EXPECT_FALSE(rssBytesFromStatm(garbage).has_value());

  std::istringstream empty{""};
  EXPECT_FALSE(rssBytesFromStatm(empty).has_value());
}
#endif

#if defined(__linux__)
// _____________________________________________________________________________
TEST(ResourceMonitor, IoStallSecondsFromPressureParsesTheSomeLine) {
  using ad_utility::resource_monitor::ioStallSecondsFromPressure;
  // The shape of `/proc/pressure/io`.
  std::istringstream valid{
      "some avg10=0.06 avg60=0.20 avg300=0.19 total=32102297014\n"
      "full avg10=0.04 avg60=0.14 avg300=0.17 total=30474640775\n"};
  auto seconds = ioStallSecondsFromPressure(valid);
  ASSERT_TRUE(seconds.has_value());
  EXPECT_DOUBLE_EQ(seconds.value(), 32102.297014);

  std::istringstream noSomeLine{"full avg10=0.04 total=30474640775\n"};
  EXPECT_FALSE(ioStallSecondsFromPressure(noSomeLine).has_value());
  std::istringstream noTotal{"some avg10=0.06 avg60=0.20 avg300=0.19\n"};
  EXPECT_FALSE(ioStallSecondsFromPressure(noTotal).has_value());
  std::istringstream garbage{"some avg10=0.06 total=notanumber\n"};
  EXPECT_FALSE(ioStallSecondsFromPressure(garbage).has_value());
  std::istringstream empty{""};
  EXPECT_FALSE(ioStallSecondsFromPressure(empty).has_value());
}

// _____________________________________________________________________________
TEST(ResourceMonitor, IoBytesFromProcIoParsesReadAndWriteBytes) {
  using ad_utility::resource_monitor::ioBytesFromProcIo;
  // The shape of `/proc/self/io`.
  std::istringstream valid{
      "rchar: 3232\nwchar: 111\nsyscr: 55\nsyscw: 66\n"
      "read_bytes: 12345\nwrite_bytes: 67890\ncancelled_write_bytes: 0\n"};
  auto bytes = ioBytesFromProcIo(valid);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_EQ(bytes.value().readBytes_, 12345u);
  EXPECT_EQ(bytes.value().writeBytes_, 67890u);

  std::istringstream missingWrite{"read_bytes: 12345\n"};
  EXPECT_FALSE(ioBytesFromProcIo(missingWrite).has_value());
  std::istringstream empty{""};
  EXPECT_FALSE(ioBytesFromProcIo(empty).has_value());
}

// _____________________________________________________________________________
TEST(ResourceMonitor, CachedBytesFromMeminfoParsesTheCachedLine) {
  using ad_utility::resource_monitor::cachedBytesFromMeminfo;
  // The shape of `/proc/meminfo` (note the `kB` unit suffixes and that
  // `SwapCached:` must not match).
  std::istringstream valid{
      "MemTotal:       197465884 kB\nMemFree:        33285968 kB\n"
      "SwapCached:     123 kB\nCached:         101672752 kB\n"};
  auto bytes = cachedBytesFromMeminfo(valid);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_EQ(bytes.value(), 101672752ull * 1024);

  std::istringstream noCachedLine{"MemTotal: 197465884 kB\n"};
  EXPECT_FALSE(cachedBytesFromMeminfo(noCachedLine).has_value());
  std::istringstream garbage{"Cached: notanumber kB\n"};
  EXPECT_FALSE(cachedBytesFromMeminfo(garbage).has_value());
}
#endif

// _____________________________________________________________________________
TEST(ResourceMonitor, FormatTsvRowFillsMissingReadingsWithEmptyCells) {
  using ad_utility::resource_monitor::IoBytes;
  EXPECT_EQ(formatTsvRow(1.0, 1000, 2048u, 50.0, 3.5, IoBytes{100, 200}, 4096u),
            "1.0\t1000\t2048\t50.0\t3.5\t100\t200\t4096\n");
  // Each missing reading becomes an empty cell (a missing `IoBytes` two of
  // them), and the number of cells never changes.
  EXPECT_EQ(formatTsvRow(1.0, 1000, std::nullopt, 50.0, std::nullopt,
                         IoBytes{100, 200}, std::nullopt),
            "1.0\t1000\t\t50.0\t\t100\t200\t\n");
  EXPECT_EQ(
      formatTsvRow(1.0, 1000, 2048u, std::nullopt, 3.5, std::nullopt, 4096u),
      "1.0\t1000\t2048\t\t3.5\t\t\t4096\n");
  EXPECT_EQ(formatTsvRow(1.0, 1000, std::nullopt, std::nullopt, std::nullopt,
                         std::nullopt, std::nullopt),
            "1.0\t1000\t\t\t\t\t\t\n");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, CpuPercentTrackerComputesUsageBetweenReadings) {
  // Baseline 0.0s at elapsed 0.0s; 0.5 CPU-s over 1.0 wall-s is 50% of a core.
  CpuPercentTracker tracker{0.0};
  auto first = tracker.update(0.5, 1.0);
  ASSERT_TRUE(first.has_value());
  EXPECT_DOUBLE_EQ(first.value(), 50.0);
  // Baseline advanced: 1.0 more CPU-s over 1.0 more wall-s is 100%.
  auto second = tracker.update(1.5, 2.0);
  ASSERT_TRUE(second.has_value());
  EXPECT_DOUBLE_EQ(second.value(), 100.0);
}

// _____________________________________________________________________________
TEST(ResourceMonitor, CpuPercentTrackerReportsNothingWhenUncomputable) {
  // No reading this tick.
  EXPECT_FALSE(CpuPercentTracker{0.0}.update(std::nullopt, 1.0).has_value());
  // No baseline yet.
  EXPECT_FALSE(CpuPercentTracker{std::nullopt}.update(0.5, 1.0).has_value());
  // No time elapsed since the baseline.
  EXPECT_FALSE(CpuPercentTracker{0.0}.update(0.5, 0.0).has_value());
}

// _____________________________________________________________________________
TEST(ResourceMonitor, StartTwiceThrows) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  ResourceMonitor monitor;
  // A long interval so the sampling thread never actually writes a row
  // during the test; we only care that the first `start` succeeds.
  monitor.start(path, ResourceMonitor::Mode::Truncate, std::chrono::hours{1});
  // Starting a second time is a usage error and must throw.
  EXPECT_ANY_THROW(monitor.start(path, ResourceMonitor::Mode::Truncate,
                                 std::chrono::hours{1}));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, SetReadersAfterStartThrows) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  ResourceMonitor monitor;
  // A long interval so the sampling thread never actually writes a row
  // during the test.
  monitor.start(path, ResourceMonitor::Mode::Truncate, std::chrono::hours{1});
  // Swapping the readers after `start` would race the sampling thread and
  // must throw.
  AD_EXPECT_THROW_WITH_MESSAGE(
      monitor.setReadersForTesting(currentRssBytes, cpuTimeSeconds),
      ::testing::HasSubstr("before `start`"));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, NonPositiveIntervalThrows) {
  for (auto interval :
       {std::chrono::milliseconds{0}, std::chrono::milliseconds{-5}}) {
    auto [path, cleanup] = ad_utility::testing::filenameForTesting();
    ResourceMonitor monitor;
    AD_EXPECT_THROW_WITH_MESSAGE(
        monitor.start(path, ResourceMonitor::Mode::Truncate, interval),
        ::testing::HasSubstr("must be positive"));
    EXPECT_FALSE(fs::exists(path));
  }
}

// _____________________________________________________________________________
TEST(ResourceMonitor, UnwritablePathDisablesMonitoring) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  ResourceMonitor monitor;
  // `path` does not exist, so treating it as a directory gives `child.tsv`
  // a non-existent parent: the file cannot be opened. Monitoring is
  // optional, so `start` warns and disables itself instead of throwing.
  auto unwritable = path / "child.tsv";
  EXPECT_NO_THROW(monitor.start(unwritable, ResourceMonitor::Mode::Truncate,
                                std::chrono::seconds{1}));
  EXPECT_FALSE(fs::exists(unwritable));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, TruncateModeWritesHeader) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    ResourceMonitor monitor;
    // A long interval so no data row is written before the destructor
    // stops the thread; the file should then hold only the header.
    monitor.start(path, ResourceMonitor::Mode::Truncate, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 1u);
  EXPECT_EQ(lines[0],
            "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tio_stall_percent"
            "\tread_bytes\twrite_bytes\tcached_bytes");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeKeepsExistingHeader) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  // A log left over from an earlier run OF THE SAME FORMAT: the current
  // header plus one data row.
  {
    std::ofstream existing{path};
    existing << ad_utility::resource_monitor::tsvHeader << "\n";
    existing << "0.0\t1000\t2048\t10.0\t0.5\t100\t200\t4096\n";
  }
  {
    ResourceMonitor monitor;
    // Long interval so no new row is written; we only check that opening a
    // non-empty file in `Append` mode does not add a second header.
    monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 2u);
  EXPECT_EQ(lines[0],
            "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tio_stall_percent"
            "\tread_bytes\twrite_bytes\tcached_bytes");
  EXPECT_EQ(lines[1], "0.0\t1000\t2048\t10.0\t0.5\t100\t200\t4096");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeRotatesFileOfOlderFormat) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  // A log left over from an older QLever version: fewer columns. Appending
  // current-format rows to it would produce a file whose rows do not match
  // its header, so it is moved to `<path>.old` and a fresh file is started.
  {
    std::ofstream existing{path};
    existing << "elapsed_s\ttimestamp_ms\trss\tcpu_percent\n";
    existing << "0.0\t1000\t2048\t10.0\n";
  }
  auto rotated = path;
  rotated += ".old";
  absl::Cleanup removeRotated{[&rotated] { fs::remove(rotated); }};
  {
    ResourceMonitor monitor;
    // Long interval so no data row is written; the fresh file holds only
    // the new header.
    monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 1u);
  EXPECT_EQ(lines[0], ad_utility::resource_monitor::tsvHeader);
  auto rotatedLines = readLines(rotated);
  ASSERT_EQ(rotatedLines.size(), 2u);
  EXPECT_EQ(rotatedLines[0], "elapsed_s\ttimestamp_ms\trss\tcpu_percent");
  EXPECT_EQ(rotatedLines[1], "0.0\t1000\t2048\t10.0");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeWritesHeaderWhenFileIsEmptyOrMissing) {
  // Append mode writes the header when there is no existing row to preserve:
  // the file is missing (its size cannot be read) or it exists but is empty.
  auto expectHeaderOnly = [](const fs::path& path) {
    {
      ResourceMonitor monitor;
      // Long interval so no data row is written; the file holds only the
      // header the destructor leaves behind.
      monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
    }
    auto lines = readLines(path);
    ASSERT_EQ(lines.size(), 1u);
    EXPECT_EQ(lines[0],
              "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tio_stall_percent"
              "\tread_bytes\twrite_bytes\tcached_bytes");
  };

  auto [missing, cleanup1] = ad_utility::testing::filenameForTesting();
  expectHeaderOnly(missing);

  auto [empty, cleanup2] = ad_utility::testing::filenameForTesting();
  { std::ofstream create{empty}; }  // a zero-byte file
  expectHeaderOnly(empty);
}

// _____________________________________________________________________________
TEST(ResourceMonitor, SamplesWriteWellFormedRows) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    ResourceMonitor monitor;
    // A short interval plus a longer sleep, so the thread writes at least
    // one sampled row before the destructor stops it.
    monitor.start(path, ResourceMonitor::Mode::Truncate,
                  std::chrono::milliseconds{5});
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }
  auto lines = readLines(path);
  // The header plus at least one sampled row.
  ASSERT_GE(lines.size(), 2u);
  EXPECT_EQ(lines[0],
            "elapsed_s\ttimestamp_ms\trss\tcpu_percent\tio_stall_percent"
            "\tread_bytes\twrite_bytes\tcached_bytes");
  // Each data row has the header's eight tab-separated columns (seven
  // tabs), even when an individual reading was empty.
  for (auto it = lines.begin() + 1; it != lines.end(); ++it) {
    EXPECT_EQ(std::count(it->begin(), it->end(), '\t'), 7)
        << "row does not have 8 columns: " << *it;
  }
}

// _____________________________________________________________________________
TEST(ResourceMonitor, SamplingThreadSurvivesAThrowingReader) {
  // A reader that throws makes `runLoop` throw; the sampler thread must catch
  // it and log, not terminate the process. Both catch arms are covered: a
  // `std::exception` and a non-exception throw.
  for (bool stdException : {true, false}) {
    auto [path, cleanup] = ad_utility::testing::filenameForTesting();
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    {
      ResourceMonitor monitor;
      monitor.setReadersForTesting(
          [stdException]() -> std::optional<uint64_t> {
            if (stdException) {
              throw std::runtime_error{"boom"};
            }
            throw 42;
          },
          cpuTimeSeconds);
      // Short interval plus a sleep so the thread ticks and hits the reader
      // before the destructor stops it.
      monitor.start(path, ResourceMonitor::Mode::Truncate,
                    std::chrono::milliseconds{5});
      std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    // The monitor is destroyed (thread joined), so the log is complete.
    EXPECT_THAT(logStream.str(), ::testing::HasSubstr("sampling stopped"));
  }
}

#if defined(__linux__)
// _____________________________________________________________________________
TEST(ResourceMonitor, WriteFailureWarnsOnceAndStopsSampling) {
  // `/dev/full` opens successfully but fails every write (it simulates a
  // full disk), which is exactly the failure the sampler must warn about.
  if (!fs::exists("/dev/full")) {
    GTEST_SKIP() << "/dev/full is not available";
  }
  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  {
    ResourceMonitor monitor;
    // Short interval plus a longer sleep, so the thread would tick many
    // times: the first failed write must warn and end the sampling, so
    // the warning appears exactly once.
    monitor.start("/dev/full", ResourceMonitor::Mode::Truncate,
                  std::chrono::milliseconds{5});
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }
  // The monitor is destroyed (thread joined), so the log is complete.
  auto log = logStream.str();
  constexpr std::string_view message = "writing to the output file failed";
  auto first = log.find(message);
  ASSERT_NE(first, std::string::npos);
  EXPECT_EQ(log.find(message, first + 1), std::string::npos)
      << "the write-failure warning was logged more than once";
}
#endif
