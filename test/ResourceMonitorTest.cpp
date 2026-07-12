// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "./util/FileTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "util/ResourceMonitor.h"

namespace {
namespace fs = std::filesystem;
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
TEST(ResourceMonitor, ParseResidentPagesReadsSecondFieldAndRejectsGarbage) {
  using ad_utility::resource_monitor::parseResidentPages;
  // `/proc/self/statm` lists total pages, then the resident pages we want.
  std::istringstream valid{"100 42 7 0 0 0 0"};
  auto pages = parseResidentPages(valid);
  ASSERT_TRUE(pages.has_value());
  EXPECT_EQ(pages.value(), 42u);

  std::istringstream garbage{"not a number"};
  EXPECT_FALSE(parseResidentPages(garbage).has_value());

  std::istringstream empty{""};
  EXPECT_FALSE(parseResidentPages(empty).has_value());
}
#endif

// _____________________________________________________________________________
TEST(ResourceMonitor, FormatTsvRowFillsMissingReadingsWithEmptyCells) {
  EXPECT_EQ(formatTsvRow(1.0, 1000, 2048u, 50.0), "1.0\t1000\t2048\t50.0\n");
  EXPECT_EQ(formatTsvRow(1.0, 1000, std::nullopt, 50.0), "1.0\t1000\t\t50.0\n");
  EXPECT_EQ(formatTsvRow(1.0, 1000, 2048u, std::nullopt),
            "1.0\t1000\t2048\t\n");
  EXPECT_EQ(formatTsvRow(1.0, 1000, std::nullopt, std::nullopt),
            "1.0\t1000\t\t\n");
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
TEST(ResourceMonitor, InvalidIntervalDisablesMonitoring) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  ResourceMonitor monitor;
  // A zero interval is invalid, but monitoring is optional: `start` warns
  // and disables itself instead of throwing, and it does not create the
  // output file.
  EXPECT_NO_THROW(monitor.start(path, ResourceMonitor::Mode::Truncate,
                                std::chrono::milliseconds{0}));
  EXPECT_FALSE(fs::exists(path));
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
  EXPECT_EQ(lines[0], "elapsed_s\ttimestamp_ms\trss\tcpu_percent");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeKeepsExistingHeader) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  // A log left over from an earlier run: a header plus one data row.
  {
    std::ofstream existing{path};
    existing << "elapsed_s\ttimestamp_ms\trss\tcpu_percent\n";
    existing << "0.0\t1000\t2048\t10.0\n";
  }
  {
    ResourceMonitor monitor;
    // Long interval so no new row is written; we only check that opening a
    // non-empty file in `Append` mode does not add a second header.
    monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 2u);
  EXPECT_EQ(lines[0], "elapsed_s\ttimestamp_ms\trss\tcpu_percent");
  EXPECT_EQ(lines[1], "0.0\t1000\t2048\t10.0");
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
    EXPECT_EQ(lines[0], "elapsed_s\ttimestamp_ms\trss\tcpu_percent");
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
  EXPECT_EQ(lines[0], "elapsed_s\ttimestamp_ms\trss\tcpu_percent");
  // Each data row has the header's four tab-separated columns (three tabs),
  // even when an individual reading was empty.
  for (auto it = lines.begin() + 1; it != lines.end(); ++it) {
    EXPECT_EQ(std::count(it->begin(), it->end(), '\t'), 3)
        << "row does not have 4 columns: " << *it;
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
