// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "./util/FileTestHelpers.h"
#include "util/ResourceMonitor.h"

namespace {
namespace fs = std::filesystem;
using ad_utility::ResourceMonitor;
using ad_utility::resource_monitor::cpuTimeSeconds;
using ad_utility::resource_monitor::currentRssBytes;
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
