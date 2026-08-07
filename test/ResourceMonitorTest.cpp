// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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
namespace rm = ad_utility::resource_monitor;
using ad_utility::ResourceMonitor;
using ad_utility::testing::readLines;
}  // namespace

// _____________________________________________________________________________
TEST(ResourceMonitor, ReadsCurrentMemoryAndCpuUsage) {
#if defined(__APPLE__) || defined(__linux__)
  // Both readings are implemented here, so each returns a value: the
  // running process always has some resident memory and has spent a
  // non-negative amount of CPU time.
  auto rss = rm::currentRssBytes();
  ASSERT_TRUE(rss.has_value());
  EXPECT_GT(rss.value(), 0u);

  auto cpu = rm::cpuTimeSeconds();
  ASSERT_TRUE(cpu.has_value());
  EXPECT_GE(cpu.value(), 0.0);
#else
  // No implementation elsewhere: both readings come back empty.
  EXPECT_FALSE(rm::currentRssBytes().has_value());
  EXPECT_FALSE(rm::cpuTimeSeconds().has_value());
#endif
}

// _____________________________________________________________________________
TEST(ResourceMonitor, ReadsCumulativeDiskIoBytes) {
#if defined(__APPLE__) || defined(__linux__)
  // On Linux this needs `CONFIG_TASK_IO_ACCOUNTING`, which mainstream kernels
  // enable; a failure here means the kernel lacks it, not a parse failure.
  auto first = rm::currentDiskIoBytes();
  ASSERT_TRUE(first.has_value());

  // The counters are cumulative, which is the property the rate computation
  // depends on: a later reading is never smaller. Not `EXPECT_GT`, since a
  // process can go a moment without touching the disk at all.
  auto second = rm::currentDiskIoBytes();
  ASSERT_TRUE(second.has_value());
  EXPECT_GE(second.value().readBytes_, first.value().readBytes_);
  EXPECT_GE(second.value().writeBytes_, first.value().writeBytes_);
#else
  EXPECT_FALSE(rm::currentDiskIoBytes().has_value());
#endif
}

// _____________________________________________________________________________
TEST(ResourceMonitor, ReadsCumulativeIoStallSeconds) {
#if defined(__linux__)
  // Unlike `/proc/self/io`, this file is genuinely optional: a kernel built
  // without `CONFIG_PSI`, or with `CONFIG_PSI_DEFAULT_DISABLED` and booted
  // without `psi=1`, does not have it. So an empty reading is skipped rather
  // than failed; on a kernel that does have it, the value must make sense.
  auto first = rm::ioStallSeconds();
  if (!first.has_value()) {
    GTEST_SKIP() << "/proc/pressure/io is not available on this kernel";
  }
  EXPECT_GE(first.value(), 0.0);

  // Cumulative, which is what the percentage computation depends on: a later
  // reading is never smaller. Not `EXPECT_GT`, since a machine can go a moment
  // without stalling on I/O at all.
  auto second = rm::ioStallSeconds();
  ASSERT_TRUE(second.has_value());
  EXPECT_GE(second.value(), first.value());
#else
  // No pressure-stall interface exists off Linux, by design.
  EXPECT_FALSE(rm::ioStallSeconds().has_value());
#endif
}

#if defined(__linux__)
// _____________________________________________________________________________
TEST(ResourceMonitor, RssBytesFromStatmScalesSecondFieldAndRejectsGarbage) {
  // `/proc/self/statm` lists total pages, then the resident pages we want,
  // which are scaled to bytes by the machine's page size.
  std::istringstream valid{"100 42 7 0 0 0 0"};
  auto bytes = rm::rssBytesFromStatm(valid);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_EQ(bytes.value(), 42u * sysconf(_SC_PAGESIZE));

  std::istringstream garbage{"not a number"};
  EXPECT_FALSE(rm::rssBytesFromStatm(garbage).has_value());

  std::istringstream empty{""};
  EXPECT_FALSE(rm::rssBytesFromStatm(empty).has_value());
}
#endif

#if defined(__linux__)
// _____________________________________________________________________________
TEST(ResourceMonitor,
     DiskIoBytesFromProcIoMatchesKeysAndRejectsIncompleteInput) {
  // A realistic `/proc/self/io`: the keys we want are neither first nor last,
  // and every value differs, so a parser reading by position would fail.
  std::istringstream valid{
      "rchar: 4036\n"
      "wchar: 12\n"
      "syscr: 9\n"
      "syscw: 1\n"
      "read_bytes: 8192\n"
      "write_bytes: 4096\n"
      "cancelled_write_bytes: 64\n"};
  auto bytes = rm::diskIoBytesFromProcIo(valid);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_EQ(bytes.value().readBytes_, 8192u);
  EXPECT_EQ(bytes.value().writeBytes_, 4096u);

  std::istringstream garbage{"not a number"};
  EXPECT_FALSE(rm::diskIoBytesFromProcIo(garbage).has_value());

  std::istringstream empty{""};
  EXPECT_FALSE(rm::diskIoBytesFromProcIo(empty).has_value());

  // One key alone is a failed reading, not a zero for the other.
  std::istringstream onlyRead{"read_bytes: 8192\n"};
  EXPECT_FALSE(rm::diskIoBytesFromProcIo(onlyRead).has_value());
}
#endif

#if defined(__linux__)
// _____________________________________________________________________________
TEST(ResourceMonitor, IoStallSecondsFromPressureReadsSomeAndRejectsGarbage) {
  // `full` carries a different `total`, so a parser that reads the wrong line
  // fails here instead of returning a number that merely looks plausible.
  std::istringstream valid{
      "some avg10=0.00 avg60=1.25 avg300=0.30 total=2500000\n"
      "full avg10=0.00 avg60=0.00 avg300=0.00 total=1000000\n"};
  auto seconds = rm::ioStallSecondsFromPressure(valid);
  ASSERT_TRUE(seconds.has_value());
  // Microseconds in the file, seconds out.
  EXPECT_DOUBLE_EQ(seconds.value(), 2.5);

  // A file with no `some` line at all: `full` must not be used instead.
  std::istringstream onlyFull{"full avg10=0.00 total=1000000\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(onlyFull).has_value());

  std::istringstream noTotal{"some avg10=0.00 avg60=0.00 avg300=0.00\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(noTotal).has_value());

  // Not a number, and a number followed by garbage. The second one would slip
  // through a parser that stops at the first non-digit and reports success.
  std::istringstream unparseable{"some avg10=0.00 total=abc\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(unparseable).has_value());

  std::istringstream trailingGarbage{"some avg10=0.00 total=12abc\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(trailingGarbage).has_value());

  std::istringstream empty{""};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(empty).has_value());
}
#endif

// _____________________________________________________________________________
TEST(ResourceMonitor, FormatTsvRowFillsMissingReadingsWithEmptyCells) {
  constexpr rm::Sample base{
      .elapsedSeconds_ = 1.0,
      .timestampMs_ = 1000,
      .rssBytes_ = 2048u,
      .cpuPercent_ = 50.0,
      .diskIoBytes_ =
          rm::DiskIoBytes{.readBytes_ = 8192u, .writeBytes_ = 4096u},
      .ioStallPercent_ = 25.0};
  EXPECT_EQ(rm::formatTsvRow(base),
            "1.0\t1000\t2048\t50.0\t8192\t4096\t25.0\n");

  auto noRss = base;
  noRss.rssBytes_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noRss), "1.0\t1000\t\t50.0\t8192\t4096\t25.0\n");

  auto noCpu = base;
  noCpu.cpuPercent_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noCpu), "1.0\t1000\t2048\t\t8192\t4096\t25.0\n");

  // One failed reading empties both counters, never just one of them.
  auto noDiskIo = base;
  noDiskIo.diskIoBytes_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noDiskIo), "1.0\t1000\t2048\t50.0\t\t\t25.0\n");

  // The row every non-Linux machine writes: the last cell is empty, so the row
  // ends in a tab. A consumer that strips trailing whitespace before splitting
  // would lose a column here.
  auto noIoStall = base;
  noIoStall.ioStallPercent_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noIoStall),
            "1.0\t1000\t2048\t50.0\t8192\t4096\t\n");

  auto nothing = base;
  nothing.rssBytes_ = std::nullopt;
  nothing.cpuPercent_ = std::nullopt;
  nothing.diskIoBytes_ = std::nullopt;
  nothing.ioStallPercent_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(nothing), "1.0\t1000\t\t\t\t\t\n");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, SecondsToPercentTrackerComputesPercentBetweenReadings) {
  // Baseline 0.0s at elapsed 0.0s; 0.5 CPU-s over 1.0 wall-s is 50% of a core.
  rm::SecondsToPercentTracker tracker{0.0};
  auto first = tracker.update(0.5, 1.0);
  ASSERT_TRUE(first.has_value());
  EXPECT_DOUBLE_EQ(first.value(), 50.0);
  // Baseline advanced: 1.0 more CPU-s over 1.0 more wall-s is 100%.
  auto second = tracker.update(1.5, 2.0);
  ASSERT_TRUE(second.has_value());
  EXPECT_DOUBLE_EQ(second.value(), 100.0);
}

// _____________________________________________________________________________
TEST(ResourceMonitor, SecondsToPercentTrackerReportsNothingWhenUncomputable) {
  // No reading this tick.
  EXPECT_FALSE(
      rm::SecondsToPercentTracker{0.0}.update(std::nullopt, 1.0).has_value());
  // No baseline yet.
  EXPECT_FALSE(
      rm::SecondsToPercentTracker{std::nullopt}.update(0.5, 1.0).has_value());
  // No time elapsed since the baseline.
  EXPECT_FALSE(rm::SecondsToPercentTracker{0.0}.update(0.5, 0.0).has_value());
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
  AD_EXPECT_THROW_WITH_MESSAGE(monitor.setReadersForTesting({}),
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
  EXPECT_EQ(lines[0], rm::tsvHeader);
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeKeepsAMatchingHeader) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  // The steady state: a log written by a server of this same version, so its
  // header is the one this build writes. Append mode adds no second header
  // and leaves the existing row alone.
  const std::string existingRow = "0.0\t1000\t2048\t10.0\t8192\t4096\t1.0";
  {
    std::ofstream existing{path};
    existing << rm::tsvHeader << "\n" << existingRow << "\n";
  }
  {
    ResourceMonitor monitor;
    // Long interval so no new row is written; we only check that opening a
    // non-empty file in `Append` mode does not add a second header.
    monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 2u);
  EXPECT_EQ(lines[0], rm::tsvHeader);
  EXPECT_EQ(lines[1], existingRow);
  // Nothing was rotated: there is only ever one log file in the steady state.
  EXPECT_FALSE(fs::exists(path.string() + ".old"));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeRotatesAFileWithAnOutdatedHeader) {
  // A whole directory, because this test produces a second file (the archive)
  // whose name it does not choose; the cleanup removes the directory's
  // contents whatever they are.
  auto [directory, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("resourceMonitorRotation");
  const fs::path path = fs::path{directory} / "resource-usage.tsv";
  // A log left over from a server that ran before the disk IO columns existed:
  // a four-column header plus one four-column row. Appending seven-column rows
  // to it would leave one file holding rows of two widths, which no
  // header-driven reader can parse. So the old file is moved aside intact and
  // a fresh one is started, and every file on disk keeps a single width.
  const std::string oldHeader = "elapsed_s\ttimestamp_ms\trss\tcpu_percent";
  const std::string oldRow = "0.0\t1000\t2048\t10.0";
  {
    std::ofstream existing{path};
    existing << oldHeader << "\n" << oldRow << "\n";
  }
  {
    ResourceMonitor monitor;
    // Long interval so no data row is written; the new file should hold
    // exactly the current header.
    monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
  }
  const fs::path rotated = path.string() + ".old";
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 1u);
  EXPECT_EQ(lines[0], rm::tsvHeader);

  // The old log is preserved unchanged next to it.
  ASSERT_TRUE(fs::exists(rotated));
  auto rotatedLines = readLines(rotated);
  ASSERT_EQ(rotatedLines.size(), 2u);
  EXPECT_EQ(rotatedLines[0], oldHeader);
  EXPECT_EQ(rotatedLines[1], oldRow);
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeWritesASecondHeaderWhenRotationFails) {
  auto [directory, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("resourceMonitorNoRotate");
  const fs::path path = fs::path{directory} / "resource-usage.tsv";
  const std::string oldHeader = "elapsed_s\ttimestamp_ms\trss\tcpu_percent";
  const std::string oldRow = "0.0\t1000\t2048\t10.0";
  {
    std::ofstream existing{path};
    existing << oldHeader << "\n" << oldRow << "\n";
  }
  // A directory of that name makes the rename fail with `EISDIR`, which is the
  // only reliable way to reach the fallback. Monitoring is optional, so this
  // warns rather than throwing, and writes a second header line so that the
  // format change is at least visible inside the file.
  fs::create_directory(path.string() + ".old");
  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  {
    ResourceMonitor monitor;
    // Long interval so no data row is written.
    monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 3u);
  EXPECT_EQ(lines[0], oldHeader);
  EXPECT_EQ(lines[1], oldRow);
  EXPECT_EQ(lines[2], rm::tsvHeader);
  EXPECT_THAT(logStream.str(), ::testing::HasSubstr("could not move"));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, TruncateModeNeverRotates) {
  // Index builds start a fresh file every run, so an outdated header is simply
  // overwritten. Rotation exists to protect rows that are being appended to,
  // and there are none here.
  auto [directory, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("resourceMonitorTruncate");
  const fs::path path = fs::path{directory} / "resource-usage.tsv";
  {
    std::ofstream existing{path};
    existing << "elapsed_s\ttimestamp_ms\trss\tcpu_percent\n";
    existing << "0.0\t1000\t2048\t10.0\n";
  }
  {
    ResourceMonitor monitor;
    // Long interval so the file holds only the header afterwards.
    monitor.start(path, ResourceMonitor::Mode::Truncate, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 1u);
  EXPECT_EQ(lines[0], rm::tsvHeader);
  EXPECT_FALSE(fs::exists(path.string() + ".old"));
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
    EXPECT_EQ(lines[0], rm::tsvHeader);
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
  EXPECT_EQ(lines[0], rm::tsvHeader);
  // Each data row has the header's seven tab-separated columns (six tabs),
  // even when an individual reading was empty.
  for (auto it = lines.begin() + 1; it != lines.end(); ++it) {
    EXPECT_EQ(std::count(it->begin(), it->end(), '\t'), 6)
        << "row does not have 7 columns: " << *it;
  }
}

// _____________________________________________________________________________
TEST(ResourceMonitor, SampledRowsCarryTheReadings) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    ResourceMonitor monitor;
    // Readers that always return the same value, so the row is known in
    // advance. Neither cumulative counter grows, so both percentages are 0.
    monitor.setReadersForTesting(
        {.rssReader_ = []() -> std::optional<uint64_t> { return 2048u; },
         .cpuReader_ = []() -> std::optional<double> { return 1.0; },
         .diskIoReader_ = []() -> std::optional<rm::DiskIoBytes> {
           return rm::DiskIoBytes{.readBytes_ = 8192u, .writeBytes_ = 4096u};
         },
         .ioStallReader_ = []() -> std::optional<double> { return 2.0; }});
    // A short interval plus a longer sleep, so at least one row is written.
    monitor.start(path, ResourceMonitor::Mode::Truncate,
                  std::chrono::milliseconds{5});
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }
  auto lines = readLines(path);
  ASSERT_GE(lines.size(), 2u);
  // Each reading lands in its own column. The first two columns are clocks,
  // so only the end of the row can be checked. The three raw readings differ
  // from each other, so a swap among them would fail here; the two computed
  // columns are both 0 and are only pinned by position.
  EXPECT_THAT(lines[1], ::testing::EndsWith("\t2048\t0.0\t8192\t4096\t0.0"));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, IoStallPercentIsClampedToAHundred) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    ResourceMonitor monitor;
    // A stall counter growing by 1000 seconds per reading, far more than the
    // elapsed time: the raw percentage is way above 100 and must be clamped.
    monitor.setReadersForTesting(
        {.ioStallReader_ = [seconds = 0.0]() mutable -> std::optional<double> {
          seconds += 1000.0;
          return seconds;
        }});
    // A short interval plus a longer sleep, so at least one row is written.
    monitor.start(path, ResourceMonitor::Mode::Truncate,
                  std::chrono::milliseconds{5});
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }
  auto lines = readLines(path);
  ASSERT_GE(lines.size(), 2u);
  EXPECT_THAT(lines[1], ::testing::EndsWith("\t100.0"));
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
      // Only the RSS reader is overridden; the CPU reader stays the real one.
      monitor.setReadersForTesting(
          {.rssReader_ = [stdException]() -> std::optional<uint64_t> {
            if (stdException) {
              throw std::runtime_error{"boom"};
            }
            throw 42;
          }});
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
