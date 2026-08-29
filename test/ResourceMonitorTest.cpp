// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_split.h>
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
#include <iterator>
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
using ::testing::DoubleEq;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Optional;
}  // namespace

// _____________________________________________________________________________
TEST(ResourceMonitor, ReadsCurrentMemoryAndCpuUsage) {
#if defined(__APPLE__) || defined(__linux__)
  // Both readings are implemented here, so each returns a value: the
  // running process always has some resident memory and has spent a
  // non-negative amount of CPU time.
  EXPECT_THAT(rm::currentRssBytes(), Optional(Gt(0u)));
  EXPECT_THAT(rm::cpuTimeSeconds(), Optional(Ge(0.0)));
#else
  // No implementation elsewhere: both readings come back empty.
  EXPECT_FALSE(rm::currentRssBytes().has_value());
  EXPECT_FALSE(rm::cpuTimeSeconds().has_value());
#endif
}

// _____________________________________________________________________________
TEST(ResourceMonitor, ReadsCumulativeDiskIoBytes) {
#if defined(__linux__)
  // The kernel creates `/proc/self/io` only when it was built with
  // `CONFIG_TASK_IO_ACCOUNTING`, which nearly all distributions enable.
  // Checking for the file separates a kernel without that option from a
  // parsing failure in the assertions below.
  if (!fs::exists("/proc/self/io")) {
    GTEST_SKIP() << "The kernel lacks `CONFIG_TASK_IO_ACCOUNTING`";
  }
#endif
#if defined(__APPLE__) || defined(__linux__)
  auto first = rm::currentDiskIoBytes();
  ASSERT_TRUE(first.numBytesRead_.has_value());
  ASSERT_TRUE(first.numBytesWritten_.has_value());

  // The counters are cumulative, which is the property the rate computation
  // depends on: a later reading is never smaller. Not `EXPECT_GT`, since a
  // process can go a moment without touching the disk at all.
  auto second = rm::currentDiskIoBytes();
  ASSERT_TRUE(second.numBytesRead_.has_value());
  ASSERT_TRUE(second.numBytesWritten_.has_value());
  EXPECT_GE(second.numBytesRead_.value(), first.numBytesRead_.value());
  EXPECT_GE(second.numBytesWritten_.value(), first.numBytesWritten_.value());
#else
  auto bytes = rm::currentDiskIoBytes();
  EXPECT_FALSE(bytes.numBytesRead_.has_value());
  EXPECT_FALSE(bytes.numBytesWritten_.has_value());
#endif
}

// _____________________________________________________________________________
TEST(ResourceMonitor, ReadsCumulativeIoStallSeconds) {
#if defined(__linux__)
  // `/proc/pressure/io` is optional. A kernel built without `CONFIG_PSI`, or
  // with `CONFIG_PSI_DEFAULT_DISABLED` and booted without `psi=1`, does not
  // have it. An empty reading is therefore skipped rather than failed. On a
  // kernel that does have the file, the value must make sense.
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
  EXPECT_THAT(rm::rssBytesFromStatm(valid),
              Optional(42u * sysconf(_SC_PAGESIZE)));

  std::istringstream garbage{"not a number"};
  EXPECT_FALSE(rm::rssBytesFromStatm(garbage).has_value());

  std::istringstream empty{""};
  EXPECT_FALSE(rm::rssBytesFromStatm(empty).has_value());
}
#endif

#if defined(__linux__)
// _____________________________________________________________________________
TEST(ResourceMonitor,
     DiskIoBytesFromProcIoMatchesKeysAndReportsCountersIndependently) {
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
  EXPECT_THAT(bytes.numBytesRead_, Optional(8192u));
  EXPECT_THAT(bytes.numBytesWritten_, Optional(4096u));

  std::istringstream garbage{"not a number"};
  auto fromGarbage = rm::diskIoBytesFromProcIo(garbage);
  EXPECT_FALSE(fromGarbage.numBytesRead_.has_value());
  EXPECT_FALSE(fromGarbage.numBytesWritten_.has_value());

  std::istringstream empty{""};
  auto fromEmpty = rm::diskIoBytesFromProcIo(empty);
  EXPECT_FALSE(fromEmpty.numBytesRead_.has_value());
  EXPECT_FALSE(fromEmpty.numBytesWritten_.has_value());

  // Each counter stands on its own: a missing key empties only its own column
  // instead of discarding the one that was there.
  std::istringstream onlyRead{"read_bytes: 8192\n"};
  auto fromOnlyRead = rm::diskIoBytesFromProcIo(onlyRead);
  EXPECT_THAT(fromOnlyRead.numBytesRead_, Optional(8192u));
  EXPECT_FALSE(fromOnlyRead.numBytesWritten_.has_value());
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
  // Microseconds in the file, seconds out.
  EXPECT_THAT(rm::ioStallSecondsFromPressure(valid), Optional(DoubleEq(2.5)));

  // A file with no `some` line at all: `full` must not be used instead.
  std::istringstream onlyFull{"full avg10=0.00 total=1000000\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(onlyFull).has_value());

  std::istringstream noTotal{"some avg10=0.00 avg60=0.00 avg300=0.00\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(noTotal).has_value());

  // Not a number, and a number followed by garbage. The second one would slip
  // through a parser that stops at the first non-digit and reports success.
  std::istringstream unparsable{"some avg10=0.00 total=abc\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(unparsable).has_value());

  std::istringstream trailingGarbage{"some avg10=0.00 total=12abc\n"};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(trailingGarbage).has_value());

  std::istringstream empty{""};
  EXPECT_FALSE(rm::ioStallSecondsFromPressure(empty).has_value());
}
#endif

// _____________________________________________________________________________
TEST(ResourceMonitor, FormatTsvRowFillsMissingReadingsWithEmptyCells) {
  // Every reading has a different value, so a value that ends up in the wrong
  // column is visible in the expected row below.
  rm::Sample base{};
  base.elapsedSeconds_ = 1.0;
  base.timestampMs_ = 1000;
  base.rssBytes_ = 2048u;
  base.cpuPercent_ = 50.0;
  base.bytesReadPerSecond_ = 8192.0;
  base.bytesWrittenPerSecond_ = 4096.0;
  base.ioStallPercent_ = 25.0;
  EXPECT_EQ(rm::formatTsvRow(base),
            "1.0\t1000\t2048\t50.0\t8192.0\t4096.0\t25.0\n");

  auto noRss = base;
  noRss.rssBytes_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noRss),
            "1.0\t1000\t\t50.0\t8192.0\t4096.0\t25.0\n");

  auto noCpu = base;
  noCpu.cpuPercent_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noCpu),
            "1.0\t1000\t2048\t\t8192.0\t4096.0\t25.0\n");

  auto noReadRate = base;
  noReadRate.bytesReadPerSecond_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noReadRate),
            "1.0\t1000\t2048\t50.0\t\t4096.0\t25.0\n");

  auto noWriteRate = base;
  noWriteRate.bytesWrittenPerSecond_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noWriteRate),
            "1.0\t1000\t2048\t50.0\t8192.0\t\t25.0\n");

  // `io_stall_percent` is the last column, so an empty value makes the row end
  // in a tab. A consumer that strips trailing whitespace before splitting
  // would lose a column.
  auto noIoStall = base;
  noIoStall.ioStallPercent_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(noIoStall),
            "1.0\t1000\t2048\t50.0\t8192.0\t4096.0\t\n");

  auto nothing = base;
  nothing.rssBytes_ = std::nullopt;
  nothing.cpuPercent_ = std::nullopt;
  nothing.bytesReadPerSecond_ = std::nullopt;
  nothing.bytesWrittenPerSecond_ = std::nullopt;
  nothing.ioStallPercent_ = std::nullopt;
  EXPECT_EQ(rm::formatTsvRow(nothing), "1.0\t1000\t\t\t\t\t\n");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, RateTrackerComputesRateBetweenReadings) {
  // Baseline 0.0s at elapsed 0.0s; 0.5 CPU-s over 1.0 wall-s is a rate of 0.5,
  // which the caller turns into the 50% of a core that the log shows.
  rm::RateTracker tracker{0.0};
  EXPECT_THAT(tracker.update(0.5, 1.0), Optional(DoubleEq(0.5)));
  // Baseline advanced: 1.0 more CPU-s over 1.0 more wall-s is a rate of 1.0.
  EXPECT_THAT(tracker.update(1.5, 2.0), Optional(DoubleEq(1.0)));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, RateTrackerReportsNothingWhenUncomputable) {
  // No reading this tick.
  EXPECT_FALSE(rm::RateTracker{0.0}.update(std::nullopt, 1.0).has_value());
  // No baseline yet.
  EXPECT_FALSE(rm::RateTracker{std::nullopt}.update(0.5, 1.0).has_value());
  // No time elapsed since the baseline.
  EXPECT_FALSE(rm::RateTracker{0.0}.update(0.5, 0.0).has_value());
}

// _____________________________________________________________________________
TEST(ResourceMonitor, RotateLogIfHeaderOutdatedKeepsAMatchingHeader) {
  auto [directory, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("resourceMonitorNoRotation");
  const fs::path path = fs::path{directory} / "resource-usage.tsv";
  const std::string row = "0.0\t1000\t2048\t10.0\t8192.0\t4096.0\t1.0";
  {
    std::ofstream existing{path};
    existing << rm::tsvHeader << "\n" << row << "\n";
  }
  // The file is already in the current format, so it stays where it is and
  // needs no new header.
  EXPECT_FALSE(rm::rotateLogIfHeaderOutdated(path));
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 2u);
  EXPECT_EQ(lines[0], rm::tsvHeader);
  EXPECT_EQ(lines[1], row);
  // Nothing was moved aside, so the log is the only file in the directory.
  EXPECT_EQ(std::distance(fs::directory_iterator{fs::path{directory}},
                          fs::directory_iterator{}),
            1);
}

// _____________________________________________________________________________
TEST(ResourceMonitor, RotateLogIfHeaderOutdatedMovesAFileOfAnOlderFormat) {
  // A whole directory, because this test produces a second file (the archive)
  // whose name it does not choose. The cleanup removes the directory's
  // contents whatever they are.
  auto [directory, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("resourceMonitorRotation");
  const fs::path path = fs::path{directory} / "resource-usage.tsv";
  // A log left over from a server that ran before the disk IO columns existed:
  // a four-column header plus one four-column row.
  const std::string oldHeader = "elapsed_s\ttimestamp_ms\trss\tcpu_percent";
  const std::string oldRow = "0.0\t1000\t2048\t10.0";
  {
    std::ofstream existing{path};
    existing << oldHeader << "\n" << oldRow << "\n";
  }
  EXPECT_TRUE(rm::rotateLogIfHeaderOutdated(path));

  // The old log left its place, and the archive is now the only file. Its name
  // carries the rotation time, so the test cannot predict it.
  EXPECT_FALSE(fs::exists(path));
  std::vector<fs::path> rotatedResourceLogs;
  for (const auto& resourceLog : fs::directory_iterator{fs::path{directory}}) {
    rotatedResourceLogs.push_back(resourceLog.path());
  }
  ASSERT_EQ(rotatedResourceLogs.size(), 1u);
  const fs::path rotated = rotatedResourceLogs.front();
  EXPECT_THAT(ql::pathFilename(rotated).string(),
              ::testing::StartsWith("resource-usage."));
  EXPECT_THAT(ql::pathFilename(rotated).string(), ::testing::EndsWith(".tsv"));

  // The old log is only renamed. Its content is unchanged.
  auto rotatedLines = readLines(rotated);
  ASSERT_EQ(rotatedLines.size(), 2u);
  EXPECT_EQ(rotatedLines[0], oldHeader);
  EXPECT_EQ(rotatedLines[1], oldRow);
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
  const std::string existingRow =
      "0.0\t1000\t2048\t10.0\t8192.0\t4096.0\t1.0\t";
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
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeRotatesAFileWithAnOutdatedHeader) {
  // In `Append` mode `start` moves a log of an older format aside and then
  // writes a fresh header.
  auto [directory, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("resourceMonitorRotation");
  const fs::path path = fs::path{directory} / "resource-usage.tsv";
  {
    std::ofstream existing{path};
    existing << "elapsed_s\ttimestamp_ms\trss\tcpu_percent\n";
    existing << "0.0\t1000\t2048\t10.0\n";
  }
  {
    ResourceMonitor monitor;
    // Long interval so no data row is written; the new file should hold
    // exactly the current header.
    monitor.start(path, ResourceMonitor::Mode::Append, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 1u);
  EXPECT_EQ(lines[0], rm::tsvHeader);
  // The old log was moved aside, so the directory holds it next to the new one.
  EXPECT_EQ(std::distance(fs::directory_iterator{fs::path{directory}},
                          fs::directory_iterator{}),
            2);
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AppendModeWritesASecondHeaderWhenRotationFails) {
  auto [directory, cleanup] =
      ad_utility::testing::makeTemporaryDirectory("resourceMonitorNoRotate");
  // A name that is close to the 255-character limit for file names: splicing
  // the 20-character timestamp into it pushes the name of the archive past
  // that limit, so the rename below fails with `ENAMETOOLONG`.
  const fs::path path = fs::path{directory} / (std::string(250, 'a') + ".tsv");
  const std::string oldHeader = "elapsed_s\ttimestamp_ms\trss\tcpu_percent";
  const std::string oldRow = "0.0\t1000\t2048\t10.0";
  {
    std::ofstream existing{path};
    if (!fs::exists(path)) {
      // Some filesystems (e.g. eCryptfs) allow much shorter names than the
      // usual 255 characters, so the trick above does not apply there.
      GTEST_SKIP_("File names of 254 characters are not supported here");
    }
    existing << oldHeader << "\n" << oldRow << "\n";
  }
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
    // Long interval so the file holds only the header afterward.
    monitor.start(path, ResourceMonitor::Mode::Truncate, std::chrono::hours{1});
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 1u);
  EXPECT_EQ(lines[0], rm::tsvHeader);
  // Nothing was rotated: the directory holds only the live log.
  EXPECT_EQ(std::distance(fs::directory_iterator{fs::path{directory}},
                          fs::directory_iterator{}),
            1);
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

namespace {
// Runs a monitor with `readers` long enough to write at least one row, then
// stops it and returns the lines of the log.
std::vector<std::string> sampledLines(rm::Readers readers) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    ResourceMonitor monitor;
    monitor.setReadersForTesting(std::move(readers));
    // A short interval plus a longer sleep, so the thread writes at least
    // one sampled row before the destructor stops it.
    monitor.start(path, ResourceMonitor::Mode::Truncate,
                  std::chrono::milliseconds{5});
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }  // The destructor stops the thread and closes the file.
  return readLines(path);
}
}  // namespace

// _____________________________________________________________________________
TEST(ResourceMonitor, SamplesWriteWellFormedRows) {
  auto lines = sampledLines({});
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
  // The RSS reading is written out as it is. The other three are cumulative
  // counters that become rates. All of them stand still except the written
  // bytes, so exactly one rate column is positive and a value landing in a
  // neighbouring column would show up.
  rm::Readers readers;
  readers.rssReader_ = []() -> std::optional<uint64_t> { return 2048u; };
  readers.cpuReader_ = []() -> std::optional<double> { return 1.0; };
  readers.diskIoReader_ = [numBytesWritten =
                               uint64_t{0}]() mutable -> rm::DiskIoBytes {
    numBytesWritten += 1'000'000;
    return {8192, numBytesWritten};
  };
  readers.ioStallReader_ = []() -> std::optional<double> { return 2.0; };

  auto lines = sampledLines(std::move(readers));
  ASSERT_GE(lines.size(), 2u);
  // Every reading has to land in its own column, so the row is checked cell by
  // cell. The first two hold the elapsed time and a timestamp, which differ on
  // every run.
  const std::vector<std::string> cells = absl::StrSplit(lines[1], '\t');
  ASSERT_EQ(cells.size(), 7u);
  EXPECT_EQ(cells[2], "2048");  // rss, taken over unchanged
  EXPECT_EQ(cells[3], "0.0");   // cpu, a counter that stands still
  EXPECT_EQ(cells[4], "0.0");   // read bytes, likewise
  // The written bytes are the only counter that grows, and its rate depends on
  // an interval the test does not control. Only the sign is predictable, so
  // this cell is parsed instead of compared against an exact string.
  EXPECT_GT(std::stod(cells[5]), 0.0);
  EXPECT_EQ(cells[6], "0.0");  // io stall, also stands still
}

// _____________________________________________________________________________
TEST(ResourceMonitor, IoStallPercentIsClampedToAHundred) {
  // A stall counter that grows by 1000 seconds on every reading, while the
  // monitor samples every 5 milliseconds. The raw percentage is therefore
  // far above 100, and the row has to show it clamped to 100.
  rm::Readers readers;
  readers.ioStallReader_ = [seconds = 0.0]() mutable -> std::optional<double> {
    seconds += 1000.0;
    return seconds;
  };

  auto lines = sampledLines(std::move(readers));
  ASSERT_GE(lines.size(), 2u);
  // `io_stall_percent` is the last column, so the row ends with the value.
  EXPECT_THAT(lines[1], ::testing::EndsWith("\t100.0"));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AMissingIoStallReadingLeavesTheColumnEmpty) {
  // What every non-Linux machine does: there is no pressure-stall interface,
  // so the reader returns nothing on every tick and nothing is clamped.
  rm::Readers readers;
  readers.ioStallReader_ = []() -> std::optional<double> {
    return std::nullopt;
  };

  auto lines = sampledLines(std::move(readers));
  ASSERT_GE(lines.size(), 2u);
  // All seven columns are still there rather than one being dropped; the
  // stall cell is simply empty, which ends the row in a tab.
  EXPECT_EQ(std::count(lines[1].begin(), lines[1].end(), '\t'), 6);
  EXPECT_THAT(lines[1], ::testing::EndsWith("\t"));
}

// _____________________________________________________________________________
TEST(ResourceMonitor, AMissingDiskIoReadingLeavesBothColumnsEmpty) {
  // When no disk I/O information is available, both columns stay empty.
  rm::Readers readers;
  readers.rssReader_ = []() -> std::optional<uint64_t> { return 2048u; };
  readers.diskIoReader_ = []() -> rm::DiskIoBytes { return {}; };

  auto lines = sampledLines(std::move(readers));
  ASSERT_GE(lines.size(), 2u);
  // Only the two disk cells are empty; the row keeps all seven columns and the
  // surrounding readings still arrive.
  const std::vector<std::string> cells = absl::StrSplit(lines[1], '\t');
  ASSERT_EQ(cells.size(), 7u);
  EXPECT_EQ(cells[2], "2048");
  EXPECT_EQ(cells[4], "");
  EXPECT_EQ(cells[5], "");
}

// _____________________________________________________________________________
TEST(ResourceMonitor, SamplingThreadSurvivesAThrowingReader) {
  // A reader that throws makes `runLoop` throw; the sampler thread must catch
  // it and log, not terminate the process. Both catch arms are covered: a
  // `std::exception` and a non-exception throw.
  for (bool stdException : {true, false}) {
    auto [path, cleanup] = ad_utility::testing::filenameForTesting();
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    rm::Readers readers;
    readers.rssReader_ = [stdException]() -> std::optional<uint64_t> {
      if (stdException) {
        throw std::runtime_error{"boom"};
      }
      throw 42;
    };
    {
      ResourceMonitor monitor;
      monitor.setReadersForTesting(std::move(readers));
      // Short interval plus a sleep so the thread ticks and hits the reader
      // before the destructor stops it.
      monitor.start(path, ResourceMonitor::Mode::Truncate,
                    std::chrono::milliseconds{5});
      std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    // The monitor is destroyed (thread joined), so the log is complete.
    const std::string expectedMessage = stdException
                                            ? "sampling stopped: boom"
                                            : "sampling stopped: unknown error";
    EXPECT_THAT(logStream.str(), ::testing::HasSubstr(expectedMessage));
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
