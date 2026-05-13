// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tanmay Garg (gargt@cs.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "util/QueryEventLog.h"

namespace {
namespace fs = std::filesystem;
using ad_utility::QueryEventLog;

// RAII helper that creates a unique temporary directory and removes it on
// destruction. Mirrors the pattern used in `FilesystemHelpersTest.cpp`.
class TempDir {
 public:
  TempDir() {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    path_ = fs::temp_directory_path() / absl::StrCat("QueryEventLogTest_",
                                                     info->test_suite_name(),
                                                     "_", info->name());
    fs::remove_all(path_);
    fs::create_directory(path_);
  }
  ~TempDir() {
    std::error_code ec;
    fs::remove_all(path_, ec);
  }
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  fs::path file(const std::string& name) const { return path_ / name; }

 private:
  fs::path path_;
};

// Read all lines from `path`. Used after a `QueryEventLog` has been destroyed,
// so the writer thread has fully drained and the file is closed.
std::vector<std::string> readLines(const fs::path& path) {
  std::ifstream in{path};
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(in, line)) {
    lines.push_back(std::move(line));
  }
  return lines;
}

}  // namespace

// _____________________________________________________________________________
TEST(QueryEventLog, PushBeforeConfigureIsNoOp) {
  TempDir dir;
  // Local instance (not the singleton) so the test is self-contained and
  // does not leak state into other tests.
  QueryEventLog log;
  // No `setOutputFile` call. `push` must silently drop the line and not
  // create any file.
  log.push("this should be discarded\n");
  EXPECT_FALSE(fs::exists(dir.file("never_written")));
}

// _____________________________________________________________________________
TEST(QueryEventLog, DoubleConfigureThrows) {
  TempDir dir;
  QueryEventLog log;
  log.setOutputFile(dir.file("first.log"));
  EXPECT_ANY_THROW(log.setOutputFile(dir.file("second.log")));
}

// _____________________________________________________________________________
TEST(QueryEventLog, SingleProducerWritesAndFlushes) {
  TempDir dir;
  auto path = dir.file("single.log");
  {
    QueryEventLog log;
    log.setOutputFile(path);
    log.push("first\n");
    log.push("second\n");
    // Destructor finishes the queue, joins the writer (which drains the
    // remaining lines and flushes), then closes the file.
  }
  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), 2u);
  EXPECT_EQ(lines[0], "first");
  EXPECT_EQ(lines[1], "second");
}

// _____________________________________________________________________________
TEST(QueryEventLog, ConcurrentProducersProduceWellFormedLines) {
  TempDir dir;
  auto path = dir.file("concurrent.log");
  constexpr size_t kThreads = 8;
  constexpr size_t kLinesPerThread = 1000;

  {
    QueryEventLog log;
    log.setOutputFile(path);

    std::vector<std::thread> producers;
    producers.reserve(kThreads);
    for (size_t t = 0; t < kThreads; ++t) {
      producers.emplace_back([&log, t] {
        for (size_t n = 0; n < kLinesPerThread; ++n) {
          // Format: "<threadId>:<seq>\n". Cheap to parse back without a
          // JSON dependency, and uniquely identifies each pushed line.
          log.push(absl::StrCat(t, ":", n, "\n"));
        }
      });
    }
    for (auto& th : producers) {
      th.join();
    }
    // Let the local `log` go out of scope here: its destructor drains
    // the queue and joins the writer before we re-open the file below.
  }

  auto lines = readLines(path);
  ASSERT_EQ(lines.size(), kThreads * kLinesPerThread)
      << "Expected exactly one line per push with no truncation or merging.";

  // Verify every line is well-formed and that the multiset of (thread, seq)
  // pairs matches what producers pushed. Using a set is enough because
  // every pair is unique by construction.
  std::set<std::pair<size_t, size_t>> seen;
  for (const auto& line : lines) {
    auto colon = line.find(':');
    ASSERT_NE(colon, std::string::npos) << "malformed line: " << line;
    size_t t = std::stoul(line.substr(0, colon));
    size_t n = std::stoul(line.substr(colon + 1));
    ASSERT_LT(t, kThreads);
    ASSERT_LT(n, kLinesPerThread);
    auto [_, inserted] = seen.emplace(t, n);
    ASSERT_TRUE(inserted) << "duplicate line: " << line;
  }
  EXPECT_EQ(seen.size(), kThreads * kLinesPerThread);
}
