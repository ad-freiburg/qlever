//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "backports/filesystem.h"
#include "engine/KeepPreviousIndexDirs.h"
#include "global/FileSuffixConstants.h"
#include "libqlever/Qlever.h"
#include "util/GTestHelpers.h"

using qlever::keepPreviousIndexDir;
using qlever::KeepPreviousIndexDirs;
using qlever::parseKeepPreviousIndexDirs;
using qlever::toString;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, parse) {
  EXPECT_EQ(parseKeepPreviousIndexDirs("all"), KeepPreviousIndexDirs::All);
  EXPECT_EQ(parseKeepPreviousIndexDirs("none"), KeepPreviousIndexDirs::None);
  EXPECT_EQ(parseKeepPreviousIndexDirs("original-only"),
            KeepPreviousIndexDirs::OriginalOnly);
  EXPECT_EQ(parseKeepPreviousIndexDirs("most-recent-only"),
            KeepPreviousIndexDirs::MostRecentOnly);
  EXPECT_EQ(parseKeepPreviousIndexDirs("original-and-most-recent"),
            KeepPreviousIndexDirs::OriginalAndMostRecent);
}

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, parseErrors) {
  auto expectThrows = [](std::string_view value) {
    AD_EXPECT_THROW_WITH_MESSAGE(parseKeepPreviousIndexDirs(value),
                                 HasSubstr("must be one of"));
  };
  expectThrows("");
  expectThrows("some");
  expectThrows("original");
  expectThrows("most-recent");
  expectThrows("ALL");
  expectThrows("original-and-most-recent-only");
}

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, engineConfigDefault) {
  // The default of the engine configuration matches the default of the
  // `--rebuild-keep-previous-index-dirs` option of `qlever-server` and of
  // the `--keep-previous-index-dirs` option of `qlever rebuild-index`.
  EXPECT_EQ(qlever::EngineConfig{}.keepPreviousIndexDirs_,
            KeepPreviousIndexDirs::OriginalAndMostRecent);
}

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, toStringIsInverseOfParse) {
  for (auto policy : {KeepPreviousIndexDirs::All, KeepPreviousIndexDirs::None,
                      KeepPreviousIndexDirs::OriginalOnly,
                      KeepPreviousIndexDirs::MostRecentOnly,
                      KeepPreviousIndexDirs::OriginalAndMostRecent}) {
    EXPECT_EQ(parseKeepPreviousIndexDirs(toString(policy)), policy);
  }
}

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, keepPreviousIndexDir) {
  // The directories are numbered from the oldest (position 0, the original)
  // to the newest (the most recent). Compute the kept positions for four
  // directories under the given policy.
  auto keptOfFour = [](KeepPreviousIndexDirs policy) {
    std::vector<size_t> result;
    for (size_t i = 0; i < 4; ++i) {
      if (keepPreviousIndexDir(policy, i, 4)) {
        result.push_back(i);
      }
    }
    return result;
  };
  EXPECT_THAT(keptOfFour(KeepPreviousIndexDirs::All), ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(keptOfFour(KeepPreviousIndexDirs::None), ElementsAre());
  EXPECT_THAT(keptOfFour(KeepPreviousIndexDirs::OriginalOnly), ElementsAre(0));
  EXPECT_THAT(keptOfFour(KeepPreviousIndexDirs::MostRecentOnly),
              ElementsAre(3));
  EXPECT_THAT(keptOfFour(KeepPreviousIndexDirs::OriginalAndMostRecent),
              ElementsAre(0, 3));

  // With a single directory, the original is also the most recent.
  EXPECT_TRUE(keepPreviousIndexDir(KeepPreviousIndexDirs::OriginalOnly, 0, 1));
  EXPECT_TRUE(
      keepPreviousIndexDir(KeepPreviousIndexDirs::MostRecentOnly, 0, 1));
  EXPECT_TRUE(
      keepPreviousIndexDir(KeepPreviousIndexDirs::OriginalAndMostRecent, 0, 1));
  EXPECT_FALSE(keepPreviousIndexDir(KeepPreviousIndexDirs::None, 0, 1));

  // The position must be smaller than the number of directories.
  EXPECT_THROW(keepPreviousIndexDir(KeepPreviousIndexDirs::All, 1, 1),
               ad_utility::Exception);
}

namespace {
// Create the directory `testDir` (deleting whatever was there before) with
// five `previous.*` subdirectories (each containing a dummy index file, one
// of them also a nested subdirectory), a subdirectory whose name does not
// start with `previous.`, and a regular file whose name does. The
// `previous.*` subdirectories are created in lexicographic name order, so
// this order is also their order from oldest to newest (equal timestamps are
// broken by name, see `Qlever::cleanUpPreviousIndexDirs`).
void setUpPreviousIndexDirs(const ql::filesystem::path& testDir) {
  namespace fs = ql::filesystem;
  fs::remove_all(testDir);
  fs::create_directory(testDir);
  for (std::string_view name :
       {"previous.a", "previous.b", "previous.c", "previous.d", "previous.e"}) {
    fs::path dir = testDir / name;
    fs::create_directory(dir);
    std::ofstream{dir / "index.meta-data.json"} << "{}";
  }
  fs::create_directory(testDir / "previous.c" / "nested");
  fs::create_directory(testDir / "other.dir");
  std::ofstream{testDir / "previous.file"} << "not a directory";
}

// Return the names of the entries of `testDir`, sorted.
std::vector<std::string> entryNames(const ql::filesystem::path& testDir) {
  std::vector<std::string> result;
  for (const auto& entry : ql::filesystem::directory_iterator{testDir}) {
    result.push_back(entry.path().filename().string());
  }
  ql::ranges::sort(result);
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, cleanUpPreviousIndexDirs) {
  namespace fs = ql::filesystem;
  fs::path testDir = "keepPreviousIndexDirsTest.dir";
  // Remove the test directory also when an assertion or an exception exits
  // this test early.
  absl::Cleanup removeTestDir{[&testDir] { fs::remove_all(testDir); }};
  // The base name of the index whose directory is cleaned up. The index files
  // themselves do not have to exist for the cleanup.
  std::string baseName = (testDir / "index").string();

  // Apply the given policy to a freshly set up directory and return the
  // names of the surviving entries. For every policy except `all`, the
  // decisions are appended to the `rebuild-index` log of the index, which
  // hence appears among the entries.
  auto survivors = [&](KeepPreviousIndexDirs policy) {
    setUpPreviousIndexDirs(testDir);
    qlever::Qlever::cleanUpPreviousIndexDirs(baseName, policy);
    return entryNames(testDir);
  };
  std::string logName = absl::StrCat("index", REBUILD_INDEX_LOG_SUFFIX);

  // Entries that are not directories or whose name does not start with
  // `previous.` are never touched.
  EXPECT_THAT(survivors(KeepPreviousIndexDirs::All),
              ElementsAre("other.dir", "previous.a", "previous.b", "previous.c",
                          "previous.d", "previous.e", "previous.file"));
  EXPECT_THAT(survivors(KeepPreviousIndexDirs::None),
              ElementsAre(logName, "other.dir", "previous.file"));
  EXPECT_THAT(survivors(KeepPreviousIndexDirs::OriginalOnly),
              ElementsAre(logName, "other.dir", "previous.a", "previous.file"));
  EXPECT_THAT(survivors(KeepPreviousIndexDirs::MostRecentOnly),
              ElementsAre(logName, "other.dir", "previous.e", "previous.file"));
  EXPECT_THAT(survivors(KeepPreviousIndexDirs::OriginalAndMostRecent),
              ElementsAre(logName, "other.dir", "previous.a", "previous.e",
                          "previous.file"));

  // The decisions are written to the `rebuild-index` log of the index (not to
  // the server log), one line per directory.
  {
    std::ifstream logStream{(testDir / logName).string()};
    std::string logContent{std::istreambuf_iterator<char>{logStream}, {}};
    EXPECT_THAT(logContent,
                HasSubstr("Checking which previous index directories to keep "
                          "or delete (original-and-most-recent)"));
    EXPECT_THAT(logContent, HasSubstr("previous.a -> KEEP"));
    EXPECT_THAT(logContent, HasSubstr("previous.b -> DELETE"));
    EXPECT_THAT(logContent, HasSubstr("previous.c -> DELETE"));
    EXPECT_THAT(logContent, HasSubstr("previous.d -> DELETE"));
    EXPECT_THAT(logContent, HasSubstr("previous.e -> KEEP"));
  }
}
