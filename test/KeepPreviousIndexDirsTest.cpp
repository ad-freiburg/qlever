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

#include <chrono>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>
#include <type_traits>
#include <vector>

#include "backports/filesystem.h"
#include "engine/KeepPreviousIndexDirs.h"
#include "global/FileSuffixConstants.h"
#include "libqlever/Qlever.h"
#include "util/GTestHelpers.h"

using qlever::keepPreviousIndexDir;
using qlever::KeepPreviousIndexDirs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, parse) {
  auto parse = [](std::string_view value) {
    return KeepPreviousIndexDirs::fromString(value);
  };
  EXPECT_EQ(parse("all"), KeepPreviousIndexDirs::All);
  EXPECT_EQ(parse("none"), KeepPreviousIndexDirs::None);
  EXPECT_EQ(parse("original-only"), KeepPreviousIndexDirs::OriginalOnly);
  EXPECT_EQ(parse("most-recent-only"), KeepPreviousIndexDirs::MostRecentOnly);
  EXPECT_EQ(parse("original-and-most-recent"),
            KeepPreviousIndexDirs::OriginalAndMostRecent);
}

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, parseErrors) {
  auto expectThrows = [](std::string_view value) {
    AD_EXPECT_THROW_WITH_MESSAGE(KeepPreviousIndexDirs::fromString(value),
                                 HasSubstr("is not a valid"));
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
  for (KeepPreviousIndexDirs policy :
       {KeepPreviousIndexDirs::All, KeepPreviousIndexDirs::None,
        KeepPreviousIndexDirs::OriginalOnly,
        KeepPreviousIndexDirs::MostRecentOnly,
        KeepPreviousIndexDirs::OriginalAndMostRecent}) {
    EXPECT_EQ(KeepPreviousIndexDirs::fromString(policy.toString()), policy);
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
// start with `previous.`, and a regular file whose name does. The `previous.*`
// subdirectories get explicit last-write times that make `previous.a` the
// oldest and `previous.e` the newest, with a deliberate timestamp tie between
// `previous.d` and `previous.e` (which the cleanup breaks by name, see
// `Qlever::cleanUpPreviousIndexDirs`). The times cannot be left implicit:
// creating the nested subdirectory below modifies `previous.c`, and on a
// filesystem with fine-grained timestamps that would make `previous.c` the
// newest directory (observed on the macOS CI runners).
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
  // Derive the timestamps from a real one read back from disk, so that this
  // works with both `std::filesystem` (`file_time_type`) and the
  // `boost::filesystem` backport (`std::time_t`).
  auto newest = fs::last_write_time(testDir / "previous.e");
  // The generic lambda makes the discarded branch dependent, so that each
  // build only instantiates the branch that matches its timestamp type.
  auto minutesEarlier = [](auto timestamp, int minutes) {
    if constexpr (std::is_integral_v<decltype(timestamp)>) {
      return timestamp - 60 * minutes;
    } else {
      return timestamp - std::chrono::minutes(minutes);
    }
  };
  fs::last_write_time(testDir / "previous.a", minutesEarlier(newest, 4));
  fs::last_write_time(testDir / "previous.b", minutesEarlier(newest, 3));
  fs::last_write_time(testDir / "previous.c", minutesEarlier(newest, 2));
  fs::last_write_time(testDir / "previous.d", minutesEarlier(newest, 1));
  fs::last_write_time(testDir / "previous.e", minutesEarlier(newest, 1));
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
  fs::path testDir = absl::StrCat(gtestCurrentTestName(), ".dir");
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

// _____________________________________________________________________________
TEST(KeepPreviousIndexDirs, cleanUpPreviousIndexDirsFailures) {
  namespace fs = ql::filesystem;
  fs::path testDir = absl::StrCat(gtestCurrentTestName(), ".dir");

  // A base name in a directory that does not exist makes the enumeration of
  // the `previous.*` directories fail. The failure must only be logged (when
  // the cleanup runs, the rebuild has already succeeded), never thrown.
  EXPECT_NO_THROW(qlever::Qlever::cleanUpPreviousIndexDirs(
      (testDir / "doesNotExist" / "index").string(),
      KeepPreviousIndexDirs::None));

  // A directory that cannot be deleted is logged as an error and kept, and
  // the cleanup continues with the remaining directories. Removing all
  // permissions from `previous.b` makes the deletion of the file it contains
  // (and hence the `remove_all`) fail.
  auto restorePermissions = [&testDir] {
    ql::error_code ignored;
    fs::permissions(testDir / "previous.b", fs::perms::owner_all, ignored);
  };
  absl::Cleanup removeTestDir{[&testDir, &restorePermissions] {
    restorePermissions();
    fs::remove_all(testDir);
  }};
  setUpPreviousIndexDirs(testDir);
  fs::permissions(testDir / "previous.b", ql::filesystem_perms_none);
  fs::path fileInB = testDir / "previous.b" / "index.meta-data.json";
  if (FILE* handle = fopen(fileInB.string().c_str(), "r")) {
    fclose(handle);
    // This can happen in docker environments (running as root).
    GTEST_SKIP_("File permissions are not effective in this environment");
  }
  qlever::Qlever::cleanUpPreviousIndexDirs((testDir / "index").string(),
                                           KeepPreviousIndexDirs::None);
  restorePermissions();

  std::string logName = absl::StrCat("index", REBUILD_INDEX_LOG_SUFFIX);
  EXPECT_THAT(entryNames(testDir),
              ElementsAre(logName, "other.dir", "previous.b", "previous.file"));
  std::ifstream logStream{(testDir / logName).string()};
  std::string logContent{std::istreambuf_iterator<char>{logStream}, {}};
  EXPECT_THAT(logContent, HasSubstr("previous.b -> DELETE"));
  EXPECT_THAT(logContent, HasSubstr("Failed to delete \"previous.b\""));
}
