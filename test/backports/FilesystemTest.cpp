// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../util/GTestHelpers.h"
#include "backports/filesystem.h"

namespace fs = ql::filesystem;
using ::testing::UnorderedElementsAre;

namespace {

// Create an empty regular file at `path`.
void touch(const fs::path& path) { std::ofstream{path.string()}; }

// Create an empty directory (with all its parents) that is unique to the
// current test, and return it together with an `absl::Cleanup` that removes the
// directory and all its contents again.
auto makeUniqueDirectory() {
  fs::path directory = fs::current_path() / gtestCurrentTestName();
  // Remove leftovers from a previous crashed run.
  fs::remove_all(directory);
  fs::create_directories(directory);
  return std::pair{directory,
                   absl::Cleanup{[directory] { fs::remove_all(directory); }}};
}
}  // namespace

// _____________________________________________________________________________
// A `ql::filesystem::path` can be constructed, assigned to, and appended to
// from a `std::string_view`. This doesn't hold for `boost::filesystem::path`
// before Boost 1.81, which is why `backports/filesystem.h` explicitly adds the
// missing support for the older versions.
TEST(BackportsFilesystem, pathFromStringView) {
  std::string_view view = "somePath";
  EXPECT_EQ(fs::path{view}.string(), view);

  fs::path assigned = "somethingElse";
  assigned = view;
  EXPECT_EQ(assigned.string(), view);

  EXPECT_EQ(fs::path{"prefix"} / view, fs::path{"prefix"} / fs::path{view});

  // The empty `string_view` also has to work (the corresponding code path in
  // `boost::filesystem` is a special case).
  EXPECT_TRUE(fs::path{std::string_view{}}.empty());
}

// _____________________________________________________________________________
TEST(BackportsFilesystem, isRegularFileAndIsDirectory) {
  auto [directory, cleanup] = makeUniqueDirectory();
  touch(directory / "file");
  fs::create_directory(directory / "subdirectory");

  std::vector<std::string> files;
  std::vector<std::string> directories;
  for (const auto& entry : ql::directoryRange(directory)) {
    if (ql::isRegularFile(entry)) {
      files.push_back(ql::pathFilename(entry.path()).string());
    }
    if (ql::isDirectory(entry)) {
      directories.push_back(ql::pathFilename(entry.path()).string());
    }
  }
  EXPECT_THAT(files, UnorderedElementsAre("file"));
  EXPECT_THAT(directories, UnorderedElementsAre("subdirectory"));
}

// _____________________________________________________________________________
// Querying an entry that has been deleted since the directory was iterated must
// not throw. This happens in practice whenever another process writes to the
// directory that is being iterated, and `boost::filesystem` runs into it
// because it stats an entry lazily when its status is queried and not while
// iterating. Note that we deliberately don't require a particular result: with
// `std::filesystem` the implementation may answer from the status that it
// cached while iterating, and then still report the deleted entry as a regular
// file.
TEST(BackportsFilesystem, statusOfDeletedEntryDoesNotThrow) {
  auto [directory, cleanup] = makeUniqueDirectory();
  touch(directory / "file");
  fs::create_directory(directory / "subdirectory");

  std::vector<fs::directory_entry> entries;
  for (const auto& entry : ql::directoryRange(directory)) {
    entries.push_back(entry);
  }
  ASSERT_EQ(entries.size(), 2u);
  fs::remove_all(directory / "file");
  fs::remove_all(directory / "subdirectory");

  for (const auto& entry : entries) {
    EXPECT_NO_THROW(ql::isRegularFile(entry));
    EXPECT_NO_THROW(ql::isDirectory(entry));
  }
}

// _____________________________________________________________________________
TEST(BackportsFilesystem, pathFilename) {
  EXPECT_EQ(ql::pathFilename(fs::path{"some/directory/file.txt"}).string(),
            "file.txt");
  // In contrast to `boost::filesystem::path::filename()`, a trailing separator
  // yields an empty filename (the `std::filesystem` semantics).
  EXPECT_TRUE(ql::pathFilename(fs::path{"some/directory/"}).empty());
}
