//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
//  You may not use this file except in compliance with the Apache 2.0 License,
//  which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "util/FilesystemHelpers.h"
#include "util/GTestHelpers.h"
#include "util/Log.h"

namespace fs = std::filesystem;
using qlever::util::doesDirectoryContainFileWithBasename;
using ::testing::HasSubstr;

namespace {

// RAII helper that creates a unique temporary directory and removes it
// (recursively) on destruction. Keeps each test isolated.
class TempDir {
 public:
  TempDir() {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    path_ = fs::temp_directory_path() / absl::StrCat("ensureNoConflict_",
                                                     info->test_suite_name(),
                                                     "_", info->name());
    fs::remove_all(path_);  // clean up leftovers from a previous crashed run
    fs::create_directory(path_);
  }

  ~TempDir() {
    std::error_code ec;
    fs::remove_all(path_, ec);
    if (ec) {
      AD_LOG_WARN << "Could not remove temporary directory " << path_ << ": "
                  << ec.message() << std::endl;
    }
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  const fs::path& path() const { return path_; }

 private:
  fs::path path_;
};

// Convenience helper to create an empty file at the given path.
void touch(const fs::path& p) {
  std::ofstream out{p};
  ASSERT_TRUE(out.good()) << "Could not create file: " << p;
}

}  // namespace

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename, emptyDirectoryReturnsFalse) {
  TempDir tmp;
  EXPECT_FALSE(doesDirectoryContainFileWithBasename(tmp.path().string() + "/"));
}

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename, unrelatedFilesDoNotConflict) {
  TempDir tmp;
  touch(tmp.path() / "other.txt");
  touch(tmp.path() / "readme.md");
  touch(tmp.path() / "ndex.bin");  // off-by-one on the prefix on purpose

  auto base = tmp.path() / "index";
  EXPECT_FALSE(doesDirectoryContainFileWithBasename(base.string()));
}

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename, exactMatchReturnsTrue) {
  TempDir tmp;
  auto conflict = tmp.path() / "index";
  touch(conflict);

  auto base = tmp.path() / "index";
  EXPECT_TRUE(doesDirectoryContainFileWithBasename(base.string()));
}

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename, prefixMatchReturnsTrue) {
  TempDir tmp;
  auto conflict = tmp.path() / "index.vocabulary";
  touch(conflict);
  // Add some unrelated noise that should be ignored.
  touch(tmp.path() / "other.txt");

  auto base = tmp.path() / "index";
  EXPECT_TRUE(doesDirectoryContainFileWithBasename(base.string()));
}

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename,
     prefixMatchingSubdirectoryReturnsTrue) {
  TempDir tmp;
  auto conflict = tmp.path() / "index.dir";
  fs::create_directory(conflict);

  auto base = tmp.path() / "index";
  EXPECT_TRUE(doesDirectoryContainFileWithBasename(base.string()));
}

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename, nonExistentParentDirectoryIsOk) {
  TempDir tmp;
  auto base = tmp.path() / "does" / "not" / "exist" / "index";
  EXPECT_FALSE(doesDirectoryContainFileWithBasename(base.string()));
}

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename, parentIsNotADirectoryReturnsTrue) {
  TempDir tmp;
  auto fileAsParent = tmp.path() / "iAmAFile";
  touch(fileAsParent);

  // baseName treats `fileAsParent` as the parent directory.
  auto base = fileAsParent / "index";
  EXPECT_TRUE(doesDirectoryContainFileWithBasename(base.string()));
}

// _____________________________________________________________________________
TEST(DoesDirectoryContainFileWithBasename,
     trailingSlashHasNoFilenameComponent) {
  TempDir tmp;
  // A path ending in a separator has an empty filename() in std::filesystem.
  std::string base = tmp.path().string() + "/";
  touch(tmp.path() / "some-file.txt");
  EXPECT_TRUE(doesDirectoryContainFileWithBasename(base));
}

// _____________________________________________________________________________
TEST(IsSubdirectoryOf, differentPaths) {
  using qlever::util::isSubdirectoryOf;
  EXPECT_TRUE(isSubdirectoryOf("/path/to/directory/subdirectory/file",
                               "/path/to/directory/file"));
  EXPECT_TRUE(
      isSubdirectoryOf("directory/subdirectory/file", "directory/file"));
  EXPECT_TRUE(isSubdirectoryOf("subdirectory/file", "file"));
  EXPECT_FALSE(isSubdirectoryOf("/path/to/directory/subdirectory/file",
                                "/path/to/directory/file/"));
  EXPECT_TRUE(isSubdirectoryOf("/some/path/", "/some/path/"));
  EXPECT_FALSE(isSubdirectoryOf("/some/path", "/some/path/"));
  EXPECT_TRUE(isSubdirectoryOf("/some/path/", "/some/path"));
  EXPECT_TRUE(isSubdirectoryOf("/some/path/", "/some/"));
  EXPECT_FALSE(isSubdirectoryOf("/some/path/file", "/some/path/other/file"));
  EXPECT_TRUE(isSubdirectoryOf("/some/path/other/file", "/some/path/file"));
  EXPECT_TRUE(isSubdirectoryOf("/", "/"));
  EXPECT_TRUE(isSubdirectoryOf("/other-file", "/file-inside-root"));
  EXPECT_FALSE(isSubdirectoryOf("/other-file", "/path/"));
  EXPECT_FALSE(isSubdirectoryOf("/some/path/../../", "/some/path"));
  EXPECT_FALSE(
      isSubdirectoryOf("/some/path/../../malicious-file", "/some/path"));
  EXPECT_FALSE(
      isSubdirectoryOf("/some/path/../../malicious-path/", "/some/path"));
  EXPECT_FALSE(isSubdirectoryOf("/some/path/other/../../malicious-path/",
                                "/some/path/"));

  // This only works if the test is not run inside `/`, but this should be fine.
  EXPECT_FALSE(isSubdirectoryOf("/malicious-path", "relative-path"));
  EXPECT_FALSE(isSubdirectoryOf("../malicious-path", "relative-path"));
}
