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

#include <fstream>
#include <string>

#include "backports/filesystem.h"
#include "util/FilesystemHelpers.h"
#include "util/GTestHelpers.h"
#include "util/Log.h"

namespace fs = ql::filesystem;
using qlever::util::deleteFilesInDirectory;
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
    ql::error_code ec;
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
  std::ofstream out{p.string()};
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
  // A path ending in a separator has an empty filename() in ql::filesystem.
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

// _____________________________________________________________________________
TEST(DeleteFilesInDirectory, deletesOnlyMatchingRegularFiles) {
  TempDir tmp;
  touch(tmp.path() / "del_1.txt");
  touch(tmp.path() / "del_2.txt");
  touch(tmp.path() / "keep.txt");

  size_t deleted = deleteFilesInDirectory(tmp.path(), [](const fs::path& p) {
    return p.filename().string().rfind("del_", 0) == 0;
  });

  EXPECT_EQ(deleted, 2u);
  EXPECT_FALSE(fs::exists(tmp.path() / "del_1.txt"));
  EXPECT_FALSE(fs::exists(tmp.path() / "del_2.txt"));
  EXPECT_TRUE(fs::exists(tmp.path() / "keep.txt"));
}

// _____________________________________________________________________________
TEST(DeleteFilesInDirectory, deletesAllMatchingFiles) {
  TempDir tmp;
  constexpr int numMatching = 25;
  for (int i = 0; i < numMatching; ++i) {
    touch(tmp.path() / absl::StrCat("del_", i, ".tmp"));
  }
  touch(tmp.path() / "keep");

  size_t deleted = deleteFilesInDirectory(tmp.path(), [](const fs::path& p) {
    return p.extension().string() == ".tmp";
  });

  EXPECT_EQ(deleted, static_cast<size_t>(numMatching));
  EXPECT_TRUE(fs::exists(tmp.path() / "keep"));
  // No matching file is left behind (this is what the collect-then-delete
  // strategy guarantees, even on platforms where deleting while iterating would
  // skip entries).
  size_t remaining = 0;
  for (const auto& entry : ql::directoryRange(tmp.path())) {
    if (entry.path().extension().string() == ".tmp") {
      ++remaining;
    }
  }
  EXPECT_EQ(remaining, 0u);
}

// _____________________________________________________________________________
TEST(DeleteFilesInDirectory, ignoresSubdirectories) {
  TempDir tmp;
  touch(tmp.path() / "match_file");
  fs::create_directory(tmp.path() / "match_dir");

  // The predicate matches both, but only the regular file must be deleted.
  size_t deleted = deleteFilesInDirectory(tmp.path(), [](const fs::path& p) {
    return p.filename().string().rfind("match_", 0) == 0;
  });

  EXPECT_EQ(deleted, 1u);
  EXPECT_FALSE(fs::exists(tmp.path() / "match_file"));
  EXPECT_TRUE(fs::exists(tmp.path() / "match_dir"));
}

// _____________________________________________________________________________
TEST(DeleteFilesInDirectory, predicateMatchingNothingDeletesNothing) {
  TempDir tmp;
  touch(tmp.path() / "a");
  touch(tmp.path() / "b");

  size_t deleted =
      deleteFilesInDirectory(tmp.path(), [](const fs::path&) { return false; });

  EXPECT_EQ(deleted, 0u);
  EXPECT_TRUE(fs::exists(tmp.path() / "a"));
  EXPECT_TRUE(fs::exists(tmp.path() / "b"));
}

// _____________________________________________________________________________
TEST(DeleteFilesInDirectory, nonExistentDirectoryReturnsZero) {
  TempDir tmp;
  fs::path missing = tmp.path() / "does-not-exist";
  size_t deleted =
      deleteFilesInDirectory(missing, [](const fs::path&) { return true; });
  EXPECT_EQ(deleted, 0u);
}

// _____________________________________________________________________________
TEST(DeleteFilesInDirectory, pathThatIsNotADirectoryReturnsZero) {
  TempDir tmp;
  auto file = tmp.path() / "a-file";
  touch(file);
  size_t deleted =
      deleteFilesInDirectory(file, [](const fs::path&) { return true; });
  EXPECT_EQ(deleted, 0u);
  EXPECT_TRUE(fs::exists(file));
}
