//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
//  You may not use this file except in compliance with the Apache 2.0 License,
//  which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "backports/filesystem.h"
#include "backports/memory_resource.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/GTestHelpers.h"
#include "util/Log.h"
#include "util/UninitializedAllocator.h"

namespace fs = ql::filesystem;
using qlever::util::copyFileAtomically;
using qlever::util::deleteFilesInDirectory;
using qlever::util::doesDirectoryContainFileWithBasename;
using qlever::util::filesWithBaseNameAndSuffix;
using qlever::util::readBinaryFile;
using qlever::util::writeFileAtomically;
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

  // This only works if the test is not run inside `/`, so we switch to a
  // temporary directory.
  TempDir tmpDir;
  auto originalCwd = fs::current_path();
  fs::current_path(tmpDir.path());
  absl::Cleanup restoreCwd{[&originalCwd]() { fs::current_path(originalCwd); }};
  EXPECT_FALSE(isSubdirectoryOf("/malicious-path", "relative-path"));
  EXPECT_FALSE(isSubdirectoryOf("../malicious-path", "relative-path"));
}

// _____________________________________________________________________________
TEST(FilesWithBaseNameAndSuffix, returnsOnlyFilesWithBaseNameAndSuffix) {
  TempDir tmp;
  auto base = tmp.path() / "index";
  // Files that match `<base>.vocabulary*`.
  touch(tmp.path() / "index.vocabulary");
  touch(tmp.path() / "index.vocabulary.words");
  touch(tmp.path() / "index.vocabulary.ids");
  // Files that must NOT match: wrong suffix, wrong base name, a prefix of the
  // base name, and a file whose name is only a prefix of `<base>.vocabulary`.
  touch(tmp.path() / "index.meta");
  touch(tmp.path() / "other.vocabulary");
  touch(tmp.path() / "index");
  touch(tmp.path() / "index.vocab");

  auto result = filesWithBaseNameAndSuffix(base, ".vocabulary");
  std::vector expected{tmp.path() / "index.vocabulary",
                       tmp.path() / "index.vocabulary.words",
                       tmp.path() / "index.vocabulary.ids"};
  EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(FilesWithBaseNameAndSuffix, baseNameWithoutDirectoryUsesCwd) {
  TempDir tmp;
  auto oldCwd = fs::current_path();
  fs::current_path(tmp.path());
  // Restore the working directory before `tmp` is removed (cleanups run in
  // reverse order of declaration).
  absl::Cleanup restoreCwd{[&oldCwd] { fs::current_path(oldCwd); }};

  touch("index.vocabulary");
  touch("index.vocabulary.words");
  touch("other.vocabulary");  // must NOT match (different base name)

  // A base name without a directory component, so `parent_path()` is empty.
  // The returned paths must be bare file names (same form as the base name),
  // so that they textually start with the base name.
  auto result = filesWithBaseNameAndSuffix("index", ".vocabulary");
  EXPECT_THAT(result, ::testing::UnorderedElementsAre(
                          fs::path{"index.vocabulary"},
                          fs::path{"index.vocabulary.words"}));
}

// _____________________________________________________________________________
TEST(FilesWithBaseNameAndSuffix, ignoresSubdirectories) {
  TempDir tmp;
  auto base = tmp.path() / "index";
  touch(tmp.path() / "index.view.a");
  // A subdirectory that matches the prefix must be ignored (only regular files
  // are returned).
  fs::create_directory(tmp.path() / "index.view.dir");

  auto result = filesWithBaseNameAndSuffix(base, ".view.");
  EXPECT_THAT(result,
              ::testing::UnorderedElementsAre(tmp.path() / "index.view.a"));
}

// _____________________________________________________________________________
TEST(FilesWithBaseNameAndSuffix, noMatchReturnsEmpty) {
  TempDir tmp;
  touch(tmp.path() / "index.meta");
  auto result = filesWithBaseNameAndSuffix(tmp.path() / "index", ".vocabulary");
  EXPECT_THAT(result, ::testing::IsEmpty());
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

// Return the names of all entries directly contained in `directory`, sorted.
std::vector<std::string> entryNames(const fs::path& directory) {
  std::vector<std::string> names;
  for (const auto& entry : fs::directory_iterator{directory}) {
    names.push_back(entry.path().filename().string());
  }
  std::sort(names.begin(), names.end());
  return names;
}

// _____________________________________________________________________________
TEST(FilesystemHelpers, readWriteAndCopyFilesAtomically) {
  TempDir dir;
  auto file = dir.path() / "file";
  auto copy = dir.path() / "copy";

  // Bytes that are not valid text (including a null byte) round-trip.
  std::vector<char> bytes{'a', '\0', '\n', static_cast<char>(0xff), 'b'};
  writeFileAtomically(file, bytes);
  EXPECT_EQ(readBinaryFile(file), bytes);
  // No temporary file is left behind.
  EXPECT_THAT(entryNames(dir.path()), ::testing::ElementsAre("file"));

  // An existing file is replaced, also by an empty one.
  writeFileAtomically(file, std::vector<char>{'x'});
  EXPECT_EQ(readBinaryFile(file), std::vector<char>{'x'});
  writeFileAtomically(file, std::vector<char>{});
  EXPECT_TRUE(readBinaryFile(file).empty());

  writeFileAtomically(file, bytes);
  copyFileAtomically(file, copy);
  EXPECT_EQ(readBinaryFile(copy), bytes);
  writeFileAtomically(file, std::vector<char>{'y'});
  copyFileAtomically(file, copy);
  EXPECT_EQ(readBinaryFile(copy), std::vector<char>{'y'});
  EXPECT_THAT(entryNames(dir.path()), ::testing::ElementsAre("copy", "file"));

  // Missing files and directories are reported.
  EXPECT_ANY_THROW(readBinaryFile(dir.path() / "missing"));
  EXPECT_ANY_THROW(writeFileAtomically(dir.path() / "missing" / "file", bytes));
  EXPECT_ANY_THROW(copyFileAtomically(dir.path() / "missing", copy));
  EXPECT_EQ(readBinaryFile(copy), std::vector<char>{'y'});
}

// Test that `readBinaryFile` reads into other containers and with a custom
// allocator, and that it rejects paths that are not regular files.
TEST(FilesystemHelpers, readBinaryFileContainersAndErrors) {
  TempDir dir;
  auto file = dir.path() / "file";
  std::string content{"some\0content", 12};
  writeFileAtomically(file, content);

  EXPECT_EQ(readBinaryFile<std::string>(file), content);
  using DefaultInitVector =
      std::vector<char, ad_utility::default_init_allocator<char>>;
  auto defaultInit = readBinaryFile<DefaultInitVector>(file);
  EXPECT_EQ((std::string{defaultInit.begin(), defaultInit.end()}), content);
  // The allocator is passed to the container.
  ql::pmr::monotonic_buffer_resource resource;
  using PmrVector = std::vector<char, ql::pmr::polymorphic_allocator<char>>;
  auto withAllocator = readBinaryFile<PmrVector>(
      file, ql::pmr::polymorphic_allocator<char>{&resource});
  EXPECT_EQ(withAllocator.get_allocator().resource(), &resource);
  EXPECT_EQ((std::string{withAllocator.begin(), withAllocator.end()}), content);

  // An empty file yields an empty container.
  auto empty = dir.path() / "empty";
  writeFileAtomically(empty, std::vector<char>{});
  EXPECT_TRUE(readBinaryFile<std::string>(empty).empty());

  // A file that does not exist and a directory are rejected with a readable
  // error message.
  AD_EXPECT_THROW_WITH_MESSAGE(
      readBinaryFile(dir.path() / "missing"),
      HasSubstr("does not exist or is not a regular file"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      readBinaryFile(dir.path()),
      HasSubstr("does not exist or is not a regular file"));
}

// Test the overload of `writeFileAtomically` that writes the temporary file via
// a callback.
TEST(FilesystemHelpers, writeFileAtomicallyWithCallback) {
  TempDir dir;
  auto file = dir.path() / "file";

  // The callback writes to a temporary file in the same directory, whose name
  // starts with the name of the target and ends with `.tmp`, and which is
  // different for each call.
  std::vector<fs::path> temporaryPaths;
  auto write = [&temporaryPaths](std::string content) {
    return [&temporaryPaths, content](const fs::path& temporaryPath) {
      temporaryPaths.push_back(temporaryPath);
      EXPECT_FALSE(fs::exists(temporaryPath));
      ad_utility::makeOfstream(temporaryPath) << content;
    };
  };
  writeFileAtomically(file, write("first"));
  writeFileAtomically(file, write("second"));
  EXPECT_EQ(readBinaryFile<std::string>(file), "second");
  ASSERT_EQ(temporaryPaths.size(), 2u);
  EXPECT_NE(temporaryPaths.at(0), temporaryPaths.at(1));
  for (const auto& temporaryPath : temporaryPaths) {
    EXPECT_EQ(temporaryPath.parent_path(), dir.path());
    EXPECT_THAT(temporaryPath.filename().string(),
                ::testing::AllOf(::testing::StartsWith("file."),
                                 ::testing::EndsWith(".tmp")));
  }
  EXPECT_THAT(entryNames(dir.path()), ::testing::ElementsAre("file"));

  // If the callback throws, the temporary file is removed, the target is left
  // unchanged, and the exception is propagated.
  AD_EXPECT_THROW_WITH_MESSAGE(
      writeFileAtomically(file,
                          [](const fs::path& temporaryPath) {
                            ad_utility::makeOfstream(temporaryPath) << "third";
                            throw std::runtime_error{"callback failed"};
                          }),
      HasSubstr("callback failed"));
  EXPECT_EQ(readBinaryFile<std::string>(file), "second");
  EXPECT_THAT(entryNames(dir.path()), ::testing::ElementsAre("file"));

  // If the renaming fails (here because the target is a non-empty directory),
  // the temporary file is removed as well.
  auto directory = dir.path() / "directory";
  fs::create_directory(directory);
  touch(directory / "entry");
  EXPECT_ANY_THROW(writeFileAtomically(directory, write("fourth")));
  EXPECT_THAT(entryNames(dir.path()),
              ::testing::ElementsAre("directory", "file"));
}

// Test that `detail::readFileIntoBuffer` throws if the file holds fewer bytes
// than requested, or if it cannot be read at all.
TEST(FilesystemHelpers, readFileIntoBufferFailures) {
  TempDir dir;
  auto file = dir.path() / "file";
  writeFileAtomically(file, std::string{"abc"});

  // Reading exactly the size of the file works.
  std::vector<char> buffer(3);
  qlever::util::detail::readFileIntoBuffer(file, buffer);
  EXPECT_EQ((std::string{buffer.begin(), buffer.end()}), "abc");

  // A file that is shorter than the buffer (e.g. because it was truncated after
  // its size was determined) is reported.
  std::vector<char> tooLarge(10);
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlever::util::detail::readFileIntoBuffer(file, tooLarge),
      HasSubstr("expected 10 bytes, but read 3"));

  // A directory can be opened on Linux, but reading from it fails (with
  // `EISDIR`), so `pread` returns a negative value, the reason of which is
  // reported. On other platforms, the opening may already fail.
  std::vector<char> oneByte(1);
#ifdef __linux__
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlever::util::detail::readFileIntoBuffer(dir.path(), oneByte),
      ::testing::AllOf(HasSubstr("Could not read the file"),
                       HasSubstr(std::strerror(EISDIR))));
#else
  EXPECT_ANY_THROW(
      qlever::util::detail::readFileIntoBuffer(dir.path(), oneByte));
#endif
}

// Test that `writeFileAtomically` throws if writing the bytes fails after the
// temporary file was successfully opened, and that it then leaves neither the
// target nor a temporary file behind. The failure is provoked by lowering the
// limit of the file size (`RLIMIT_FSIZE`) of this process, so that the `write`
// fails with `EFBIG` (the signal `SIGXFSZ` that is raised in this case is
// ignored, as it would otherwise terminate the process). The reason of the
// failure is part of the message.
TEST(FilesystemHelpers, writeFileAtomicallyWriteFailure) {
  TempDir dir;
  auto file = dir.path() / "file";

  struct rlimit oldLimit {};
  ASSERT_EQ(getrlimit(RLIMIT_FSIZE, &oldLimit), 0);
  auto oldHandler = std::signal(SIGXFSZ, SIG_IGN);
  ASSERT_NE(oldHandler, SIG_ERR);
  absl::Cleanup restore = [&oldLimit, oldHandler] {
    setrlimit(RLIMIT_FSIZE, &oldLimit);
    std::signal(SIGXFSZ, oldHandler);
  };
  struct rlimit newLimit = oldLimit;
  newLimit.rlim_cur = 1;
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &newLimit), 0);

  AD_EXPECT_THROW_WITH_MESSAGE(
      writeFileAtomically(file, std::vector<char>(100, 'x')),
      ::testing::AllOf(HasSubstr("Could not write the file"),
                       HasSubstr(std::strerror(EFBIG))));
  std::move(restore).Invoke();

  EXPECT_TRUE(entryNames(dir.path()).empty());
}

// Test that `writeFileAtomically` throws if the temporary file cannot be
// opened (here because the directory is not writable), with the reason in the
// message, and that it leaves no file behind. (When the test runs as `root`,
// the directory is writable anyway, so the test is skipped.)
TEST(FilesystemHelpers, writeFileAtomicallyOpenFailure) {
  if (geteuid() == 0) {
    GTEST_SKIP() << "`root` can write to a read-only directory";
  }
  TempDir dir;
  auto subdir = dir.path() / "subdir";
  fs::create_directory(subdir);
  fs::permissions(subdir, fs::perms::owner_read | fs::perms::owner_exec);
  absl::Cleanup restorePermissions = [&subdir] {
    fs::permissions(subdir, fs::perms::owner_all);
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      writeFileAtomically(subdir / "file", std::string{"x"}),
      ::testing::AllOf(HasSubstr("Could not open the file"),
                       HasSubstr(std::strerror(EACCES))));
  std::move(restorePermissions).Invoke();
  EXPECT_TRUE(entryNames(subdir).empty());
}

// Test that `writeFileAtomically` works for a file name of the maximal length,
// for which the name of the temporary file has to be shortened.
TEST(FilesystemHelpers, writeFileAtomicallyLongFilename) {
  TempDir dir;
  std::string filename(255, 'a');
  auto file = dir.path() / filename;
  writeFileAtomically(file, [](const fs::path& temporaryPath) {
    EXPECT_LE(temporaryPath.filename().string().size(), 255u);
    EXPECT_THAT(temporaryPath.filename().string(), ::testing::EndsWith(".tmp"));
    ad_utility::makeOfstream(temporaryPath) << "content";
  });
  EXPECT_EQ(readBinaryFile<std::string>(file), "content");
  EXPECT_THAT(entryNames(dir.path()), ::testing::ElementsAre(filename));
}

// Test that `writeFileAtomically` and `copyFileAtomically` write to the file
// that a symbolic link points to (and keep the link), and that `readBinaryFile`
// reads through a symbolic link.
TEST(FilesystemHelpers, symbolicLinks) {
  TempDir dir;
  auto real = dir.path() / "real";
  auto link = dir.path() / "link";
  auto linkToLink = dir.path() / "linkToLink";
  writeFileAtomically(real, std::string{"original"});
  // A relative link, which is resolved relative to its directory.
  fs::create_symlink("real", link);
  fs::create_symlink(link, linkToLink);
  EXPECT_EQ(readBinaryFile<std::string>(linkToLink), "original");

  writeFileAtomically(linkToLink, std::string{"new"});
  EXPECT_TRUE(fs::is_symlink(link));
  EXPECT_TRUE(fs::is_symlink(linkToLink));
  EXPECT_EQ(readBinaryFile<std::string>(real), "new");

  auto source = dir.path() / "source";
  writeFileAtomically(source, std::string{"copied"});
  copyFileAtomically(source, link);
  EXPECT_TRUE(fs::is_symlink(link));
  EXPECT_EQ(readBinaryFile<std::string>(real), "copied");
  EXPECT_THAT(entryNames(dir.path()),
              ::testing::ElementsAre("link", "linkToLink", "real", "source"));

  // A dangling link cannot be read, but writing to it creates its target.
  auto dangling = dir.path() / "dangling";
  fs::create_symlink("target", dangling);
  AD_EXPECT_THROW_WITH_MESSAGE(
      readBinaryFile(dangling),
      HasSubstr("does not exist or is not a regular file"));
  writeFileAtomically(dangling, std::string{"created"});
  EXPECT_TRUE(fs::is_symlink(dangling));
  EXPECT_EQ(readBinaryFile<std::string>(dir.path() / "target"), "created");

  // A cycle of links is reported.
  fs::create_symlink("cycleB", dir.path() / "cycleA");
  fs::create_symlink("cycleA", dir.path() / "cycleB");
  AD_EXPECT_THROW_WITH_MESSAGE(
      writeFileAtomically(dir.path() / "cycleA", std::string{"x"}),
      HasSubstr("too many levels of symbolic links"));
}

// Test that a file that is replaced by `writeFileAtomically` keeps its
// permissions.
TEST(FilesystemHelpers, writeFileAtomicallyKeepsPermissions) {
  TempDir dir;
  auto file = dir.path() / "file";
  writeFileAtomically(file, std::string{"first"});
  const auto ownerOnly = fs::perms::owner_read | fs::perms::owner_write;
  fs::permissions(file, ownerOnly);
  writeFileAtomically(file, std::string{"second"});
  EXPECT_EQ(fs::status(file).permissions(), ownerOnly);
  EXPECT_EQ(readBinaryFile<std::string>(file), "second");
}

// Test that the exception of a failing callback is propagated (and the process
// is not terminated), also if the temporary file cannot be removed because the
// directory is not writable. (When the test runs as `root`, the removal
// succeeds anyway.)
TEST(FilesystemHelpers, writeFileAtomicallyFailingRemovalOfTemporaryFile) {
  TempDir dir;
  auto subdir = dir.path() / "subdir";
  fs::create_directory(subdir);
  absl::Cleanup restorePermissions = [&subdir] {
    fs::permissions(subdir, fs::perms::owner_all);
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      writeFileAtomically(subdir / "file",
                          [&subdir](const fs::path& temporaryPath) {
                            ad_utility::makeOfstream(temporaryPath) << "x";
                            fs::permissions(subdir, fs::perms::owner_read |
                                                        fs::perms::owner_exec);
                            throw std::runtime_error{"callback failed"};
                          }),
      HasSubstr("callback failed"));
}
