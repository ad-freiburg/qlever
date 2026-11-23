//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/FilesystemHelpers.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "Views.h"
#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "util/File.h"
#include "util/Random.h"

namespace qlever::util {
namespace fs = ql::filesystem;

// _____________________________________________________________________________
std::vector<fs::path> filesWithBaseNameAndSuffix(const fs::path& onDiskBase,
                                                 std::string_view suffix) {
  fs::path parent = onDiskBase.parent_path();
  // For a base name without a directory component, iterate the current
  // directory, but do NOT prepend it to the returned paths (see below).
  fs::path directory = parent.empty() ? fs::current_path() : parent;
  std::string prefix =
      absl::StrCat(ql::pathFilename(onDiskBase).string(), suffix);
  namespace v = ql::views;
  // With an InputRangeTypeErased (instead of `to_vector`), `ql::directoryRange`
  // backed by the boost filesystem library doesn't work.
  return ::ranges::to_vector(ql::directoryRange(directory)) |
         v::filter(ql::isRegularFile) |
         // Return the paths in the same form as `onDiskBase` (directory part of
         // `onDiskBase` plus the file name; an empty `parent` yields the bare
         // file name), so that they textually start with `onDiskBase`. Callers
         // like `Qlever::moveRebuiltIndexIntoPlace` rely on this to replace the
         // base-name prefix of each file.
         v::transform([&parent](const auto& entry) {
           return parent / entry.path().filename();
         }) |
         v::filter([&prefix](const auto& path) {
           return ql::starts_with(path.filename().string(), prefix);
         }) |
         ::ranges::to_vector;
}

// _____________________________________________________________________________
bool doesDirectoryContainFileWithBasename(const std::string& baseName) {
  fs::path base = fs::absolute(baseName);
  fs::path dir = base.parent_path();
  if (!fs::exists(dir)) {
    return false;
  }

  if (!fs::is_directory(dir)) {
    return true;
  }

  // Use `ql::pathFilename` (not `base.filename()`) so that a trailing separator
  // yields an empty prefix (matching any file in `dir`) for both `std` and
  // `boost` filesystem; see the comment on `ql::pathFilename`.
  std::string prefix = ql::pathFilename(base).string();
  return ql::ranges::any_of(
      ql::directoryRange(dir), [&prefix](const auto& entry) {
        std::string name = entry.path().filename().string();
        return ql::starts_with(name, prefix);
      });
}

// _____________________________________________________________________________
size_t deleteFilesInDirectory(
    const fs::path& directory,
    absl::FunctionRef<bool(const fs::path&)> shouldDelete, bool warnOnFailure) {
  if (!fs::exists(directory) || !fs::is_directory(directory)) {
    return 0;
  }
  // Collect the matching regular files first and delete them only afterwards.
  // Deleting entries while iterating a directory is unspecified behavior and
  // can cause entries to be skipped on some platforms (observed on macOS),
  // leaving leftover files behind.
  std::vector<fs::path> toDelete;
  for (const auto& entry : ql::directoryRange(directory)) {
    if (ql::isRegularFile(entry) && shouldDelete(entry.path())) {
      toDelete.push_back(entry.path());
    }
  }
  for (const auto& path : toDelete) {
    ad_utility::deleteFile(path, warnOnFailure);
  }
  return toDelete.size();
}

// _____________________________________________________________________________
std::vector<fs::path> directoriesWithPrefix(const fs::path& directory,
                                            std::string_view prefix) {
  std::vector<fs::path> result;
  for (const auto& entry : ql::directoryRange(directory)) {
    if (ql::isDirectory(entry) &&
        ql::starts_with(entry.path().filename().string(), prefix)) {
      result.push_back(entry.path());
    }
  }
  return result;
}

// _____________________________________________________________________________
bool isSubdirectoryOf(const std::string& path,
                      const std::string& containerPath) {
  auto normalize = [](const auto& p) {
    return fs::weakly_canonical(fs::absolute(p)).parent_path();
  };
  return ::ranges::starts_with(normalize(path), normalize(containerPath));
}

// _____________________________________________________________________________
size_t detail::sizeOfRegularFile(const fs::path& path) {
  if (!fs::is_regular_file(path)) {
    throw std::runtime_error{
        absl::StrCat("The file \"", path.string(),
                     "\" does not exist or is not a regular file")};
  }
  return static_cast<size_t>(fs::file_size(path));
}

namespace {
// Throw an exception that says that the `operation` (e.g. "write") on the file
// `path` failed, including the description of the `errno` value
// `errorNumber`.
[[noreturn]] void throwFileError(std::string_view operation,
                                 const fs::path& path, int errorNumber) {
  throw std::runtime_error{absl::StrCat("Could not ", operation, " the file \"",
                                        path.string(),
                                        "\": ", std::strerror(errorNumber))};
}

// Return the path that `path` refers to after following all symbolic links of
// its last component (also if the final target does not exist yet). The
// directories of `path` are not resolved, as they do not matter for where the
// file is written.
fs::path resolveSymlinks(const fs::path& path) {
  // The same limit as `MAXSYMLINKS` on Linux, to detect cycles.
  constexpr size_t maxNumSymlinks = 40;
  fs::path resolved = path;
  for (size_t i = 0; fs::is_symlink(resolved); ++i) {
    if (i == maxNumSymlinks) {
      throw std::runtime_error{
          absl::StrCat("Could not resolve the file \"", path.string(),
                       "\": too many levels of symbolic links")};
    }
    fs::path target = fs::read_symlink(resolved);
    resolved = target.is_absolute() ? target : resolved.parent_path() / target;
  }
  return resolved;
}

// Return the path of a fresh temporary file in the same directory as `path`:
// the file name of `path`, followed by a random component and the suffix
// `.tmp`. If that name would exceed the maximal length of a file name, the
// part that stems from the file name of `path` is shortened.
fs::path temporaryPathFor(const fs::path& path) {
  // The maximal length of a file name in bytes (`NAME_MAX`) on Linux and macOS.
  constexpr size_t maxFilenameLength = 255;
  constexpr size_t numRandomChars = 8;
  constexpr std::string_view alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  ad_utility::SlowRandomIntGenerator<size_t> randomIndex{0,
                                                         alphabet.size() - 1};
  std::string suffix = ".";
  for (size_t i = 0; i < numRandomChars; ++i) {
    suffix.push_back(alphabet[randomIndex()]);
  }
  suffix += ".tmp";
  std::string filename = path.filename().string();
  filename.resize(std::min(filename.size(), maxFilenameLength - suffix.size()));
  return path.parent_path() / absl::StrCat(filename, suffix);
}
}  // namespace

// _____________________________________________________________________________
void detail::readFileIntoBuffer(const fs::path& path, ql::span<char> buffer) {
  ad_utility::File file{path.string(), "r"};
  auto numBytesRead = file.read(buffer.data(), buffer.size(), 0);
  if (numBytesRead < 0) {
    throwFileError("read", path, errno);
  }
  if (static_cast<size_t>(numBytesRead) != buffer.size()) {
    throw std::runtime_error{absl::StrCat(
        "Could not read the complete file \"", path.string(), "\" (expected ",
        buffer.size(), " bytes, but read ", numBytesRead,
        "), was it modified while it was being read?")};
  }
}

// _____________________________________________________________________________
void writeFileAtomically(
    const fs::path& path,
    absl::FunctionRef<void(const fs::path& temporaryPath)> writeTemporaryFile) {
  // Replace the file a symbolic link points to, not the link itself.
  fs::path target = resolveSymlinks(path);
  // The random component makes the name unique, so that two concurrent writers
  // of the same `path` never write to the same temporary file.
  fs::path temporary = temporaryPathFor(target);
  // Remove the temporary file if writing or renaming it fails. This runs while
  // an exception is propagated, so it must not throw itself; a temporary file
  // that cannot be removed is harmless.
  absl::Cleanup removeTemporary = [&temporary] {
    ql::error_code ignored;
    fs::remove(temporary, ignored);
  };
  writeTemporaryFile(temporary);
  // An existing file keeps its permissions (the temporary file is a new file
  // and has the default permissions).
  ql::error_code statusError;
  auto status = fs::status(target, statusError);
  if (!statusError && fs::exists(status)) {
    fs::permissions(temporary, status.permissions());
  }
  fs::rename(temporary, target);
  std::move(removeTemporary).Cancel();
}

// _____________________________________________________________________________
void writeFileAtomically(const fs::path& path, ql::span<const char> bytes) {
  // Use the POSIX functions directly (and not `std::ofstream`), so that every
  // failure is detected, including one that is only reported when the file is
  // closed, and that its reason (`errno`) is reported.
  writeFileAtomically(path, [bytes](const fs::path& temporary) {
    int fd = ::open(temporary.string().c_str(),
                    O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0666);
    if (fd < 0) {
      throwFileError("open", temporary, errno);
    }
    absl::Cleanup closeOnError = [fd] { ::close(fd); };
    const char* data = bytes.data();
    size_t numBytesLeft = bytes.size();
    while (numBytesLeft > 0) {
      ssize_t numBytesWritten = ::write(fd, data, numBytesLeft);
      if (numBytesWritten < 0) {
        // A `write` to a regular file is practically never interrupted by a
        // signal, so this case cannot be provoked reliably in a unit test.
        // LCOV_EXCL_START
        if (errno == EINTR) {
          continue;
        }
        // LCOV_EXCL_STOP
        throwFileError("write", temporary, errno);
      }
      data += numBytesWritten;
      numBytesLeft -= static_cast<size_t>(numBytesWritten);
    }
    std::move(closeOnError).Cancel();
    if (::close(fd) != 0) {
      // `close` only fails on rare errors of the file system (e.g. a deferred
      // write error of a network file system), which cannot be provoked in a
      // unit test without mocking the POSIX functions.
      throwFileError("close", temporary, errno);  // LCOV_EXCL_LINE
    }
  });
}

// _____________________________________________________________________________
void copyFileAtomically(const fs::path& from, const fs::path& to) {
  // The temporary file has a fresh random name and therefore does not exist
  // yet, so no option to overwrite an existing file is needed (the spelling of
  // that option differs between `std::filesystem` and older versions of
  // `boost::filesystem`).
  writeFileAtomically(to, [&from](const fs::path& temporary) {
    fs::copy_file(from, temporary);
  });
}

}  // namespace qlever::util
