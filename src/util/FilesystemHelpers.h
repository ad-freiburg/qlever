//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
#define QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H

#include <absl/functional/function_ref.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "backports/filesystem.h"
#include "backports/span.h"

namespace qlever::util {
// Return the paths of all regular files directly contained in the directory of
// `onDiskBase` whose file name starts with the file name of `onDiskBase`
// immediately followed by `suffix`. For example, for `onDiskBase = "dir/wiki"`
// and `suffix = ".vocabulary"`, all files matching `dir/wiki.vocabulary*` are
// returned. The returned paths have the same directory part as `onDiskBase`
// (in particular, for a base name without a directory component, the bare file
// names are returned), so that each returned path textually starts with
// `onDiskBase`. This is used to enumerate the on-disk files of an index whose
// exact set of names is not known in advance (the vocabulary files and the
// materialized-view files).
std::vector<ql::filesystem::path> filesWithBaseNameAndSuffix(
    const ql::filesystem::path& onDiskBase, std::string_view suffix);

// Return `true` if the directory from `path` contains at least one file whose
// name starts with the base name of `path`; otherwise return `false`. If the
// base name is empty, return `false` if and only if the directory is empty.
bool doesDirectoryContainFileWithBasename(const std::string& path);

// Delete every regular file directly contained in `directory` for which
// `shouldDelete(entryPath)` returns `true`, and return the number of files that
// were deleted. The matching files are collected first and deleted only
// afterwards, because deleting directory entries while iterating a directory is
// unspecified behavior that can cause entries to be skipped on some platforms
// (observed on macOS), leaving leftover files behind. Non-regular entries (e.g.
// subdirectories) are never passed to `shouldDelete` and never deleted. If
// `directory` does not exist or is not a directory, nothing is deleted and `0`
// is returned. If `warnOnFailure` is `false`, a file that cannot be deleted is
// skipped silently (see `ad_utility::deleteFile`).
size_t deleteFilesInDirectory(
    const ql::filesystem::path& directory,
    absl::FunctionRef<bool(const ql::filesystem::path&)> shouldDelete,
    bool warnOnFailure = true);

// Return the paths of the directories directly contained in `directory` whose
// name starts with `prefix`, in unspecified order. This is used to enumerate
// the `previous.*` index directories left behind by index rebuilds.
std::vector<ql::filesystem::path> directoriesWithPrefix(
    const ql::filesystem::path& directory, std::string_view prefix);

// Return `true` if the directory from `path1` is a subdirectory of the
// directory from `path2`; otherwise return `false`.
//
// NOTE: The paths are canonicalized. For example, `/to/dir1/../dir2/sub/base`
// (as `path1`) would be detected as a subdirectory of `/to/dir2/` (as `path2`),
// but not of `/to/dir1/` (as `path2`).
bool isSubdirectoryOf(const std::string& path1, const std::string& path2);

namespace detail {
// Return the size of the file `path` in bytes. Symbolic links are followed.
// Throw if `path` does not exist or is not a regular file (a directory, for
// example, can be opened like a file, but its reported size is meaningless).
size_t sizeOfRegularFile(const ql::filesystem::path& path);

// Read exactly `buffer.size()` bytes from the beginning of the file `path` into
// `buffer`. Throw if the file cannot be opened or read (with the reason in the
// message), or if it holds fewer bytes.
void readFileIntoBuffer(const ql::filesystem::path& path,
                        ql::span<char> buffer);
}  // namespace detail

// Read the whole file `path` into a `Container` of bytes (a contiguous
// container of `char` with an `allocator_type` and a `resize` member function,
// for example `std::vector<char>`, `std::string`, or a vector with a custom
// allocator), which uses `allocator` for its storage. The size of the file is
// determined up front, so the bytes are read with a single allocation and
// without a byte-wise copy; with an allocator that default-initializes (see
// `ad_utility::default_init_allocator`) the buffer is also not zeroed before it
// is overwritten. Symbolic links are followed. Throw if `path` does not exist,
// is not a regular file (or a symbolic link to one), or cannot be read
// completely (for example because it is truncated while it is being read).
template <typename Container = std::vector<char>>
Container readBinaryFile(const ql::filesystem::path& path,
                         typename Container::allocator_type allocator = {}) {
  static_assert(std::is_same_v<typename Container::value_type, char>,
                "`readBinaryFile` reads into a container of `char`");
  Container result(allocator);
  result.resize(detail::sizeOfRegularFile(path));
  detail::readFileIntoBuffer(path,
                             ql::span<char>{result.data(), result.size()});
  return result;
}

// Create the file `path` atomically: call `writeTemporaryFile` with the path of
// a temporary file in the same directory, which has to create that file, and
// then rename the temporary file to `path`. The name of the temporary file
// consists of the file name of `path` (shortened if necessary, so that the name
// does not exceed the maximal length of a file name), a random component (so
// that concurrent writers of the same `path` do not collide), and the suffix
// `.tmp`. If `path` is a symbolic link, the file it (finally) points to is
// written instead, so that the link is kept. An existing file is replaced (see
// the NOTE below for which of its properties are kept). If `writeTemporaryFile`
// or the renaming throws, the temporary file is removed, the file is left
// unchanged, and the exception is propagated.
//
// NOTE: The replacement creates a new file. Its permission bits are copied from
// the replaced file, but its owner and group are those of the writing process
// (only a privileged process could keep the original owner), and other hard
// links to the replaced file keep referring to the old contents. Creating the
// new file requires write permission on the directory of the file. The data is
// not explicitly synced to disk (no `fsync`), so the replacement is atomic with
// respect to other processes and to crashes of this process, but not
// necessarily with respect to a crash of the operating system.
void writeFileAtomically(
    const ql::filesystem::path& path,
    absl::FunctionRef<void(const ql::filesystem::path& temporaryPath)>
        writeTemporaryFile);

// Write `bytes` atomically to `path` (via a temporary file, see the overload
// above). An existing file `path` is replaced. Throw if the file cannot be
// opened, written, or closed, with the reason (e.g. a full disk) in the
// message.
void writeFileAtomically(const ql::filesystem::path& path,
                         ql::span<const char> bytes);

// Copy the file `from` atomically to `to` (via a temporary file, see
// `writeFileAtomically`). An existing file `to` is replaced.
void copyFileAtomically(const ql::filesystem::path& from,
                        const ql::filesystem::path& to);
}  // namespace qlever::util

#endif  // QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
