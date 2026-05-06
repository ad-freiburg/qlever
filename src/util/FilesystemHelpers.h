//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
#define QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H

#include <string>

namespace qlever::util {
// Return `true` if the directory from `path` contains at least one file whose
// name starts with the base name of `path`; otherwise return `false`. If the
// base name is empty, return `false` if and only if the directory is empty.
bool doesDirectoryContainFileWithBasename(const std::string& path);

// Return `true` if the directory from `path1` is a subdirectory of the
// directory from `path2`; otherwise return `false`.
//
// NOTE: The paths are canonicalized. For example, `/to/dir1/../dir2/sub/base`
// (as `path1`) would be detected as a subdirectory of `/to/dir2/` (as `path2`),
// but not of `/to/dir1/` (as `path2`).
bool isSubdirectoryOf(const std::string& path1, const std::string& path2);
}  // namespace qlever::util

#endif  // QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
