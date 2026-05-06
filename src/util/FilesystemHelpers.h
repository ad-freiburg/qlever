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
// Return true if the filesystem contains a file with the prefix `baseName`.
// If no such file exists, this function returns false, and it is safe to write
// an index with the given `baseName` without overwriting any existing files.
bool filesWithPrefixExist(const std::string& baseName);

// Return true iff `path` is a transitive subdirectory of `containerPath`.
// Filenames in the path are ignored, so `/path/to/directory/subdirectory/file`
// counts as subdirectory of `/path/to/directory/file`, but not as subdirectory
// of `/path/to/directory/file/`.
bool prefixPathIsInsideDirectory(const std::string& path,
                                 const std::string& containerPath);
}  // namespace qlever::util

#endif  // QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
