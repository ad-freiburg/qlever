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
// Check if `baseName` is a prefix of an already existing file. If yes, throw an
// exception, otherwise do nothing.
void ensureNoConflictingFiles(const std::string& baseName);
}  // namespace qlever::util

#endif  // QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
