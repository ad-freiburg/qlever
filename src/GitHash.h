// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Several constants. The values of these constants reside in the
// File `GitHash.cpp` which is created and linked by CMake.

#pragma once
namespace qlever::version {
// The git hash of the commit that was used to build QLever.
extern const char* GitHash;
// The date and time at which qlever was compiled.
extern const char* DatetimeOfCompilation;
}  // namespace qlever::version
