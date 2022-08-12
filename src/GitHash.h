// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Several constants. The values of these constants reside in the
// File `GitHash.cpp` which is created and linked by Cmake.

#pragma once
namespace qlever::version {
// The git hash of the commit that was used to build qlever.
extern const char* GitHash;
// The date and time during which qlever was compiled.
extern const char* DatetimeOfCompilation;
}  // namespace qlever::version
