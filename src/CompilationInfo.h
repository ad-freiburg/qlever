// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Several constants. The values of these constants reside in the
// File `CompilationInfo.cpp` which is created and linked by CMake.

#pragma once
#include <string>
namespace qlever::version {
// The git hash of the commit that was used to build QLever.
extern std::string GitHash;
// The date and time at which QLever was compiled.
extern std::string DatetimeOfCompilation;
}  // namespace qlever::version
