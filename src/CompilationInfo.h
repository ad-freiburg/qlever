// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Several constants. The values of these constants reside in the
// File `CompilationInfo.cpp` which is created and linked by Cmake.

#pragma once
#include <string>
namespace qlever::version {
// The git hash of the commit that was used to build qlever.
extern std::string GitHash;
// The date and time during which qlever was compiled.
extern std::string DatetimeOfCompilation;
}  // namespace qlever::version
