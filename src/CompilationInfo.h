// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Several constants. The values of these constants reside in the
// File `CompilationInfo.cpp` which is created and linked by CMake.

#pragma once
#include <string_view>
namespace qlever::version {
// Short version of the hash of the commit that was used to QLever.
extern const std::string_view GitShortHash;
// The date and time at which QLever was compiled.
extern const std::string_view DatetimeOfCompilation;
}  // namespace qlever::version
