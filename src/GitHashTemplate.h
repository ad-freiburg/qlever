// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// The file `GitHash.h` is automatically created by Cmake from the file
// `GitHashTemplate.h`. The value of the variables `${VAR_NAME}` is filled in by the `CMakeLists.txt` (see there for details).

#pragma once
#include <string_view>
namespace qlever::version {
// The git hash of the commit was used to build qlever.
static constexpr std::string_view GitHash = ${GIT_HASH};
// The date and time during which qlever was compiled.
static constexpr std::string_view DatetimeOfCompilation =${DATETIME_OF_COMPILATION};
}
