// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Several constants. The values of these constants reside in the
// File `CompilationInfo.cpp` which is created and linked by CMake.

#ifndef QLEVER_SRC_COMPILATIONINFO_H
#define QLEVER_SRC_COMPILATIONINFO_H

#include <atomic>
#include <string_view>

#include "util/Synchronized.h"
namespace qlever::version {

// The following constants require linking against the `compilationInfo`
// library which is recreated on every compilation.

// Short version of the hash of the commit that was used to compile QLever.
extern const std::string_view GitShortHash;
// The date and time at which QLever was compiled.
extern const std::string_view DatetimeOfCompilation;
// The project version from `git describe --tags --always`.
extern const std::string_view ProjectVersion;

// The following versions of the above constants do NOT require linking
// against the `compilationInfo` library, but only the inclusion of this header.
// They only have meaningful values once the `copyVersionInfo` function (below)
// was called. This is currently done in the `main` functions of
// `IndexBuilderMain.cpp` and `ServerMain.cpp`.
inline ad_utility::Synchronized<std::string_view> gitShortHashWithoutLinking{
    std::string_view{"git short hash not set"}};
inline ad_utility::Synchronized<std::string_view>
    datetimeOfCompilationWithoutLinking{
        std::string_view{"datetime of compilation not set"}};
inline ad_utility::Synchronized<std::string_view> projectVersionWithoutLinking{
    std::string_view{"project version not set"}};

// Copy the values from the constants that require linking to the `inline`
// variables that don't require linking. For details see above.
void copyVersionInfo();
}  // namespace qlever::version

#endif  // QLEVER_SRC_COMPILATIONINFO_H
