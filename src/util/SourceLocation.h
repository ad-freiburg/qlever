//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

// This compatibility header allows the usage of `ad_utility::source_location`
// (an alias to C++20's `std::source_location`) consistently across GCC and
// Clang. Since in the meantime we only support compilers and standard
// libraries that include the `<source_location>` header,
// `ad_utility::source_location` is merely an alias for `std::source_location`.

#ifndef QLEVER_SRC_UTIL_SOURCELOCATION_H
#define QLEVER_SRC_UTIL_SOURCELOCATION_H

#include <source_location>
namespace ad_utility {
using source_location = std::source_location;
}

#endif  // QLEVER_SRC_UTIL_SOURCELOCATION_H
