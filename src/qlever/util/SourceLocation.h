//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

//  This compatibility header allows the usage of `ad_utility::source_location`
//  (an alias to C++20's `std::source_location`) consistently across GCC and
//  Clang. It is necessary, because `source_location` is currently still in the
//  `std::experimental` namespace for Clang.

#pragma once

#if defined(__clang__)
#include <experimental/source_location>
namespace ad_utility {
using source_location = std::experimental::source_location;
}
#else
#include <source_location>
namespace ad_utility {
using source_location = std::source_location;
}
#endif
