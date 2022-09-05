//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

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
