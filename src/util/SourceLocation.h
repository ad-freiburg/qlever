//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

//  This compatibility header allows the usage of `ad_utility::source_location`
//  (an alias to C++20's `std::source_location`) consistently across GCC and
//  Clang. It is necessary, because `source_location` is currently still in the
//  `std::experimental` namespace for Clang.

#pragma once

#if __has_include(<source_location>)
#include <source_location>
namespace ad_utility {
using source_location = std::source_location;
}
#else
namespace ad_utility {
class source_location {
 public:
  constexpr std::uint32_t line() const noexcept {
    return 42;
  }
  constexpr std::uint32_t column() const noexcept {
    return 42;
  }

  constexpr const char* file_name() const noexcept {
    return "QLever was compiled without support for std::source_location";
  }

  constexpr const char* function_name() const noexcept {
    return "QLever was compiled without support for std::source_location";
  }

  static source_location current() {
    return {};
  }
};

#define QLEVER_NO_SOURCE_LOCATION
}
#endif
