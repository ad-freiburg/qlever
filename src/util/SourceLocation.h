// Copyright 2022 - 2025 The QLever Authors, in particular:
//
// 2022 Julian Mundhahs (mundhahj@informatik.uni-freiburg.de), UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// This compatibility header allows the usage of `ad_utility::source_location`
// consistently in `C++17` and `C++20` mode. It consists of:
// 1. A class [alias] `ad_utility::source_location`, which in `C++20` is just an
// alias for `std::source_location`, and in C++17 is a struct with the same
// structure as `std::source_location`.
// 2. A macro `AD_CURRENT_SOURCE_LOC()` which in C++20 is just
// `source_location::current()`, and in C++17 it implements the similar
// functionality using the `__builtin_FILE()` etc. macros. Note: In C++17 mode
// currently the `column` of a `source_location` is always reported as `0`,
// because the builtin for the column info doesn't yet exist in GCC 8.3.

#ifndef QLEVER_SRC_UTIL_SOURCELOCATION_H
#define QLEVER_SRC_UTIL_SOURCELOCATION_H

#include <cstdint>

#ifndef QLEVER_CPP_17
#include <source_location>
#endif
namespace ad_utility {

#ifdef QLEVER_CPP_17
// Backported implementation of `source_location` for C++17.
struct source_location {
  const char* file_;
  std::uint_least32_t line_;
  const char* function_;

  static source_location currentImpl(const char* file, std::uint_least32_t line,
                                     const char* function) {
    return {file, line, function};
  }

  constexpr std::uint_least32_t line() const noexcept { return line_; }
  constexpr const char* file_name() const noexcept { return file_; }
  constexpr const char* function_name() const noexcept { return function_; }

  // Columns are not supported in GCC 8.3
  // This can be updated once we have newer compilers etc. available.
  constexpr std::uint_least32_t column() const { return 0; }
};

#define AD_CURRENT_SOURCE_LOC()               \
  ::ad_utility::source_location::currentImpl( \
      __builtin_FILE(), __builtin_LINE(), __builtin_FUNCTION())

#else
using std::source_location;
#define AD_CURRENT_SOURCE_LOC() ::std::source_location::current()
#endif

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_SOURCELOCATION_H
