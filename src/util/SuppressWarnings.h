// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <functional>
#include <type_traits>

// This file contains helper functions and macros to suppress false-positive
// warnings on certain compilers. These should be used rarely and only if it is
// really sure that the suppressed warnings are false positives and that there
// is no feasible way to avoid them.

// Macros to disable and re-enable certain warnings on GCC13. Currently some
// (valid) uses of `std::sort` trigger a false positive `-Warray-bounds`
// warning. It is important that there is a corresponding `ENABLE_...` macro
// issued for every `DISABLE_...` macro.
#if __GNUC__ == 13
#define DISABLE_WARNINGS_GCC_13  \
  _Pragma("GCC diagnostic push") \
      _Pragma("GCC diagnostic ignored \"-Warray-bounds\"")

#define ENABLE_WARNINGS_GCC_13 _Pragma("GCC diagnostic pop")
#endif

#ifndef DISABLE_WARNINGS_GCC_13
#define DISABLE_WARNINGS_GCC_13
#define ENABLE_WARNINGS_GCC_13
#endif
