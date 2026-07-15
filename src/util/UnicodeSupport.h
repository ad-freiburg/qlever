// Copyright 2026 The QLever Authors, in particular:
//
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_UNICODESUPPORT_H
#define QLEVER_SRC_UTIL_UNICODESUPPORT_H

#include <stdexcept>

namespace ad_utility {
// The default value for the `useICU` template parameter of the various string
// functions that have both an ICU-based and an ICU-free implementation (for
// example `ad_utility::utf8ToLower`). It is `false` exactly if the `NO_UNICODE`
// CMake option was set (which defines the `QLEVER_NO_UNICODE` macro).
#ifdef QLEVER_NO_UNICODE
inline constexpr bool useICUDefault = false;
#else
inline constexpr bool useICUDefault = true;
#endif
}  // namespace ad_utility

// Macro for code that requires ICU. In a regular build
// `QLEVER_UNICODE_ONLY(msg,
// ...)` expands to its variadic arguments (the ICU code). In an ICU-free build
// (`QLEVER_NO_UNICODE` defined) it instead throws a `std::runtime_error` and
// discards the ICU code, so that the code is never compiled and no ICU symbols
// are required. `msg` must be a string literal naming the call site. Terminate
// the invocation with a semicolon like any statement.
//
// Note: the ICU code must not contain unbalanced parentheses or preprocessor
// directives, as it is passed as a macro argument.
#ifdef QLEVER_NO_UNICODE
#define QLEVER_UNICODE_ONLY(msg, ...)                             \
  throw std::runtime_error(                                       \
      "The operation '" msg                                       \
      "' requires ICU, but QLever was built without ICU support " \
      "(the `NO_UNICODE` CMake option was set).")
#else
#define QLEVER_UNICODE_ONLY(msg, ...) __VA_ARGS__
#endif

#endif  // QLEVER_SRC_UTIL_UNICODESUPPORT_H
