// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COVERAGE_H
#define QLEVER_SRC_UTIL_COVERAGE_H

// Exclude a function (typically a small, immediately called lambda) from the
// coverage instrumentation. Use it for the few code paths that cannot be
// reached in a unit test (e.g. the failure of a system call that cannot be
// provoked), so that they do not show up as coverage gaps again whenever the
// surrounding code is touched. Example:
//
//   [fd]() QL_EXCLUDE_FROM_COVERAGE {
//     if (::close(fd) != 0) {
//       throw ...;
//     }
//   }();
//
// NOTE: Clang's source-based coverage (which the coverage build uses)
// attributes the lines of such a function to the enclosing code region and
// creates no branch regions for them; GCC's `gcov` honors the attribute only
// partially. The `LCOV_EXCL_...` comments do not work for this purpose, as
// neither `llvm-cov` (before version 23) nor Codecov evaluate them.
#if defined(__GNUC__) || defined(__clang__)
#define QL_EXCLUDE_FROM_COVERAGE __attribute__((no_profile_instrument_function))
#else
#define QL_EXCLUDE_FROM_COVERAGE
#endif

#endif  // QLEVER_SRC_UTIL_COVERAGE_H
