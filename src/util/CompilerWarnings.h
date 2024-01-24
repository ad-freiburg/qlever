//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILERWARNINGS_H
#define QLEVER_COMPILERWARNINGS_H

/// Header file that allows suppressing specific warnings in certain compiler
/// versions that turn out to be false positives.

#if defined(__GNUC__) && (__GNUC__ == 12 || __GNUC__ == 13)
#define SUPPRESS_WARNINGS(code)                          \
  _Pragma("GCC diagnostic push")                         \
      _Pragma("GCC diagnostic ignored \"-Weverything\"") \
          code _Pragma("GCC diagnostic pop")
#else
#define SUPPRESS_WARNINGS(code) code
#endif

#endif  // QLEVER_COMPILERWARNINGS_H
