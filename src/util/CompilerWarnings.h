//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILERWARNINGS_H
#define QLEVER_COMPILERWARNINGS_H

/// Helper macros that allow suppressing specific warnings in certain compiler
/// versions that turn out to be false positives.

#if defined(__GNUC__) && (__GNUC__ >= 11 && __GNUC__ <= 13)

// Disable the `maybe-uninitialized` warning, which has many false positives.
#define DISABLE_UNINITIALIZED_WARNINGS \
  _Pragma("GCC diagnostic push")       \
      _Pragma("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")

// Disable the `stringop-overread` warning, which has many false positives.
#define DISABLE_OVERREAD_WARNINGS \
  _Pragma("GCC diagnostic push")  \
      _Pragma("GCC diagnostic ignored \"-Wstringop-overread\"")

// Disable the `non-template-friend` warning which sometimes can't be avoided
// in generic code.
#define DISABLE_WARNINGS_GCC_TEMPLATE_FRIEND \
  _Pragma("GCC diagnostic push")             \
      _Pragma("GCC diagnostic ignored \"-Wnon-template-friend\"")

// Re-enable the warnings disabled by the last `DISABLE_...` call.
#define GCC_REENABLE_WARNINGS _Pragma("GCC diagnostic pop")

#else
#define DISABLE_UNINITIALIZED_WARNINGS
#define DISABLE_OVERREAD_WARNINGS
#define DISABLE_WARNINGS_GCC_TEMPLATE_FRIEND
#define GCC_REENABLE_WARNINGS
#endif

#ifdef __clang__
#define DISABLE_CLANG_SELF_ASSIGN_WARNING \
  _Pragma("clang diagnostic push")        \
      _Pragma("clang diagnostic ignored \"-Wself-assign-overloaded\"")

#define ENABLE_CLANG_WARNINGS _Pragma("clang diagnostic pop")
#else
#define DISABLE_CLANG_SELF_ASSIGN_WARNING
#define ENABLE_CLANG_WARNINGS
#endif

#endif  // QLEVER_COMPILERWARNINGS_H
