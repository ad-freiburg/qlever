//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILERWARNINGS_H
#define QLEVER_COMPILERWARNINGS_H

/// Helper macros that allow suppressing specific warnings in certain compiler
/// versions that turn out to be false positives.
///
/// NOTE: Several of the warnings below (`-Wuninitialized`,
/// `-Wmaybe-uninitialized`, `-Warray-bounds`, `-Wstringop-overflow`) are
/// emitted by GCC's *middle end*, long after the preprocessor has run. Two
/// consequences follow, and together they explain where the suppressions in
/// QLever have to be placed:
///
/// * Such a warning is NOT suppressed by the offending header being a system
///   header (`-isystem`, `/usr/include`), so a false positive inside a
///   dependency cannot be silenced by the include alone. It has to be disabled
///   for the whole target instead, see for example the options of the `s2` and
///   `spatialjoin` targets in the top-level `CMakeLists.txt`.
/// * It IS suppressed by an explicit `#pragma GCC diagnostic` region, because
///   GCC walks the inlining chain when it decides whether a warning is
///   disabled. Wrapping the *definition* of the offending function (or the
///   `#include` that provides it) in the macros below therefore covers every
///   translation unit that instantiates it, which is why they are used at
///   definitions and not at the individual call sites.

#if defined(__GNUC__) && (__GNUC__ >= 11 && __GNUC__ <= 16)

// Disable the `maybe-uninitialized` warning, which has many false positives.
#define DISABLE_UNINITIALIZED_WARNINGS \
  _Pragma("GCC diagnostic push")       \
      _Pragma("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")

// Disable the `stringop-overread` warning, which has many false positives.
#define DISABLE_OVERREAD_WARNINGS \
  _Pragma("GCC diagnostic push")  \
      _Pragma("GCC diagnostic ignored \"-Wstringop-overread\"")

// Disable the `stringop-overflow` warning, which has many false positives.
#define DISABLE_STRINGOP_OVERFLOW_WARNINGS \
  _Pragma("GCC diagnostic push")           \
      _Pragma("GCC diagnostic ignored \"-Wstringop-overflow\"")

// Disable the `free-non-heap-object` warning, which has false positives in
// certain tests in modern versions of GTest.
#define DISABLE_FREE_NONHEAP_WARNINGS \
  _Pragma("GCC diagnostic push")      \
      _Pragma("GCC diagnostic ignored \"-Wfree-nonheap-object\"")

// Disable the `non-template-friend` warning which sometimes can't be avoided
// in generic code.
#define DISABLE_WARNINGS_GCC_TEMPLATE_FRIEND \
  _Pragma("GCC diagnostic push")             \
      _Pragma("GCC diagnostic ignored \"-Wnon-template-friend\"")

// Disable the `aggressive-loop-optimizations` warning, which produces false
// positives on GCC 13 when `std::advance` is inlined into
// `std::vector::assign` through a `ranges::elements_view` over a hash map.
#define DISABLE_AGGRESSIVE_LOOP_OPT_WARNINGS \
  _Pragma("GCC diagnostic push")             \
      _Pragma("GCC diagnostic ignored \"-Waggressive-loop-optimizations\"")

// Disable the `dangling-reference` warning, which produces false positives
// when `std::visit` returns a reference or when a reference is bound to the
// result of an immediately-invoked lambda that returns a reference to a static
// local. The warning was introduced in GCC 13; the pragma is a no-op on
// earlier versions that don't know the flag.
#define DISABLE_DANGLING_REFERENCE_WARNINGS \
  _Pragma("GCC diagnostic push")            \
      _Pragma("GCC diagnostic ignored \"-Wdangling-reference\"")

// Disable the `array-bounds` warning, which produces false positives on GCC 13
// when the comparators in `ExternalSortFunctors.h` are inlined into
// `std::__insertion_sort`. GCC then conflates the `Row<ValueId, 5>` and
// `Row<ValueId, 4>` instantiations and wrongly believes an out-of-bounds access
// happens.
#define DISABLE_ARRAY_BOUNDS_WARNINGS \
  _Pragma("GCC diagnostic push")      \
      _Pragma("GCC diagnostic ignored \"-Warray-bounds\"")

// Re-enable the warnings disabled by the last `DISABLE_...` call.
#define GCC_REENABLE_WARNINGS _Pragma("GCC diagnostic pop")

#else
#define DISABLE_UNINITIALIZED_WARNINGS
#define DISABLE_OVERREAD_WARNINGS
#define DISABLE_STRINGOP_OVERFLOW_WARNINGS
#define DISABLE_WARNINGS_GCC_TEMPLATE_FRIEND
#define DISABLE_FREE_NONHEAP_WARNINGS
#define DISABLE_AGGRESSIVE_LOOP_OPT_WARNINGS
#define DISABLE_DANGLING_REFERENCE_WARNINGS
#define DISABLE_ARRAY_BOUNDS_WARNINGS
#define GCC_REENABLE_WARNINGS
#endif

#ifdef __clang__
#define DISABLE_CLANG_SELF_ASSIGN_WARNING \
  _Pragma("clang diagnostic push")        \
      _Pragma("clang diagnostic ignored \"-Wself-assign-overloaded\"")

#define DISABLE_CLANG_UNUSED_RESULT_WARNING \
  _Pragma("clang diagnostic push")          \
      _Pragma("clang diagnostic ignored \"-Wunused-result\"")

#define ENABLE_CLANG_WARNINGS _Pragma("clang diagnostic pop")
#else
#define DISABLE_CLANG_SELF_ASSIGN_WARNING
#define DISABLE_CLANG_UNUSED_RESULT_WARNING
#define ENABLE_CLANG_WARNINGS
#endif

#endif  // QLEVER_COMPILERWARNINGS_H
