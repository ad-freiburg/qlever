//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

// Clang 13 has a bug that in certain cases issues the following warning for
// capturing lambdas:
// "class '' does not declare any constructor to initialize its non-modifiable
// members". This warning false-positive and this is considered a bug in clang
// 13 which has been fixed in clang 14 and newer. Unfortunately there is no flag
// to disable this warning. This file provides macros to disable ALL warnings on
// clang 13, but only for a small snippet of code. Before the line that issues
// the undesired warnings use the macro `DISABLE_WARNINGS_CLANG_13` and after
// that line use `ENABLE_WARNINGS_CLANG_13` to reenable the warnings for the
// following lines of code. On compilers other than clang 13 those macros are
// defined to be empty and thus have no effect. For example usages of this
// feature see `CallFixedSize.h`.

// TODO<joka921, Clang 16> Throw out these macros as soon as we don't support
// clang 13 anymore.

#ifdef __clang__
#if __clang_major__ == 13

#define DISABLE_WARNINGS_CLANG_13 \
  _Pragma("GCC diagnostic push")  \
      _Pragma("GCC diagnostic ignored \"-Weverything\"")

#define ENABLE_WARNINGS_CLANG_13 _Pragma("GCC diagnostic pop")
#endif
#endif

#ifndef DISABLE_WARNINGS_CLANG_13
#define DISABLE_WARNINGS_CLANG_13
#define ENABLE_WARNINGS_CLANG_13
#endif
