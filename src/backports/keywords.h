// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

#ifndef QLEVER_SRC_BACKPORTS_KEYWORDS_H
#define QLEVER_SRC_BACKPORTS_KEYWORDS_H

// Add C++17 replacements for C++20 keywords like `consteval` and
// `explicit(condition)`
// 1. `QL_CONSTEVAL` is `consteval` in C++20 mode, and `constexpr` in C++17
// mode.
// 2. `QL_EXPLICIT(trueOrFalse)` is `explicit(trueOrFalse)`in C++20 mode and the
// empty string (not explicit) in C++17 mode.
#ifdef QLEVER_CPP_17
#define QL_CONSTEVAL constexpr
#define QL_EXPLICIT(...)

#else
#define QL_CONSTEVAL consteval
#define QL_EXPLICIT(...) explicit(__VA_ARGS__)
#endif

#endif  // QLEVER_SRC_BACKPORTS_KEYWORDS_H
