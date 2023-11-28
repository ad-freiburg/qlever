//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILEREXTENSIONS_H
#define QLEVER_COMPILEREXTENSIONS_H

// A generic macro that forces inlining during compilation across compilers
#ifdef __CLANG__
#define AD_ALWAYS_INLINE [[clang::always_inline]]
#elif __GNUC__
#define AD_ALWAYS_INLINE [[gnu::always_inline]]
#else
#warning \
    "For this compiler we don't know how to force the inlining of functions. \
There might be some performance degradations."
#define AD_ALWAYS_INLINE
#endif

#endif  // QLEVER_COMPILEREXTENSIONS_H
