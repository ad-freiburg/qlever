//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILEREXTENSIONS_H
#define QLEVER_COMPILEREXTENSIONS_H

// A generic macro that forces inlining during compilation across compilers
// Forces inlining across compilers. The `inline` keyword is essential. It gives
// the function vague linkage so it can't be interposed at link time, which is
// what GCC's `always_inline` requires under -fPIC + semantic interposition.
#if defined(__GNUC__) || \
    defined(__clang__)  // clang defines __GNUC__ too, but be explicit
#define AD_ALWAYS_INLINE [[gnu::always_inline]] inline
#else
#warning \
    "For this compiler we don't know how to force the inlining of functions. \
There might be some performance degradations."
#define AD_ALWAYS_INLINE inline
#endif

#endif  // QLEVER_COMPILEREXTENSIONS_H
