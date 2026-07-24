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

// A macro for the `[[clang::lifetimebound]]` attribute, which marks a function
// parameter (or the implicit `this`) as the owner of the storage that the
// return value borrows from. Clang then warns (`-Wdangling`) when the returned
// reference or view outlives the annotated argument (e.g. when it is bound to a
// temporary). Other compilers don't understand the attribute (and would warn
// about it under `-Werror`), so there it expands to nothing.
#ifdef __clang__
#define AD_LIFETIMEBOUND [[clang::lifetimebound]]
#else
#define AD_LIFETIMEBOUND
#endif

#endif  // QLEVER_COMPILEREXTENSIONS_H
