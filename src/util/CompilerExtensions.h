//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILEREXTENSIONS_H
#define QLEVER_COMPILEREXTENSIONS_H

#ifdef __CLANG__
#define AD_ALWAYS_INLINE [[clang::always_inline]]
#else
#ifdef __GNUC__
#define AD_ALWAYS_INLINE [[gnu::always_inline]]
#else
#warning Unsupported compiler, AD_ALWAYS_INLINE will be no-op
#define AD_ALWAYS_INLINE
#endif
#endif

#endif  // QLEVER_COMPILEREXTENSIONS_H
