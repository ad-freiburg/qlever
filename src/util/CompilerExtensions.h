//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILEREXTENSIONS_H
#define QLEVER_COMPILEREXTENSIONS_H

// A generic macro that forces the inlining of the annotated function where the
// compiler supports it. It always expands to (at least) `inline`, so every
// caller must see the definition: a function that is defined in a `.cpp` file
// but called from other translation units must not use this macro (the link
// would break, loudly; forced inlining would be impossible for such a function
// anyway).
#ifdef QLEVER_BUILD_SHARED_LIBRARIES
// In a shared-library build the request to always inline cannot be honored for
// a function whose body may be replaced at link time. GCC rejects such a
// definition with `error: inlining failed in call to 'always_inline' ...:
// function body can be overwritten at link time`, preceded by a `-Wattributes`
// warning. The macro hence expands to only `inline` here, and the compiler
// inlines at its own discretion.
#define AD_ALWAYS_INLINE inline
#elif defined(__GNUC__) || \
    defined(__clang__)  // clang defines __GNUC__ too, but be explicit; it also
                        // understands the `gnu::` spelling of the attribute.
#define AD_ALWAYS_INLINE [[gnu::always_inline]] inline
#else
#warning \
    "For this compiler we don't know how to force the inlining of functions. \
There might be some performance degradations."
#define AD_ALWAYS_INLINE inline
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
