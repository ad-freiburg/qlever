//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILEREXTENSIONS_H
#define QLEVER_COMPILEREXTENSIONS_H

// A generic macro that forces inlining during compilation across compilers.
//
// The macro always expands to (at least) `inline`, so it must only be used on a
// function whose definition is visible in every translation unit that calls it.
// That is not a restriction, but the precondition for forcing the inlining in
// the first place: a function that is declared in a header but defined in a
// `.cpp` file cannot be inlined into its callers in other translation units,
// and annotating such a definition only earns a `-Wattributes` warning from GCC
// (`'always_inline' function might not be inlinable unless also declared
// 'inline'`). With the `inline` in the macro, that mistake now breaks the link
// instead of silently doing nothing.
//
// The `inline` is also what keeps the attribute usable in a shared-library
// build. GCC refuses to force the inlining of a function whose body may be
// replaced at link time (`error: inlining failed in call to 'always_inline'
// ...: function body can be overwritten at link time`), which applies to
// functions with external linkage in a shared library, but not to `inline`
// functions: those are emitted as COMDAT and bind to the definition at hand.
#if defined(__GNUC__) || defined(__clang__)
// NOTE: Clang defines `__GNUC__` as well and understands the `gnu::` spelling
// of the attribute, so a single branch covers both compilers.
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
