//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_COMPILEREXTENSIONS_H
#define QLEVER_COMPILEREXTENSIONS_H

// A generic macro that forces inlining during compilation across compilers.
// It expands to (at least) `inline`, hence the single rule for using it: every
// translation unit that calls the function must see its definition. Concretely:
//
// 1. In a header: always fine, and the typical use case. This covers free
//    functions, member functions defined inside the class body, and templates.
// 2. In a `.cpp` file: only for functions that no other translation unit can
//    call anyway, that is, functions with internal linkage (`static` or in an
//    anonymous namespace). There the `inline` makes no difference.
// 3. NOT for a function that is declared in a header, but defined out of line
//    in a `.cpp` file, no matter whether it is public or private. Calls from
//    other translation units then fail to link. Note that for private member
//    functions all calls typically do live in the same `.cpp` file, so it will
//    usually link, but an `inline` definition whose declaration in the header
//    is not `inline` is still an ODR violation, so don't rely on that.
//
// The `inline` in the macro is not an arbitrary restriction, but the
// precondition for forcing the inlining in the first place, and it turns
// violations of the rule above into errors: without it, GCC merely warns
// (`'always_inline' function might not be inlinable unless also declared
// 'inline'`) and silently doesn't inline anything. It is also what keeps the
// attribute usable in a shared-library build, where GCC refuses to force the
// inlining of a function whose body may be replaced at link time (`error:
// inlining failed in call to 'always_inline' ...: function body can be
// overwritten at link time`). That applies to functions with external linkage,
// but not to `inline` functions, which are emitted as COMDAT and bind to the
// definition at hand.
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
