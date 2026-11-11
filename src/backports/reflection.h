// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_REFLECTION_H
#define QLEVER_SRC_BACKPORTS_REFLECTION_H

// Detect support for C++26 static reflection (P2996) together with the
// expansion statements (`template for`, P1306), and define the macro
// `QLEVER_HAS_REFLECTION` if both are available. QLever doesn't depend on
// reflection, but uses it (guarded by `#ifdef QLEVER_HAS_REFLECTION`) for
// additional compile-time checks of invariants that the C++17/C++20 code has to
// state redundantly. Currently, only GCC 16 with `-std=c++26 -freflection`
// supports reflection, which CMake passes automatically for that compiler (see
// the `QLEVER_USE_REFLECTION` option).

// Include `<version>` (which defines the library feature test macros) only if
// it exists. It is a C++20 header that the standard library of GCC 8 doesn't
// have, and without it `__cpp_lib_reflection` stays undefined, so the check
// below correctly finds no reflection.
#if __has_include(<version>)
#include <version>
#endif

#if defined(__cpp_impl_reflection) && defined(__cpp_lib_reflection) && \
    defined(__cpp_expansion_statements)
#define QLEVER_HAS_REFLECTION
#endif

#ifdef QLEVER_HAS_REFLECTION
#include <meta>
#endif

#endif  // QLEVER_SRC_BACKPORTS_REFLECTION_H
