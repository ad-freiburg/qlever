// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// Clang up to version 13 does not link again libstdc++,
// because coroutines are still expected in the std::experimental
// namespace. This is a helper file that overcomes this limitation,
// simply include this file as a replacement of <coroutine>

// TODO<LLVM14> Remove this file as soon as we have a stable
// clang version that doesn't require this hack.
#ifndef QLEVER_COROUTINES_H
#define QLEVER_COROUTINES_H

#include <version>

#if defined(__clang__) && defined(__GLIBCXX__) && !__cpp_impl_coroutine
// This is the constant that is defined by g++ when using the
// -fcoroutines flag. We have to specify it manually for clang
// before including the coroutine header.
#define __cpp_impl_coroutine 1

#include <coroutine>

namespace std::experimental {
template <typename T, typename... Ts>
struct coroutine_traits : std::coroutine_traits<T, Ts...> {};

template <typename T = void>
struct coroutine_handle : std::coroutine_handle<T> {};
}  // namespace std::experimental

#else
// Simply include the coroutine header, no special treatment
// necessary.
#include <coroutine>
#endif

#endif  // QLEVER_COROUTINES_H
