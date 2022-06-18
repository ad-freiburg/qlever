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
namespace ad_std {
using suspend_always = std::suspend_always;
using suspend_never = std::suspend_never;

template <typename T = void>
using coroutine_handle = std::coroutine_handle<T>;
}  // namespace ad_std

#else

#ifdef __clang__
//#error "case three"
#include <experimental/coroutine>
// This is technically not allowed, but it works.
namespace std {
struct suspend_always : experimental::suspend_always {};
struct suspend_never : experimental::suspend_never {};

template <typename T, typename... Ts>
struct coroutine_traits : experimental::coroutine_traits<T, Ts...> {
  using Base = experimental::coroutine_traits<T>;
  constexpr coroutine_traits(Base b) : Base{b} {}
};

template <typename T = void>
struct coroutine_handle : experimental::coroutine_handle<T> {
  using Base = experimental::coroutine_handle<T>;
  using Base::operator=;
  constexpr coroutine_handle(Base b) : Base{b} {}
  constexpr operator Base() { return static_cast<Base>(*this); }
};

}  // namespace std

namespace ad_std {
using suspend_always = std::experimental::suspend_always;
using suspend_never = std::experimental::suspend_never;

template <typename T = void>
using coroutine_handle = std::experimental::coroutine_handle<T>;
}  // namespace ad_std
#define AD_FREIBURG_COROUTINES_EXPERIMENTAL
#else
// Simply include the coroutine header, no special treatment
// necessary.
#include <coroutine>
namespace ad_std {
using suspend_always = std::suspend_always;
using suspend_never = std::suspend_never;
template <typename T = void>
using coroutine_handle = std::coroutine_handle<T>;
}  // namespace ad_std
#endif
#endif

#endif  // QLEVER_COROUTINES_H
