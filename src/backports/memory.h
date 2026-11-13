//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_MEMORY_H
#define QLEVER_SRC_BACKPORTS_MEMORY_H

// This file defines `ql::make_unique_for_overwrite` as a drop-in replacement
// for `std::make_unique_for_overwrite` (C++20), which default-initializes the
// pointee instead of value-initializing it. In particular, for arrays of
// trivial types it doesn't zero the memory, which `std::make_unique` does.
// In the C++17 backports mode we provide our own implementation.
//
// It also defines `ql::to_address` as a drop-in replacement for
// `std::to_address` (C++20), which returns the raw pointer of a pointer-like
// object, e.g. of an iterator. Before version 18, libc++ fails to compile
// `std::to_address` for some iterators (which broke the macOS build), so we
// also use our own implementation there.

#include <cstddef>
#include <memory>
#include <type_traits>

namespace ql {

#ifdef __cpp_lib_smart_ptr_for_overwrite
using std::make_unique_for_overwrite;
#else

// Non-array version.
template <typename T>
std::enable_if_t<!std::is_array<T>::value, std::unique_ptr<T> >
make_unique_for_overwrite() {
  // `new T` (as opposed to `new T()`) performs default-initialization.
  return std::unique_ptr<T>{new T};
}

// Version for arrays of unknown bound, e.g. `char[]`.
template <typename T>
std::enable_if_t<std::is_array<T>::value && std::extent<T>::value == 0,
                 std::unique_ptr<T> >
make_unique_for_overwrite(std::size_t size) {
  return std::unique_ptr<T>{new std::remove_extent_t<T>[size]};
}

// Arrays of known bound, e.g. `char[42]`, are not supported (same as in the
// standard).
template <typename T, typename... Args>
std::enable_if_t<std::extent<T>::value != 0> make_unique_for_overwrite(
    Args&&...) = delete;

#endif

#if defined(__cpp_lib_to_address) && \
    !(defined(_LIBCPP_VERSION) && _LIBCPP_VERSION < 180000)
using std::to_address;
#else

// Version for raw pointers, which are returned unchanged (also if they are
// `nullptr`).
template <typename T>
constexpr T* to_address(T* pointer) noexcept {
  static_assert(!std::is_function<T>::value);
  return pointer;
}

// Version for pointer-like objects. Unlike for `std::to_address`, `pointer` has
// to be dereferenceable, e.g. it must not be an end iterator.
template <typename Pointer>
constexpr auto to_address(const Pointer& pointer) noexcept {
  return std::addressof(*pointer);
}

#endif

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_MEMORY_H
