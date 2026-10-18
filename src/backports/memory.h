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

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_MEMORY_H
