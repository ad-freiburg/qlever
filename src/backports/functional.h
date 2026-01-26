// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_FUNCTIONAL_H
#define QLEVER_SRC_BACKPORTS_FUNCTIONAL_H

#include <functional>

namespace ql {
// Backport of `std::identity` that can be used in C++17.
// See https://en.cppreference.com/w/cpp/utility/functional/identity.html for
// details.
struct identity {
  template <typename T>
  constexpr T&& operator()(T&& t) const noexcept {
    return std::forward<T>(t);
  }
};
}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_FUNCTIONAL_H
