// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_NUMERIC_H
#define QLEVER_SRC_BACKPORTS_NUMERIC_H

// This file defines drop-in replacements for algorithms from `<numeric>` that
// are not available on all toolchains targeted by the C++17 backports mode
// (e.g. GCC 8 has no `std::transform_reduce`). By default (in C++20 mode) they
// simply forward to the `std::` versions. If `QLEVER_CPP_17` is defined, a
// trivial serial implementation is used instead. Only the overloads without
// execution policies are provided.

#include <functional>
#include <numeric>
#include <utility>

namespace ql {

// Drop-in replacement for the overload of `std::transform_reduce` that applies
// the binary `transform` to each pair of elements from `[first1, last1)` and
// the range starting at `first2`, and reduces the results together with `init`
// using the binary `reduce`.
template <typename InputIt1, typename InputIt2, typename T,
          typename BinaryReduceOp, typename BinaryTransformOp>
T transform_reduce(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init,
                   BinaryReduceOp reduce, BinaryTransformOp transform) {
#ifdef QLEVER_CPP_17
  for (; first1 != last1; ++first1, (void)++first2) {
    init = reduce(std::move(init), transform(*first1, *first2));
  }
  return init;
#else
  return std::transform_reduce(std::move(first1), std::move(last1),
                               std::move(first2), std::move(init),
                               std::move(reduce), std::move(transform));
#endif
}

// Drop-in replacement for the overload of `std::transform_reduce` that
// computes the inner product of `[first1, last1)` and the range starting at
// `first2`, plus `init`.
template <typename InputIt1, typename InputIt2, typename T>
T transform_reduce(InputIt1 first1, InputIt1 last1, InputIt2 first2, T init) {
  return ql::transform_reduce(std::move(first1), std::move(last1),
                              std::move(first2), std::move(init), std::plus<>{},
                              std::multiplies<>{});
}

// Drop-in replacement for the overload of `std::transform_reduce` that applies
// the unary `transform` to each element of `[first, last)` and reduces the
// results together with `init` using the binary `reduce`.
template <typename InputIt, typename T, typename BinaryReduceOp,
          typename UnaryTransformOp>
T transform_reduce(InputIt first, InputIt last, T init, BinaryReduceOp reduce,
                   UnaryTransformOp transform) {
#ifdef QLEVER_CPP_17
  for (; first != last; ++first) {
    init = reduce(std::move(init), transform(*first));
  }
  return init;
#else
  return std::transform_reduce(std::move(first), std::move(last),
                               std::move(init), std::move(reduce),
                               std::move(transform));
#endif
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_NUMERIC_H
