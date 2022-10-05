//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author:
//   2022 -    Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TRANSPARENTFUNCTORS_H
#define QLEVER_TRANSPARENTFUNCTORS_H

#include <util/Forward.h>
#include <util/TypeTraits.h>

#include <utility>

/// Contains several function object types with templated operator() that wrap
/// overloaded functions from the standard library. This enables passing them as
/// function parameters.

/// Note that in theory all of them could be implemented shorter as captureless
/// lambda expressions. We have chosen not to do this because the STL also does
/// not choose this approach (see e.g. `std::less`, `std::plus`, etc.) and
/// because global inline lambdas in header files might in theory cause ODR (one
/// definition rule) problems, especially  when using different compilers.

namespace ad_utility {

namespace detail {

// Implementation of `first` (see below).
struct FirstImpl {
  template <typename T>
  constexpr decltype(auto) operator()(T&& pair) const noexcept {
    return std::get<0>(AD_FWD(pair));
  }
};

// Implementation of `second` (see below).
struct SecondImpl {
  template <typename T>
  constexpr decltype(auto) operator()(T&& pair) const noexcept {
    return std::get<1>(AD_FWD(pair));
  }
};
}  // namespace detail

/// Return the first element via perfect forwarding of any type for which
/// `std::get<0>(x)` is valid. This holds e.g. for `std::pair`, `std::tuple`,
/// and `std::array`.
static constexpr detail::FirstImpl first;

/// Return the second element via perfect forwarding of any type for which
/// `std::get<1>(x)` is valid. This holds e.g. for `std::pair`, `std::tuple`,
/// and `std::array`.
static constexpr detail::SecondImpl second;
}  // namespace ad_utility

#endif  // QLEVER_TRANSPARENTFUNCTORS_H
