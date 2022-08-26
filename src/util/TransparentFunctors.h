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

// Implementation of `firstOfPair` (see below).
struct FirstOfPairImpl {
  template <typename T>
  requires similarToInstantiation<std::pair, T> constexpr decltype(auto)
  operator()(T&& pair) const noexcept {
    return AD_FWD(pair).first;
  }
};

// Implementation of `secondOfPair` (see below).
struct SecondOfPairImpl {
  template <typename T>
  requires similarToInstantiation<std::pair, T> constexpr decltype(auto)
  operator()(T&& pair) const noexcept {
    return AD_FWD(pair).second;
  }
};
}  // namespace detail

/// Return the first element of a `std::pair`.
static constexpr detail::FirstOfPairImpl firstOfPair;

/// Return the second element of a `std::pair`.
static constexpr detail::SecondOfPairImpl secondOfPair;
}  // namespace ad_utility

#endif  // QLEVER_TRANSPARENTFUNCTORS_H
