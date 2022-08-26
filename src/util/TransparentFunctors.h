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

namespace ad_utility {
/// Return the first element of a `std::pair`.
struct FirstOfPair {
  template <typename T>
  requires similarToInstantiation<std::pair, T> decltype(auto) operator()(
      T&& pair) {
    return AD_FWD(pair).first;
  }
};

/// Return the second element of a `std::pair`.
struct SecondOfPair {
  template <typename T>
  requires similarToInstantiation<std::pair, T> decltype(auto) operator()(
      T&& pair) {
    return AD_FWD(pair).second;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_TRANSPARENTFUNCTORS_H
