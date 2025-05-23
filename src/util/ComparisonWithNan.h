//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_COMPARISONWITHNAN_H
#define QLEVER_SRC_UTIL_COMPARISONWITHNAN_H

#include <cmath>
#include <type_traits>

#include "util/TypeTraits.h"

namespace ad_utility {

// Convert one of the transparent comparators from the standard library
// (`std::less<>, std::equal_to<>, etc.`) into a comparator that imposes a
// partial ordering on elements even for not a number (NaN) values. These
// comparators can be for example safely used in combination with `std::sort`,
// which has undefined behavior for the "normal" comparisons as soon as the
// input contains NaN values. The semantics are that of the `comparator` with
// the following changes:
// 1. NaN values are greater than any other value (in particular, `Nan >
// infinity`).
// 2. NaN values compare equal to other NaN values (that is, `Nan == Nan`, other
// than for the default comparison, where `Nan != Nan`). For detailed examples
// see the corresponding tests which contain all relevant corner cases.
template <typename Comparator>
inline auto makeComparatorForNans(Comparator comparator) {
  return [comparator](const auto& a, const auto& b)
      requires std::is_invocable_r_v<bool, Comparator,
                                     std::decay_t<decltype(a)>,
                                     std::decay_t<decltype(b)>> {
    auto isNan = [](const auto& t) {
      using T = std::decay_t<decltype(t)>;
      if constexpr (std::is_floating_point_v<T>) {
        return std::isnan(t);
      } else {
        (void)t;
        return false;
      }
    };

    bool aIsNan = isNan(a);
    bool bIsNan = isNan(b);
    if (aIsNan && bIsNan) {
      return comparator(0.0, 0.0);
    } else if (aIsNan) {
      return comparator(1.0, 0.0);
    } else if (bIsNan) {
      return comparator(0.0, 1.0);
    } else {
      return comparator(a, b);
    }
  };
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COMPARISONWITHNAN_H
