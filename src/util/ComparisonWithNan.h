//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <cmath>
#include <type_traits>

#include "util/TypeTraits.h"

namespace ad_utility {
namespace detail {
template <typename T>
static constexpr bool isTransparentComparator = isTypeContainedIn<
    T, std::tuple<std::less<>, std::less_equal<>, std::equal_to<>,
                  std::not_equal_to<>, std::greater<>, std::greater_equal<>>>;
}

// Convert one of the transparent comparators from the standard library
// (`std::less<>, std::equal_to<>, etc.`) into a comparator that imposes a
// partial ordering on elements even for not a number (NaN) values. These
// comparators can be for example safely used in combination with `std::sort`,
// which has undefined behavior for the "normal" comparisons as soon as the
// input contains NaN values. The semantics are that of the `comparator` with
// the following changes:
// 1. NaN values are always at the end of the ordering, so  `3 < Nan` and
//    `!(Nan < 3)`, but also `3 > Nan` and `!(Nan > 3)`.
//    Note that for this reason `<` is NOT the exact opposite of `>=` because of
//    the position of the `NaN` values, but
// 2. Nan == Nan (other than the default comparison where Nan != Nan).
// For detailed examples see the corresponding tests which contain all relevant
// corner cases.
template <typename Comparator>
inline auto makeComparatorForNans(Comparator comparator)
    requires detail::isTransparentComparator<Comparator> {
  return [comparator]<typename A, typename B>(const A& a, const B& b)
      requires std::is_invocable_r_v<bool, Comparator, A, B> {
    static constexpr bool isNotEqual =
        ad_utility::isSimilar<Comparator, std::not_equal_to<>>;
    static constexpr bool isEqual =
        ad_utility::isSimilar<Comparator, std::equal_to<>>;
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>) {
      bool aNan = std::isnan(a);
      bool bNan = std::isnan(b);
      if (aNan && bNan) {
        return comparator(0.0, 0.0);
      } else if (aNan) {
        return isNotEqual;
      } else if (bNan) {
        return !isEqual;
      } else {
        return comparator(a, b);
      }
    } else if constexpr (std::is_floating_point_v<A>) {
      if constexpr (isNotEqual) {
        return std::isnan(a) || comparator(a, b);
      } else {
        return !std::isnan(a) && comparator(a, b);
      }
    } else if constexpr (std::is_floating_point_v<B>) {
      return (std::isnan(b) && !isEqual) || comparator(a, b);
    } else {
      return comparator(a, b);
    }
  };
}
}  // namespace ad_utility
