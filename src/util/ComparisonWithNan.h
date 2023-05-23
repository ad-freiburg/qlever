//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <cmath>
#include <type_traits>

namespace ad_utility {
template <typename Comparator>
inline auto makeComparatorForNans(Comparator comparator) {
  return [comparator]<typename A, typename B>(const A& a, const B& b)
      requires std::is_invocable_r_v<bool, Comparator, A, B> {
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>) {
      bool aNan = std::isnan(a);
      bool bNan = std::isnan(b);
      if (aNan && bNan) {
        return false;
      } else if (aNan) {
        return false;
      } else if (bNan) {
        return true;
      } else {
        return comparator(a, b);
      }
    } else if constexpr (std::is_floating_point_v<A>) {
      return !std::isnan(a) && comparator(a, b);
    } else if constexpr (std::is_floating_point_v<B>) {
      return std::isnan(b) || comparator(a, b);
    } else {
      return comparator(a, b);
    }
  };
}
}  // namespace ad_utility
