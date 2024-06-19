//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TUPLEFOREACH_H
#define QLEVER_TUPLEFOREACH_H

#include <tuple>

namespace ad_utility {
template <typename Tuple, typename Function>
void forEachInTuple(Tuple&& tuple, Function&& function) {
  auto forEachInParamPack = [&]<typename... Ts>(Ts&&... parameters) {
    (..., function(std::forward<Ts>(parameters)));
  };
  std::apply(forEachInParamPack, std::forward<Tuple>(tuple));
}

/// Apply the `function` to each element of the `tuple`. Return an `array` of
/// the results. The function is applied on the tuple elements sequentially
/// from left to right.
/// \param tuple must be a std::tuple<...>.
/// \param function function(forward<T>(t)) must work for every type T
///                 in the `tuple` and must always return the same type.
template <typename Tuple, typename Function>
auto tupleToArray(Tuple&& tuple, Function&& function) {
  auto paramPackToArray = [&]<typename... Ts>(Ts&&... parameters) {
    return std::array{function(std::forward<Ts>(parameters))...};
  };
  return std::apply(paramPackToArray, std::forward<Tuple>(tuple));
}

}  // namespace ad_utility

#endif  // QLEVER_TUPLEFOREACH_H
