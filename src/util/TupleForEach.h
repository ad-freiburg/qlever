//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_TUPLEFOREACH_H
#define QLEVER_TUPLEFOREACH_H

#include <tuple>

namespace ad_utility {
template <typename Tuple, typename Function>
void forEachInTuple(Tuple&& tuple, Function&& function) {
  auto forEachInParamPack = [&](auto&&... parameters) {
    (..., function(AD_FWD(parameters)));
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
  auto paramPackToArray = [&](auto&&... parameters) {
    return std::array{function(AD_FWD(parameters))...};
  };
  return std::apply(paramPackToArray, std::forward<Tuple>(tuple));
}

}  // namespace ad_utility

#endif  // QLEVER_TUPLEFOREACH_H
