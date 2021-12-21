//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TUPLEFOREACH_H
#define QLEVER_TUPLEFOREACH_H

#include <tuple>

namespace ad_utility {

/// Apply the `function` on each element of the `tuple`. The function is
/// applied on the tuple elements sequentially from left to right.
/// \param tuple must be a std::tuple<...>.
/// \param function function(forward<T>(t)) must work for every type T
///                 in the `tuple`
template <typename Tuple, typename Function>
void forEachTuple(Tuple&& tuple, Function&& function) {
  auto forEachParamPack = [&]<typename... Ts>(Ts && ... parameters) {
    (..., function(std::forward<Ts>(parameters)));
  };
  std::apply(forEachParamPack, std::forward<Tuple>(tuple));
}
}  // namespace ad_utility

#endif  // QLEVER_TUPLEFOREACH_H
