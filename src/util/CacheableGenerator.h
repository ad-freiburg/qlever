//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <optional>

#include "util/Generator.h"
#include "util/TypeTraits.h"

namespace ad_utility {

template <typename T>
cppcoro::generator<T> wrapGeneratorWithCache(
    cppcoro::generator<T> generator,
    InvocableWithExactReturnType<bool, std::optional<T>&, const T&> auto
        aggregator,
    InvocableWithExactReturnType<void, std::optional<T>> auto onFullyCached) {
  std::optional<T> aggregatedData{};
  bool aggregate = true;
  for (auto&& element : generator) {
    if (aggregate) {
      aggregate = aggregator(aggregatedData, element);
      if (!aggregate) {
        aggregatedData.reset();
      }
    }
    co_yield AD_FWD(element);
  }
  if (aggregate) {
    onFullyCached(std::move(aggregatedData));
  }
}
};  // namespace ad_utility
