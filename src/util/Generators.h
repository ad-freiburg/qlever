//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <optional>

#include "util/Generator.h"
#include "util/TypeTraits.h"

namespace ad_utility {

// Wrap the given `generator` inside another generator that aggregates a cache
// by calling `aggregator` on every iteration of the inner `generator` until it
// returns false. If the `aggregator` returns false, the cached value is
// discarded. If the cached value is still present once the generator is fully
// consumed, `onFullyCached` is called with the cached value.
template <typename T>
cppcoro::generator<T> wrapGeneratorWithCache(
    cppcoro::generator<T> generator,
    InvocableWithExactReturnType<bool, std::optional<T>&, const T&> auto
        aggregator,
    InvocableWithExactReturnType<void, T> auto onFullyCached) {
  std::optional<T> aggregatedData{};
  bool shouldBeAggregated = true;
  for (T& element : generator) {
    if (shouldBeAggregated) {
      shouldBeAggregated = aggregator(aggregatedData, element);
      if (!shouldBeAggregated) {
        aggregatedData.reset();
      }
    }
    co_yield element;
  }
  if (aggregatedData.has_value()) {
    onFullyCached(std::move(aggregatedData).value());
  }
}
};  // namespace ad_utility
