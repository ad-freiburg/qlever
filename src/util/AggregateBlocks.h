//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_AGGREGATEBLOCKS_H
#define QLEVER_AGGREGATEBLOCKS_H

#include <functional>

#include "./CoroToStateMachine.h"
#include "./Forward.h"

namespace ad_utility {

struct Empty {};

template <typename T, typename ElementAction, typename BlockAction,
          typename Projection, typename Equality = std::equal_to<>>
CoroToStateMachine<T> aggregateBlocks(ElementAction elementAction,
                                      BlockAction blockAction,
                                      Projection projection,
                                      Equality equality = Equality{}) {
  if (!co_await ad_utility::valueWasPushedTag) {
    co_return;
  }
  auto currentElement =
      std::invoke(projection, co_await ad_utility::nextValueTag);
  do {
    auto&& value = co_await ad_utility::nextValueTag;
    if (!equality(currentElement, std::invoke(projection, value))) {
      blockAction(std::move(currentElement));
      currentElement = std::invoke(projection, value);
    }
    std::invoke(elementAction, std::move(value));

  } while (co_await ad_utility::valueWasPushedTag);
  blockAction(std::move(currentElement));
}
}  // namespace ad_utility

#endif  // QLEVER_AGGREGATEBLOCKS_H
