//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <tuple>

#include "Exception.h"

namespace ad_utility {

/// Helper class allowing to use range-like datastructures with arguments for
/// their begin() member function within range-based for loops like this:
///
/// This calls something.begin(1, 2, 3):
/// for (auto elem : IteratorWrapper{something, 1, 2, 3}) {}
template <typename OriginalIterable, typename... Args>
class IteratorWrapper {
  bool used_ = false;
  OriginalIterable& iterable_;
  std::tuple<Args...> args_;

 public:
  explicit IteratorWrapper(OriginalIterable& iterator, Args... args)
      : iterable_{iterator}, args_{std::move(args)...} {}

  auto begin() {
    AD_CONTRACT_CHECK(!used_);
    used_ = true;
    return std::apply(
        [this](auto... args) { return iterable_.begin(std::move(args)...); },
        std::move(args_));
  }

  auto end() { return iterable_.end(); }
};

};  // namespace ad_utility
