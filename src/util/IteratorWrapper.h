//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <tuple>

#include "Exception.h"

namespace ad_utility {

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
