//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef ITERATORWRAPPER_H
#define ITERATORWRAPPER_H

#include <tuple>

namespace ad_utility {

template <typename OriginalIterable, typename... Args>
class IteratorWrapper {
  OriginalIterable& iterable_;
  std::tuple<Args...> args_;

 public:
  explicit IteratorWrapper(OriginalIterable& iterator, Args... args)
      : iterable_{iterator}, args_{std::move(args)...} {}

  auto begin() {
    return std::apply([this](auto... args) { return iterable_.begin(args...); },
                      args_);
  }

  auto end() { return iterable_.end(); }
};

};  // namespace ad_utility

#endif  // ITERATORWRAPPER_H
