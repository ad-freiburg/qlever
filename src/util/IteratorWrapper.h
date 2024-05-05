//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef ITERATORWRAPPER_H
#define ITERATORWRAPPER_H

#include <tuple>

namespace ad_utility {

template <typename OriginalIterator, typename... Args>
class IteratorWrapper {
  OriginalIterator& iterator_;
  std::tuple<Args...> args_;

 public:
  explicit IteratorWrapper(OriginalIterator& iterator, Args... args)
      : iterator_{iterator}, args_{std::move(args)...} {}

  auto begin() {
    return std::apply([this](auto... args) { return iterator_.begin(args...); },
                      args_);
  }

  auto end() { return iterator_.end(); }

  auto& operator++() { return iterator_++; }

  auto& operator*() { return *iterator_; }

  auto operator->() { return std::addressof(operator*()); }
};

};  // namespace ad_utility

#endif  // ITERATORWRAPPER_H
