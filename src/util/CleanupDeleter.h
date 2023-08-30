//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CLEANUPDELETER_H
#define QLEVER_CLEANUPDELETER_H

#include <functional>
#include <memory>

namespace ad_utility::cleanup_deleter {

template <typename T>
class CleanupDeleter {
  using type = std::remove_reference_t<T>;
  std::function<void(type&)> function_;
  explicit CleanupDeleter(std::function<void(type&)> function)
      : function_{std::move(function)} {}

 public:
  void operator()(T* ptr) const {
    function_(*ptr);
    delete ptr;
  }

  static std::unique_ptr<T, CleanupDeleter<T>> cleanUpAfterUse(
      std::remove_reference_t<T> obj, std::function<void(type&)> function) {
    return {new T(std::move(obj)), CleanupDeleter{std::move(function)}};
  }
};

}  // namespace ad_utility::cleanup_deleter

#endif  // QLEVER_CLEANUPDELETER_H
