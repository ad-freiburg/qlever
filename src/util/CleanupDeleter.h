//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CLEANUPDELETER_H
#define QLEVER_CLEANUPDELETER_H

#include <functional>
#include <memory>

namespace ad_utility::cleanup_deleter {

/// Special type of deleter class that allows to call a function
/// just before the wrapper value T is deleted.
template <typename T>
class CleanupDeleter {
  using type = std::remove_reference_t<T>;

  std::function<void(type&)> function_;

  explicit CleanupDeleter(std::function<void(type&)> function)
      : function_{std::move(function)} {}

 public:
  /// Calls function_ and deletes the value afterwards.
  void operator()(T* ptr) const {
    function_(*ptr);
    delete ptr;
  }

  /// Creates a std::unique_ptr that calls the passed function right before
  /// obj is about to be deleted.
  static std::unique_ptr<T, CleanupDeleter<T>> cleanUpAfterUse(
      std::remove_reference_t<T> obj, std::function<void(type&)> function) {
    return {new T(std::move(obj)), CleanupDeleter{std::move(function)}};
  }
};

}  // namespace ad_utility::cleanup_deleter

#endif  // QLEVER_CLEANUPDELETER_H
