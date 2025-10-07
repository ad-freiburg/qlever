//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_ONDESTRUCTIONDONTTHROWDURINGSTACKUNWINDING_H
#define QLEVER_SRC_UTIL_ONDESTRUCTIONDONTTHROWDURINGSTACKUNWINDING_H

#include "util/ExceptionHandling.h"

namespace ad_utility {
namespace detail {

// The implementation of `makeOnDestructionDontThrowDuringStackUnwinding` (see
// below).
CPP_template(typename F)(requires std::is_invocable_v<
                         F>) class OnDestructionDontThrowDuringStackUnwinding {
  static_assert(
      !std::is_nothrow_invocable_v<F>,
      "When using a non-throwing callback, use the simpler `absl::Cleanup`");

 private:
  F f_;
  ThrowInDestructorIfSafe throwInDestructorIfSafe_;
  bool isCanceled_ = false;

 public:
  // It is forbidden to copy or move these objects.
  OnDestructionDontThrowDuringStackUnwinding& operator=(
      OnDestructionDontThrowDuringStackUnwinding&&) = delete;
  OnDestructionDontThrowDuringStackUnwinding(
      OnDestructionDontThrowDuringStackUnwinding&&) noexcept = delete;

  // Cancel the cleanup. When this has been called, the destructor will do
  // nothing.
  void cancel() { isCanceled_ = true; }
  // The destructor that calls `f_`.
  ~OnDestructionDontThrowDuringStackUnwinding() noexcept(false) {
    if (isCanceled_) {
      return;
    }
    throwInDestructorIfSafe_(std::move(f_));
  }
  friend class OnDestructionCreator;

 private:
  // The destructor is deliberately private. Creating an object is only allowed
  // via the explicit (public) function
  // `makeOnDestructionDontThrowDuringStackUnwinding`. This disables the storing
  // of the objects in all sorts of containers (for details see below).
  explicit OnDestructionDontThrowDuringStackUnwinding(F f) : f_{std::move(f)} {}
};

// Part of the implementation of
// `makeOnDestructionDontThrowDuringStackUnwinding` (see below).
class OnDestructionCreator {
 public:
  template <typename F>
  static OnDestructionDontThrowDuringStackUnwinding<F> create(F f) {
    return OnDestructionDontThrowDuringStackUnwinding{std::move(f)};
  }
};
}  // namespace detail

// At the exit of the current scope (when the destructor of the returned object
// is called), execute the function `f`, no matter if the scope exits normally
// or because of exception handling. This is different from `absl::Cleanup` as
// it enforces a function `f` that might potentially throw (for non-throwing
// functions the much simpler `absl::Cleanup` should be used). If `f` throws an
// exception and if it is safe to rethrow that exception, then the exception is
// thrown (for example if the scope is exited normally). If it is not safe to
// throw the exception because we are currently handling another exception, then
// the exception from `f` is ignored. Note that it is not possible to store the
// return object in a container because all of its constructors are either
// private or deleted. This is disabled deliberately as it might lead to program
// termination (for `std::vector`) or to uncalled destructors.
CPP_template(typename F)(requires ql::concepts::invocable<F>)
    [[nodiscard("")]] detail::OnDestructionDontThrowDuringStackUnwinding<
        F> makeOnDestructionDontThrowDuringStackUnwinding(F f) {
  static_assert(
      !std::is_nothrow_invocable_v<F>,
      "When using a non-throwing callback, use the simpler `absl::Cleanup`");
  return detail::OnDestructionCreator::create(std::move(f));
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ONDESTRUCTIONDONTTHROWDURINGSTACKUNWINDING_H
