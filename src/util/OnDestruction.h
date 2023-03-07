//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_ONDESTRUCTION_H
#define QLEVER_ONDESTRUCTION_H

#include <concepts>
#include <exception>
#include <iostream>

#include "util/Forward.h"
#include "util/SourceLocation.h"

namespace ad_utility {

namespace detail {
[[maybe_unused]] static constexpr auto callStdTerminate = []() noexcept {
  std::terminate();
};

// The implementation of `makeOnDestructionDontThrowDuringStackUnwinding` (see
// below).
template <typename F>
requires(
    std::is_invocable_v<F>) class OnDestructionDontThrowDuringStackUnwinding {
  static_assert(
      !std::is_nothrow_invocable_v<F>,
      "When using a non-throwing callback, use the simpler `absl::Cleanup`");

 private:
  F f_;
  int numExceptionsDuringConstruction_ = std::uncaught_exceptions();

 public:
  // It is forbidden to copy or move these objects.
  OnDestructionDontThrowDuringStackUnwinding& operator=(
      OnDestructionDontThrowDuringStackUnwinding&&) = delete;
  OnDestructionDontThrowDuringStackUnwinding(
      OnDestructionDontThrowDuringStackUnwinding&&) noexcept = delete;

  // The destructor that calls `f_`.
  ~OnDestructionDontThrowDuringStackUnwinding() noexcept(false) {
    // If the number of uncaught exceptions is the same as when then constructor
    // was called, then it is safe to throw a possible exception For details see
    // https://en.cppreference.com/w/cpp/error/uncaught_exception, especially
    // the links at the bottom of the page.
    if (numExceptionsDuringConstruction_ == std::uncaught_exceptions()) {
      std::invoke(f_);
    } else {
      // We must not throw, so we simply ignore possible exceptions.
      try {
        std::invoke(f_);
      } catch (const std::exception& e) {
        // It is not safe to throw an exception because stack unwinding is in
        // progress. We thus catch and ignore all possible exceptions.
        LOG(INFO) << "Ignored an exception because it would have been thrown "
                     "during stack unwinding. The exception message was:\""
                  << e.what() << '"' << std::endl;
      } catch (...) {
        // See the `catch` clause above for details.
        LOG(INFO) << "Ignored an exception of an unknown type because it would "
                     "have been thrown during stack unwinding"
                  << std::endl;
      }
    }
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
// functions the much simpler `absl::Cleanup` should be used. If `f` throws an
// exception and it is safe to rethrow the exception, then the exception is
// thrown (for example if the scope exits normally). If it is not safe to throw
// the exception because we are currently handling another exception, then the
// exception from `f` is ignored. Note that it is not possible to store the
// return object in a container because all of its constructors are either
// private or deleted. This is disabled deliberately as it might lead to program
// termination (for `std::vector`) or to uncalled destructors.
template <std::invocable F>
[[nodiscard("")]] detail::OnDestructionDontThrowDuringStackUnwinding<F>
makeOnDestructionDontThrowDuringStackUnwinding(F f) {
  static_assert(
      !std::is_nothrow_invocable_v<F>,
      "When using a non-throwing callback, use the simpler `absl::Cleanup`");
  return detail::OnDestructionCreator::create(std::move(f));
}

// Call `f()`. If this call throws, then terminate the program after logging an
// error message that includes the `message`, information on the thrown
// exception, and the location of the call. This can be used to make destructors
// `noexcept()` that have to perform some non-trivial logic (e.g. writing a
// trailer to a file), when such a failure should never occur in practice and
// also is not easily recovarable. For an example usage see `PatternCreator.h`.
// The actual termination call can be configured for testing purposes. Note that
// this function must never throw an exception.
template <typename F,
          typename TerminateAction = decltype(detail::callStdTerminate)>
requires(
    std::invocable<std::remove_cvref_t<F>>&& std::is_nothrow_invocable_v<
        TerminateAction>) void terminateIfThrows(F&& f,
                                                 std::string_view message,
                                                 TerminateAction
                                                     terminateAction = {},
                                                 ad_utility::source_location l =
                                                     ad_utility::source_location::
                                                         current()) noexcept {
  try {
    std::invoke(AD_FWD(f));
  } catch (const std::exception& e) {
    std::cerr << "A function that should never throw has thrown an exception "
                 "with message \""
              << e.what() << "\". The function was called in file "
              << l.file_name() << " on line " << l.line()
              << ". Additional information: " << message
              << ". Please report this. Terminating" << std::endl;
    terminateAction();
  } catch (...) {
    std::cerr << "A function that should never throw has thrown an exception. "
                 "The function was called in file "
              << l.file_name() << " on line " << l.line()
              << ". Additional information: " << message
              << ". Please report this. Terminating" << std::endl;
    terminateAction();
  }
}
}  // namespace ad_utility

#endif  // QLEVER_ONDESTRUCTION_H
