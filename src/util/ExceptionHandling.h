//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_EXCEPTIONHANDLING_H
#define QLEVER_SRC_UTIL_EXCEPTIONHANDLING_H

#include <absl/strings/str_cat.h>

#include <concepts>
#include <exception>
#include <iostream>

#include "backports/type_traits.h"
#include "util/Forward.h"
#include "util/Log.h"
#include "util/SourceLocation.h"

namespace ad_utility {
namespace detail {
[[maybe_unused]] static constexpr auto callStdTerminate = []() noexcept {
  std::terminate();
};
}  // namespace detail
// Call `f()`. If this call throws, catch the exception and log it, but do not
// propagate it. Can be used to make destructors `noexcept` when the destructor
// has to perform actions that might throw, but when handling these exceptions
// is not important.
CPP_template(typename F)(
    requires std::invocable<ql::remove_cvref_t<
        F>>) void ignoreExceptionIfThrows(F&& f,
                                          std::string_view additionalNote =
                                              "") noexcept {
  if constexpr (std::is_nothrow_invocable_v<ql::remove_cvref_t<F>>) {
    std::invoke(AD_FWD(f));
    return;
  }
  try {
    std::invoke(AD_FWD(f));
  } catch (const std::exception& e) {
    LOG(INFO) << "Ignored an exception. The exception message was:\""
              << e.what() << "\". " << additionalNote << std::endl;
  } catch (...) {
    LOG(INFO) << "Ignored an exception of an unknown type. " << additionalNote
              << std::endl;
  }
}

// Call `f()`. If this call throws, then terminate the program after logging an
// error message that includes the `message`, information on the thrown
// exception, and the location of the call. This can be used to make destructors
// `noexcept()` that have to perform some non-trivial logic (e.g. writing a
// trailer to a file), when such a failure should never occur in practice and
// also is not easily recoverable. For an example usage see `PatternCreator.h`.
// The actual termination call can be configured for testing purposes. Note that
// this function must never throw an exception.
CPP_template(typename F,
             typename TerminateAction = decltype(detail::callStdTerminate))(
    requires std::invocable<ql::remove_cvref_t<F>> CPP_and
        std::is_nothrow_invocable_v<
            TerminateAction>) void terminateIfThrows(F&& f,
                                                     std::string_view message,
                                                     TerminateAction
                                                         terminateAction = {},
                                                     ad_utility::source_location
                                                         l = ad_utility::
                                                             source_location::
                                                                 current()) noexcept {
  auto getErrorMessage =
      [&message, &l](const auto&... additionalMessages) -> std::string {
    return absl::StrCat(
        "A function that should never throw has thrown an exception",
        additionalMessages..., ". The function was called in file ",
        l.file_name(), " on line ", l.line(),
        ". Additional information: ", message,
        ". Please report this. Terminating");
  };

  auto logAndTerminate = [&terminateAction](std::string_view msg) {
    try {
      LOG(ERROR) << msg << std::endl;
      std::cerr << msg << std::endl;
    } catch (...) {
      std::cerr << msg << std::endl;
    }
    terminateAction();
  };
  try {
    std::invoke(AD_FWD(f));
  } catch (const std::exception& e) {
    auto msg = getErrorMessage(" with message \"", e.what(), "\"");
    logAndTerminate(msg);
  } catch (...) {
    auto msg = getErrorMessage();
    logAndTerminate(msg);
  }
}

// This class is used to safely throw or ignore exceptions in destructors. It is
// used if a destructor of a class must call a function that might fail and
// report that failure via an exception (e.g. writing some final metadata to a
// file). Typically, exceptions in C++ destructors are very unsafe, because they
// immediately lead to program termination if the destructor was called while
// another exception was already handled. The use case is as follows:
// 1. Declare a class member of type `ThrowInDestructorIfSafe`. This has the
// effect that automatically the destructor of your class will implicitly be
// `noexcept(false)` which enables exceptions in destructors.
// 2. Pass the potentially throwing code in the destructor as a callable to the
// `ThrowInDestructorIfSafe` object. If this callable. throws, and it is safe to
// throw, then the exception is normally propagated. If it is not safe to throw,
// then the exception is caught and logged, but otherwise ignored. Example:
// class C {
//   ThrowOnDestructorIfSafe throwIfSafe_;
//   ~C() {
//     throwIfSafe_([]{throw std::runtime_error("haha");});
//   }
class ThrowInDestructorIfSafe {
  int numExceptionsDuringConstruction_ = std::uncaught_exceptions();

 public:
  CPP_template(typename FuncType,
               typename... Args)(requires std::invocable<FuncType> CPP_and(
      ...&& std::convertible_to<Args, std::string_view>)) void
  operator()(FuncType f, const Args&... additionalMessages) const {
    auto logIgnoredException = [&additionalMessages...](std::string_view what) {
      std::string_view sep = sizeof...(additionalMessages) == 0 ? "" : " ";
      LOG(WARN) << absl::StrCat(
                       "An exception was ignored because it would have led to "
                       "program termination",
                       sep, additionalMessages...,
                       ". Exception message: ", what)
                << std::endl;
    };
    // If the number of uncaught exceptions is the same as when then constructor
    // was called, then it is safe to throw a possible exception. For details
    // see https://en.cppreference.com/w/cpp/error/uncaught_exception,
    // especially the links at the bottom of the page.
    auto potentiallyThrow =
        [this, &logIgnoredException](std::string_view logMessage) {
          if (std::uncaught_exceptions() == numExceptionsDuringConstruction_) {
            throw;
          } else {
            logIgnoredException(logMessage);
          }
        };
    try {
      std::invoke(std::move(f));
    } catch (const std::exception& e) {
      potentiallyThrow(e.what());
    } catch (...) {
      potentiallyThrow("Exception not inheriting from `std::exception`");
    }
  }
  ~ThrowInDestructorIfSafe() noexcept(false) = default;
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_EXCEPTIONHANDLING_H
