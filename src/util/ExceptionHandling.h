//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_EXCEPTIONHANDLING_H
#define QLEVER_SRC_UTIL_EXCEPTIONHANDLING_H

#include <absl/strings/str_cat.h>

#include <exception>
#include <iostream>
#include <mutex>
#include <utility>

#include "backports/type_traits.h"
#include "util/Forward.h"
#include "util/Log.h"
#include "util/SourceLocation.h"

namespace ad_utility {
namespace detail {
struct CallStdTerminate {
  [[noreturn]] void operator()() const noexcept { std::terminate(); }
};
}  // namespace detail
// Call `f()`. If this call throws, catch the exception and log it, but do not
// propagate it. Can be used to make destructors `noexcept` when the destructor
// has to perform actions that might throw, but when handling these exceptions
// is not important.
CPP_template(typename F)(
    requires ql::concepts::invocable<ql::remove_cvref_t<
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
    AD_LOG_INFO << "Ignored an exception. The exception message was:\""
                << e.what() << "\". " << additionalNote << std::endl;
  } catch (...) {
    AD_LOG_INFO << "Ignored an exception of an unknown type. " << additionalNote
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
CPP_template(typename F, typename TerminateAction = detail::CallStdTerminate)(
    requires ql::concepts::invocable<ql::remove_cvref_t<F>> CPP_and
        std::is_nothrow_invocable_v<
            TerminateAction>) void terminateIfThrows(F&& f,
                                                     std::string_view message,
                                                     TerminateAction
                                                         terminateAction = {},
                                                     ad_utility::source_location l =
                                                         AD_CURRENT_SOURCE_LOC()) noexcept {
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
      AD_LOG_ERROR << msg << std::endl;
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
// 1. Declare a class member of type `ThrowInDestructorIfSafe` and explicitly
// mark the enclosing destructor `noexcept(false)` so that an exception can
// actually propagate out of it.
// 2. Pass the potentially throwing code in the destructor as a callable to the
// `ThrowInDestructorIfSafe` object. If this callable throws, and it is safe to
// throw, then the exception is normally propagated. If it is not safe to throw,
// then the exception is caught and logged, but otherwise ignored. Example:
// class C {
//   ThrowInDestructorIfSafe throwIfSafe_;
//   ~C() noexcept(false) {
//     throwIfSafe_([]{throw std::runtime_error("haha");});
//   }
class ThrowInDestructorIfSafe {
  int numExceptionsDuringConstruction_ = std::uncaught_exceptions();

 public:
  CPP_template(typename FuncType, typename... Args)(
      requires ql::concepts::invocable<FuncType> CPP_and(
          ...&& ql::concepts::convertible_to<Args, std::string_view>)) void
  operator()(FuncType f, const Args&... additionalMessages) const {
    auto logIgnoredException = [&additionalMessages...](std::string_view what) {
      std::string_view sep = sizeof...(additionalMessages) == 0 ? "" : " ";
      AD_LOG_WARN
          << absl::StrCat(
                 "An exception was ignored because it would have led to "
                 "program termination",
                 sep, additionalMessages..., ". Exception message: ", what)
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
  ~ThrowInDestructorIfSafe() = default;
};

// Collect exceptions produced by concurrent tasks so the first one can be
// rethrown to the caller after the tasks have finished. The natural use case
// is with `boost::asio` (see example).
//
// The collector itself satisfies the `void(std::exception_ptr)` completion
// handler signature, so it can be used as `std::ref(collector)` instead of
// `net::detached`. For executors that do not pass a completion handler (e.g.
// `net::post`), use `wrap()` to adapt an arbitrary callable.
//
// Example:
//   ad_utility::ExceptionCollector collector;
//   net::post(pool, collector.wrap([&] { mayThrow(); }));
//   net::co_spawn(pool, createCoroutineThatMightThrow(), std::ref(collector));
//   pool.join();
//   collector.rethrowIfException();
class ExceptionCollector {
  std::mutex mutex_;
  std::exception_ptr firstException_;
  ThrowInDestructorIfSafe throwIfSafe_;

 public:
  ExceptionCollector() = default;
  ExceptionCollector(const ExceptionCollector&) = delete;
  ExceptionCollector& operator=(const ExceptionCollector&) = delete;
  ExceptionCollector(ExceptionCollector&&) = delete;
  ExceptionCollector& operator=(ExceptionCollector&&) = delete;

  ~ExceptionCollector() noexcept(false) {
    if (!firstException_) {
      return;
    }
    AD_LOG_WARN << "`ExceptionCollector` was destroyed without a call to "
                   "`rethrow()` even though an exception was captured. The "
                   "exception will be rethrown now (or logged if rethrowing "
                   "would call `std::terminate`)."
                << std::endl;
    throwIfSafe_(
        [this]() { std::rethrow_exception(std::move(firstException_)); },
        std::string_view{"in `ExceptionCollector` destructor"});
  }

  // Stores the first non-null exception. Subsequent exceptions are logged.
  // Thread-safe.
  void operator()(std::exception_ptr exception) noexcept {
    if (!exception) return;
    std::lock_guard lock{mutex_};
    if (!firstException_) {
      firstException_ = std::move(exception);
      return;
    }
    try {
      std::rethrow_exception(std::move(exception));
    } catch (const std::exception& ex) {
      AD_LOG_WARN << "Additional exception captured by `ExceptionCollector` "
                     "while one was already stored; only the first one will "
                     "be rethrown. Message: "
                  << ex.what() << std::endl;
    } catch (...) {
      AD_LOG_WARN << "Additional exception of unknown type captured by "
                     "`ExceptionCollector` while one was already stored."
                  << std::endl;
    }
  }

  // Adapt a nullary callable for executors that do not pass a completion
  // handler (e.g. `boost::asio::post`). The returned callable holds a pointer
  // to this collector, which must outlive any invocation of it.
  template <typename Func>
  auto wrap(Func func) {
    return [this, func = std::move(func)]() mutable {
      try {
        std::invoke(func);
      } catch (...) {
        (*this)(std::current_exception());
      }
    };
  }

  // Rethrow the first captured exception, if any. After this call the exception
  // is consumed and this object is ready to collect new exceptions.
  void rethrowIfException() {
    std::lock_guard lock{mutex_};
    if (firstException_) {
      std::rethrow_exception(std::exchange(firstException_, nullptr));
    }
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_EXCEPTIONHANDLING_H
