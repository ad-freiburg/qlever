// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: 2011-2017 Björn Buchhold <buchholb@cs.uni-freiburg.de>
//         2020-     Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_EXCEPTION_H
#define QLEVER_SRC_UTIL_EXCEPTION_H

#include <absl/strings/str_cat.h>

#include <exception>
#include <sstream>
#include <string>

#include "backports/concepts.h"
#include "backports/functional.h"
#include "util/SourceLocation.h"
#include "util/TypeTraits.h"

// -------------------------------------------
// Exception class code
// -------------------------------------------
namespace ad_utility {

// Exception class for rethrowing exceptions during a query abort.
// Such exceptions are never printed but still keep the original what()
// message just in case
class AbortException : public std::exception {
 private:
  std::string what_;

 public:
  explicit AbortException(const std::exception& original)
      : what_{original.what()} {}
  explicit AbortException(std::string what) : what_{std::move(what)} {}
  [[nodiscard]] const char* what() const noexcept override {
    return what_.c_str();
  }
};

// An exception that stores a message as well as the `source_location` where
// the exception was triggered. Mostly used by the `AD_THROW` and
// `AD_CONTRACT_CHECK` macros (see below).
class Exception : public std::exception {
 private:
  std::string message_;
  ad_utility::source_location location_;

 public:
  explicit Exception(
      const std::string& message,
      ad_utility::source_location location = AD_CURRENT_SOURCE_LOC())
      : location_{location} {
    std::stringstream str;
    // TODO<GCC13> Use `std::format`.
    str << message << ". In file \"" << location_.file_name() << " \" at line "
        << location_.line();
    message_ = std::move(str).str();
  }
  [[nodiscard]] const char* what() const noexcept override {
    return message_.c_str();
  }
};
}  // namespace ad_utility

// Throw exception with additional assert-like info.
[[noreturn]] inline void AD_THROW(
    std::string_view message,
    ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) {
  throw ad_utility::Exception{std::string{message}, location};
}

// --------------------------------------------------------------------------
// Macros for assertions that will throw Exceptions.
// --------------------------------------------------------------------------
//! NOTE: Should be used only for asserts which affect the total running only
//! very insignificantly. Counterexample: don't use this in an inner loop that
//! is executed million of times, and has otherwise little code.
// --------------------------------------------------------------------------

// Custom assert that will always fail and report the file and line. Note that
// for code that is truly unreachable (not even by setting up artificially
// faulty input in a unit test), our code coverage tools will always consider
// this macro uncovered. This cannot even be changed by changing it to something
// like `__builtin_unreachable()` (last checked by Johannes in Jan 2023).
#define AD_FAIL() AD_THROW("This code should be unreachable")

namespace ad_utility::detail {

template <typename S>
CPP_requires(is_str_catable_, requires(S&& s)(absl::StrCat(AD_FWD(s))));

template <typename S>
CPP_concept CanStrCat = CPP_requires_ref(is_str_catable_, S);

// Helper functions that convert the various arguments to the
// `AD_CONTRACT_CHECK` etc. macros to strings.
// The argument must be
// * A type that can be passed to `absl::StrCat` (e.g. `string`, `string_views`,
// builtin numeric types) [first overload]
// * A callbable that takes no arguments and returns a string [second overload]
CPP_template(typename S)(requires CanStrCat<S>) std::string
    getMessageImpl(S&& s) {
  return absl::StrCat(AD_FWD(s));
}
CPP_template(typename T)(
    requires ad_utility::InvocableWithConvertibleReturnType<T, std::string>)
    std::string getMessageImpl(T&& f) {
  return std::invoke(f);
}

// Helper function used to format the arguments passed to `AD_CONTRACT_CHECK`
// etc. Return "<concatenation of `getMessageImpl(messages)...`>" followed by
// a full stop and space if there is at least one message.
template <typename... Args>
std::string concatMessages(Args&&... messages) {
  if constexpr (sizeof...(messages) == 0) {
    return "";
  } else {
    return absl::StrCat(getMessageImpl(messages)..., ". ");
  }
}
}  // namespace ad_utility::detail

// A macro for the common implementation of `AD_CONTRACT_CHECK` and
// `AD_CORRECTNESS_CHECK` (see below).
#define AD_CHECK_IMPL(condition, conditionString, sourceLocation, ...)      \
  using namespace std::string_literals;                                     \
  if (!(condition)) [[unlikely]] {                                          \
    AD_THROW("Assertion `"s + std::string(conditionString) + "` failed. " + \
                 ad_utility::detail::concatMessages(__VA_ARGS__) +          \
                 "Please report this to the developers"s,                   \
             sourceLocation);                                               \
  }                                                                         \
  void(0)

// Custom assert which does not abort but throws an exception. Use this for
// conditions that can be violated by calling a public (member) function with
// invalid inputs. Since it is a macro, the code coverage check will only report
// a partial coverage for this macro if the condition is never violated, so make
// sure to integrate a unit test that violates the condition.
// The first argument is the condition (anything that is convertible to `bool`).
// Additional arguments are concatenated to get a detailed error message in the
// failure case. each of these arguments can be either a type that can be
// converted to a string via `absl::StrCat` (e.g. strings or builtin numeric
// types) or a callable that produce a `std::string`. The latter case is useful
// if the error message is expensive to construct because the callables are only
// invoked if the assertion fails. For examples see `ExceptionTest.cpp`.
#define AD_CONTRACT_CHECK(condition, ...)       \
  AD_CHECK_IMPL(condition, __STRING(condition), \
                AD_CURRENT_SOURCE_LOC() __VA_OPT__(, ) __VA_ARGS__)

// Custom assert which does not abort but throws an exception. Use this for
// conditions that can never be violated via a public (member) function. It is
// thus impossible to test the failure of such an assertion in a unit test.
// Because of the additional indirection via the function call, code coverage
// tools will consider this call fully covered as soon as the check is
// performed, even if it never fails.
// For  details on the usage see the documentation of `AD_CONTRACT_CHECK` above
// as well as the examples in `ExceptionTest.cpp`
namespace ad_utility::detail {
inline void adCorrectnessCheckImpl(bool condition, std::string_view message,
                                   ad_utility::source_location location,
                                   auto&&... additionalMessages) {
  AD_CHECK_IMPL(condition, message, location, additionalMessages...);
}
}  // namespace ad_utility::detail
#define AD_CORRECTNESS_CHECK(condition, ...)             \
  ad_utility::detail::adCorrectnessCheckImpl(            \
      static_cast<bool>(condition), __STRING(condition), \
      AD_CURRENT_SOURCE_LOC() __VA_OPT__(, ) __VA_ARGS__)

// This check is similar to `AD_CORRECTNESS_CHECK` (see above), but the check is
// only compiled and executed when either the `NDEBUG` constant is NOT defined
// (which also enables the classic `assert()` macro), or the constant
// `AD_ENABLE_EXPENSIVE_CHECKS` is enabled (which can be set via cmake). This
// check should be used when checking has a significant and measurable runtime
// overhead, but is still feasible for a release build on a large dataset.
#if (!defined(NDEBUG) || defined(AD_ENABLE_EXPENSIVE_CHECKS))
namespace ad_utility {
static constexpr bool areExpensiveChecksEnabled = true;
}
#define AD_EXPENSIVE_CHECK(condition, ...) \
  AD_CORRECTNESS_CHECK(condition, __VA_ARGS__)
#else
namespace ad_utility {
static constexpr bool areExpensiveChecksEnabled = false;
}
#define AD_EXPENSIVE_CHECK(condition, ...) void(0)
#endif

#endif  // QLEVER_SRC_UTIL_EXCEPTION_H
