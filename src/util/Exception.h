// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: 2011-2017 Björn Buchhold <buchholb@cs.uni-freiburg.de>
//         2020-     Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include <exception>
#include <sstream>
#include <string>

#include "util/SourceLocation.h"
#include "util/TypeTraits.h"

using std::string;

// -------------------------------------------
// Exception class code
// -------------------------------------------
namespace ad_utility {

// Exception class for rethrowing exceptions during a query abort.
// Such exceptions are never printed but still keep the original what()
// message just in case
class AbortException : public std::exception {
 private:
  string what_;

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
  explicit Exception(const std::string& message,
                     ad_utility::source_location location =
                         ad_utility::source_location::current())
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
[[noreturn]] inline void AD_THROW(std::string_view message,
                                  ad_utility::source_location location =
                                      ad_utility::source_location::current()) {
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

// Custom assert which does not abort but throws an exception. Use this for
// conditions that can be violated by calling a public (member) function with
// invalid inputs. Since it is a macro, the code coverage check will only report
// a partial coverage for this macro if the condition is never violated, so make
// sure to integrate a unit test that violates the condition.
#define AD_CONTRACT_CHECK(condition)                                                                                                                       \
  if (!(condition)) [[unlikely]] {                                                                                                                         \
    using namespace std::string_literals;                                                                                                                  \
    AD_THROW(                                                                                                                                              \
        "Assertion `"s + std::string(__STRING(condition)) +                                                                                                \
        "` failed. Likely cause: A function was called with arguments that violate the contract of the function. Please report this to the developers."s); \
  }                                                                                                                                                        \
  void(0)

// Custom assert which does not abort but throws an exception. Use this for
// conditions that can never be violated via a public (member) function. It is
// thus impossible to test the failure of such an assertion in a unit test.
// Because of the additional indirection via the function call, code coverage
// tools will consider this call fully covered as soon as the check is
// performed, even if it never fails.
namespace ad_utility::detail {
inline void adCorrectnessCheckImpl(bool condition, std::string_view message,
                                   ad_utility::source_location location) {
  if (!(condition)) [[unlikely]] {
    using namespace std::string_literals;
    // TODO<GCC13> Use `std::format`.
    AD_THROW(
        "Assertion `"s + std::string(message) +
            "` failed. This indicates that an internal function was not implemented correctly, which should never happen. Please report this to the developers"s,
        location);
  }
}
}  // namespace ad_utility::detail
#define AD_CORRECTNESS_CHECK(condition)                  \
  ad_utility::detail::adCorrectnessCheckImpl(            \
      static_cast<bool>(condition), __STRING(condition), \
      ad_utility::source_location::current())

// This check is similar to `AD_CORRECTNESS_CHECK` (see above), but the check is
// only compiled and executed when either the `NDEBUG` constant is NOT defined
// (which also enables the classic `assert()` macro), or the constant
// `AD_ENABLE_EXPENSIVE_CHECKS` is enabled (which can be set via cmake). This
// check should be used when checking has a significant and measurable runtime
// overhead, but is still feasible for a release build on a large dataset.
#if (!defined(NDEBUG) || defined(AD_ENABLE_EXPENSIVE_CHECKS))
#define AD_EXPENSIVE_CHECK(condition) \
  AD_CORRECTNESS_CHECK(condition);    \
  void(0)
#else
#define AD_EXPENSIVE_CHECK(condition) void(0)
#endif
