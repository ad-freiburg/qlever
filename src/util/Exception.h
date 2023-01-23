// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: mostly copied from CompleteSearch code, original author unknown.
// Adapted by: Björn Buchhold <buchholb>

#ifndef GLOBALS_EXCEPTION_H_
#define GLOBALS_EXCEPTION_H_

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

//! Exception class for rethrowing exceptions during a query abort
//! such exceptions are never printed but still keep the original what()
//! message just in case
class AbortException : public std::exception {
 public:
  AbortException(const std::exception& original) : _what(original.what()) {}
  AbortException(const std::string& whatthe) : _what(whatthe) {}
  const char* what() const noexcept { return _what.c_str(); }

 private:
  string _what;
};

class Exception : public std::exception {
 private:
  std::string message_;
  ad_utility::source_location location_;

 public:
  Exception(const std::string& message,
            ad_utility::source_location location =
                ad_utility::source_location::current())
      : message_{std::move(message)}, location_{location} {}
  const char* what() const noexcept override { return message_.c_str(); }
  const char* getErrorDetailsNoFileAndLines() const noexcept { return what(); }
};
}  // namespace ad_utility

// -------------------------------------------
// Macros for throwing exceptions comfortably.
// -------------------------------------------
// Throw exception with additional assert-like info
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
// Needed for Cygwin (will be an identical redefine  for *nixes)
#define __STRING(x) #x

/// Custom assert that will always fail and report the file and line.
#define AD_FAIL() AD_THROW("This code should be unreachable");
/// Custom assert which does not abort but throws an exception.
// TODO<joka921> readd the `source_location` to make this more useful.
inline void adCheckImpl(bool condition, std::string_view message,
                        ad_utility::source_location location) {
  if (!(condition)) [[unlikely]] {
    using namespace std::string_literals;
    // TODO<GCC13> Use `std::format`.
    AD_THROW("Assertion `"s + std::string(message) + "` failed."s, location);
  }
}

#define AD_CHECK(condition)                                      \
  adCheckImpl(static_cast<bool>(condition), __STRING(condition), \
              ad_utility::source_location::current())

#endif  // GLOBALS_EXCEPTION_H_
