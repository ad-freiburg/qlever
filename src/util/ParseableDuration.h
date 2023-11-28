//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_PARSEABLEDURATION_H
#define QLEVER_PARSEABLEDURATION_H

#include <absl/strings/str_cat.h>
#include <ctre/ctre.h>

#include <boost/lexical_cast.hpp>
#include <iostream>

#include "util/Exception.h"

namespace ad_utility {

// Wrapper type for std::chrono::duration<> to avoid having to declare
// this in the std::chrono namespace.
template <typename DurationType>
class ParseableDuration {
  DurationType duration_{};

  template <typename CharT>
  friend inline std::basic_istream<CharT>& operator>>(
      std::basic_istream<CharT>& is, ParseableDuration<DurationType>& result) {
    using namespace std::chrono;
    std::basic_string<CharT> arg;

    is >> arg;

    if (auto matcher = ctre::match<"(-?\\d+)(ns|us|ms|s|min|h)">(arg)) {
      auto amount = matcher.template get<1>().to_view();
      auto unit = matcher.template get<2>().to_view();

      auto toDuration = []<typename OriginalDuration>(std::string_view amount) {
        return duration_cast<DurationType>(OriginalDuration{
            boost::lexical_cast<typename OriginalDuration::rep>(amount)});
      };

      if (unit == "ns") {
        result = toDuration.template operator()<nanoseconds>(amount);
      } else if (unit == "us") {
        result = toDuration.template operator()<microseconds>(amount);
      } else if (unit == "ms") {
        result = toDuration.template operator()<milliseconds>(amount);
      } else if (unit == "s") {
        result = toDuration.template operator()<seconds>(amount);
      } else if (unit == "min") {
        result = toDuration.template operator()<minutes>(amount);
      } else if (unit == "h") {
        result = toDuration.template operator()<hours>(amount);
      } else {
        // Unsupported suffix
        AD_FAIL();
      }
    } else {
      is.setstate(is.rdstate() | std::ios_base::failbit);
    }

    return is;
  }

  template <typename CharT>
  friend inline std::basic_ostream<CharT>& operator<<(
      std::basic_ostream<CharT>& os,
      const ParseableDuration<DurationType>& duration) {
    using namespace std::chrono;
    using period = DurationType::period;

    os << duration.duration_.count();

    if constexpr (std::is_same_v<period, std::nano>) {
      os << "ns";
    } else if constexpr (std::is_same_v<period, std::micro>) {
      os << "us";
    } else if constexpr (std::is_same_v<period, std::milli>) {
      os << "ms";
    } else if constexpr (std::is_same_v<period, seconds::period>) {
      os << "s";
    } else if constexpr (std::is_same_v<period, minutes::period>) {
      os << "min";
    } else if constexpr (std::is_same_v<period, hours::period>) {
      os << "h";
    } else {
      static_assert(ad_utility::alwaysFalse<period>,
                    "Unsupported std::chrono::duration period");
    }

    return os;
  }

 public:
  // Implicit conversion is on purpose!
  ParseableDuration() = default;
  ParseableDuration(DurationType duration) : duration_{duration} {}
  ParseableDuration(DurationType::rep rep) : duration_{rep} {}
  operator DurationType() const { return duration_; }
  auto operator<=>(const ParseableDuration&) const noexcept = default;

  static ParseableDuration<DurationType> fromString(const std::string& str) {
    std::istringstream is{str};
    ParseableDuration<DurationType> result;
    is >> result;
    if (is.rdstate() & std::ios_base::failbit) {
      throw std::runtime_error{absl::StrCat("Failed to convert string '", str,
                                            "' to duration type.")};
    }
    return result;
  }

  std::string toString() const {
    std::ostringstream os;
    os << *this;
    return std::move(os).str();
  }
};

static_assert(
    std::is_move_constructible_v<ParseableDuration<std::chrono::seconds>>);
static_assert(
    std::is_move_assignable_v<ParseableDuration<std::chrono::seconds>>);
static_assert(
    std::is_copy_constructible_v<ParseableDuration<std::chrono::seconds>>);
static_assert(
    std::is_copy_assignable_v<ParseableDuration<std::chrono::seconds>>);
}  // namespace ad_utility

#endif  // QLEVER_PARSEABLEDURATION_H
