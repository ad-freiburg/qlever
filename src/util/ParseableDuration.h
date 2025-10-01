//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_PARSEABLEDURATION_H
#define QLEVER_PARSEABLEDURATION_H

#include <absl/strings/str_cat.h>

#include <boost/lexical_cast.hpp>
#include <chrono>
#include <ctre-unicode.hpp>
#include <iostream>

#include "backports/keywords.h"
#include "util/Exception.h"
#include "util/TypeIdentity.h"
#include "util/TypeTraits.h"

namespace ad_utility {

// Wrapper type for std::chrono::duration<> to avoid having to declare
// this in the std::chrono namespace.
template <typename DT>
class ParseableDuration {
 public:
  using DurationType = DT;

 private:
  DurationType duration_{};

  template <typename Other>
  friend class ParseableDuration;

 public:
  ParseableDuration() = default;
  // Implicit conversion is on purpose!
  QL_EXPLICIT(false)
  ParseableDuration(DurationType duration) : duration_{duration} {}
  QL_EXPLICIT(false) operator DurationType() const { return duration_; }

  // TODO default this implementation (and remove explicit equality) once libc++
  // supports it.
  auto operator<=>(const ParseableDuration& other) const noexcept {
    return duration_.count() <=> other.duration_.count();
  }

  template <typename Other>
  auto operator<=>(const ParseableDuration<Other>& other) const noexcept {
    using CommonType = std::common_type_t<DurationType, Other>;
    return CommonType{duration_}.count() <=>
           CommonType{other.duration_}.count();
  }

  bool operator==(const ParseableDuration&) const noexcept = default;

  // ___________________________________________________________________________
  template <typename CharT>
  friend inline std::basic_istream<CharT>& operator>>(
      std::basic_istream<CharT>& is, ParseableDuration<DurationType>& result) {
    using namespace std::chrono;
    std::basic_string<CharT> arg;

    is >> arg;

    try {
      result = ParseableDuration<DurationType>::fromString(arg);
    } catch (const std::runtime_error&) {
      // >> operator does not throw, it sets the error state
      is.setstate(is.rdstate() | std::ios_base::failbit);
    }

    return is;
  }

  // ___________________________________________________________________________
  static ParseableDuration<DurationType> fromString(std::string_view arg) {
    using namespace std::chrono;
    using ad_utility::use_type_identity::ti;

    if (auto m = ctre::match<R"(\s*(-?\d+)\s*(ns|us|ms|s|min|h)\s*)">(arg)) {
      auto unit = m.template get<2>().to_view();

      auto toDuration = [&m](auto t) {
        using OriginalDuration = typename decltype(t)::type;
        auto amount = m.template get<1>()
                          .template to_number<typename OriginalDuration::rep>();
        return duration_cast<DurationType>(OriginalDuration{amount});
      };

      if (unit == "ns") {
        return toDuration(ti<nanoseconds>);
      } else if (unit == "us") {
        return toDuration(ti<microseconds>);
      } else if (unit == "ms") {
        return toDuration(ti<milliseconds>);
      } else if (unit == "s") {
        return toDuration(ti<seconds>);
      } else if (unit == "min") {
        return toDuration(ti<minutes>);
      } else {
        // Verify unit was checked exhaustively
        AD_CORRECTNESS_CHECK(unit == "h");
        return toDuration(ti<hours>);
      }
    }
    throw std::runtime_error{absl::StrCat(
        "Failed to convert string '", arg,
        "' to duration type. Examples for valid strings: '100ms', '3s'.")};
  }

  // ___________________________________________________________________________
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

  // ___________________________________________________________________________
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
