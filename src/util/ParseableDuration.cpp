// Copyright 2026 The QLever Authors, in particular:
//
// 2023 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ParseableDuration.h"

#include <charconv>
#include <ctre-unicode.hpp>

#include "util/TypeIdentity.h"
#include "util/TypeTraits.h"

// ___________________________________________________________________________
namespace ad_utility {

namespace detail {
// CTRE regex pattern for C++17 compatibility.
static constexpr ctll::fixed_string durationPatternRegex =
    R"(\s*(-?\d+)\s*(ns|us|ms|s|min|h)\s*)";
}  // namespace detail

// _____________________________________________________________________________
template <typename DurationType>
ParseableDuration<DurationType> ParseableDuration<DurationType>::fromString(
    std::string_view arg) {
  using namespace std::chrono;
  using use_type_identity::ti;

  if (auto m = ctre::match<detail::durationPatternRegex>(arg)) {
    auto unit = m.template get<2>().to_view();
    // Convert the amount from the unit of the string to `DurationType`. Throw
    // if the amount does not fit into the unit of the string or into
    // `DurationType` (the `duration_cast` would silently overflow).
    auto toDuration = [&m, &arg](auto t) {
      using OriginalDuration = typename decltype(t)::type;
      using Factor = std::ratio_divide<typename OriginalDuration::period,
                                       typename DurationType::period>;
      // Parse the amount (the regex guarantees an optional minus followed by
      // digits, so the only possible error is a number that does not fit into
      // the type).
      auto amountView = m.template get<1>().to_view();
      typename OriginalDuration::rep amount{};
      auto ec = std::from_chars(amountView.data(),
                                amountView.data() + amountView.size(), amount)
                    .ec;
      if (ec != std::errc{} ||
          amount > DurationType::max().count() / Factor::num ||
          amount < DurationType::min().count() / Factor::num) {
        throw std::runtime_error{
            absl::StrCat("Failed to convert string '", arg,
                         "' to duration type, the value is out of range.")};
      }
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
      // Verify unit was checked exhaustively.
      AD_CORRECTNESS_CHECK(unit == "h");
      return toDuration(ti<hours>);
    }
  }
  throw std::runtime_error{absl::StrCat(
      "Failed to convert string '", arg,
      "' to duration type. Examples for valid strings: '100ms', '3s'.")};
}

// ___________________________________________________________________________
template <typename D>
std::istream& ParseableDuration<D>::operatorIstreamImpl(
    std::istream& is, ParseableDuration<D>& result) {
  using namespace std::chrono;
  std::string arg;

  is >> arg;

  try {
    result = ParseableDuration::fromString(arg);
  } catch (const std::runtime_error&) {
    // >> operator does not throw, it sets the error state
    is.setstate(is.rdstate() | std::ios_base::failbit);
  }

  return is;
}

// ___________________________________________________________________________
template <typename D>
std::ostream& ParseableDuration<D>::operatorOstreamImpl(
    std::ostream& os, const ParseableDuration& duration) {
  using namespace std::chrono;
  using period = typename D::period;

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
template <typename D>
std::string ParseableDuration<D>::toString() const {
  std::ostringstream os;
  os << *this;
  return std::move(os).str();
}

static_assert(
    std::is_move_constructible_v<ParseableDuration<std::chrono::seconds>>);
static_assert(
    std::is_move_assignable_v<ParseableDuration<std::chrono::seconds>>);
static_assert(
    std::is_copy_constructible_v<ParseableDuration<std::chrono::seconds>>);
static_assert(
    std::is_copy_assignable_v<ParseableDuration<std::chrono::seconds>>);

}  // namespace ad_utility.

// Explicit instantiations for the chrono types used in this project.
template class ad_utility::ParseableDuration<std::chrono::seconds>;
template class ad_utility::ParseableDuration<std::chrono::milliseconds>;
template class ad_utility::ParseableDuration<std::chrono::microseconds>;
template class ad_utility::ParseableDuration<std::chrono::nanoseconds>;
template class ad_utility::ParseableDuration<std::chrono::minutes>;
template class ad_utility::ParseableDuration<std::chrono::hours>;
