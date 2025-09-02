// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "util/MemorySize/MemorySize.h"

#include <absl/strings/charconv.h>
#include <absl/strings/str_cat.h>

#include <cctype>
#include <charconv>
#include <cmath>
#include <ctre-unicode.hpp>
#include <string_view>

#include "util/Algorithm.h"
#include "util/ConstexprMap.h"
#include "util/ConstexprUtils.h"

namespace ad_utility {
// _____________________________________________________________________________
std::string MemorySize::asString() const {
  // Convert number and memory unit name to the string, we want to return.
  auto toString = [](const auto number, std::string_view unitName) {
    using T = std::decay_t<decltype(number)>;
    if constexpr (std::integral<T>) {
      return absl::StrCat(number, " ", unitName);
    } else {
      static_assert(std::floating_point<T>);
      return number * 10 == std::floor(number * 10)
                 ? absl::StrCat(number, " ", unitName)
                 : absl::StrCat(std::round(number * 10) / 10, " ", unitName);
    }
  };

  // Lower bounds on the size for the various memory units. Used to determine
  // the memory unit type below.
  //
  // NOTE: the lower bound for `kB` is `100'000` instead of `1'000` because we
  // want exact values below `100'000` bytes (for example, block or page sizes
  // are often larger than 1'000 bytes but below 100'000 bytes).
  constexpr ad_utility::ConstexprMap<char, size_t, 4> memoryUnitLowerBound(
      {std::pair<char, size_t>{'k', ad_utility::pow(10, 5)},
       std::pair<char, size_t>{'M', detail::numBytesPerUnit.at("MB")},
       std::pair<char, size_t>{'G', detail::numBytesPerUnit.at("GB")},
       std::pair<char, size_t>{'T', detail::numBytesPerUnit.at("TB")}});

  // Go through the units from top to bottom, in terms of size, and choose the
  // first one, that is smaller/equal to `memoryInBytes_`.
  if (memoryInBytes_ >= memoryUnitLowerBound.at('T')) {
    return toString(getTerabytes(), "TB");
  } else if (memoryInBytes_ >= memoryUnitLowerBound.at('G')) {
    return toString(getGigabytes(), "GB");
  } else if (memoryInBytes_ >= memoryUnitLowerBound.at('M')) {
    return toString(getMegabytes(), "MB");
  } else if (memoryInBytes_ >= memoryUnitLowerBound.at('k')) {
    return toString(getKilobytes(), "kB");
  } else {
    // Just return the amount of bytes.
    return toString(memoryInBytes_, "B");
  }
}

// _____________________________________________________________________________
MemorySize MemorySize::parse(std::string_view str) {
  constexpr ctll::fixed_string regex =
      "(?<amount>\\d+(?:\\.\\d+)?)\\s*(?<unit>[kKmMgGtT][bB]?|[bB])";
  if (auto matcher = ctre::match<regex>(str)) {
    auto amountString = matcher.get<"amount">().to_view();
    // Even though CTRE supports to_number() with double values, this relies on
    // `std::from_chars` which is currently not supported by the standard
    // library used by our macOS build.
    double amount;
    absl::from_chars(amountString.begin(), amountString.end(), amount);
    auto unitString = matcher.get<"unit">().to_view();
    switch (std::tolower(unitString.at(0))) {
      case 'b':
        if (ad_utility::contains(amountString, '.')) {
          throw std::runtime_error(absl::StrCat(
              "'", str,
              "' could not be parsed as a memory size. When using bytes as "
              "units only unsigned integers are allowed."));
        }
        return MemorySize::bytes(static_cast<size_t>(amount));
      case 'k':
        return MemorySize::kilobytes(amount);
      case 'm':
        return MemorySize::megabytes(amount);
      case 'g':
        return MemorySize::gigabytes(amount);
      case 't':
        return MemorySize::terabytes(amount);
      default:
        // Whatever this is, it is false.
        AD_FAIL();
    }
  }
  throw std::runtime_error(absl::StrCat(
      "'", str,
      R"(' could not be parsed as a memory size. Examples for valid memory sizes are "4 B", "3.21 MB", "2.392 TB".)"));
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const ad_utility::MemorySize& mem) {
  os << mem.asString();
  return os;
}

}  // namespace ad_utility
