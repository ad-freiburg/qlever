// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "util/MemorySize/MemorySize.h"

#include <absl/strings/charconv.h>
#include <absl/strings/str_cat.h>

#include <charconv>
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
    return absl::StrCat(number, " ", unitName);
  };

  /*
  Choosing the memory unit type is done by choosing the unit type, in which
  range `memoryInBytes_` is contained.
  A memory unit type normally has the range `[hisSize,
  sizeOfTheNextBiggerUnit)`.
  Only exceptions are:
  - `TB`, which has no upper bound, because it's our biggest unit.
  - `kB`, which has the lower bound `100'000` instead of `1'000`. Typically, for
  such small sizes you still want the exact value because they mean something
  ,e.g. a block size or a page size etc..
  */
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
  if (auto matcher = ctre::match<
          "(?<amount>\\d+(?:\\.\\d+)?) ?(?<unit>[kKmMgGtT][bB]?|[bB])">(str)) {
    auto amountString = matcher.get<"amount">().to_view();
    double amount;
    absl::from_chars(amountString.begin(), amountString.end(), amount);
    auto unitString = matcher.get<"unit">().to_view();
    switch (unitString.at(0)) {
      case 'b':
      case 'B':
        if (ad_utility::contains(amountString, '.')) {
          break;
        }
        return MemorySize::bytes(static_cast<size_t>(amount));
      case 'k':
      case 'K':
        return MemorySize::kilobytes(amount);
      case 'm':
      case 'M':
        return MemorySize::megabytes(amount);
      case 'g':
      case 'G':
        return MemorySize::gigabytes(amount);
      case 't':
      case 'T':
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
