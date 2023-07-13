// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include <string_view>
#include <tuple>

#include "util/AbstractMemory/Memory.h"

namespace ad_utility {
// _____________________________________________________________________________
Memory& Memory::operator=(const size_t& amountOfMemoryInBytes) {
  memoryInBytes_ = amountOfMemoryInBytes;
  return *this;
}

// _____________________________________________________________________________
Memory& Memory::operator=(size_t&& amountOfMemoryInBytes) {
  memoryInBytes_ = std::move(amountOfMemoryInBytes);
  return *this;
}

// _____________________________________________________________________________
size_t Memory::bytes() const { return memoryInBytes_; }

// Helper function for dividing two instances of `size_t`. Mainly exists for
// code reduction.
static double sizetDivision(const size_t& dividend, const size_t& divisor) {
  auto dv = std::lldiv(dividend, divisor);
  return static_cast<double>(dv.quot) + static_cast<double>(dv.rem) / divisor;
}

// _____________________________________________________________________________
double Memory::kilobytes() const {
  return sizetDivision(memoryInBytes_, detail::numberOfBytesPerKB);
}

// _____________________________________________________________________________
double Memory::megabytes() const {
  return sizetDivision(memoryInBytes_, detail::numberOfBytesPerMB);
}

// _____________________________________________________________________________
double Memory::gigabytes() const {
  return sizetDivision(memoryInBytes_, detail::numberOfBytesPerGB);
}

// _____________________________________________________________________________
double Memory::terabytes() const {
  return sizetDivision(memoryInBytes_, detail::numberOfBytesPerTB);
}

// _____________________________________________________________________________
double Memory::petabytes() const {
  return sizetDivision(memoryInBytes_, detail::numberOfBytesPerPB);
}

// _____________________________________________________________________________
std::string Memory::asString() const {
  // Convert number and memory unit name to the string, we want to return.
  auto toString = [](const auto& number, std::string_view unitName) {
    return absl::StrCat(number, " ", unitName);
  };

  // Just use the first unit, which is bigger/equal to `memoryInBytes_`.
  if (memoryInBytes_ >= detail::numberOfBytesPerPB) {
    return toString(petabytes(), "PB");
  } else if (memoryInBytes_ >= detail::numberOfBytesPerTB) {
    return toString(terabytes(), "TB");
  } else if (memoryInBytes_ >= detail::numberOfBytesPerGB) {
    return toString(gigabytes(), "GB");
  } else if (memoryInBytes_ >= detail::numberOfBytesPerMB) {
    return toString(megabytes(), "MB");
  } else if (memoryInBytes_ >= detail::numberOfBytesPerKB) {
    return toString(kilobytes(), "KB");
  } else {
    // Just return the amount of bytes.
    return toString(memoryInBytes_, "Byte");
  }
}

}  // namespace ad_utility
