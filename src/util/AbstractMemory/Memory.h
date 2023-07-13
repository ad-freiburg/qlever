// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <cmath>
#include <concepts>
#include <cstddef>
#include <string>
#include <type_traits>

#include "util/ConstexprUtils.h"
#include "util/Exception.h"

namespace ad_utility {

// An abstract class, that represents an amount of memory.
class Memory {
  // Because of `sizeof` we know, that any size of memory, in bytes, can be
  // encoded as a `size_t` in cpp.
  size_t memoryInBytes_;

 public:
  // Very simple custom constructor.
  Memory(const size_t& amountOfMemoryInBytes = 0)
      : memoryInBytes_{amountOfMemoryInBytes} {}

  // Default assignment operator.
  Memory& operator=(const Memory&) = default;
  Memory& operator=(Memory&&) = default;

  // Custom assignment operator, so that we can assign user literals.
  Memory& operator=(const size_t& amountOfMemoryInBytes);
  Memory& operator=(size_t&& amountOfMemoryInBytes);

  /*
  Return the internal memory amount in the wanted memory unit format.
  For example: If the internal memory amount is 1024 bytes, than `kilobytes()`
  would return `1.0`.
  */
  size_t bytes() const;
  double kilobytes() const;
  double megabytes() const;
  double gigabytes() const;
  double terabytes() const;
  double petabytes() const;

  /*
  Return the string representation of the internal memory amount in the
  biggest memory unit, that is equal to, or smaller than, the internal memory
  amount.
  Example: 1024 bytes would be returned as `"1 KB"`.
  */
  std::string asString() const;

  /*
  @brief Parse the given string and set the internal memory amount to the amount
  described.

  @param str A string following `./generated/MemoryDefinitionLanguage.g4`. In
  short: An amount of bytes described via a user defined literal.
  */
  void parse(std::string_view str);
};

namespace detail {
/*
@brief Calculate the amount of bytes for a given amount of untis and a given
amount of bytes per unit.

@return The amount of bytes. Rounded up, if needed.
*/
template <typename T>
requires std::integral<T> || std::floating_point<T>
size_t constexpr convertMemoryUnitsToBytes(const T& amountOfUnits,
                                           const size_t& numberOfBytesPerUnit) {
  if constexpr (std::is_floating_point_v<T>) {
    // We (maybe) have to round up.
    return static_cast<size_t>(std::ceil(amountOfUnits * numberOfBytesPerUnit));
  } else {
    AD_CORRECTNESS_CHECK(std::is_integral_v<T>);
    return amountOfUnits * numberOfBytesPerUnit;
  }
}

// Just the number of bytes per memory unit.
constexpr size_t numberOfBytesPerKB = ad_utility::pow<size_t>(2, 10);
constexpr size_t numberOfBytesPerMB = ad_utility::pow<size_t>(2, 20);
constexpr size_t numberOfBytesPerGB = ad_utility::pow<size_t>(2, 30);
constexpr size_t numberOfBytesPerTB = ad_utility::pow<size_t>(2, 40);
constexpr size_t numberOfBytesPerPB = ad_utility::pow<size_t>(2, 50);

}  // namespace detail

// User defined literals for memory units.
size_t constexpr operator""_Byte(unsigned long long int bytes) { return bytes; }

size_t constexpr operator""_KB(long double kilobytes) {
  return detail::convertMemoryUnitsToBytes(kilobytes,
                                           detail::numberOfBytesPerKB);
}

size_t constexpr operator""_KB(unsigned long long int kilobytes) {
  return detail::convertMemoryUnitsToBytes(kilobytes,
                                           detail::numberOfBytesPerKB);
}

size_t constexpr operator""_MB(long double megabytes) {
  return detail::convertMemoryUnitsToBytes(megabytes,
                                           detail::numberOfBytesPerMB);
}

size_t constexpr operator""_MB(unsigned long long int megabytes) {
  return detail::convertMemoryUnitsToBytes(megabytes,
                                           detail::numberOfBytesPerMB);
}

size_t constexpr operator""_GB(long double gigabytes) {
  return detail::convertMemoryUnitsToBytes(gigabytes,
                                           detail::numberOfBytesPerGB);
}

size_t constexpr operator""_GB(unsigned long long int gigabytes) {
  return detail::convertMemoryUnitsToBytes(gigabytes,
                                           detail::numberOfBytesPerGB);
}

size_t constexpr operator""_TB(long double terabytes) {
  return detail::convertMemoryUnitsToBytes(terabytes,
                                           detail::numberOfBytesPerTB);
}

size_t constexpr operator""_TB(unsigned long long int terabytes) {
  return detail::convertMemoryUnitsToBytes(terabytes,
                                           detail::numberOfBytesPerTB);
}

size_t constexpr operator""_PB(long double petabytes) {
  return detail::convertMemoryUnitsToBytes(petabytes,
                                           detail::numberOfBytesPerPB);
}

size_t constexpr operator""_PB(unsigned long long int petabytes) {
  return detail::convertMemoryUnitsToBytes(petabytes,
                                           detail::numberOfBytesPerPB);
}
}  // namespace ad_utility
