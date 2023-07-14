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

#include "util/AbstractMemory/CalculationUtil.h"
#include "util/Exception.h"

namespace ad_utility {

/*
An abstract class, that represents an amount of memory.
Note: Literals for easier usage of the class were defined after under the class
definition.
*/
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

// User defined literals for memory units.
namespace memory_literals {
size_t constexpr operator""_Byte(unsigned long long int bytes) { return bytes; }

size_t constexpr operator""_KB(long double kilobytes) {
  return convertMemoryUnitsToBytes(kilobytes, numberOfBytesPerKB);
}

size_t constexpr operator""_KB(unsigned long long int kilobytes) {
  return convertMemoryUnitsToBytes(kilobytes, numberOfBytesPerKB);
}

size_t constexpr operator""_MB(long double megabytes) {
  return convertMemoryUnitsToBytes(megabytes, numberOfBytesPerMB);
}

size_t constexpr operator""_MB(unsigned long long int megabytes) {
  return convertMemoryUnitsToBytes(megabytes, numberOfBytesPerMB);
}

size_t constexpr operator""_GB(long double gigabytes) {
  return convertMemoryUnitsToBytes(gigabytes, numberOfBytesPerGB);
}

size_t constexpr operator""_GB(unsigned long long int gigabytes) {
  return convertMemoryUnitsToBytes(gigabytes, numberOfBytesPerGB);
}

size_t constexpr operator""_TB(long double terabytes) {
  return convertMemoryUnitsToBytes(terabytes, numberOfBytesPerTB);
}

size_t constexpr operator""_TB(unsigned long long int terabytes) {
  return convertMemoryUnitsToBytes(terabytes, numberOfBytesPerTB);
}

size_t constexpr operator""_PB(long double petabytes) {
  return convertMemoryUnitsToBytes(petabytes, numberOfBytesPerPB);
}

size_t constexpr operator""_PB(unsigned long long int petabytes) {
  return convertMemoryUnitsToBytes(petabytes, numberOfBytesPerPB);
}
}  // namespace memory_literals

}  // namespace ad_utility
