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
  // Default constructors.
  Memory() : memoryInBytes_(0) {}
  Memory(const Memory&) = default;
  Memory(Memory&&) = default;

  // Default assignment operator.
  Memory& operator=(const Memory&) = default;
  Memory& operator=(Memory&&) = default;

  /*
  Factory functions for creating an instance of this class with the wanted
  memory size saved internally. Always requries the exact memory size unit and
  size wanted.
  */
  static Memory bytes(size_t numBytes);
  static Memory kilobytes(size_t numKilobytes);
  static Memory kilobytes(double numKilobytes);
  static Memory megabytes(size_t numMegabytes);
  static Memory megabytes(double numMegabytes);
  static Memory gigabytes(size_t numGigabytes);
  static Memory gigabytes(double numGigabytes);
  static Memory terabytes(size_t numTerabytes);
  static Memory terabytes(double numTerabytes);
  static Memory petabytes(size_t numPetabytes);
  static Memory petabytes(double numPetabytes);

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

  // Custom `<<` overload.
  friend std::ostream& operator<<(std::ostream& os, const Memory& mem);

  /*
  @brief Parse the given string and create a `Memory` object, set to the memory
  amount described.

  @param str A string following `./generated/MemoryDefinitionLanguage.g4`. In
  short: An amount of bytes described via a user defined literal.
  */
  static Memory parse(std::string_view str);

 private:
  // Constructor for the factory functions.
  Memory(size_t amountOfMemoryInBytes)
      : memoryInBytes_{amountOfMemoryInBytes} {}
};

// User defined literals for memory units.
namespace memory_literals {
Memory operator""_Byte(unsigned long long int bytes);
Memory operator""_KB(long double kilobytes);
Memory operator""_KB(unsigned long long int kilobytes);
Memory operator""_MB(long double megabytes);
Memory operator""_MB(unsigned long long int megabytes);
Memory operator""_GB(long double gigabytes);
Memory operator""_GB(unsigned long long int gigabytes);
Memory operator""_TB(long double terabytes);
Memory operator""_TB(unsigned long long int terabytes);
Memory operator""_PB(long double petabytes);
Memory operator""_PB(unsigned long long int petabytes);
}  // namespace memory_literals

}  // namespace ad_utility
