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

#include "util/Exception.h"

namespace ad_utility {

/*
An abstract class, that represents an amount of memory.
Note:
 - Literals for easier usage of the class were defined after under the class
definition.
 - Memory size units use base 10. In particular, a kilobyte has 1'000 bytes and
a megabyte has 1'000'000 bytes.
*/
class MemorySize {
  // Because of `sizeof` we know, that any size of memory, in bytes, can be
  // encoded as a `size_t` in cpp.
  size_t memoryInBytes_ = 0;

 public:
  // Default constructors.
  MemorySize() = default;
  MemorySize(const MemorySize&) = default;
  MemorySize(MemorySize&&) = default;

  // Default assignment operator.
  MemorySize& operator=(const MemorySize&) = default;
  MemorySize& operator=(MemorySize&&) = default;

  /*
  Factory functions for creating an instance of this class with the wanted
  memory size saved internally. Always requries the exact memory size unit and
  size wanted.
  */
  static MemorySize bytes(size_t numBytes);
  static MemorySize kilobytes(size_t numKilobytes);
  static MemorySize kilobytes(double numKilobytes);
  static MemorySize megabytes(size_t numMegabytes);
  static MemorySize megabytes(double numMegabytes);
  static MemorySize gigabytes(size_t numGigabytes);
  static MemorySize gigabytes(double numGigabytes);
  static MemorySize terabytes(size_t numTerabytes);
  static MemorySize terabytes(double numTerabytes);

  /*
  Return the internal memory amount in the wanted memory unit format.
  For example: If the internal memory amount is 1000 bytes, than `kilobytes()`
  would return `1.0`.
  */
  size_t getBytes() const;
  double getKilobytes() const;
  double getMegabytes() const;
  double getGigabytes() const;
  double getTerabytes() const;

  /*
  Return the string representation of the internal memory amount in the
  biggest memory unit, that is equal to, or smaller than, the internal memory
  amount.
  Example: 1000 bytes would be returned as `"1 kB"`.
  */
  std::string asString() const;

  // Custom `<<` overload.
  friend std::ostream& operator<<(std::ostream& os, const MemorySize& mem);

  /*
  @brief Parse the given string and create a `MemorySize` object, set to the
  memory size described.

  @param str A string following `./generated/MemorySizeLanguage.g4`. In short:
  An amount of bytes described via a user defined literal.
  */
  static MemorySize parse(std::string_view str);

 private:
  // Constructor for the factory functions.
  explicit MemorySize(size_t amountOfMemoryInBytes)
      : memoryInBytes_{amountOfMemoryInBytes} {}
};

/*
User defined literals for memory units.
Note that user defined literals only allow very specific types for function
arguments, so I couldn't use more fitting types.
*/
namespace memory_literals {
MemorySize operator""_B(unsigned long long int bytes);
MemorySize operator""_kB(long double kilobytes);
MemorySize operator""_kB(unsigned long long int kilobytes);
MemorySize operator""_MB(long double megabytes);
MemorySize operator""_MB(unsigned long long int megabytes);
MemorySize operator""_GB(long double gigabytes);
MemorySize operator""_GB(unsigned long long int gigabytes);
MemorySize operator""_TB(long double terabytes);
MemorySize operator""_TB(unsigned long long int terabytes);
}  // namespace memory_literals

}  // namespace ad_utility
