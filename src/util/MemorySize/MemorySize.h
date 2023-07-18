// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <concepts>
#include <cstddef>
#include <string>
#include <type_traits>

#include "util/Cache.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"

namespace ad_utility {
/*
##############
# Definition #
##############
*/

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
  constexpr MemorySize() = default;
  constexpr MemorySize(const MemorySize&) = default;
  constexpr MemorySize(MemorySize&&) = default;

  // Default assignment operator.
  constexpr MemorySize& operator=(const MemorySize&) = default;
  constexpr MemorySize& operator=(MemorySize&&) = default;

  /*
  Factory functions for creating an instance of this class with the wanted
  memory size saved internally. Always requries the exact memory size unit and
  size wanted.
  */
  static constexpr MemorySize bytes(size_t numBytes);
  static constexpr MemorySize kilobytes(size_t numKilobytes);
  static constexpr MemorySize kilobytes(double numKilobytes);
  static constexpr MemorySize megabytes(size_t numMegabytes);
  static constexpr MemorySize megabytes(double numMegabytes);
  static constexpr MemorySize gigabytes(size_t numGigabytes);
  static constexpr MemorySize gigabytes(double numGigabytes);
  static constexpr MemorySize terabytes(size_t numTerabytes);
  static constexpr MemorySize terabytes(double numTerabytes);

  /*
  Return the internal memory amount in the wanted memory unit format.
  For example: If the internal memory amount is 1000 bytes, than `kilobytes()`
  would return `1.0`.
  */
  size_t constexpr getBytes() const;
  double constexpr getKilobytes() const;
  double constexpr getMegabytes() const;
  double constexpr getGigabytes() const;
  double constexpr getTerabytes() const;

  /*
  Return the string representation of the internal memory amount in the
  biggest memory unit, that is equal to, or smaller than, the internal memory
  amount, with the exception of `kB`.
  `kB` is only used, when the internal memory amount is in the range ``[10^5,
  10^6)`.
  Example: 10^9 bytes would be returned as `"1 GB"`, 1'000 bytes as `1000 B` and
  100'000 as `100 kB`.
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
  explicit constexpr MemorySize(size_t amountOfMemoryInBytes)
      : memoryInBytes_{amountOfMemoryInBytes} {}
};

/*
User defined literals for memory units.
Note that user defined literals only allow very specific types for function
arguments, so I couldn't use more fitting types.
*/
namespace memory_literals {
constexpr MemorySize operator""_B(unsigned long long int bytes);
constexpr MemorySize operator""_kB(long double kilobytes);
constexpr MemorySize operator""_kB(unsigned long long int kilobytes);
constexpr MemorySize operator""_MB(long double megabytes);
constexpr MemorySize operator""_MB(unsigned long long int megabytes);
constexpr MemorySize operator""_GB(long double gigabytes);
constexpr MemorySize operator""_GB(unsigned long long int gigabytes);
constexpr MemorySize operator""_TB(long double terabytes);
constexpr MemorySize operator""_TB(unsigned long long int terabytes);
}  // namespace memory_literals

/*
#######################################################
# Implementation of the previously declared functions #
#######################################################
*/

// Helper functions.
namespace details {
// Just the number of bytes per memory unit.
constexpr size_t numBytesPerkB = ad_utility::pow<size_t>(10, 3);
constexpr size_t numBytesPerMB = ad_utility::pow<size_t>(10, 6);
constexpr size_t numBytesPerGB = ad_utility::pow<size_t>(10, 9);
constexpr size_t numBytesPerTB = ad_utility::pow<size_t>(10, 12);

/*
Helper function for dividing two instances of `size_t`.
Needed, because there is no `std` division function, that takes unconverted
`size_t`.This tends to lead to error and unprecise results. This function
however, should be about as precise as possible, when having the return type
`double`.
*/
static constexpr double sizeTDivision(const size_t dividend,
                                      const size_t divisor) {
  size_t quotient = dividend / divisor;
  return static_cast<double>(quotient) +
         static_cast<double>(dividend % divisor) / static_cast<double>(divisor);
}

/*
@brief Calculate the amount of bytes for a given amount of untis and a given
amount of bytes per unit.

@return The amount of bytes. Rounded up, if needed.
*/
template <typename T>
requires std::integral<T> || std::floating_point<T>
size_t constexpr convertMemoryUnitsToBytes(const T amountOfUnits,
                                           const size_t numBytesPerUnit) {
  // Negativ values makes no sense.
  AD_CONTRACT_CHECK(amountOfUnits >= 0);

  // Max value for `amountOfUnits`.
  AD_CONTRACT_CHECK(static_cast<T>(sizeTDivision(
                        size_t_max, numBytesPerUnit)) >= amountOfUnits);

  if constexpr (std::is_floating_point_v<T>) {
    // We (maybe) have to round up.
    return static_cast<size_t>(
        std::ceil(amountOfUnits * static_cast<double>(numBytesPerUnit)));
  } else {
    AD_CORRECTNESS_CHECK(std::is_integral_v<T>);
    return amountOfUnits * numBytesPerUnit;
  }
}
}  // namespace details

// _____________________________________________________________________________
constexpr MemorySize MemorySize::bytes(size_t numBytes) {
  return MemorySize{numBytes};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::kilobytes(size_t numKilobytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numKilobytes, details::numBytesPerkB)};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::kilobytes(double numKilobytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numKilobytes, details::numBytesPerkB)};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::megabytes(size_t numMegabytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numMegabytes, details::numBytesPerMB)};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::megabytes(double numMegabytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numMegabytes, details::numBytesPerMB)};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::gigabytes(size_t numGigabytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numGigabytes, details::numBytesPerGB)};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::gigabytes(double numGigabytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numGigabytes, details::numBytesPerGB)};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::terabytes(size_t numTerabytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numTerabytes, details::numBytesPerTB)};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::terabytes(double numTerabytes) {
  return MemorySize{
      details::convertMemoryUnitsToBytes(numTerabytes, details::numBytesPerTB)};
}

// _____________________________________________________________________________
constexpr size_t MemorySize::getBytes() const { return memoryInBytes_; }

// _____________________________________________________________________________
constexpr double MemorySize::getKilobytes() const {
  return details::sizeTDivision(memoryInBytes_, details::numBytesPerkB);
}

// _____________________________________________________________________________
constexpr double MemorySize::getMegabytes() const {
  return details::sizeTDivision(memoryInBytes_, details::numBytesPerMB);
}

// _____________________________________________________________________________
constexpr double MemorySize::getGigabytes() const {
  return details::sizeTDivision(memoryInBytes_, details::numBytesPerGB);
}

// _____________________________________________________________________________
constexpr double MemorySize::getTerabytes() const {
  return details::sizeTDivision(memoryInBytes_, details::numBytesPerTB);
}

namespace memory_literals {
// _____________________________________________________________________________
constexpr MemorySize operator""_B(unsigned long long int bytes) {
  return MemorySize::bytes(bytes);
}

// _____________________________________________________________________________
constexpr MemorySize operator""_kB(long double kilobytes) {
  return MemorySize::kilobytes(static_cast<double>(kilobytes));
}

// _____________________________________________________________________________
constexpr MemorySize operator""_kB(unsigned long long int kilobytes) {
  return MemorySize::kilobytes(static_cast<size_t>(kilobytes));
}

// _____________________________________________________________________________
constexpr MemorySize operator""_MB(long double megabytes) {
  return MemorySize::megabytes(static_cast<double>(megabytes));
}

// _____________________________________________________________________________
constexpr MemorySize operator""_MB(unsigned long long int megabytes) {
  return MemorySize::megabytes(static_cast<size_t>(megabytes));
}

// _____________________________________________________________________________
constexpr MemorySize operator""_GB(long double gigabytes) {
  return MemorySize::gigabytes(static_cast<double>(gigabytes));
}

// _____________________________________________________________________________
constexpr MemorySize operator""_GB(unsigned long long int gigabytes) {
  return MemorySize::gigabytes(static_cast<size_t>(gigabytes));
}

// _____________________________________________________________________________
constexpr MemorySize operator""_TB(long double terabytes) {
  return MemorySize::terabytes(static_cast<double>(terabytes));
}

// _____________________________________________________________________________
constexpr MemorySize operator""_TB(unsigned long long int terabytes) {
  return MemorySize::terabytes(static_cast<size_t>(terabytes));
}
}  // namespace memory_literals
}  // namespace ad_utility
