// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>

#include <cassert>
#include <concepts>
#include <cstddef>
#include <string>
#include <type_traits>

#include "util/Cache.h"
#include "util/ConstexprMap.h"
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
  constexpr static MemorySize bytes(size_t numBytes);
  constexpr static MemorySize kilobytes(size_t numKilobytes);
  constexpr static MemorySize kilobytes(double numKilobytes);
  constexpr static MemorySize megabytes(size_t numMegabytes);
  constexpr static MemorySize megabytes(double numMegabytes);
  constexpr static MemorySize gigabytes(size_t numGigabytes);
  constexpr static MemorySize gigabytes(double numGigabytes);
  constexpr static MemorySize terabytes(size_t numTerabytes);
  constexpr static MemorySize terabytes(double numTerabytes);

  /*
  Return the internal memory amount in the wanted memory unit format.
  For example: If the internal memory amount is 1000 bytes, than `kilobytes()`
  would return `1.0`.
  */
  constexpr size_t getBytes() const;
  constexpr double getKilobytes() const;
  constexpr double getMegabytes() const;
  constexpr double getGigabytes() const;
  constexpr double getTerabytes() const;

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
consteval MemorySize operator""_B(unsigned long long int bytes);
consteval MemorySize operator""_kB(long double kilobytes);
consteval MemorySize operator""_kB(unsigned long long int kilobytes);
consteval MemorySize operator""_MB(long double megabytes);
consteval MemorySize operator""_MB(unsigned long long int megabytes);
consteval MemorySize operator""_GB(long double gigabytes);
consteval MemorySize operator""_GB(unsigned long long int gigabytes);
consteval MemorySize operator""_TB(long double terabytes);
consteval MemorySize operator""_TB(unsigned long long int terabytes);
}  // namespace memory_literals

/*
#######################################################
# Implementation of the previously declared functions #
#######################################################
*/

// Helper functions.
namespace detail {
// Just the number of bytes per memory unit.
constexpr ConstexprMap<std::string_view, size_t, 5> numBytesPerUnit(
    {std::pair{"B", 1uL}, std::pair{"kB", ad_utility::pow<size_t>(10, 3)},
     std::pair{"MB", ad_utility::pow<size_t>(10, 6)},
     std::pair{"GB", ad_utility::pow<size_t>(10, 9)},
     std::pair{"TB", ad_utility::pow<size_t>(10, 12)}});

/*
Helper function for dividing two instances of `size_t`.
Needed, because there is no `std` division function, that takes unconverted
`size_t`.This tends to lead to error and unprecise results. This function
however, should be about as precise as possible, when having the return type
`double`.
*/
constexpr static double sizeTDivision(const size_t dividend,
                                      const size_t divisor) {
  size_t quotient = dividend / divisor;
  return static_cast<double>(quotient) +
         static_cast<double>(dividend % divisor) / static_cast<double>(divisor);
}

/*
@brief Calculate the amount of bytes for a given amount of units.

@param unitName Must be `B`, `kB`, `MB`, `GB`, or `TB`.

@return The amount of bytes. Rounded up, if needed.
*/
template <typename T>
requires std::integral<T> || std::floating_point<T>
constexpr size_t convertMemoryUnitsToBytes(const T amountOfUnits,
                                           std::string_view unitName) {
  // Negativ values makes no sense.
  AD_CONTRACT_CHECK(amountOfUnits >= 0);

  // Must be one of the supported units.
  AD_CONTRACT_CHECK(numBytesPerUnit.contains(unitName));

  // Max value for `amountOfUnits`.
  if (static_cast<T>(sizeTDivision(size_t_max, numBytesPerUnit.at(unitName))) <
      amountOfUnits) {
    throw std::runtime_error(
        absl::StrCat(amountOfUnits, " ", unitName,
                     " is larger than the maximum amount of memory that can be "
                     "addressed using 64 bits"));
  }

  if constexpr (std::is_floating_point_v<T>) {
    // TODO<c++23> As of `c++23`, `std::ceil` is constexpr and can be used.
    const double doubleResult =
        amountOfUnits * static_cast<double>(numBytesPerUnit.at(unitName));
    const auto unroundedResult = static_cast<size_t>(doubleResult);
    // We (maybe) have to round up.
    return doubleResult > static_cast<double>(unroundedResult)
               ? unroundedResult + 1
               : unroundedResult;
  } else {
    static_assert(std::is_integral_v<T>);
    return amountOfUnits * numBytesPerUnit.at(unitName);
  }
}
}  // namespace detail

// _____________________________________________________________________________
constexpr MemorySize MemorySize::bytes(size_t numBytes) {
  return MemorySize{numBytes};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::kilobytes(size_t numKilobytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numKilobytes, "kB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::kilobytes(double numKilobytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numKilobytes, "kB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::megabytes(size_t numMegabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numMegabytes, "MB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::megabytes(double numMegabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numMegabytes, "MB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::gigabytes(size_t numGigabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numGigabytes, "GB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::gigabytes(double numGigabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numGigabytes, "GB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::terabytes(size_t numTerabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numTerabytes, "TB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::terabytes(double numTerabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numTerabytes, "TB")};
}

// _____________________________________________________________________________
constexpr size_t MemorySize::getBytes() const { return memoryInBytes_; }

// _____________________________________________________________________________
constexpr double MemorySize::getKilobytes() const {
  return detail::sizeTDivision(memoryInBytes_,
                               detail::numBytesPerUnit.at("kB"));
}

// _____________________________________________________________________________
constexpr double MemorySize::getMegabytes() const {
  return detail::sizeTDivision(memoryInBytes_,
                               detail::numBytesPerUnit.at("MB"));
}

// _____________________________________________________________________________
constexpr double MemorySize::getGigabytes() const {
  return detail::sizeTDivision(memoryInBytes_,
                               detail::numBytesPerUnit.at("GB"));
}

// _____________________________________________________________________________
constexpr double MemorySize::getTerabytes() const {
  return detail::sizeTDivision(memoryInBytes_,
                               detail::numBytesPerUnit.at("TB"));
}

namespace memory_literals {
// _____________________________________________________________________________
consteval MemorySize operator""_B(unsigned long long int bytes) {
  return MemorySize::bytes(bytes);
}

// _____________________________________________________________________________
consteval MemorySize operator""_kB(long double kilobytes) {
  return MemorySize::kilobytes(static_cast<double>(kilobytes));
}

// _____________________________________________________________________________
consteval MemorySize operator""_kB(unsigned long long int kilobytes) {
  return MemorySize::kilobytes(static_cast<size_t>(kilobytes));
}

// _____________________________________________________________________________
consteval MemorySize operator""_MB(long double megabytes) {
  return MemorySize::megabytes(static_cast<double>(megabytes));
}

// _____________________________________________________________________________
consteval MemorySize operator""_MB(unsigned long long int megabytes) {
  return MemorySize::megabytes(static_cast<size_t>(megabytes));
}

// _____________________________________________________________________________
consteval MemorySize operator""_GB(long double gigabytes) {
  return MemorySize::gigabytes(static_cast<double>(gigabytes));
}

// _____________________________________________________________________________
consteval MemorySize operator""_GB(unsigned long long int gigabytes) {
  return MemorySize::gigabytes(static_cast<size_t>(gigabytes));
}

// _____________________________________________________________________________
consteval MemorySize operator""_TB(long double terabytes) {
  return MemorySize::terabytes(static_cast<double>(terabytes));
}

// _____________________________________________________________________________
consteval MemorySize operator""_TB(unsigned long long int terabytes) {
  return MemorySize::terabytes(static_cast<size_t>(terabytes));
}
}  // namespace memory_literals
}  // namespace ad_utility
