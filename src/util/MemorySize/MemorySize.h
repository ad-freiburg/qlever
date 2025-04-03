// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_MEMORYSIZE_MEMORYSIZE_H
#define QLEVER_SRC_UTIL_MEMORYSIZE_MEMORYSIZE_H

#include <absl/strings/str_cat.h>

#include <cassert>
#include <concepts>
#include <cstddef>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "backports/algorithm.h"
#include "util/ConstexprMap.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"

namespace ad_utility {
/*
##############
# Definition #
##############
*/

// A concept, for when a type should be an integral, or a floating point.
template <typename T>
CPP_concept Arithmetic = (std::integral<T> || std::floating_point<T>);

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

  // Default comparison operator.
  constexpr auto operator<=>(const MemorySize&) const = default;

  // Hashing.
  template <typename H>
  friend H AbslHashValue(H h, const MemorySize& mem) {
    return H::combine(std::move(h), mem.memoryInBytes_);
  }

  /*
  Factory functions for creating an instance of this class with the wanted
  memory size saved internally. Always requires the exact memory size unit and
  size wanted.
  */
  CPP_template(typename T)(requires std::integral<T>)  //
      constexpr static MemorySize bytes(T numBytes);
  CPP_template(typename T)(requires Arithmetic<T>)  //
      constexpr static MemorySize kilobytes(T numKilobytes);
  CPP_template(typename T)(requires Arithmetic<T>)  //
      constexpr static MemorySize megabytes(T numMegabytes);
  CPP_template(typename T)(requires Arithmetic<T>)  //
      constexpr static MemorySize gigabytes(T numGigabytes);
  CPP_template(typename T)(requires Arithmetic<T>)  //
      constexpr static MemorySize terabytes(T numTerabytes);

  // Factory for max size instance.
  constexpr static MemorySize max();

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

  // Arithmetic operators and arithmetic assignment operators.
  constexpr MemorySize operator+(const MemorySize& m) const;
  constexpr MemorySize& operator+=(const MemorySize& m);
  constexpr MemorySize operator-(const MemorySize& m) const;
  constexpr MemorySize& operator-=(const MemorySize& m);

  CPP_template(typename T)(requires Arithmetic<T>)  //
      constexpr MemorySize
      operator*(const T c) const;

  template <typename T>
  friend constexpr auto operator*(const T c, const MemorySize m)
      -> CPP_ret(MemorySize)(requires Arithmetic<T>);

  CPP_template(typename T)(requires Arithmetic<T>)  //
      constexpr MemorySize&
      operator*=(const T c);

  CPP_template(typename T)(requires Arithmetic<T>) constexpr MemorySize
  operator/(const T c) const;

  CPP_template(typename T)(requires Arithmetic<T>) constexpr MemorySize&
  operator/=(const T c);

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
`size_t`.This tends to lead to error and imprecise results. This function
however, should be about as precise as possible, when having the return type
`double`.
*/
constexpr static double sizeTDivision(const size_t dividend,
                                      const size_t divisor) {
  size_t quotient = dividend / divisor;
  return static_cast<double>(quotient) +
         static_cast<double>(dividend % divisor) / static_cast<double>(divisor);
}

// Max value for a size_t.
static constexpr size_t size_t_max = std::numeric_limits<size_t>::max();

// The maximal amount of a memory unit, that a `MemorySize` can remember.
constexpr ConstexprMap<std::string_view, double, 5> maxAmountOfUnit(
    {std::pair{"B", sizeTDivision(size_t_max, numBytesPerUnit.at("B"))},
     std::pair{"kB", sizeTDivision(size_t_max, numBytesPerUnit.at("kB"))},
     std::pair{"MB", sizeTDivision(size_t_max, numBytesPerUnit.at("MB"))},
     std::pair{"GB", sizeTDivision(size_t_max, numBytesPerUnit.at("GB"))},
     std::pair{"TB", sizeTDivision(size_t_max, numBytesPerUnit.at("TB"))}});

// Converts a given number to `size_t`. Rounds up, if needed.
CPP_template(typename T)(requires Arithmetic<T>) constexpr size_t
    ceilAndCastToSizeT(const T d) {
  if constexpr (std::is_floating_point_v<T>) {
    // TODO<c++23> As of `c++23`, `std::ceil` is constexpr and can be used.
    const auto unrounded = static_cast<size_t>(d);
    // We (maybe) have to round up.
    return d > static_cast<T>(unrounded) ? unrounded + 1 : unrounded;
  } else {
    static_assert(std::is_integral_v<T>);
    return static_cast<size_t>(d);
  }
}

/*
@brief Calculate the amount of bytes for a given amount of units.

@param unitName Must be `kB`, `MB`, `GB`, or `TB`.

@return The amount of bytes. Rounded up, if needed.
*/
CPP_template(typename T)(requires Arithmetic<T>)  //
    constexpr size_t convertMemoryUnitsToBytes(const T amountOfUnits,
                                               std::string_view unitName) {
  if constexpr (std::is_signed_v<T>) {
    // Negative values makes no sense.
    AD_CONTRACT_CHECK(amountOfUnits >= 0);
  }

  // Must be one of the supported units.
  // TODO Replace with correctness check, should it ever become constexpr.
  AD_CONTRACT_CHECK(numBytesPerUnit.contains(unitName));

  /*
  Max value for `amountOfUnits`.
  Note, that max amount of units for a unit of `unitName` is sometimes bigger
  than what can represented with `T`.
  */
  if (static_cast<T>(
          std::min(maxAmountOfUnit.at(unitName),
                   static_cast<double>(std::numeric_limits<T>::max()))) <
      amountOfUnits) {
    throw std::runtime_error(
        absl::StrCat(amountOfUnits, " ", unitName,
                     " is larger than the maximum amount of memory that can be "
                     "addressed using 64 bits."));
  }

  if constexpr (std::is_floating_point_v<T>) {
    return ceilAndCastToSizeT(
        static_cast<double>(amountOfUnits) *
        static_cast<double>(numBytesPerUnit.at(unitName)));
  } else {
    static_assert(std::is_integral_v<T>);
    return static_cast<size_t>(amountOfUnits) * numBytesPerUnit.at(unitName);
  }
}

/*
@brief The implementation for the `MemorySize` multiplication and division
operator.

@param m The `MemorySize` instance, which will deliver the amount of bytes for
the first argument of `func`.
@param c The constant, with which the given `MemorySize` will be
multiplied/divied with.
@param func This function will calculate the amount of bytes, that the new
`MemorySize` records. It will be given the amount of bytes in `m`, followed by
`c` for this calculation. Both will either have been cast to `size_t`, or
`double.` Note, that the rounding and casting to `size_t` for floating point
return types will be automatically done, and can be ignored by `func`.
 */
CPP_template(typename T, typename Func)(requires Arithmetic<T> CPP_and(
    std::invocable<Func, const double, const double> ||
    std::invocable<Func, const size_t, const size_t>))               //
    constexpr MemorySize magicImplForDivAndMul(const MemorySize& m,  //
                                               const T c, Func func) {
  // In order for the results to be as precise as possible, we cast to highest
  // precision data type variant of `T`.
  using PrecisionType =
      std::conditional_t<std::is_integral_v<T>, size_t, double>;

  return MemorySize::bytes(detail::ceilAndCastToSizeT(
      std::invoke(func, static_cast<PrecisionType>(m.getBytes()),
                  static_cast<PrecisionType>(c))));
}
}  // namespace detail

// _____________________________________________________________________________
CPP_template_def(typename T)(requires std::integral<T>) constexpr MemorySize
    MemorySize::bytes(T numBytes) {
  if constexpr (std::is_signed_v<T>) {
    // Doesn't make much sense to a negative amount of memory.
    AD_CONTRACT_CHECK(numBytes >= 0);
  }

  return MemorySize{static_cast<size_t>(numBytes)};
}

// _____________________________________________________________________________
CPP_template_def(typename T)(requires Arithmetic<T>) constexpr MemorySize
    MemorySize::kilobytes(T numKilobytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numKilobytes, "kB")};
}

// _____________________________________________________________________________
CPP_template_def(typename T)(requires Arithmetic<T>) constexpr MemorySize
    MemorySize::megabytes(T numMegabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numMegabytes, "MB")};
}

// _____________________________________________________________________________
CPP_template_def(typename T)(requires Arithmetic<T>) constexpr MemorySize
    MemorySize::gigabytes(T numGigabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numGigabytes, "GB")};
}

// _____________________________________________________________________________
CPP_template_def(typename T)(requires Arithmetic<T>) constexpr MemorySize
    MemorySize::terabytes(T numTerabytes) {
  return MemorySize{detail::convertMemoryUnitsToBytes(numTerabytes, "TB")};
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::max() {
  return MemorySize{detail::size_t_max};
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

// _____________________________________________________________________________
constexpr MemorySize MemorySize::operator+(const MemorySize& m) const {
  // Check for overflow.
  if (memoryInBytes_ > detail::size_t_max - m.memoryInBytes_) {
    throw std::overflow_error(
        "Overflow error: Addition of the two given 'MemorySize's is not "
        "possible. "
        "It would result in a size_t overflow.");
  }
  return MemorySize::bytes(memoryInBytes_ + m.memoryInBytes_);
}

// _____________________________________________________________________________
constexpr MemorySize& MemorySize::operator+=(const MemorySize& m) {
  *this = *this + m;
  return *this;
}

// _____________________________________________________________________________
constexpr MemorySize MemorySize::operator-(const MemorySize& m) const {
  // Check for underflow.
  if (memoryInBytes_ < m.memoryInBytes_) {
    throw std::underflow_error(
        "Underflow error: Subtraction of the two given 'MemorySize's is not "
        "possible. It would result in a size_t underflow.");
  }
  return MemorySize::bytes(memoryInBytes_ - m.memoryInBytes_);
}

// _____________________________________________________________________________
constexpr MemorySize& MemorySize::operator-=(const MemorySize& m) {
  *this = *this - m;
  return *this;
}

// _____________________________________________________________________________
CPP_template_def(typename T)(requires Arithmetic<T>) constexpr MemorySize
    MemorySize::operator*(const T c) const {
  if constexpr (std::is_signed_v<T>) {
    // A negative amount of memory wouldn't make much sense.
    AD_CONTRACT_CHECK(c >= static_cast<T>(0));
  }

  // Check for overflow.
  if (static_cast<double>(c) >
      detail::sizeTDivision(detail::size_t_max, memoryInBytes_)) {
    throw std::overflow_error(
        "Overflow error: Multiplicaton of the given 'MemorySize' with the "
        "given constant is not possible. It would result in a size_t "
        "overflow.");
  }
  return detail::magicImplForDivAndMul(*this, c, std::multiplies{});
}

// _____________________________________________________________________________
template <typename T>
constexpr auto operator*(const T c, const MemorySize m)
    -> CPP_ret(MemorySize)(requires Arithmetic<T>) {
  return m * c;
}

// _____________________________________________________________________________
CPP_template_def(typename T)(
    requires Arithmetic<
        T>) constexpr MemorySize& MemorySize::operator*=(const T c) {
  *this = *this * c;
  return *this;
}

// _____________________________________________________________________________
CPP_template_def(typename T)(requires Arithmetic<T>) constexpr MemorySize
    MemorySize::operator/(const T c) const {
  if constexpr (std::is_signed_v<T>) {
    // A negative amount of memory wouldn't make much sense.
    AD_CONTRACT_CHECK(c > static_cast<T>(0));
  }

  /*
  Check for overflow. Underflow isn't possible, because neither `MemorySize`,
  nor `c`, can be negative.

  Furthermore, overflow is only possible, if `T` is a floating point. Because
  the quoutient, of the division between two natural numbers, is always smaller
  than the dividend. Which is not always true, if the divisor is a floating
  point number.
  For example: 1/(1/2) = 2
  */
  if (std::floating_point<T> &&
      static_cast<double>(memoryInBytes_) >
          static_cast<double>(detail::size_t_max) * static_cast<double>(c)) {
    throw std::overflow_error(
        "Overflow error: Division of the given 'MemorySize' with the given "
        "constant is not possible. It would result in a size_t overflow.");
  }

  /*
  The default division for `size_t` doesn't always round up, which is the
  wanted behavior for the calculation of memory sizes. Instead, we calculate
  the division with as much precision as possible and leave the rounding to
  `magicImpl`.
  */
  return detail::magicImplForDivAndMul(
      *this, c, [](const auto& a, const auto& b) {
        static_assert(std::is_same_v<decltype(a), decltype(b)>,
                      "Arguments shall be of the same type");
        using DivisionType = std::decay_t<decltype(a)>;
        if constexpr (std::is_floating_point_v<DivisionType>) {
          return a / b;
        } else {
          static_assert(std::is_integral_v<DivisionType>);
          return detail::sizeTDivision(a, b);
        }
      });
}

// _____________________________________________________________________________
CPP_template_def(typename T)(
    requires Arithmetic<
        T>) constexpr MemorySize& MemorySize::operator/=(const T c) {
  *this = *this / c;
  return *this;
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

#endif  // QLEVER_SRC_UTIL_MEMORYSIZE_MEMORYSIZE_H
