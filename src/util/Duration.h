//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

//  Tests for this class can be found in DurationTest.cpp

#ifndef QLEVER_DURATION_H
#define QLEVER_DURATION_H

#include <absl/strings/str_cat.h>

#include <bit>
#include <cmath>
#include <cstdint>
#include <exception>
#include <string>
#include <string_view>

#include "backports/three_way_comparison.h"
#include "util/Exception.h"

//______________________________________________________________________________
class DurationOverflowException : public std::exception {
 private:
  std::string message_;

 public:
  DurationOverflowException(std::string_view className,
                            std::string_view datatype)
      : message_(absl::StrCat("Overflow exception raised by ", className,
                              ", please provide smaller values for ", datatype,
                              ".")) {}
  [[nodiscard]] const char* what() const noexcept override {
    return message_.c_str();
  }
};

//______________________________________________________________________________
namespace detail {
template <typename T>
constexpr void checkBoundDuration(const T& value, const T& max,
                                  std::string_view className,
                                  std::string_view xsdDatatype) {
  if (value >= max) {
    throw DurationOverflowException{className, xsdDatatype};
  }
}
}  //  namespace detail

//______________________________________________________________________________
class DurationParseException : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

//______________________________________________________________________________
// Class similar to `Date` which holds an `xsd:dayTimeDuration` value in 64
// bits. `DayTimeDuration` objects can be also converted to an Id (Date Id) if a
// `DateYearOrDuration` is constructed with the given `DayTimeDuration` object
// as an argument, and passed to Id::makeFromDate(). The provided values to this
// class (days, hours, minutes, seconds) will be factorized to a total
// millisecond value, which is then stored in the reserved bits (48 bits). The
// operations == and <=> will be performed directly on the underlying bit
// representation, which is highly efficient. If the provided values for hours,
// minutes and seconds are larger than their typical limits given by a daytime
// (hence hours > 23, minutes > 59 or seconds > 59.999), they will be internally
// normalized over the conversion procedure.
class DayTimeDuration {
 public:
  // seconds, minutes, hours and days (multiplier for conversion to
  // milliseconds)
  static constexpr int secondMultiplier = 1000;
  static constexpr int minuteMultiplier = 60 * secondMultiplier;
  static constexpr int hourMultiplier = 60 * minuteMultiplier;
  static constexpr int dayMultiplier = 24 * hourMultiplier;

  // The maxDays value of 1048575 corresponds to approximately 2870 years.
  static constexpr unsigned long maxDays = 1048575;

  // With the given specifications above, the total number of milliseconds we
  // have to store at the limit (days -> milliseconds) + (hours -> milliseconds)
  // + (minutes -> silliseconds) + (seconds -> milliseconds).
  static constexpr unsigned long boundTotalMilliseconds =
      (maxDays + 1) * dayMultiplier;

  // The number of bits reserved to store the total number of milliseconds.
  // numMillisecondBits amounts to 48 under the specifications given above.
  // Multiply by 2 because xsd:dayTimeDuration can be negative, hence
  // we have to shift the total milliseconds value by boundTotalMilliseconds
  // into the positive value range to store an unsigned value in
  // totalMilliseconds_.
  static constexpr uint8_t numMillisecondBits =
      absl::bit_width(boundTotalMilliseconds * 2);
  static constexpr uint8_t numUnusedBits = 64 - numMillisecondBits;
  static_assert(numUnusedBits == 16,
                "The number of unused bits for Duration should be 16");

 private:
  // Given that `totalMilliseconds_` is of type `uint64_t`, to correctly store
  // the provided values here, even for durations smaller zero, they must be
  // positive. This is achieved by adding `boundTotalMilliseconds` to the actual
  // number of milliseconds for the respective duration values, thus all values,
  // also negative ones, get shifted into the positive range.
  //
  // Initial range: [-boundTotalMilliseconds, +boundTotalMilliseconds]
  // shift by adding `boundTotalMilliSeconds` (line 170-171)
  // => Shifted range: [0,  2 * boundTotalMilliseconds] (storable as unsigned)
  //
  // To retrieve the initial number of milliseconds, we just subtract
  // boundTotalMilliseconds from totalMilliSeconds_. (line 181-182)
  uint64_t totalMilliseconds_ : numMillisecondBits = 0;

  // The value of unusedBits_ is relevant w.r.t. to the DateYearOrDuration class
  // for properly shifting a given input value of type dayTimeDuration and
  // thus make it convertible to an intern Id (ValueId) -> The underlying
  // DayTimeDuration and corresponding values can be extracted again.
  uint64_t unusedBits_ : numUnusedBits = 0;

 public:
  // `DurationValue` is used to return all xsd:dayTimeDuration components over
  // `getValues()`.
  struct DurationValue {
    int days_;
    int hours_;
    int minutes_;
    double seconds_;
  };

  // make the handling of positive and negative durations more intuitive
  enum struct Type { Negative = 0, Positive = 1 };

  // Construct a `DayTimeDuration` (xsd:DayTimeDuration) object. Implicitly
  // supported operations (directly on the underlying bits) with
  // `DayTimeDuration` are `==` and `<=>`. The value range, which is not
  // affected by normalization, is for each argument: `days: {0 - 1048575}`;
  // `hours: {0 - 23}`; `minutes: {0 - 59}`; `seconds: {0 - 59.99..}`. Negative
  // durations are handled over the provided `Type`. If the provided values
  // for hours, minutes or seconds exceed the typical time limits, they will be
  // normalized in the sense of conversion to milliseconds by default. Default
  // duration: `P0DT0H0M0S` (as xsd:dayTimeDuration string).
  constexpr DayTimeDuration(Type signType = Type::Positive, int days = 0,
                            int hours = 0, int minutes = 0,
                            double seconds = 0.00) {
    setValues(days, hours, minutes, seconds, signType);
  }

  // ___________________________________________________________________________
  // Returns `true` if `DayTimeDuration` w.r.t. this object is `Type::Positive`.
  constexpr bool isPositive() const {
    return static_cast<long long>(totalMilliseconds_) >=
           static_cast<long long>(boundTotalMilliseconds);
  }

  //____________________________________________________________________________
  constexpr void setValues(int days, int hours, int minutes, double seconds,
                           Type signType) {
    AD_CONTRACT_CHECK(days >= 0 && hours >= 0 && minutes >= 0 &&
                      seconds >= 0.00);
    int sign = signType == Type::Positive ? 1 : -1;
    auto totalMilliseconds =
        static_cast<long long>(days) * dayMultiplier +
        static_cast<long long>(hours) * hourMultiplier +
        static_cast<long long>(minutes) * minuteMultiplier +
        static_cast<long long>(std::round(seconds * secondMultiplier));
    detail::checkBoundDuration(totalMilliseconds,
                               static_cast<long long>(boundTotalMilliseconds),
                               "DayTimeDuration", "xsd:dayTimeDuration");
    totalMilliseconds_ = totalMilliseconds * sign +
                         static_cast<long long>(boundTotalMilliseconds);
  }

  //____________________________________________________________________________
  // Returns a `DurationValue` struct containing `{days, hours, minutes,
  // seconds}`.
  [[nodiscard]] constexpr DurationValue getValues() const {
    // It is more efficient to extract all values at once from
    // totalMilliseconds_ w.r.t. days, hours, minutes and seconds because we can
    // reuse the remainder (updated totalMilliseconds).
    auto totalMilliseconds = static_cast<long long>(totalMilliseconds_) -
                             static_cast<long long>(boundTotalMilliseconds);

    // Given the current duration logic, the sign of a duration is defined by
    // its type and not its values, hence the respective values should be always
    // positive (days, hours, minutes, seconds).
    // -> totalMilliseconds range is symmetric to 0, thus if
    // totalMilliseconds < 0, just map it to it's positive value.
    totalMilliseconds =
        totalMilliseconds < 0 ? -totalMilliseconds : totalMilliseconds;

    auto numDays = totalMilliseconds / dayMultiplier;
    totalMilliseconds -= numDays * dayMultiplier;
    auto numHours = totalMilliseconds / hourMultiplier;
    totalMilliseconds -= numHours * hourMultiplier;
    auto numMinutes = totalMilliseconds / minuteMultiplier;
    auto numSeconds = totalMilliseconds - numMinutes * minuteMultiplier;
    return {static_cast<int>(numDays), static_cast<int>(numHours),
            static_cast<int>(numMinutes),
            static_cast<double>(numSeconds) / secondMultiplier};
  }

  //____________________________________________________________________________
  // Converts the underlying `dayTimeDuration` representation to a compact
  // bit representation (necessary for the == and <=> implementation).
  [[nodiscard]] constexpr uint64_t toBits() const {
    return absl::bit_cast<uint64_t>(*this);
  }

  // From a given bit representation, retrieve the actual `dayTimeDuration`
  // object again.
  static constexpr DayTimeDuration fromBits(uint64_t bytes) {
    return absl::bit_cast<DayTimeDuration>(bytes);
  }

  //____________________________________________________________________________
  // Comparison == on bits
  [[nodiscard]] constexpr bool operator==(const DayTimeDuration& rhs) const {
    return toBits() == rhs.toBits();
  }

  // Comparison <=> on bits
  [[nodiscard]] constexpr auto compareThreeWay(
      const DayTimeDuration& rhs) const {
    return ql::compareThreeWay(toBits(), rhs.toBits());
  }
  [[nodiscard]] QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(
      DayTimeDuration);

  //____________________________________________________________________________
  template <typename H>
  friend H AbslHashValue(H h, const DayTimeDuration& d) {
    return H::combine(std::move(h), d.toBits());
  }

  //____________________________________________________________________________
  // Parse a potential `xsd:dayTimeDuration` string, if the provided string is
  // valid, return a `DayTimeDuration` object. If the provided string is
  // invalid, a parse exception is raised.
  static DayTimeDuration parseXsdDayTimeDuration(
      std::string_view dayTimeDurationStr);

  //____________________________________________________________________________
  // Return the the minimal representation of a `xsd:dayTimeDuration` value.
  // Hence we omit the segments which have a respective value of 0 at given
  // positions. E.g. `P12DT4H23M2.54S`, `-PT0S` (the full representation is
  // equal to P0DT0H0M0S) or `-PT3H4.321S`
  std::pair<std::string, const char*> toStringAndType() const;

  //____________________________________________________________________________
  // !!! The methods here are in use for testing this class, but are in general
  // rather inefficient for extracting all underlying values of a
  // DayTimeDuration object. Usage of getValues() should be mostly considered.

  // get days over getValues()
  constexpr int getDays() const { return getValues().days_; }

  // get hours over getValues()
  constexpr int getHours() const { return getValues().hours_; }

  // get minutes over getValues()
  constexpr int getMinutes() const { return getValues().minutes_; }

  // get seconds over getValues()
  constexpr double getSeconds() const { return getValues().seconds_; }
};

#endif  // QLEVER_DURATION_H
