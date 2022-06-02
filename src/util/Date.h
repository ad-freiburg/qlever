//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATE_H
#define QLEVER_DATE_H

#include <bit>
#include <cmath>
#include <exception>
#include <sstream>

// Exception that is thrown when a value for a component of the `Date`, `Time`
// or `Datetime` classes below is out of range (e.g. the month 13, or the hour
// 26)
class DateOutOfRangeException : public std::exception {
 private:
  std::string message_;

 public:
  DateOutOfRangeException(std::string_view name, const auto& value) {
    std::stringstream s;
    s << name << " " << value << " is out of range for a DateTime";
    message_ = std::move(s).str();
  }
  [[nodiscard]] const char* what() const noexcept override {
    return message_.c_str();
  }
};

namespace detail {
// Check that `min <= element <= max`. Throw `DateOutOfRangeException` if the
// check fails.
inline constexpr void checkBoundsIncludingMax(const auto& element,
                                              const auto& min, const auto& max,
                                              std::string_view name) {
  if (element < min || element > max) {
    throw DateOutOfRangeException{name, element};
  }
}

// Check that `min <= element < max`. Throw `DateOutOfRangeException` if the
// check fails.
inline constexpr void checkBoundsExcludingMax(const auto& element,
                                              const auto& min, const auto& max,
                                              std::string_view name) {
  if (element < min || element >= max) {
    throw DateOutOfRangeException{name, element};
  }
}
}  // namespace detail

/**
 * @brief This class encodes a xsd:DateTime value in 64 bits. Comparisons
 * (==, <=>) are maximally efficient, as they are directly performed on the
 * underlying 64-bit representation.
 *
 * It has a static constexpr member Date::numUnusedBits (currently 7).
 * This means that the `numUnusedBits` most significant bits are always 0 in the
 * binary representation and can be used for other purposes. It is important to
 * set these bits back to 0 before calling `fromBits` because otherwise the
 * comparisons (== and <=>) don't work.
 *
 * The following limitations hold:
 * - Years can be in the range -9999...9999
 * - Seconds are stored with a millisecond precision.
 * - Timezones can only be encoded as full hours.
 * - The ordering via operator <=> uses the timezone only as a tie breaker when
 *   all other values are equal. This means that for example the timestamp
 *   "12:00 with a timezone of 0" (Central Europe) will be sorted before
 *   "13:00 with a timezone of -6" (US East coast) because 12 < 13, although
 *   the second timestamp actually happens before the first one.
 * TODO<joka921> Use this class as "all times are in UTC, and the timezone is
 * stored additionally" and write converters for this (correctly comparable)
 * format for the input and output to and from string literals.0
 */
class Date {
 public:
  // Define the minimal and maximal values for the different fields (year, day,
  // ...) and calculate the number of bits required to store these values.

  // Year takes values in -9999..9999, which are stored shifted to
  // the positive range 0.. 2*9999 This makes the sorting of dates easier.
  static constexpr int minYear = -9999;
  static constexpr int maxYear = 9999;
  static constexpr uint8_t numBitsYear =
      std::bit_width(unsigned{maxYear - minYear});

  static constexpr int minMonth = 1;
  static constexpr unsigned maxMonth = 12;
  static constexpr uint8_t numBitsMonth = std::bit_width(maxMonth);

  static constexpr int minDay = 1;
  static constexpr unsigned maxDay = 31;
  static constexpr uint8_t numBitsDay = std::bit_width(maxDay);

  static constexpr int minHour = 0;
  static constexpr unsigned maxHour = 23;
  static constexpr uint8_t numBitsHour = std::bit_width(maxHour);

  static constexpr int minMinute = 0;
  static constexpr unsigned maxMinute = 59;
  static constexpr uint8_t numBitsMinute = std::bit_width(maxMinute);

  // Seconds are imported and exported as double, but internally stored as fixed
  // point decimals with millisecond precision.
  static constexpr double minSecond = 0.0;
  static constexpr double maxSecond = 60.0;
  static constexpr double secondMultiplier = 1024.0;
  static constexpr uint8_t numBitsSecond =
      std::bit_width(static_cast<unsigned>(maxSecond * secondMultiplier));

  // The timezone is an hour in -23..23. It is shifted to the positive range
  // 0..46 (similar to the years)
  static constexpr int minTimezone = -23;
  static constexpr int maxTimezone = 23;
  static constexpr uint8_t numBitsTimezone =
      std::bit_width(static_cast<unsigned>(maxTimezone - minTimezone));

  // The number of bits that are not needed for the encoding of the Date value.
  static constexpr uint8_t numUnusedBits =
      64 - numBitsYear - numBitsMonth - numBitsDay - numBitsHour -
      numBitsMinute - numBitsSecond - numBitsTimezone;

 private:
  // TODO<joka921> The details of bitfields are implementation-specific, but
  // this date class only works for platforms/compilers where the following
  // bitfields are stored without any padding bits and starting at the least
  // significant bits. If this is not the case for a platform, then the unit
  // tests will fail. If we need support for such platforms, we have to
  // implement the bitfields manually using bit shifting.
  uint64_t _timezone : numBitsTimezone = 0;
  uint64_t _second : numBitsSecond = 0;
  uint64_t _minute : numBitsMinute = 0;
  uint64_t _hour : numBitsHour = 0;
  uint64_t _day : numBitsDay = 1;
  uint64_t _month : numBitsMonth = 1;
  uint64_t _year : numBitsYear = 0;
  // These bits (the `numUsedBits` most significant bits) are always zero.
  uint64_t _unusedBits : numUnusedBits = 0;

 public:
  /// Construct a `Date` from values for the different components. If any of the
  /// components is out of range, a `DateOutOfRangeException` is thrown.
  constexpr Date(int year, int month, int day, int hour = 0, int minute = 0,
                 double second = 0.0, int timezone = 0) {
    setYear(year);
    setMonth(month);
    setDay(day);
    setHour(hour);
    setMinute(minute);
    setSecond(second);
    setTimezone(timezone);
    // Suppress the "unused member" warning on clang.
    (void)_unusedBits;
  }

  /// Convert the `Date` to a `uint64_t`. This just casts the underlying
  /// representation.
  [[nodiscard]] constexpr uint64_t toBits() const {
    return std::bit_cast<uint64_t>(*this);
  }

  /// Convert a `uint64_t` to a `Date`. This is only valid if the `uint64_t` was
  /// obtained via a call to `Date::toBits()`. This just casts the underlying
  /// representaion.
  static constexpr Date fromBits(uint64_t bytes) {
    return std::bit_cast<Date>(bytes);
  }

  /// Equality comparison is performed directly on the underlying
  /// representation.
  [[nodiscard]] constexpr bool operator==(const Date& rhs) const {
    return toBits() == rhs.toBits();
  }

  /// Comparison is performed directly on the underlying representation. This is
  /// very efficient but has some caveats concerning the ordering of dates with
  /// different timezone values (see the docstring of this class).
  [[nodiscard]] constexpr auto operator<=>(const Date& rhs) const {
    return toBits() <=> rhs.toBits();
  }

  /// Getters and setters for the different components. The setters will check,
  /// if the input is valid for the respective component. Otherwise a
  /// `DateOutOfRangeException` is thrown.

  /// Getter and setter for the year.
  [[nodiscard]] auto getYear() const {
    return static_cast<int>(_year) + minYear;
  }
  constexpr void setYear(int year) {
    detail::checkBoundsIncludingMax(year, minYear, maxYear, "year");
    _year = static_cast<unsigned>(year - minYear);
  }

  /// Getter and setter for the month.
  [[nodiscard]] int getMonth() const { return static_cast<int>(_month); }
  constexpr void setMonth(int month) {
    detail::checkBoundsIncludingMax(month, minMonth, int{maxMonth}, "month");
    _month = static_cast<unsigned>(month);
  }

  /// Getter and setter for the day.
  [[nodiscard]] int getDay() const { return static_cast<int>(_day); }
  constexpr void setDay(int day) {
    detail::checkBoundsIncludingMax(day, minDay, int{maxDay}, "day");
    _day = static_cast<unsigned>(day);
  }

  /// Getter and setter for the hour.
  [[nodiscard]] int getHour() const { return static_cast<int>(_hour); }
  constexpr void setHour(int hour) {
    detail::checkBoundsIncludingMax(hour, minHour, int{maxHour}, "hour");
    _hour = static_cast<unsigned>(hour);
  }

  /// Getter and setter for the minute.
  [[nodiscard]] int getMinute() const { return static_cast<int>(_minute); }
  constexpr void setMinute(int minute) {
    detail::checkBoundsIncludingMax(minute, minMinute, int{maxMinute},
                                    "minute");
    _minute = static_cast<unsigned>(minute);
  }

  /// Getter and setter for the second.
  [[nodiscard]] double getSecond() const {
    return static_cast<double>(_second) / secondMultiplier;
  }
  constexpr void setSecond(double second) {
    detail::checkBoundsExcludingMax(second, minSecond, maxSecond, "second");
    _second = static_cast<unsigned>(std::round(second * secondMultiplier));
  }

  /// Getter and setter for the timezone.
  [[nodiscard]] int getTimezone() const {
    return static_cast<int>(_timezone) + minTimezone;
  }
  constexpr void setTimezone(int timezone) {
    detail::checkBoundsIncludingMax(timezone, minTimezone, maxTimezone,
                                    "timezone");
    _timezone = static_cast<unsigned>(timezone - minTimezone);
  }
};

#endif  // QLEVER_DATE_H
