//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATE_H
#define QLEVER_DATE_H

#include <exception>
#include <sstream>
#include <bit>

// Exception that is thrown when a value for a component of the `Date`, `Time`
// or `Datetime` classes below is out of range (e.g. the month 13, or the hour
// 26)
class DateOutOfRangeException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

namespace detail {
// Check that `element` is `>=` min` and `< max`.
inline void checkUpperBoundInclusive(const auto& element, const auto& min,
                                     const auto& max, std::string_view name) {
  if (element < min || element >= max) {
    std::stringstream s;
    s << name << " " << element << " is out of range.";
    throw DateOutOfRangeException{s.str()};
  }
}

// Check that `element` is `>=` min` and `<= max`.
inline void checkUpperBoundExclusive(const auto& element, const auto& min,
                                     const auto& max, std::string_view name) {
  if (element < min || element > max) {
    std::stringstream s;
    s << name << " " << element << "is out of range.";
    throw DateOutOfRangeException{s.str()};
  }
}
}  // namespace detail

/// Represent a date, consisting of a year, month and day, as well as the
/// functionality to pack such a date into 24 bits, and to restore a date from
/// this packed representation.
class Date {
 public:
  // Year takes logical values from -9999 to 9999, which are stored shifted into
  // the positive range from 0 to 2*9999 This makes the sorting of dates easier.
  static constexpr short minYear = -9999;
  static constexpr short maxYear = 9999;

  static constexpr uint8_t numBitsYear = std::bit_width(unsigned{maxYear - minYear});
  static constexpr uint8_t numBitsMonth = std::bit_width(12u);
  static constexpr uint8_t numBitsDay = std::bit_width(31u);
  static constexpr uint8_t numBitsHour = std::bit_width(23u);
  static constexpr uint8_t numBitsMinute = std::bit_width(59u);
  static constexpr uint8_t numBitsSecond = std::bit_width(60u);

  static constexpr uint8_t numBitsTimezone = std::bit_width(24u);

  static constexpr uint8_t remainingBits = 64 - numBitsYear - numBitsMonth - numBitsDay - numBitsHour - numBitsMinute - numBitsSecond - numBitsTimezone - 10;


 private:
  unsigned _year : numBitsYear;
  unsigned _month : numBitsMonth;
  unsigned _day : numBitsDay;
  unsigned _hour : numBitsHour = 0;
  unsigned _minute : numBitsMinute = 0;
  // a fixed point representation;
  unsigned _seconds : numBitsSecond = 0;
  unsigned _secFrac : 5;
  unsigned _secFrac2 : 5;
  // Timezone is currently only supported as an hour;
  unsigned _timezone : numBitsTimezone = 0;
  //unsigned _emptyBytesForTag : remainingBits = 0;


 public:
  // Construct a date from year, month and day, e.g. `Date(1992, 7, 3)`. Throw
  // `DateOutOfRangeException` if one of the values is illegal.
  constexpr Date(int year, unsigned char month, unsigned day)
      : _year{static_cast<unsigned>(year - minYear)}, _month{month}, _day{day} {
    detail::checkUpperBoundInclusive(year, minYear, maxYear, "year");
    detail::checkUpperBoundInclusive(month, 1, 12, "month");
    detail::checkUpperBoundInclusive(day, 1, 31, "day");
  }

 private:

 public:
  static constexpr Date fromBytes(uint64_t bytes) {
    return std::bit_cast<Date>(bytes);
  }

  [[nodiscard]] constexpr uint64_t toBytes() const {
    return std::bit_cast<uint64_t>(*this);
  }

  [[nodiscard]] auto year() const { return _year + minYear; }
  [[nodiscard]] auto month() const { return _month; }
  [[nodiscard]] auto day() const { return _day; }
  [[nodiscard]] auto hour() const { return _hour; }
  [[nodiscard]] auto minute() const { return _minute; }
  [[nodiscard]] auto second() const { return ((_secFrac << 5u) + (_secFrac2)) / 1000.0 + _seconds; }
  [[nodiscard]] auto timezone() const { return _timezone; }
};

#endif  // QLEVER_DATE_H