//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATE_H
#define QLEVER_DATE_H

// Exception that is thrown when a value for a component of the `Date`, `Time` or `Datetime` classes below is out of range (e.g. the month 13, or the hour 26)
class DateOutOfRangeException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

namespace detail {
// Check that `element` is `>=` min` and `< max`.
inline void checkUpperBoundInclusive(const auto& element, const auto& min, const auto& max,
                  std::string_view name) {
  if (element < min || element >= max) {
    std::stringstream s;
    s << name << " " << element
      << "is out of range.";
    throw DateOutOfRangeException{s.str()};
  }
}

// Check that `element` is `>=` min` and `<= max`.
inline void checkUpperBoundExclusive(const auto& element, const auto& min, const auto& max,
                                     std::string_view name) {
  if (element < min || element > max) {
    std::stringstream s;
    s << name << " " << element
      << "is out of range.";
    throw DateOutOfRangeException{s.str()};
  }
}
}  // namespace detail

/// Represent a date, consisting of a year, month and day, as well as the functionality to pack such a date into 24 bits, and to restore a date from this packed representation.
class Date {
 private:
  // Year takes logical values from -9999 to 9999, which are stored shifted into the positive range from 0 to 2*9999
  // This makes the sorting of dates easier.
  short _year;
  signed char _month;
  signed char _day;

  static constexpr short minYear = -9999;
  static constexpr short maxYear = 9999;

  // Used as a tag for certain unchecked constructor that may not be called from outside.
  struct NoShiftOrCheckTag{};

 public:
  // Construct a date from year, month and day, e.g. `Date(1992, 7, 3)`. Throw `DateOutOfRangeException` if one of the values is illegal.
  constexpr Date(short year, signed char month, signed char day)
      : _year{year}, _month{month}, _day{day} {
    detail::checkUpperBoundInclusive(year, minYear, maxYear, "year");
    // Shift the year into the positive range.
    _year -= minYear;
    detail::checkUpperBoundInclusive(month, 1, 12, "month");
    detail::checkUpperBoundInclusive(day, 1, 31, "day");
  }

 private:

  // Construct without checking the range, and without performing the shift for the year. This constructor is just an implementation detail.
  constexpr Date(short year, signed char month, signed char day, NoShiftOrCheckTag)
      : _year{year}, _month{month}, _day{day} {}


  static constexpr uint64_t onlyLastBits(uint64_t input, uint64_t numBits) {
    return ~(std::numeric_limits<uint64_t>::max() << numBytes) & input;
  }
 public:

  static constexpr Date fromBytes(uint64_t bytes) {
    auto day = onlyLastBits(bytes, 5);
    auto month = onlyLastBits(bytes >> 5, 4);
    auto year = onlyLastBits(bytes >> 9, 15);
    return Date(year, month, day, NoShiftOrCheckTag{});
  }
  // 15 bits for the year, 4 bits for the month, 5 bits for the day
  static constexpr uint64_t numBitsRequired = 24;


  [[nodiscard]] auto year() const {return _year + minYear;}
  [[nodiscard]] auto month() const {return _month;}
  [[nodiscard]] auto day() const {return _day;}
};

class Time {
 private:
  signed char _hour;
  signed char _minute;
  // a fixed point representation;
  unsigned int _seconds;
  // Timezone is currently only supported as an hour;
  signed char _timezone = 0;

 public:
  constexpr Time(signed char hour, signed char minute, float seconds)
      : _hour{hour}, _minute{minute} {
    detail::check(hour, 0, 24, "hour");
    detail::check(minute, 0, 60, "minute");
    detail::check(seconds, 0.0f, 60.0f, "seconds");

    constexpr auto numBitsForFraction = numBitsForSeconds - 6;
    _seconds = static_cast<unsigned>(seconds * pow(2, numBitsForFraction));
  }
  constexpr Time(signed char hour, signed char minute, float seconds,
                 signed char timezone)
      : Time(hour, minute, seconds) {
    detail::check(timezone, -23, 25, "timezone");
    // The minimal timezone is -23 which will become 1, so
    // 0 stands for "undefined timezone"
    _timezone = timezone + static_cast<signed char>(24);
  }

  [[nodiscard]] constexpr uint64_t toBytes() const {
    // timezone has 5 bits
    constexpr auto secShift = 5;
    constexpr auto minShift = numBitsForSeconds + secShift;
    constexpr auto hourShift = minShift + 6;
    return (static_cast<uint64_t>(_hour) << hourShift) |
           (static_cast<uint64_t>(minShift) << 5) | (_seconds << secShift) |
           _timezone;
  }
  // 15 bits for the year, 4 bits for the month, 5 bits for the day
  static constexpr uint64_t numBitsForSeconds = 20;
  static constexpr uint64_t numBitsForRequired = numBitsForSeconds + 6 + 5;

 private:
  static constexpr size_t pow(size_t base, size_t exp) {
    size_t res = 1;
    while (exp--) {
      res *= base;
    }
    return res;
  }
};

class DateTime {
  Date _date;
  Time _time;
};

#endif  // QLEVER_DATE_H
