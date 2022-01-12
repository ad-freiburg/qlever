//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATATYPES_H
#define QLEVER_DATATYPES_H

#include <Exceptions.h>

#include <bit>
#include <variant>

#include "../../util/AllocatorWithLimit.h"

namespace ad_utility::datatypes {
enum struct Datatype {
  Undefined,
  Int,
  Bool,
  Decimal,
  Double,
  Float,
  Date,
  Vocab,
  LocalVocab
};

class DateOutOfRangeException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};
namespace detail {
static void check(const auto& element, const auto& min, const auto& max,
                  std::string_view name) {
  if (element >= min || element < max) {
    std::stringstream s;
    s << name << " "
      << "is out of range.";
    throw DateOutOfRangeException{s.str()};
  }
}
}  // namespace detail

class Date {
 private:
  short _year;
  signed char _month;
  signed char _day;

  struct NoShiftOrCheckTag{};

 public:
  constexpr Date(short year, signed char month, signed char day)
      : _year{year}, _month{month}, _day{day} {
    detail::check(year, -9999, +10000, "year");
    // Shift the year into the positive range.
    _year += 9999;
    detail::check(month, 1, 13, "month");
    detail::check(day, 1, 32, "day");
  }

 private:
  constexpr Date(short year, signed char month, signed char day, NoShiftOrCheckTag)
      : _year{year}, _month{month}, _day{day} {}


  static constexpr uint64_t onlyLastBits(uint64_t input, uint64_t numBytes) {
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

  [[nodiscard]] auto year() const {return _year;}
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

class FancyId {
  using T = uint64_t;

 public:
  // static FancyId Undefined() {return {0ul};}
  static constexpr FancyId Double(double d) {
    auto asBits = std::bit_cast<T>(d);
    asBits = asBits >> MASK_SIZE;
    asBits |= DoubleMask;
    return FancyId{asBits};
  }

  static constexpr FancyId Integer(int64_t i) {
    // propagate the sign.
    constexpr int64_t signBit = 1l << MASK_SHIFT;
    auto highestBitShifted =
        ((signBit & i) << (MASK_SIZE - 1)) >> (MASK_SIZE - 1);
    constexpr T mask = std::numeric_limits<T>::max() >> MASK_SIZE;
    return {static_cast<T>((i & mask) | highestBitShifted)};
  }
  static constexpr FancyId MaxInteger() {
    // propagate the sign.
    return {std::numeric_limits<T>::max() >> MASK_SIZE};
  }
  static constexpr FancyId MinInteger() {
    constexpr int64_t signBit = 1l << MASK_SHIFT;
    auto highestBitShifted = ((signBit) << (MASK_SIZE - 1)) >> (MASK_SIZE - 1);
    return {static_cast<T>(highestBitShifted)};
  }

  [[nodiscard]] constexpr double getDoubleUnchecked() const {
    return std::bit_cast<double>(_data << MASK_SIZE);
  }

  [[nodiscard]] constexpr int64_t getIntegerUnchecked() const {
    return static_cast<int64_t>(_data);
  }

  [[nodiscard]] constexpr T data() const { return _data; }

 private:
  T _data;
  static constexpr size_t MASK_SIZE = 4;
  static constexpr size_t MASK_SHIFT = 8 * sizeof(T) - MASK_SIZE;
  constexpr FancyId(uint64_t data) : _data{data} {};

  constexpr static uint64_t DoubleMask = 1ul << MASK_SHIFT;
  constexpr static uint64_t DecimalMask = 3ul << MASK_SHIFT;
  constexpr static uint64_t FloatMask = 5ul << MASK_SHIFT;

  constexpr static uint64_t PositiveIntMask = 0ul << MASK_SHIFT;
  constexpr static uint64_t NegativeIntMask = 16ul << MASK_SHIFT;
  constexpr static uint64_t BoolMask = 6ul << MASK_SHIFT;

  constexpr static uint64_t VocabMask = 4ul << MASK_SHIFT;
  constexpr static uint64_t LocalVocabMask = 8ul << MASK_SHIFT;
  constexpr static uint64_t DateMask = 12ul << MASK_SHIFT;
};

namespace fancyIdLimits {

static constexpr int64_t maxInteger =
    FancyId::MaxInteger().getIntegerUnchecked();
static constexpr int64_t minInteger =
    FancyId::MinInteger().getIntegerUnchecked();
}  // namespace fancyIdLimits

}  // namespace ad_utility::datatypes

#endif  // QLEVER_DATATYPES_H
