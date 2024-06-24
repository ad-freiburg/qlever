//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATE_H
#define QLEVER_DATE_H

#include <bit>
#include <charconv>
#include <cmath>
#include <exception>
#include <sstream>

#include "global/Constants.h"
#include "util/CtreHelpers.h"
#include "util/NBitInteger.h"

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

// An exception that is thrown when the parsing of a date fails because the
// input does not match the expected regular expression.
class DateParseException : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

namespace detail {
// Check that `min <= element <= max`. Throw `DateOutOfRangeException` if the
// check fails.
constexpr void checkBoundsIncludingMax(const auto& element, const auto& min,
                                       const auto& max, std::string_view name) {
  if (element < min || element > max) {
    throw DateOutOfRangeException{name, element};
  }
}

// Check that `min <= element < max`. Throw `DateOutOfRangeException` if the
// check fails.
constexpr void checkBoundsExcludingMax(const auto& element, const auto& min,
                                       const auto& max, std::string_view name) {
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
 * - Time zones can only be encoded as full hours.
 * - The ordering via operator <=> uses the time zone only as a tie breaker when
 *   all other values are equal. This means that for example the timestamp
 *   "12:00 with a time zone of 0" (Central Europe) will be sorted before
 *   "13:00 with a time zone of -6" (US East coast) because 12 < 13, although
 *   the second timestamp actually happens before the first one.
 * TODO<joka921> Use this class as "all times are in UTC, and the time zone is
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

  // The special month value `0` is used to encode "no month was specified" (i.e
  // "This is a `xsd:gYear`).
  static constexpr int minMonth = 0;
  static constexpr unsigned maxMonth = 12;
  static constexpr uint8_t numBitsMonth = std::bit_width(maxMonth);

  // The special day value `0` is used to encode "no day was specified" (i.e
  // "This is a `xsd:gYearMonth`).
  static constexpr int minDay = 0;
  static constexpr unsigned maxDay = 31;
  static constexpr uint8_t numBitsDay = std::bit_width(maxDay);

  // The special hour value `-1` is used to encode "no hour was specified" (i.e
  // "This is a `xsd:Date`).
  static constexpr int minHour = -1;
  static constexpr unsigned maxHour = 23;
  static constexpr uint8_t numBitsHour =
      std::bit_width(unsigned{maxHour - minHour});

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

  // The time zone is an hour in -23..23. It is shifted to the positive range
  // 0..22 (similar to the years). There are two additional "special" time
  // zones: `no time zone` (undefined) `Z` (a special encoding for UTC/ 00:00).
  // We store these as values `1` and `2` to be able to retrieve them again when
  // exporting, all other time zones are shifted accordingly.
  static constexpr int minTimeZoneActually = -23;
  static constexpr int maxTimeZoneActually = 25;
  static constexpr int minTimeZone = -23;
  static constexpr int maxTimeZone = 23;
  static constexpr uint8_t numBitsTimeZone = std::bit_width(
      static_cast<unsigned>(maxTimeZoneActually - minTimeZoneActually));

  // The number of bits that are not needed for the encoding of the Date value.
  static constexpr uint8_t numUnusedBits =
      64 - numBitsYear - numBitsMonth - numBitsDay - numBitsHour -
      numBitsMinute - numBitsSecond - numBitsTimeZone;

 private:
  // TODO<joka921> The details of bitfields are implementation-specific, but
  // this date class only works for platforms/compilers where the following
  // bitfields are stored without any padding bits and starting at the least
  // significant bits. If this is not the case for a platform, then the unit
  // tests will fail. If we need support for such platforms, we have to
  // implement the bitfields manually using bit shifting.
  uint64_t timeZone_ : numBitsTimeZone = 0;
  uint64_t second_ : numBitsSecond = 0;
  uint64_t minute_ : numBitsMinute = 0;
  uint64_t hour_ : numBitsHour = 0;
  uint64_t day_ : numBitsDay = 1;
  uint64_t month_ : numBitsMonth = 1;
  uint64_t year_ : numBitsYear = 0;
  // These bits (the `numUsedBits` most significant bits) are always zero.
  uint64_t unusedBits_ : numUnusedBits = 0;

 public:
  struct NoTimeZone {
    bool operator==(const NoTimeZone&) const = default;
  };
  struct TimeZoneZ {
    bool operator==(const TimeZoneZ&) const = default;
  };
  using TimeZone = std::variant<NoTimeZone, TimeZoneZ, int>;
  /// Construct a `Date` from values for the different components. If any of the
  /// components is out of range, a `DateOutOfRangeException` is thrown.
  constexpr Date(int year, int month, int day, int hour = -1, int minute = 0,
                 double second = 0.0, TimeZone timeZone = NoTimeZone{}) {
    setYear(year);
    setMonth(month);
    setDay(day);
    setHour(hour);
    setMinute(minute);
    setSecond(second);
    setTimeZone(timeZone);
    // Suppress the "unused member" warning on clang.
    (void)unusedBits_;
  }

  /// Convert the `Date` to a `uint64_t`. This just casts the underlying
  /// representation.
  [[nodiscard]] constexpr uint64_t toBits() const {
    return std::bit_cast<uint64_t>(*this);
  }

  /// Convert a `uint64_t` to a `Date`. This is only valid if the `uint64_t` was
  /// obtained via a call to `Date::toBits()`. This just casts the underlying
  /// representation.
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
  /// different time zone values (see the docstring of this class).
  [[nodiscard]] constexpr auto operator<=>(const Date& rhs) const {
    return toBits() <=> rhs.toBits();
  }

  template <typename H>
  friend H AbslHashValue(H h, const Date& d) {
    return H::combine(std::move(h), d.toBits());
  }

  /// Getters and setters for the different components. The setters will check,
  /// if the input is valid for the respective component. Otherwise a
  /// `DateOutOfRangeException` is thrown.

  /// Getter and setter for the year.
  [[nodiscard]] auto getYear() const {
    return static_cast<int>(year_) + minYear;
  }
  constexpr void setYear(int year) {
    detail::checkBoundsIncludingMax(year, minYear, maxYear, "year");
    year_ = static_cast<unsigned>(year - minYear);
  }

  /// Getter and setter for the month.
  [[nodiscard]] int getMonth() const { return static_cast<int>(month_); }
  constexpr void setMonth(int month) {
    detail::checkBoundsIncludingMax(month, minMonth, int{maxMonth}, "month");
    month_ = static_cast<unsigned>(month);
  }

  /// Getter and setter for the day.
  [[nodiscard]] int getDay() const { return static_cast<int>(day_); }
  constexpr void setDay(int day) {
    detail::checkBoundsIncludingMax(day, minDay, int{maxDay}, "day");
    day_ = static_cast<unsigned>(day);
  }

  /// Getter and setter for the hour.
  [[nodiscard]] int getHour() const { return hour_ + minHour; }
  constexpr void setHour(int hour) {
    detail::checkBoundsIncludingMax(hour, minHour, int{maxHour}, "hour");
    hour_ = static_cast<unsigned>(hour - minHour);
  }

  /// Getter and setter for the minute.
  [[nodiscard]] int getMinute() const { return static_cast<int>(minute_); }
  constexpr void setMinute(int minute) {
    detail::checkBoundsIncludingMax(minute, minMinute, int{maxMinute},
                                    "minute");
    minute_ = static_cast<unsigned>(minute);
  }

  /// Getter and setter for the second.
  [[nodiscard]] double getSecond() const {
    return static_cast<double>(second_) / secondMultiplier;
  }
  constexpr void setSecond(double second) {
    detail::checkBoundsExcludingMax(second, minSecond, maxSecond, "second");
    second_ = static_cast<unsigned>(std::round(second * secondMultiplier));
  }

  /// Getter and setter for the time zone.
  [[nodiscard]] TimeZone getTimeZone() const {
    auto tz = static_cast<int>(timeZone_) + minTimeZoneActually;
    // Again factor out the special values.
    if (tz == 0) {
      return NoTimeZone{};
    } else if (tz == 1) {
      return TimeZoneZ{};
    } else {
      return tz > 0 ? tz - 2 : tz;
    }
  }
  int getTimeZoneAsInternalIntForTesting() const {
    return static_cast<int>(timeZone_) + minTimeZoneActually;
  }

  constexpr void setTimeZone(TimeZone timeZone) {
    auto getTimeZone = []<typename T>(const T& value) -> int {
      if constexpr (std::is_same_v<T, NoTimeZone>) {
        return 0;
      } else if constexpr (std::is_same_v<T, TimeZoneZ>) {
        return 1;
      } else {
        static_assert(std::is_same_v<T, int>);
        detail::checkBoundsIncludingMax(value, minTimeZone, maxTimeZone,
                                        "timeZone");
        // Make room for the special timeZone values `0` and `1` from above.
        return value < 0 ? value : value + 2;
      }
    };
    auto actualTimeZone = std::visit(getTimeZone, timeZone);
    timeZone_ = static_cast<unsigned>(actualTimeZone - minTimeZoneActually);
  }

  // True iff Date object has set time values (hours, minutes, seconds) (i.e
  //  "This is a `xsd:dateTime`")
  [[nodiscard]] bool hasTime() const { return getHour() != -1; }

  // Correctly format the time zone according to the `xsd` standard. This is a
  // helper function for `toStringAndType` below.
  std::string formatTimeZone() const;

  // Convert to a string (without quotes) that represents the stored date, and a
  // pointer to the IRI of the corresponding datatype (currently always
  // `xsd:dateTime`). Large years are currently always exported as
  // `xsd:dateTime` pointing to the January 1st, 00:00 hours. (This is the
  // format used by Wikidata).
  std::pair<std::string, const char*> toStringAndType() const;
};

// This class either encodes a `Date` or a year that is outside the range that
// can be currently represented by the year class [-9999, 9999]. The underlying
// format is as follows (starting from the most significant bit):
// - 5 bits that are always zero and ignored by the representation.
// - 2 bits that store 0 (negative year), 1 (datetime with a year in [-9999,
// 9999]), or 2 (positive year).
//   These values are chosen s.t. that the order of the bits is correct.
// - 57 bits that either encode the `Date`, or a year as a signed integer via
// the `ad_utility::NBitInteger` class.
class DateOrLargeYear {
 private:
  // The tags to discriminate between the stored formats.
  static constexpr uint64_t negativeYear = 0;
  static constexpr uint64_t datetime = 1;
  static constexpr uint64_t positiveYear = 2;
  // The number of bits for the actual value.
  static constexpr uint8_t numPayloadBits = 64 - Date::numUnusedBits;

  // The number of bits used to distinguish the different XSD types.
  static constexpr uint8_t numTypeBits = 2;
  // The integer type for the case of a large year.
  using NBit = ad_utility::NBitInteger<numPayloadBits - numTypeBits>;
  // The bits.
  uint64_t bits_;

 public:
  enum struct Type { Year = 0, YearMonth, Date, DateTime };

  // The number of high bits that are unused by this class (currently 5).
  static constexpr uint64_t numUnusedBits = Date::numUnusedBits - 2;
  static constexpr int64_t maxYear = NBit::max();
  static constexpr int64_t minYear = NBit::min();

  // Construct from a `Date`.
  explicit DateOrLargeYear(Date d) {
    bits_ = std::bit_cast<uint64_t>(d) | (datetime << numPayloadBits);
  }

  // Construct from a `year`. Only valid if the year is outside the legal range
  // for a year in the `Date` class.
  explicit DateOrLargeYear(int64_t year, Type type) {
    AD_CONTRACT_CHECK(year < Date::minYear || year > Date::maxYear);
    AD_CONTRACT_CHECK(year >= DateOrLargeYear::minYear &&
                      year <= DateOrLargeYear::maxYear);
    auto flag = year < 0 ? negativeYear : positiveYear;
    bits_ = NBit::toNBit(year) << numTypeBits;
    bits_ |= flag << numPayloadBits;
    bits_ |= static_cast<uint64_t>(type);
  }

  // Return the underlying bit representation.
  uint64_t toBits() const { return bits_; }

  // True iff a complete `Date` is stored and not only a large year.
  bool isDate() const { return bits_ >> numPayloadBits == datetime; }

  // Return the underlying `Date` object. The behavior is undefined if
  // `isDate()` is `false`.
  Date getDateUnchecked() const { return std::bit_cast<Date>(bits_); }

  // Return the underlying `Date` object. An assertion fails if `isDate()` is
  // `false`.
  Date getDate() const {
    AD_CONTRACT_CHECK(bits_ >> numPayloadBits == datetime);
    return getDateUnchecked();
  }

  // Get the stored year, no matter if it's stored inside a `Date` object or
  // directly.
  int64_t getYear() const;

  // Get the stored month. If the datatype actually stores a month (all the
  // types except for `xsd:gYear` do), then the month is returned, else
  // `std::nullopt` is returned. In the case of a large year with a datatype
  // that stores a month, the month will always be `1`.
  std::optional<int> getMonth() const;

  // Similar to `getMonth`, but get the day.
  std::optional<int> getDay() const;

  // Get the timezone if there is an underlying complete `Date` w.r.t. this
  // DateOrLargeYear object. This implementation is necessary for the
  // `tz()`-function.
  std::string getStrTimezone() const;

  Type getType() const {
    return static_cast<Type>(ad_utility::bitMaskForLowerBits(numTypeBits) &
                             bits_);
  }

  // Convert to a string (without quotes) that represents the stored date, and a
  // pointer to the IRI of the corresponding datatype (currently always
  // `xsd:dateTime`). Large years are currently always exported as
  // `xsd:dateTime` with January 1, 00:00 hours (This is the
  // format used by Wikidata).
  std::pair<std::string, const char*> toStringAndType() const;

  // The bitwise comparison also corresponds to the semantic ordering of years
  // and dates.
  auto operator<=>(const DateOrLargeYear&) const = default;

  // Bitwise hashing.
  template <typename H>
  friend H AbslHashValue(H h, const DateOrLargeYear& d) {
    return H::combine(std::move(h), d.bits_);
  }

  // Parsing functions. They all have the following properties:
  // 1. The input is not contained in quotes (for example 1900-12-13 for a date,
  // not "1900-12-13").
  // 2. If the year is outside the range [-9999, 9999], then the date must be
  // January 1, 00:00 hours.

  // Parse from xsd:dateTime (e.g. 1900-12-13T03:12:00.33Z)
  static DateOrLargeYear parseXsdDatetime(std::string_view dateString);

  // Parse from xsd:date (e.g. 1900-12-13)
  static DateOrLargeYear parseXsdDate(std::string_view dateString);

  // Parse from xsd:gYearMonth (e.g. 1900-03)
  static DateOrLargeYear parseGYearMonth(std::string_view dateString);

  // Parse from xsd:gYear (e.g. 1900)
  static DateOrLargeYear parseGYear(std::string_view dateString);
};

#endif  // QLEVER_DATE_H
