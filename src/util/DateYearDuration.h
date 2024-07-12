//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_DATES_AND_DURATION_H
#define QLEVER_DATES_AND_DURATION_H

#include <util/Date.h>
#include <util/Duration.h>

#include "global/Constants.h"
#include "util/NBitInteger.h"

// This class either encodes a `Date`, a year that is outside the range that
// can be currently represented by the year class [-9999, 9999], or a
// `DayTimeDuration` object (more information in Duration.h). The underlying
// format is as follows (starting from the most significant bit):
// - 5 bits (or 14 bits with DayTimeDuration) that are always zero and ignored
// by the representation.
// - 2 bits that store 0 (negative year), 1 (datetime with a year in [-9999,
// 9999]), 2 (positive year), or 3 (xsd:dayTimeDuration).
//   These values are chosen s.t. that the order of the bits is correct.
// - 57 bits that either encode the `Date`, or a year as a signed integer via
// the `ad_utility::NBitInteger` class.
// - Or 48 bits that encode a `DayTimeDuration` object.
class DateYearOrDuration {
 private:
  // The tags to discriminate between the stored formats.
  static constexpr uint64_t negativeYear = 0;
  static constexpr uint64_t datetime = 1;
  static constexpr uint64_t positiveYear = 2;
  static constexpr uint64_t daytimeDuration = 3;
  // The number of bits for the actual values.
  static constexpr uint8_t numPayloadDateBits = 64 - Date::numUnusedBits;
  static constexpr uint8_t numPayloadDurationBits =
      64 - DayTimeDuration::numUnusedBits;

  // The number of bits used to distinguish the different XSD types.
  static constexpr uint8_t numTypeBits = 2;
  // The integer type for the case of a large year.
  using NBit = ad_utility::NBitInteger<numPayloadDateBits - numTypeBits>;
  // The bits.
  uint64_t bits_;

 public:
  enum struct Type { Year = 0, YearMonth, Date, DateTime };

  // The number of high bits that are unused by this class (currently 5).
  static constexpr uint64_t numUnusedBits = Date::numUnusedBits - 2;
  static constexpr int64_t maxYear = NBit::max();
  static constexpr int64_t minYear = NBit::min();

  // Construct from a `Date`.
  explicit DateYearOrDuration(Date d) {
    bits_ = std::bit_cast<uint64_t>(d) | (datetime << numPayloadDateBits);
  }

  // Construct a `DateYearOrDuration` given a `DayTimeDuration` object.
  explicit DateYearOrDuration(DayTimeDuration dayTimeDuration) {
    bits_ = std::bit_cast<uint64_t>(dayTimeDuration) |
            (daytimeDuration << numPayloadDurationBits);
  }

  // Construct from a `year`. Only valid if the year is outside the legal range
  // for a year in the `Date` class.
  explicit DateYearOrDuration(int64_t year, Type type) {
    AD_CONTRACT_CHECK(year < Date::minYear || year > Date::maxYear);
    AD_CONTRACT_CHECK(year >= DateYearOrDuration::minYear &&
                      year <= DateYearOrDuration::maxYear);
    auto flag = year < 0 ? negativeYear : positiveYear;
    bits_ = NBit::toNBit(year) << numTypeBits;
    bits_ |= flag << numPayloadDateBits;
    bits_ |= static_cast<uint64_t>(type);
  }

  // Return the underlying bit representation.
  uint64_t toBits() const { return bits_; }

  // True iff a complete `Date` is stored and not only a large year.
  bool isDate() const { return bits_ >> numPayloadDateBits == datetime; }

  // True iff constructed with `DayTimeDuration`.
  bool isDayTimeDuration() const {
    return bits_ >> numPayloadDurationBits == daytimeDuration;
  };

  // Return the underlying `Date` object. The behavior is undefined if
  // `isDate()` is `false`.
  Date getDateUnchecked() const { return std::bit_cast<Date>(bits_); }

  // Return the underlying `Date` object. An assertion fails if `isDate()` is
  // `false`.
  Date getDate() const {
    AD_CONTRACT_CHECK(bits_ >> numPayloadDateBits == datetime);
    return getDateUnchecked();
  }

  // Return the underlying `DayTimeDuration` object. The behavior is undefined
  // if `isDayTimeDuration()` is `false`.
  DayTimeDuration getDayTimeDurationUnchecked() const {
    return std::bit_cast<DayTimeDuration>(bits_);
  }

  // Return the underlying `DayTimeDuration` object, with assertion check.
  DayTimeDuration getDayTimeDuration() const {
    AD_CONTRACT_CHECK(bits_ >> numPayloadDurationBits == daytimeDuration);
    return getDayTimeDurationUnchecked();
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
  // DateYearOrDuration object. This implementation is necessary for the
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
  // format used by Wikidata). If a DayTimeDuration (significant bit set to 3)
  // is contained, the exported value will be a `xsd:dayTimeDuration` value.
  std::pair<std::string, const char*> toStringAndType() const;

  // The bitwise comparison also corresponds to the semantic ordering of years
  // and dates.
  auto operator<=>(const DateYearOrDuration&) const = default;

  // Bitwise hashing.
  template <typename H>
  friend H AbslHashValue(H h, const DateYearOrDuration& d) {
    return H::combine(std::move(h), d.bits_);
  }

  // Parsing functions. They all have the following properties:
  // 1. The input is not contained in quotes (for example 1900-12-13 for a date,
  // not "1900-12-13").
  // 2. If the year is outside the range [-9999, 9999], then the date must be
  // January 1, 00:00 hours.

  // Parse from xsd:dateTime (e.g. 1900-12-13T03:12:00.33Z)
  static DateYearOrDuration parseXsdDatetime(std::string_view dateString);

  // Parse from xsd:date (e.g. 1900-12-13)
  static DateYearOrDuration parseXsdDate(std::string_view dateString);

  // Parse from xsd:gYearMonth (e.g. 1900-03)
  static DateYearOrDuration parseGYearMonth(std::string_view dateString);

  // Parse from xsd:gYear (e.g. 1900)
  static DateYearOrDuration parseGYear(std::string_view dateString);

  // Parse from xsd:dayTimeDuration (e.g. P2DT3H59M59.99S)
  static DateYearOrDuration parseXsdDayTimeDuration(
      std::string_view dayTimeDurationString);

  // Parse `xsd:dayTimeDuration` from a `DateYearOrDuration`.
  static std::optional<DateYearOrDuration> xsdDayTimeDurationFromDate(
      const DateYearOrDuration& dateOrLargeYear);
};

#endif  //  QLEVER_DATES_AND_DURATION_H
