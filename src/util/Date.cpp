//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "util/Date.h"

// _________________________________________________________________
std::pair<std::string, const char*> DateOrLargeYear::toStringAndType() const {
    auto flag = bits_ >> numPayloadBits;
    if (flag == datetime) {
        return std::bit_cast<Date>(bits_).toStringAndType();
    }
    int64_t date = NBit::fromNBit(bits_);
    constexpr std::string_view formatString = "%d-01-01T00:00:00";
    return {absl::StrFormat(formatString, date), XSD_DATETIME_TYPE};
}

//  Convert a `match` from `ctre` to an integer. The behavior is undefined if the `match` cannot be completely
// converted to an integer.
template <ctll::fixed_string Name>
static int64_t toInt(const auto& match) {
    int64_t result = 0;
    const auto& s = match.template get<Name>();
    std::from_chars(s.data(), s.data() + s.size(), result);
    return result;
}

// Regex objects with explicitly named groupes to parse dates and times.
constexpr static ctll::fixed_string dateRegex{
        R"((?<year>-?\d{4,})-(?<month>\d{2})-(?<day>\d{2}))"};
constexpr static ctll::fixed_string timeRegex{
        R"((?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2}(\.\d+)?))"};
constexpr static ctll::fixed_string timezoneRegex{
        R"((?<tzZ>Z)|(?<tzSign>[+\-])(?<tzHours>\d{2}):(?<tzMinutes>\d{2}))"};

// Get the correct `Timezone` from a regex match for the `timezoneRegex`.
static Date::Timezone parseTimezone(const auto& match) {
    if (match.template get<"tzZ">() == "Z") {
      return Date::TimezoneZ{};
    } else if (!match.template get<"tzHours">()) {
      return Date::NoTimezone{};
    }
    int tz = toInt<"tzHours">(match);
    if (match.template get<"tzSign">() == "-") {
      tz *= -1;
    }
    if (match.template get<"tzMinutes">() != "00") {
      throw std::runtime_error{"Qlever supports only full hours as timezones"};
    }
    return tz;
}

// Create a `DateOrLargeYear` from the given input. If the `year` is in the range `[-9999, 9999]` then the date is stored
// regularly, otherwise only the year is stored, and it is checked whether `month` and `day` are both `1`, and
// `hour, minute, second` are all `0`.
static DateOrLargeYear makeDateOrLargeYear(int64_t year, int month, int day, int hour, int minute, double second, Date::Timezone timezone) {
    if (year < Date::minYear || year > Date::maxYear) {
      if (month != 1 || day != 1 || hour != 0 || minute != 0 || second != 0.0) {
        throw std::runtime_error{"When the year of a datetime object is smaller than -9999 or larger than 9999 then the month and day have to be 1 and the hour, minute, and second must be all 0 in QLever's implementation of Dates."};
      }
      if (year < DateOrLargeYear::minYear || year > DateOrLargeYear::maxYear) {
        throw std::runtime_error{absl::StrCat("QLever cannot encode dates that are less than ", DateOrLargeYear::minYear, " or larger than ", DateOrLargeYear::maxYear)};
      }
        return DateOrLargeYear(year);
    }
    return DateOrLargeYear{Date{static_cast<int>(year), month, day, hour, minute, second, timezone}};
}

// __________________________________________________________________________________
    DateOrLargeYear DateOrLargeYear::parseXsdDatetime(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + "T" + timeRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat("The value ", dateString, " cannot be parsed as an `xsd:dateTime`.")};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  int day = toInt<"day">(match);
  int hour = toInt<"hour">(match);
  int minute = toInt<"minute">(match);
  double second = std::strtod(match.get<"second">().data(), nullptr);
  return makeDateOrLargeYear(year, month, day, hour, minute, second, parseTimezone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseXsdDate(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
      throw std::runtime_error{absl::StrCat("The value ", dateString, " cannot be parsed as an `xsd:date`.")};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  int day = toInt<"day">(match);
  return makeDateOrLargeYear(year, month, day, 0, 0, 0.0, parseTimezone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseGYear(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex = "(?<year>-?\\d{4})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
      throw std::runtime_error{absl::StrCat("The value ", dateString, " cannot be parsed as an `xsd:gYear`.")};
  }
  int64_t year = toInt<"year">(match);
  // TODO<joka921> How should we distinguish between `dateTime`, `date`,
  // `year` and `yearMonth` in the underlying representation?
  return makeDateOrLargeYear(year, 1, 1, 0, 0, 0.0, parseTimezone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseGYearMonth(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex =
      "(?<year>-?\\d{4})-(?<month>\\d{2})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
      throw std::runtime_error{absl::StrCat("The value ", dateString, " cannot be parsed as an `xsd:gYearMonth`.")};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  // TODO<joka921> How should we distinguish between `dateTime`, `date`,
  // `year` and `yearMonth` in the underlying representation?
  return makeDateOrLargeYear(year, month, 1, 0, 0, 0.0, parseTimezone(match));
}
