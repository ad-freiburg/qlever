//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "util/Date.h"

template <ctll::fixed_string Name>
static int64_t toInt(const auto& match) {
    int64_t result = 0;
    const auto& s = match.template get<Name>();
    // TODO<joka921> Check the result.
    std::from_chars(s.data(), s.data() + s.size(), result);
    return result;
}
constexpr static ctll::fixed_string dateRegex{
        R"((?<year>-?\d{4,})-(?<month>\d{2})-(?<day>\d{2}))"};
constexpr static ctll::fixed_string timeRegex{
        R"((?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2}(\.\d{1,12})?))"};
constexpr static ctll::fixed_string timezoneRegex{
        R"((?<tzZ>Z)|(?<tzSign>[+\-])(?<tzHours>\d{2}):(?<tzMinutes>\d{2}))"};

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

static DateOrLargeYear makeDateOrLargeYear(int64_t year, int month, int day, int hour, int minute, double second, Date::Timezone timezone) {
    if (year < Date::minYear || year > Date::maxYear) {
        return DateOrLargeYear(year);
    }
    return DateOrLargeYear{Date{static_cast<int>(year), month, day, hour, minute, second, timezone}};
}

    DateOrLargeYear DateOrLargeYear::parseXsdDatetime(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + "T" + timeRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat("Illegal dateTime ", dateString)};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  int day = toInt<"day">(match);
  int hour = toInt<"hour">(match);
  int minute = toInt<"minute">(match);
  double second = std::strtod(match.get<"second">().data(), nullptr);
  return makeDateOrLargeYear(year, month, day, hour, minute, second, parseTimezone(match));
}

DateOrLargeYear DateOrLargeYear::parseXsdDate(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat("Illegal date ", dateString)};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  int day = toInt<"day">(match);
  return makeDateOrLargeYear(year, month, day, 0, 0, 0.0, parseTimezone(match));
}

DateOrLargeYear DateOrLargeYear::parseGYear(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex = "(?<year>-?\\d{4})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat("Illegal gyear", dateString)};
  }
  int64_t year = toInt<"year">(match);
  // TODO<joka921> How should we distinguish between `dateTime`, `date`,
  // `year` and `yearMonth` in the underlying representation?
  return makeDateOrLargeYear(year, 1, 1, 0, 0, 0.0, parseTimezone(match));
}

DateOrLargeYear DateOrLargeYear::parseGYearMonth(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex =
      "(?<year>-?\\d{4})-(?<month>\\d{2})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat("Illegal yearMonth", dateString)};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  // TODO<joka921> How should we distinguish between `dateTime`, `date`,
  // `year` and `yearMonth` in the underlying representation?
  return makeDateOrLargeYear(year, month, 1, 0, 0, 0.0, parseTimezone(match));
}
