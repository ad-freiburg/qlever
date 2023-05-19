//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "util/Date.h"

// ____________________________________________________________________________________________________
std::string Date::formatTimezone() const {
  auto impl = []<typename T>(const T& value) -> std::string {
    if constexpr (std::is_same_v<T, NoTimezone>) {
      return "";
    } else if constexpr (std::is_same_v<T, TimezoneZ>) {
      return "Z";
    } else {
      static_assert(std::is_same_v<T, int>);
      constexpr static std::string_view format = "%0+3d:00";
      return absl::StrFormat(format, value);
    }
  };
  return std::visit(impl, getTimezone());
}

// ____________________________________________________________________________________________________
std::pair<std::string, const char*> Date::toStringAndType() const {
  std::string dateString;
  const char* type = nullptr;

  if (getMonth() == 0) {
    constexpr static std::string_view formatString = "%04d";
    dateString = absl::StrFormat(formatString, getYear());
    type = XSD_GYEAR_TYPE;
  } else if (getDay() == 0) {
    constexpr static std::string_view formatString = "%04d-%02d";
    dateString = absl::StrFormat(formatString, getYear(), getMonth());
    type = XSD_GYEARMONTH_TYPE;
  } else if (getHour() == -1) {
    constexpr static std::string_view formatString = "%04d-%02d-%02d";
    dateString = absl::StrFormat(formatString, getYear(), getMonth(), getDay());
    type = XSD_DATE_TYPE;
  } else {
    type = XSD_DATETIME_TYPE;
    double seconds = getSecond();
    double dIntPart;
    if (std::modf(seconds, &dIntPart) == 0.0) {
      constexpr std::string_view formatString = "%04d-%02d-%02dT%02d:%02d:%02d";
      dateString =
          absl::StrFormat(formatString, getYear(), getMonth(), getDay(),
                          getHour(), getMinute(), static_cast<int>(seconds));
    } else {
      constexpr std::string_view formatString =
          "%04d-%02d-%02dT%02d:%02d:%06.3f";
      dateString =
          absl::StrFormat(formatString, getYear(), getMonth(), getDay(),
                          getHour(), getMinute(), getSecond());
    }
  }
  return {absl::StrCat(dateString, formatTimezone()), type};
}

// _________________________________________________________________
std::pair<std::string, const char*> DateOrLargeYear::toStringAndType() const {
  if (isDate()) {
    return getDateUnchecked().toStringAndType();
  }

  std::string dateString;
  const char* type = nullptr;

  switch (getType()) {
    case Type::DateTime: {
      constexpr std::string_view formatString = "%d-01-01T00:00:00";
      dateString = absl::StrFormat(formatString, getYear());
      type = XSD_DATETIME_TYPE;
      break;
    }
    case Type::Date: {
      constexpr std::string_view formatString = "%d-01-01";
      dateString = absl::StrFormat(formatString, getYear());
      type = XSD_DATE_TYPE;
      break;
    }
    case Type::YearMonth: {
      constexpr std::string_view formatString = "%d-01";
      dateString = absl::StrFormat(formatString, getYear());
      type = XSD_GYEARMONTH_TYPE;
      break;
    }
    case Type::Year: {
      constexpr std::string_view formatString = "%d";
      dateString = absl::StrFormat(formatString, getYear());
      type = XSD_GYEAR_TYPE;
      break;
    }
    default:
      AD_FAIL();
  }
  return {std::move(dateString), type};
}

// Convert a `match` from `ctre` to an integer. The behavior is undefined if
// the `match` cannot be completely converted to an integer.
template <ctll::fixed_string Name>
static int64_t toInt(const auto& match) {
  int64_t result = 0;
  const auto& s = match.template get<Name>();
  std::from_chars(s.data(), s.data() + s.size(), result);
  return result;
}

// Regex objects with explicitly named groups to parse dates and times.
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

// Create a `DateOrLargeYear` from the given input. If the `year` is in the
// range `[-9999, 9999]` then the date is stored regularly, otherwise only the
// year is stored, and it is checked whether `month` and `day` are both `1`, and
// `hour, minute, second` are all `0`.
static DateOrLargeYear makeDateOrLargeYear(int64_t year, int month, int day,
                                           int hour, int minute, double second,
                                           Date::Timezone timezone) {
  if (year < Date::minYear || year > Date::maxYear) {
    if (year < DateOrLargeYear::minYear || year > DateOrLargeYear::maxYear) {
      throw std::runtime_error{
          absl::StrCat("QLever cannot encode dates that are less than ",
                       DateOrLargeYear::minYear, " or larger than ",
                       DateOrLargeYear::maxYear)};
    }

    if (month == 0) {
      return DateOrLargeYear(year, DateOrLargeYear::Type::Year);
    } else if (month != 1) {
      /*
      throw std::runtime_error{
          "When the year of a datetime object is smaller than -9999 or larger "
          "than 9999 then the month has to be 1 in QLever's implementation of "
          "dates"};
          */
    }
    if (day == 0) {
      return DateOrLargeYear(year, DateOrLargeYear::Type::YearMonth);
    } else if (day != 1) {
      /*
      throw std::runtime_error{
          "When the year of a datetime object is smaller than -9999 or larger "
          "than 9999 then the day has to be 1 in QLever's implementation of "
          "dates"};
          */
    }
    if (hour == -1) {
      return DateOrLargeYear(year, DateOrLargeYear::Type::Date);
    } else if (hour != 0) {
      /*
      throw std::runtime_error{
          "When the year of a datetime object is smaller than -9999 or larger "
          "than 9999 then the hour has to be 0 in QLever's implementation of "
          "dates"};
          */
    }

    if (minute != 0 || second != 0.0) {
      /*
      throw std::runtime_error{
          "When the year of a datetime object is smaller than -9999 or larger "
          "than 9999 then the minute and second must both be 0 in QLever's "
          "implementation of Dates."};
          */
    }
    return DateOrLargeYear(year, DateOrLargeYear::Type::DateTime);
  }
  return DateOrLargeYear{
      Date{static_cast<int>(year), month, day, hour, minute, second, timezone}};
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseXsdDatetime(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + "T" + timeRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:dateTime`.")};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  int day = toInt<"day">(match);
  int hour = toInt<"hour">(match);
  int minute = toInt<"minute">(match);
  double second = std::strtod(match.get<"second">().data(), nullptr);
  return makeDateOrLargeYear(year, month, day, hour, minute, second,
                             parseTimezone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseXsdDate(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:date`.")};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  int day = toInt<"day">(match);
  return makeDateOrLargeYear(year, month, day, -1, 0, 0.0,
                             parseTimezone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseGYear(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex = "(?<year>-?\\d{4,})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:gYear`.")};
  }
  int64_t year = toInt<"year">(match);
  return makeDateOrLargeYear(year, 0, 0, -1, 0, 0.0, parseTimezone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseGYearMonth(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex =
      "(?<year>-?\\d{4,})-(?<month>\\d{2})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timezoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw std::runtime_error{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:gYearMonth`.")};
  }
  int64_t year = toInt<"year">(match);
  int month = toInt<"month">(match);
  return makeDateOrLargeYear(year, month, 0, -1, 0, 0.0, parseTimezone(match));
}
