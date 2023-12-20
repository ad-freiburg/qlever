//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "util/Date.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <ctre-unicode.hpp>

#include "util/Log.h"

// ____________________________________________________________________________________________________
std::string Date::formatTimeZone() const {
  auto impl = []<typename T>(const T& value) -> std::string {
    if constexpr (std::is_same_v<T, NoTimeZone>) {
      return "";
    } else if constexpr (std::is_same_v<T, TimeZoneZ>) {
      return "Z";
    } else {
      static_assert(std::is_same_v<T, int>);
      constexpr static std::string_view format = "%0+3d:00";
      return absl::StrFormat(format, value);
    }
  };
  return std::visit(impl, getTimeZone());
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
  } else if (!hasTime()) {
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
  return {absl::StrCat(dateString, formatTimeZone()), type};
}

// _________________________________________________________________
std::pair<std::string, const char*> DateOrLargeYear::toStringAndType() const {
  if (isDate()) {
    return getDateUnchecked().toStringAndType();
  }

  using F = absl::ParsedFormat<'d'>;

  // Format the year using `format` and return the pair of `(formattedYear,
  // type)`.
  auto impl = [this](const F& format, const char* actualType) {
    return std::pair{absl::StrFormat(format, getYear()), actualType};
  };

  switch (getType()) {
    case Type::DateTime:
      return impl(F{"%d-01-01T00:00:00"}, XSD_DATETIME_TYPE);
    case Type::Date:
      return impl(F{"%d-01-01"}, XSD_DATE_TYPE);
    case Type::YearMonth:
      return impl(F{"%d-01"}, XSD_GYEARMONTH_TYPE);
    case Type::Year:
      return impl(F{"%d"}, XSD_GYEAR_TYPE);
  }
  AD_FAIL();
}

// Regex objects with explicitly named groups to parse dates and times.
constexpr static ctll::fixed_string dateRegex{
    R"((?<year>-?\d{4,})-(?<month>\d{2})-(?<day>\d{2}))"};
constexpr static ctll::fixed_string timeRegex{
    R"((?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2}(\.\d+)?))"};
constexpr static ctll::fixed_string timeZoneRegex{
    R"((?<tzZ>Z)|(?<tzSign>[+\-])(?<tzHours>\d{2}):(?<tzMinutes>\d{2}))"};

// Get the correct `TimeZone` from a regex match for the `timeZoneRegex`.
static Date::TimeZone parseTimeZone(const auto& match) {
  if (match.template get<"tzZ">() == "Z") {
    return Date::TimeZoneZ{};
  } else if (!match.template get<"tzHours">()) {
    return Date::NoTimeZone{};
  }
  int tz = match.template get<"tzHours">().to_number();
  if (match.template get<"tzSign">() == "-") {
    tz *= -1;
  }
  if (match.template get<"tzMinutes">() != "00") {
    LOG(DEBUG) << "Qlever supports only full hours as timezones, timezone"
               << match.template get<0>() << "will be rounded down to " << tz
               << ":00" << std::endl;
  }
  return tz;
}

// Create a `DateOrLargeYear` from the given input. If the `year` is in the
// range `[-9999, 9999]` then the date is stored regularly, otherwise only the
// year is stored, and it is checked whether `month` and `day` are both `1`, and
// `hour, minute, second` are all `0`.
static DateOrLargeYear makeDateOrLargeYear(std::string_view fullInput,
                                           int64_t year, int month, int day,
                                           int hour, int minute, double second,
                                           Date::TimeZone timeZone) {
  if (year < Date::minYear || year > Date::maxYear) {
    if (year < DateOrLargeYear::minYear || year > DateOrLargeYear::maxYear) {
      LOG(DEBUG) << "QLever cannot encode dates that are less than "
                 << DateOrLargeYear::minYear << " or larger than "
                 << DateOrLargeYear::maxYear << ". Input " << fullInput
                 << " will be clamped to this range";
      year =
          std::clamp(year, DateOrLargeYear::minYear, DateOrLargeYear::maxYear);
    }

    // Print a warning if the value of a component of a date (e.g. month or day)
    // will be lost because of the large year representation.
    auto warn = [&fullInput, alreadyWarned = false](std::string_view component,
                                                    auto actualValue,
                                                    auto defaultValue) mutable {
      if (actualValue == defaultValue || alreadyWarned) {
        return;
      }
      // Warn only for one component per input.
      alreadyWarned = true;
      LOG(DEBUG) << "When the year of a datetime object is smaller than -9999 "
                    "or larger than 9999 then the "
                 << component << " will always be set to " << defaultValue
                 << " in QLever's implementation of dates. Full input was "
                 << fullInput << std::endl;
    };

    if (month == 0) {
      return DateOrLargeYear(year, DateOrLargeYear::Type::Year);
    }
    warn("month", month, 1);

    if (day == 0) {
      return DateOrLargeYear(year, DateOrLargeYear::Type::YearMonth);
    }
    warn("day", day, 1);
    if (hour == -1) {
      return DateOrLargeYear(year, DateOrLargeYear::Type::Date);
    }
    warn("hour", hour, 0);
    warn("minute", minute, 0);
    warn("second", second, 0.0);
    return DateOrLargeYear(year, DateOrLargeYear::Type::DateTime);
  }
  return DateOrLargeYear{
      Date{static_cast<int>(year), month, day, hour, minute, second, timeZone}};
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseXsdDatetime(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + "T" + timeRegex + grp(timeZoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw DateParseException{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:dateTime`.")};
  }
  int64_t year = match.template get<"year">().to_number<int64_t>();
  int month = match.template get<"month">().to_number();
  int day = match.template get<"day">().to_number();
  int hour = match.template get<"hour">().to_number();
  int minute = match.template get<"minute">().to_number();
  double second = std::strtod(match.get<"second">().data(), nullptr);
  return makeDateOrLargeYear(dateString, year, month, day, hour, minute, second,
                             parseTimeZone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseXsdDate(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + grp(timeZoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw DateParseException{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:date`.")};
  }
  int64_t year = match.template get<"year">().to_number<int64_t>();
  int month = match.template get<"month">().to_number();
  int day = match.template get<"day">().to_number();
  return makeDateOrLargeYear(dateString, year, month, day, -1, 0, 0.0,
                             parseTimeZone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseGYear(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex = "(?<year>-?\\d{4,})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timeZoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw DateParseException{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:gYear`.")};
  }
  int64_t year = match.template get<"year">().to_number<int64_t>();
  return makeDateOrLargeYear(dateString, year, 0, 0, -1, 0, 0.0,
                             parseTimeZone(match));
}

// __________________________________________________________________________________
DateOrLargeYear DateOrLargeYear::parseGYearMonth(std::string_view dateString) {
  constexpr static ctll::fixed_string yearRegex =
      "(?<year>-?\\d{4,})-(?<month>\\d{2})";
  constexpr static ctll::fixed_string dateTime =
      yearRegex + grp(timeZoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    throw DateParseException{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:gYearMonth`.")};
  }
  int64_t year = match.template get<"year">().to_number<int64_t>();
  int month = match.template get<"month">().to_number();
  return makeDateOrLargeYear(dateString, year, month, 0, -1, 0, 0.0,
                             parseTimeZone(match));
}

// _____________________________________________________________________-
int64_t DateOrLargeYear::getYear() const {
  if (isDate()) {
    return getDateUnchecked().getYear();
  } else {
    return NBit::fromNBit(bits_ >> numTypeBits);
  }
}

// _____________________________________________________________________-
std::optional<int> DateOrLargeYear::getMonth() const {
  if (isDate()) {
    int month = getDateUnchecked().getMonth();
    if (month == 0) {
      return std::nullopt;
    } else {
      return month;
    }
  } else if (getType() != Type::Year) {
    return 1;
  } else {
    return std::nullopt;
  }
}

// _____________________________________________________________________-
std::optional<int> DateOrLargeYear::getDay() const {
  if (isDate()) {
    int day = getDateUnchecked().getDay();
    if (day == 0) {
      return std::nullopt;
    } else {
      return day;
    }
  } else if (getType() == Type::Year || getType() == Type::YearMonth) {
    return std::nullopt;
  } else {
    return 1;
  }
}
