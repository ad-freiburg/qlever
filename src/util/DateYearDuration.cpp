//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "util/DateYearDuration.h"

#include <absl/strings/str_format.h>

#include "util/CtreHelpers.h"
#include "util/Log.h"

// Regex objects with explicitly named groups to parse dates and times.
constexpr static ctll::fixed_string dateRegex{
    R"((?<year>-?\d{4,})-(?<month>\d{2})-(?<day>\d{2}))"};
constexpr static ctll::fixed_string timeRegex{
    R"((?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2}(\.\d+)?))"};
constexpr static ctll::fixed_string timeZoneRegex{
    R"((?<tzZ>Z)|(?<tzSign>[+\-])(?<tzHours>\d{2}):(?<tzMinutes>\d{2}))"};

// _____________________________________________________________________________
std::pair<std::string, const char*> DateYearOrDuration::toStringAndType()
    const {
  if (isDate()) {
    return getDateUnchecked().toStringAndType();
  }

  // If a DayTimeDuration is contained it will be exported as a
  // `xsd:dayTimeDuration` type.
  if (isDayTimeDuration()) {
    return getDayTimeDurationUnchecked().toStringAndType();
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

// _____________________________________________________________________________
template <typename T>
static Date::TimeZone parseTimeZone(const T& match) {
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

// _____________________________________________________________________________
// Create a `DateYearOrDuration` from the given input. If the `year` is in the
// range `[-9999, 9999]` then the date is stored regularly, otherwise only the
// year is stored, and it is checked whether `month` and `day` are both `1`, and
// `hour, minute, second` are all `0`.
static DateYearOrDuration makeDateOrLargeYear(std::string_view fullInput,
                                              int64_t year, int month, int day,
                                              int hour, int minute,
                                              double second,
                                              Date::TimeZone timeZone) {
  if (year < Date::minYear || year > Date::maxYear) {
    if (year < DateYearOrDuration::minYear ||
        year > DateYearOrDuration::maxYear) {
      LOG(DEBUG) << "QLever cannot encode dates that are less than "
                 << DateYearOrDuration::minYear << " or larger than "
                 << DateYearOrDuration::maxYear << ". Input " << fullInput
                 << " will be clamped to this range";
      year = std::clamp(year, DateYearOrDuration::minYear,
                        DateYearOrDuration::maxYear);
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
      return DateYearOrDuration(year, DateYearOrDuration::Type::Year);
    }
    warn("month", month, 1);

    if (day == 0) {
      return DateYearOrDuration(year, DateYearOrDuration::Type::YearMonth);
    }
    warn("day", day, 1);
    if (hour == -1) {
      return DateYearOrDuration(year, DateYearOrDuration::Type::Date);
    }
    warn("hour", hour, 0);
    warn("minute", minute, 0);
    warn("second", second, 0.0);
    return DateYearOrDuration(year, DateYearOrDuration::Type::DateTime);
  }
  return DateYearOrDuration{
      Date{static_cast<int>(year), month, day, hour, minute, second, timeZone}};
}

// _____________________________________________________________________________
std::optional<DateYearOrDuration>
DateYearOrDuration::parseXsdDatetimeGetOptDate(std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + "T" + timeRegex + grp(timeZoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    return std::nullopt;
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

// _____________________________________________________________________________
DateYearOrDuration DateYearOrDuration::parseXsdDatetime(
    std::string_view dateString) {
  if (auto optDate = parseXsdDatetimeGetOptDate(dateString); optDate) {
    return optDate.value();
  }
  throw DateParseException{absl::StrCat(
      "The value ", dateString, " cannot be parsed as an `xsd:dateTime`.")};
}

// _____________________________________________________________________________
std::optional<DateYearOrDuration> DateYearOrDuration::parseXsdDateGetOptDate(
    std::string_view dateString) {
  constexpr static ctll::fixed_string dateTime =
      dateRegex + grp(timeZoneRegex) + "?";
  auto match = ctre::match<dateTime>(dateString);
  if (!match) {
    return std::nullopt;
  }
  int64_t year = match.template get<"year">().to_number<int64_t>();
  int month = match.template get<"month">().to_number();
  int day = match.template get<"day">().to_number();
  return makeDateOrLargeYear(dateString, year, month, day, -1, 0, 0.0,
                             parseTimeZone(match));
}

// _____________________________________________________________________________
DateYearOrDuration DateYearOrDuration::parseXsdDate(
    std::string_view dateString) {
  if (auto optDate = parseXsdDateGetOptDate(dateString); optDate) {
    return optDate.value();
  }
  throw DateParseException{absl::StrCat("The value ", dateString,
                                        " cannot be parsed as an `xsd:date`.")};
}

// _____________________________________________________________________________
DateYearOrDuration DateYearOrDuration::parseGYear(std::string_view dateString) {
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

// _____________________________________________________________________________
DateYearOrDuration DateYearOrDuration::parseGYearMonth(
    std::string_view dateString) {
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

//_____________________________________________________________________________
DateYearOrDuration DateYearOrDuration::parseXsdDayTimeDuration(
    std::string_view dayTimeDurationString) {
  return DateYearOrDuration{
      DayTimeDuration::parseXsdDayTimeDuration(dayTimeDurationString)};
}

// _____________________________________________________________________________
std::optional<DateYearOrDuration>
DateYearOrDuration::xsdDayTimeDurationFromDate(
    const DateYearOrDuration& dateOrLargeYear) {
  if (!dateOrLargeYear.isDate()) {
    return std::nullopt;
  } else {
    Date::TimeZone timezone = dateOrLargeYear.getDateUnchecked().getTimeZone();
    if (std::holds_alternative<int>(timezone)) {
      int hour = std::get<int>(timezone);
      auto type = hour < 0 ? DayTimeDuration::Type::Negative
                           : DayTimeDuration::Type::Positive;
      return DateYearOrDuration{
          DayTimeDuration(type, 0, std::abs(hour), 0, 0.0)};
    } else if (std::holds_alternative<Date::TimeZoneZ>(timezone)) {
      return DateYearOrDuration{DayTimeDuration{}};
    } else {
      AD_CORRECTNESS_CHECK(std::holds_alternative<Date::NoTimeZone>(timezone));
      return std::nullopt;
    }
  }
}

// _____________________________________________________________________________
int64_t DateYearOrDuration::getYear() const {
  if (isDate()) {
    return getDateUnchecked().getYear();
  } else {
    return NBit::fromNBit(bits_ >> numTypeBits);
  }
}

// _____________________________________________________________________________
std::optional<int> DateYearOrDuration::getMonth() const {
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

// _____________________________________________________________________________
std::optional<int> DateYearOrDuration::getDay() const {
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

// _____________________________________________________________________________
std::string DateYearOrDuration::getStrTimezone() const {
  if (isDate()) {
    return getDateUnchecked().formatTimeZone();
  } else {
    return "";
  }
}

// _____________________________________________________________________________
std::optional<DateYearOrDuration> DateYearOrDuration::convertToXsdDatetime(
    const DateYearOrDuration& dateValue) {
  if (dateValue.isDayTimeDuration() || !dateValue.isDate()) {
    return std::nullopt;
  }
  const Date& date = dateValue.getDate();
  if (date.hasTime()) {
    // Is already xsd:dateTime value.
    return dateValue;
  }
  return DateYearOrDuration(
      Date(date.getYear(), date.getMonth(), date.getDay(), 0, 0, 0.0));
}

// _____________________________________________________________________________
std::optional<DateYearOrDuration> DateYearOrDuration::convertToXsdDate(
    const DateYearOrDuration& dateValue) {
  if (dateValue.isDayTimeDuration() || !dateValue.isDate()) {
    return std::nullopt;
  }
  const Date& date = dateValue.getDate();
  return DateYearOrDuration(
      Date(date.getYear(), date.getMonth(), date.getDay()));
}
