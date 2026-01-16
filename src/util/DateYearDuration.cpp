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

namespace detail {
// Combined regex patterns for different date/time formats (namespace scope for
// C++17)
constexpr static ctll::fixed_string dateTimeRegex =
    dateRegex + "T" + timeRegex + grp(timeZoneRegex) + "?";
constexpr static ctll::fixed_string dateOnlyRegex =
    dateRegex + grp(timeZoneRegex) + "?";
constexpr static ctll::fixed_string gYearRegex = "(?<year>-?\\d{4,})";
constexpr static ctll::fixed_string gYearRegexWithTz =
    gYearRegex + grp(timeZoneRegex) + "?";
constexpr static ctll::fixed_string gYearMonthRegex =
    "(?<year>-?\\d{4,})-(?<month>\\d{2})";
constexpr static ctll::fixed_string gYearMonthRegexWithTz =
    gYearMonthRegex + grp(timeZoneRegex) + "?";

// CTRE named capture group identifiers for C++17 compatibility
constexpr ctll::fixed_string yearGroup = "year";
constexpr ctll::fixed_string monthGroup = "month";
constexpr ctll::fixed_string dayGroup = "day";
constexpr ctll::fixed_string hourGroup = "hour";
constexpr ctll::fixed_string minuteGroup = "minute";
constexpr ctll::fixed_string secondGroup = "second";
constexpr ctll::fixed_string tzZGroup = "tzZ";
constexpr ctll::fixed_string tzSignGroup = "tzSign";
constexpr ctll::fixed_string tzHoursGroup = "tzHours";
constexpr ctll::fixed_string tzMinutesGroup = "tzMinutes";
}  // namespace detail

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
  if (match.template get<detail::tzZGroup>() == "Z") {
    return Date::TimeZoneZ{};
  } else if (!match.template get<detail::tzHoursGroup>()) {
    return Date::NoTimeZone{};
  }
  int tz = match.template get<detail::tzHoursGroup>().to_number();
  if (match.template get<detail::tzSignGroup>() == "-") {
    tz *= -1;
  }
  if (match.template get<detail::tzMinutesGroup>() != "00") {
    AD_LOG_DEBUG << "Qlever supports only full hours as timezones, timezone"
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
      AD_LOG_DEBUG << "QLever cannot encode dates that are less than "
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
      AD_LOG_DEBUG
          << "When the year of a datetime object is smaller than -9999 "
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
  auto match = ctre::match<detail::dateTimeRegex>(dateString);
  if (!match) {
    return std::nullopt;
  }
  int64_t year = match.template get<detail::yearGroup>().to_number<int64_t>();
  int month = match.template get<detail::monthGroup>().to_number();
  int day = match.template get<detail::dayGroup>().to_number();
  int hour = match.template get<detail::hourGroup>().to_number();
  int minute = match.template get<detail::minuteGroup>().to_number();
  double second = std::strtod(match.get<detail::secondGroup>().data(), nullptr);
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
  auto match = ctre::match<detail::dateOnlyRegex>(dateString);
  if (!match) {
    return std::nullopt;
  }
  int64_t year = match.template get<detail::yearGroup>().to_number<int64_t>();
  int month = match.template get<detail::monthGroup>().to_number();
  int day = match.template get<detail::dayGroup>().to_number();
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
  auto match = ctre::match<detail::gYearRegexWithTz>(dateString);
  if (!match) {
    throw DateParseException{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:gYear`.")};
  }
  int64_t year = match.template get<detail::yearGroup>().to_number<int64_t>();
  return makeDateOrLargeYear(dateString, year, 0, 0, -1, 0, 0.0,
                             parseTimeZone(match));
}

// _____________________________________________________________________________
DateYearOrDuration DateYearOrDuration::parseGYearMonth(
    std::string_view dateString) {
  auto match = ctre::match<detail::gYearMonthRegexWithTz>(dateString);
  if (!match) {
    throw DateParseException{absl::StrCat(
        "The value ", dateString, " cannot be parsed as an `xsd:gYearMonth`.")};
  }
  int64_t year = match.template get<detail::yearGroup>().to_number<int64_t>();
  int month = match.template get<detail::monthGroup>().to_number();
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

// _____________________________________________________________________________
#ifndef REDUCED_FEATURE_SET_FOR_CPP17
void updatePassedTimes(const Date& date1, const Date& date2, int& daysPassed,
                       int& hoursPassed, int& minutesPassed,
                       double& secondsPassed) {
  // helper function for Subtraction
  // this function allows to swap the dates, if daysPassed was negative before
  // applying abs. updating daysPassed, hoursPassed, minutesPassed,
  // secondsPassed accordingly
  if (date1.hasTime()) {
    int hour1 = date1.getHour();
    int minute1 = date1.getMinute();
    double second1 = date1.getSecond();
    if (date2.hasTime()) {
      int hour2 = date2.getHour();
      int minute2 = date2.getMinute();
      double second2 = date2.getSecond();
      if (hour1 < hour2) {
        daysPassed--;  // counted one day to much
        hoursPassed =
            24 - (hour2 - hour1);  // total hours of a day - difference
      } else {
        hoursPassed = (hour1 - hour2);
      }
      if (minute1 < minute2) {
        hoursPassed--;  // same as above just one level down
        minutesPassed = 60 - (minute2 - minute1);
      } else {
        minutesPassed = (minute1 - minute2);
      }
      if (second1 < second2) {
        minutesPassed--;
        secondsPassed = 60 - (second2 - second1);
      } else {
        secondsPassed = (second1 - second2);
      }
    } else {
      // if there is no time given, assume 00:00h 0seconds
      hoursPassed = hour1;
      minutesPassed = minute1;
      secondsPassed = second1;
    }
  } else {
    // date1 has no time, therefore we are assuming time 00:00:00
    if (date2.hasTime()) {
      int hour2 = date2.getHour();
      int minute2 = date2.getMinute();
      double second2 = date2.getSecond();
      daysPassed--;
      secondsPassed = 60.0 - second2;
      minutesPassed =
          60 -
          (minute2 + 1 * (secondsPassed >
                          0.0));  // we add 1 because the seconds added a minute
      hoursPassed = 24 - (hour2 + 1 * (minutesPassed > 0));
    }
  }
}

DateYearOrDuration DateYearOrDuration::operator-(
    const DateYearOrDuration& rhs) const {
  // TODO: also support hours, minutes and seconds
  if (isDate() && rhs.isDate()) {
    // TODO: handle time as well
    // Date - Date = Duration | getting time between the two Dates
    const Date& ownDate = getDateUnchecked();
    const Date& otherDate = rhs.getDateUnchecked();

    // Calculate number of days between the two Dates
    auto date1 =
        std::chrono::year_month_day{std::chrono::year(ownDate.getYear()) /
                                    ownDate.getMonth() / ownDate.getDay()};
    auto date2 =
        std::chrono::year_month_day{std::chrono::year(otherDate.getYear()) /
                                    otherDate.getMonth() / otherDate.getDay()};

    int daysPassed =
        (std::chrono::sys_days{date1} - std::chrono::sys_days{date2}).count();
    int hoursPassed = 0;
    int minutesPassed = 0;
    double secondsPassed = 0.0;

    bool isDaysPassedPos = true;

    if (daysPassed < 0) {
      isDaysPassedPos = false;
      daysPassed = abs(daysPassed);
    }
    // Calculate time passed between the two Dates if at least one of them has a
    // Time.
    if (isDaysPassedPos) {
      updatePassedTimes(ownDate, otherDate, daysPassed, hoursPassed,
                        minutesPassed, secondsPassed);
    } else {
      updatePassedTimes(otherDate, ownDate, daysPassed, hoursPassed,
                        minutesPassed, secondsPassed);
    }
    return DateYearOrDuration(DayTimeDuration(DayTimeDuration::Type::Positive,
                                              daysPassed, hoursPassed,
                                              minutesPassed, secondsPassed));
  }

  if (isDayTimeDuration() && rhs.isDayTimeDuration()) {
    // Duration - Duration = Duration | getting new duration that is
    // rhs.duration-time smaller return;
  }

  if (isDate() && rhs.isDayTimeDuration()) {
    // Date - Duration = Date | getting new Date from rhs.duration-time earlier
    // return;
  }

  if (isDayTimeDuration() && rhs.isDate()) {
    // Duration - Date |  not implemented
    // return;
  }

  // no viable subtraction
  else {
    throw std::invalid_argument("No Subtraction possible!");
  }
}
#endif
