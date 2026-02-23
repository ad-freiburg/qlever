//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "util/Date.h"

#include <absl/strings/str_format.h>

#include "global/Constants.h"

// _____________________________________________________________________________
std::string Date::formatTimeZone() const {
  auto impl = [](const auto& value) -> std::string {
    using T = std::decay_t<decltype(value)>;
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

// _____________________________________________________________________________
std::pair<std::string, const char*> Date::toStringAndType() const {
  std::string dateString;
  const char* type = nullptr;

  if (getMonth() == 0) {
    dateString = getFormattedYear();
    type = XSD_GYEAR_TYPE;
  } else if (getDay() == 0) {
    dateString = absl::StrFormat("%s-%02d", getFormattedYear(), getMonth());
    type = XSD_GYEARMONTH_TYPE;
  } else if (!hasTime()) {
    dateString = absl::StrFormat("%s-%02d-%02d", getFormattedYear(), getMonth(),
                                 getDay());
    type = XSD_DATE_TYPE;
  } else {
    type = XSD_DATETIME_TYPE;
    double seconds = getSecond();
    double dIntPart;
    if (std::modf(seconds, &dIntPart) == 0.0) {
      dateString = absl::StrFormat(
          "%s-%02d-%02dT%02d:%02d:%02d", getFormattedYear(), getMonth(),
          getDay(), getHour(), getMinute(), static_cast<int>(seconds));
    } else {
      dateString = absl::StrFormat("%s-%02d-%02dT%02d:%02d:%06.3f",
                                   getFormattedYear(), getMonth(), getDay(),
                                   getHour(), getMinute(), getSecond());
    }
  }
  return {absl::StrCat(dateString, formatTimeZone()), type};
}

// _____________________________________________________________________________
std::string Date::getFormattedYear() const {
  return getYear() >= 0 ? absl::StrFormat("%04d", getYear())
                        : absl::StrFormat("%05d", getYear());
}

#ifndef REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
std::optional<DayTimeDuration> Date::operator-(const Date& rhs) const {
  auto epoch1 = toEpoch();
  auto epoch2 = rhs.toEpoch();

  if (!epoch1.has_value() || !epoch2.has_value()) {
    return std::nullopt;
  }

  std::chrono::sys_time<std::chrono::nanoseconds> date1 = epoch1.value();
  std::chrono::sys_time<std::chrono::nanoseconds> date2 = epoch2.value();

  DayTimeDuration::Type durationType = DayTimeDuration::Type::Positive;
  auto difference = date1 - date2;
  if (date1 < date2) {
    durationType = DayTimeDuration::Type::Negative;
    difference = -difference;
  }
  // Get total nanoseconds and convert them so a seconds double.
  double second =
      std::chrono::duration_cast<std::chrono::nanoseconds>(difference).count() /
      1000000000;
  return DayTimeDuration(durationType, 0, 0, 0, second);
}

// _____________________________________________________________________________
std::optional<std::chrono::sys_time<std::chrono::nanoseconds>> Date::toEpoch()
    const {
  using namespace std::chrono;
  auto date = year_month_day{year(getYear()) / getMonth() / getDay()};
  if (date.ok()) {
    // Build timestamp from date
    auto second = duration<double>{getSecond()};
    std::chrono::sys_time<std::chrono::nanoseconds> result =
        sys_days(date) + hours{getHour() + getTimeZoneOffsetToUTCInHours()} +
        minutes{getMinute()} +
        duration_cast<nanoseconds>(
            second);  // Here all times are converted to a UTC time.
    return result;
  } else {
    // Invalid date does not have Unix Epoch time.
    return std::nullopt;
  }
}

// _____________________________________________________________________________
int8_t Date::getTimeZoneOffsetToUTCInHours() const {
  TimeZone tz = getTimeZone();
  return std::visit(
      [](auto& value) {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, NoTimeZone>) {
          return 0;  // Assume UTC time zone
        } else if constexpr (std::is_same_v<T, TimeZoneZ>) {
          return 0;
        } else if constexpr (std::is_same_v<T, int>) {
          return value;
        }
      },
      tz);
}
#endif
