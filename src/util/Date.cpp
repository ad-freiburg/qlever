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

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
std::optional<DayTimeDuration> Date::operator-(const Date& rhs) const {
  auto epoch1 = toEpoch();
  auto epoch2 = rhs.toEpoch();

  if (!epoch1.has_value() || !epoch2.has_value()) {
    return std::nullopt;
  }

  Date::Seconds date1 = epoch1.value();
  Date::Seconds date2 = epoch2.value();

  DayTimeDuration::Type durationType = DayTimeDuration::Type::Positive;
  auto difference = date1 - date2;
  if (date1 < date2) {
    durationType = DayTimeDuration::Type::Negative;
    difference = -difference;
  }
  // Get total seconds.
  auto second =
      std::chrono::duration_cast<std::chrono::seconds>(difference).count();
  // Only passing seconds to `DayTimeDuration`. The object itself will convert
  // the input to days, hours, minutes and seconds.
  return DayTimeDuration{durationType, 0, 0, 0, second};
}

// _____________________________________________________________________________
std::optional<Date> Date::operator-(const DayTimeDuration& rhs) const {
  auto epochLhs = toEpoch();
  if (!epochLhs.has_value()) {
    return std::nullopt;
  }
  auto totalMillisecondsRhs = rhs.getTotalMilliseconds();
  Date::Seconds newDate =
      epochLhs.value() -
      std::chrono::seconds(totalMillisecondsRhs /
                           1'000);  // milliseconds to seconds
  return makeFromEpoch(newDate, getTimeZone());
}

// _____________________________________________________________________________
std::optional<Date::Seconds> Date::toEpoch() const {
  using namespace std::chrono;
  auto date = year_month_day{year(getYear()) / getMonth() / getDay()};
  if (date.ok()) {
    // Build timestamp from `Date`.
    auto second = duration<double>{getSecond()};
    Date::Seconds result =
        sys_days(date) + hours{getHour() - getTimeZoneOffsetToUTCInHours()} +
        minutes{getMinute()} +
        duration_cast<seconds>(
            second);  // Here all times are converted to a UTC time.
    return result;
  } else {
    // Invalid `Date` does not have Unix Epoch time.
    return std::nullopt;
  }
}

// _____________________________________________________________________________
std::optional<int64_t> Date::toEpochInt() const {
  std::optional<Date::Nanoseconds> result = toEpoch();
  if (!result.has_value()) {
    return std::nullopt;  // Invalid date.
  } else {
    // First convert the timepoint to its duration representation and then cast
    // to total seconds.
    return std::chrono::duration_cast<std::chrono::seconds>(
               result.value().time_since_epoch())
        .count();
  }
}

// _____________________________________________________________________________
Date Date::makeFromEpoch(Nanoseconds timestamp, TimeZone tz) {
  int8_t offset = Date::getTimeZoneOffsetToUTCInHours(tz);
  // Shift the timestamp according to the given `TimeZone`offset.
  timestamp = timestamp + std::chrono::hours{offset};

  // Extract date from epoch timestamp.
  auto days = std::chrono::floor<std::chrono::days>(timestamp);
  std::chrono::year_month_day date = std::chrono::year_month_day{days};

  // Extract time from remaining seconds.
  auto seconds = std::chrono::floor<std::chrono::seconds>(timestamp - days);
  std::chrono::hh_mm_ss remainder = std::chrono::hh_mm_ss{seconds};

  // The methods `year`, `month`, `day` return
  // `std::chrono::year`/`std::chrono::month`/`std::chrono::day`, therefore
  // static casts are necessary. For `month` and `day` only `operator unsigned`
  // is supported, therefore two casts are necessary.
  return Date{static_cast<int>(date.year()),
              static_cast<int>(static_cast<unsigned>(date.month())),
              static_cast<int>(static_cast<unsigned>(date.day())),
              static_cast<int>(remainder.hours().count()),
              static_cast<int>(remainder.minutes().count()),
              static_cast<double>(remainder.seconds().count()),
              tz};
}

#endif

// _____________________________________________________________________________
int8_t Date::getTimeZoneOffsetToUTCInHours(TimeZone tz) {
  // Handle different types contained in variant `TimeZone`.
  return std::visit(
      [](auto& value) {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, NoTimeZone>) {
          return 0;  // Assume UTC time zone.
        } else if constexpr (std::is_same_v<T, TimeZoneZ>) {
          return 0;
        } else if constexpr (std::is_same_v<T, int>) {
          return value;
        }
      },
      tz);
}

int8_t Date::getTimeZoneOffsetToUTCInHours() const {
  return Date::getTimeZoneOffsetToUTCInHours(getTimeZone());
}
