//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "util/Date.h"

#include <absl/strings/str_format.h>

#include "global/Constants.h"

// _____________________________________________________________________________
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

// _____________________________________________________________________________
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
