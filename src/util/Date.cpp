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
