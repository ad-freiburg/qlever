//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "util/Duration.h"

#include <absl/strings/str_format.h>

#include <ctre.hpp>

#include "global/Constants.h"

//______________________________________________________________________________
std::pair<std::string, const char*> DayTimeDuration::toStringAndType() const {
  std::string str = isPositive() ? "P" : "-P";

  // Get the underlying values of DayTimeDuration object as DurationValue
  // struct. {days, hours, minutes, seconds, fractionalSeconds}
  const auto& dv = getValues();
  int days = dv.days_;
  int hours = dv.hours_;
  int minutes = dv.minutes_;
  double seconds = dv.seconds_;

  // handle days
  if (days != 0) {
    absl::StrAppend(&str, days, std::string_view("D"));
  }
  // T is not always necessary, P10D would be valid too.
  // check hours, minutes, seconds, fractional seconds
  if (hours != 0 || minutes != 0 || seconds != 0.00) {
    absl::StrAppend(&str, "T");
  } else {
    if (days == 0) {
      absl::StrAppend(&str, std::string_view("T0S"));
    }
    return {str, XSD_DAYTIME_DURATION_TYPE};
  }

  if (hours != 0) {
    absl::StrAppend(&str, hours, std::string_view("H"));
  }
  if (minutes != 0) {
    absl::StrAppend(&str, minutes, std::string_view("M"));
  }
  if (seconds == 0) {
    return {str, XSD_DAYTIME_DURATION_TYPE};
  }

  // Handle the seconds with its decimal places properly.
  double dIntPart;
  if (std::modf(seconds, &dIntPart) == 0.0) {
    absl::StrAppend(&str, static_cast<int>(seconds), std::string_view("S"));
  } else {
    absl::StrAppend(&str, absl::StrFormat("%.3f", seconds),
                    std::string_view("S"));
  }
  return {str, XSD_DAYTIME_DURATION_TYPE};
}

//______________________________________________________________________________
DayTimeDuration DayTimeDuration::parseXsdDayTimeDuration(
    std::string_view dayTimeDurationStr) {
  static constexpr ctll::fixed_string dayTimePattern =
      "(?<negation>-?)P((?<days>\\d+)D)?(T((?<hours>\\d+)H)?((?<"
      "minutes>\\d+)M)?((?<seconds>\\d+(\\.\\d+)?)S)?)?";

  // Try to match the given pattern with the provided string. If the matching
  // procedure fails, raise DurationParseException (for Turtle Parser).
  auto match = ctre::match<dayTimePattern>(dayTimeDurationStr);
  if (!match) {
    throw DurationParseException{
        absl::StrCat("The value ", dayTimeDurationStr,
                     " cannot be parsed as an `xsd:dayTimeDuration`.")};
  } else {
    Type negation = match.get<"negation">().to_string() == "-" ? Type::Negative
                                                               : Type::Positive;
    int days = 0;
    int hours = 0;
    int minutes = 0;
    double seconds = 0.00;

    // Certain duration strings could trigger segmentations faults, thus a
    // check for value is necessary here.
    if (auto matchedDays = match.template get<"days">(); matchedDays) {
      days = matchedDays.to_number();
    }
    if (auto matchedHours = match.template get<"hours">(); matchedHours) {
      hours = matchedHours.to_number();
    }
    if (auto matchedMinutes = match.template get<"minutes">(); matchedMinutes) {
      minutes = matchedMinutes.to_number();
    }
    if (auto matchedSeconds = match.get<"seconds">(); matchedSeconds) {
      seconds = std::strtod(matchedSeconds.data(), nullptr);
    }
    return DayTimeDuration(negation, days, hours, minutes, seconds);
  }
}
