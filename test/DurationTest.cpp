//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "global/Constants.h"
#include "util/CtreHelpers.h"
#include "util/DateYearDuration.h"
#include "util/Random.h"

//______________________________________________________________________________
// make for some of the following tests assertion more convenient
#define EXPECT_EQ3(val1, val2, val3) \
  EXPECT_EQ(val1, val2);             \
  EXPECT_EQ(val2, val3);             \
  EXPECT_EQ(val1, val3)

//______________________________________________________________________________
TEST(DayTimeDuration, sizeInitDayTimeDuration) {
  static_assert(sizeof(DayTimeDuration) == 8);
  static_assert(DayTimeDuration::numUnusedBits == 16);
  DayTimeDuration defaultDuration{};
  ASSERT_TRUE(defaultDuration.isPositive());
  ASSERT_EQ(defaultDuration.getSeconds(), 0.00);
  ASSERT_EQ(defaultDuration.getMinutes(), 0);
  ASSERT_EQ(defaultDuration.getHours(), 0);
  ASSERT_EQ(defaultDuration.getDays(), 0);
}

//______________________________________________________________________________
TEST(DayTimeDuration, setAndGetValues) {
  ad_utility::SlowRandomIntGenerator randomDay{0, 1048575};
  ad_utility::SlowRandomIntGenerator randomHour{0, 23};
  ad_utility::SlowRandomIntGenerator randomMinute{0, 59};
  ad_utility::RandomDoubleGenerator randomSecond{0, 59.9990};
  DayTimeDuration::Type positive = DayTimeDuration::Type::Positive;
  DayTimeDuration::Type negative = DayTimeDuration::Type::Negative;
  size_t max = 3333;
  for (size_t i = 0; i < max; ++i) {
    auto seconds = randomSecond();
    auto minutes = randomMinute();
    auto days = randomDay();
    auto hours = randomHour();
    DayTimeDuration duration1{positive, days, hours, minutes, seconds};
    DayTimeDuration duration2{negative, days, hours, minutes, seconds};
    ASSERT_TRUE(duration1.isPositive());
    ASSERT_FALSE(duration2.isPositive());
    EXPECT_EQ3(duration1.getMinutes(), duration2.getMinutes(), minutes);
    EXPECT_EQ3(duration1.getHours(), duration2.getHours(), hours);
    EXPECT_EQ3(duration1.getDays(), duration2.getDays(), days);
    EXPECT_NEAR(duration1.getSeconds(), seconds, 0.001);
    EXPECT_NEAR(duration2.getSeconds(), seconds, 0.001);

    // Basic comparison
    // duration1 is set to positive, duration2 to negative, this should
    // always hold.
    ASSERT_TRUE(duration1 == duration1);
    ASSERT_TRUE(duration2 == duration2);
    ASSERT_TRUE(duration2 < duration1);
    ASSERT_FALSE(duration2 == duration1);
  }
}

//______________________________________________________________________________
MATCHER_P2(MatchesStrAndType, expectedDurationStr, expectedTypeStr, "") {
  DayTimeDuration durationClass;
  const auto& strAndType =
      durationClass.parseXsdDayTimeDuration(arg).toStringAndType();
  return expectedDurationStr == strAndType.first &&
         strcmp(expectedTypeStr, strAndType.second) == 0;
}

//______________________________________________________________________________
TEST(DayTimeDuration, checkParseAndGetStringForSpecialValues) {
  EXPECT_THAT("P0DT0H0M0S",
              MatchesStrAndType("PT0S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("PT0H0M0S", MatchesStrAndType("PT0S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("PT0H0.00S",
              MatchesStrAndType("PT0S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("PT0S", MatchesStrAndType("PT0S", XSD_DAYTIME_DURATION_TYPE));

  // Test w.r.t. maximum values where we don't expect given the current bounds a
  // normalization effect yet.
  EXPECT_THAT(
      "P1048575DT23H59M59.999S",
      MatchesStrAndType("P1048575DT23H59M59.999S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT(
      "-P1048575DT23H59M59.999S",
      MatchesStrAndType("-P1048575DT23H59M59.999S", XSD_DAYTIME_DURATION_TYPE));

  // provided invalid xsd:dayTimeDuration strings
  ASSERT_THROW(DayTimeDuration::parseXsdDayTimeDuration("P0D0H0M0S"),
               DurationParseException);
  ASSERT_THROW(DayTimeDuration::parseXsdDayTimeDuration("0DT0H0M0S"),
               DurationParseException);
  ASSERT_THROW(DayTimeDuration::parseXsdDayTimeDuration("-P0D0HMS"),
               DurationParseException);
  ASSERT_THROW(DayTimeDuration::parseXsdDayTimeDuration("P0DABH0M0S"),
               DurationParseException);
}

//______________________________________________________________________________
TEST(DayTimeDuration, checkParseAndGetStringForGeneralValues) {
  EXPECT_THAT("PT0.033S",
              MatchesStrAndType("PT0.033S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("-PT0.033S",
              MatchesStrAndType("-PT0.033S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("P45D", MatchesStrAndType("P45D", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("-P103978D",
              MatchesStrAndType("-P103978D", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("P35DT11H45M2.22S", MatchesStrAndType("P35DT11H45M2.220S",
                                                    XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT(
      "-P789DT11H45M2.22S",
      MatchesStrAndType("-P789DT11H45M2.220S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("PT59.00S",
              MatchesStrAndType("PT59S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("-P0DT4H32M",
              MatchesStrAndType("-PT4H32M", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("P0DT17H32M0.00S",
              MatchesStrAndType("PT17H32M", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("P43274DT1H0M",
              MatchesStrAndType("P43274DT1H", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("P43274DT0M33.988S",
              MatchesStrAndType("P43274DT33.988S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("-P0DT7H31M45.00S",
              MatchesStrAndType("-PT7H31M45S", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("-P11DT31M0.00S",
              MatchesStrAndType("-P11DT31M", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("PT60.00S", MatchesStrAndType("PT1M", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT("-PT24H", MatchesStrAndType("-P1D", XSD_DAYTIME_DURATION_TYPE));
  EXPECT_THAT(
      "-P2071DT0H21M1.11S",
      MatchesStrAndType("-P2071DT21M1.110S", XSD_DAYTIME_DURATION_TYPE));
}

//______________________________________________________________________________
TEST(DayTimeDuration, checkToAndFromBits) {
  auto d1 = DayTimeDuration(DayTimeDuration::Type::Positive, 1, 23, 23, 59.99);
  auto bits = d1.toBits();
  d1 = DayTimeDuration::fromBits(bits);
  DayTimeDuration::DurationValue dv = d1.getValues();
  ASSERT_EQ(dv.days_, 1);
  ASSERT_EQ(dv.hours_, 23);
  ASSERT_EQ(dv.minutes_, 23);
  ASSERT_EQ(dv.seconds_, 59.99);

  auto d2 =
      DayTimeDuration(DayTimeDuration::Type::Negative, 1048574, 3, 0, 0.99);
  bits = d2.toBits();
  d2 = DayTimeDuration::fromBits(bits);
  dv = d2.getValues();
  ASSERT_EQ(dv.days_, 1048574);
  ASSERT_EQ(dv.hours_, 3);
  ASSERT_EQ(dv.minutes_, 0);
  ASSERT_EQ(dv.seconds_, 0.99);
}

//______________________________________________________________________________
TEST(DayTimeDuration, DurationOverflowException) {
  // test overflow for positive duration
  try {
    [[maybe_unused]] auto d1 =
        DayTimeDuration(DayTimeDuration::Type::Positive, 643917423, 4, 7, 1);
    FAIL() << "DurationOverflowException was expected.";
  } catch (const DurationOverflowException& e) {
    EXPECT_STREQ(e.what(),
                 "Overflow exception raised by DayTimeDuration, please provide "
                 "smaller values for xsd:dayTimeDuration.");
  }

  // test overflow for negative duration
  try {
    [[maybe_unused]] auto d1 =
        DayTimeDuration(DayTimeDuration::Type::Negative, 643917423, 4, 7, 1);
    FAIL() << "DurationOverflowException was expected.";
  } catch (const DurationOverflowException& e) {
    EXPECT_STREQ(e.what(),
                 "Overflow exception raised by DayTimeDuration, please provide "
                 "smaller values for xsd:dayTimeDuration.");
  }
}

//______________________________________________________________________________
// Given that we have a slightly different approach for retrieving the
// individual units in DayTimeDuration (see getValues()) from the total
// millisecond value, we use this helper with the more intuitive approach (from
// milliseconds) to check that it yields the same values, especially for large
// values. The actual check is in test 'checkInternalConversionForLargeValues'.
// Large durations are hard to test over an expected duration string, since the
// retrieved duration string will be normalized w.r.t. its values. Thus it makes
// sense to test them in the following way.
DayTimeDuration::DurationValue toAndFromMilliseconds(int days, int hours,
                                                     int minutes,
                                                     double seconds) {
  // to milliseconds
  auto millisecDays = static_cast<long long>(days) * 24 * 60 * 60 * 1000;
  auto millisecHours = static_cast<long long>(hours) * 60 * 60 * 1000;
  auto millisecMinutes = static_cast<long long>(minutes) * 60 * 1000;
  auto millisecSeconds = static_cast<long long>(std::round(seconds * 1000));
  auto totalMilliseconds =
      millisecDays + millisecHours + millisecMinutes + millisecSeconds;

  // from milliseconds
  long long remainingMilliseconds = totalMilliseconds;
  days = remainingMilliseconds / (24 * 60 * 60 * 1000);
  remainingMilliseconds %= 24 * 60 * 60 * 1000;
  hours = remainingMilliseconds / (60 * 60 * 1000);
  remainingMilliseconds %= 60 * 60 * 1000;
  minutes = remainingMilliseconds / (60 * 1000);
  remainingMilliseconds %= 60 * 1000;
  seconds = remainingMilliseconds / 1000.0;
  return {days, hours, minutes, seconds};
}

//______________________________________________________________________________
TEST(DayTimeDuration, checkInternalConversionForLargeValues) {
  int maxDays = static_cast<int>(DayTimeDuration::maxDays);
  ad_utility::SlowRandomIntGenerator randomDay{1040000, maxDays + 8575};
  ad_utility::SlowRandomIntGenerator randomHour{22, 10000};
  ad_utility::SlowRandomIntGenerator randomMinute{55, 10000};
  ad_utility::RandomDoubleGenerator randomSecond{60.000, 99999.999};

  size_t max = 4096;
  for (size_t i = 0; i < max; ++i) {
    int randDay = randomDay();
    int randHour = randomHour();
    int randMinute = randomMinute();
    int randSeconds = randomSecond();

    DayTimeDuration::DurationValue dv1 =
        toAndFromMilliseconds(randDay, randHour, randMinute, randSeconds);

    if (dv1.days_ > maxDays) {
      ASSERT_THROW(DayTimeDuration(DayTimeDuration::Type::Positive, randDay,
                                   randHour, randMinute, randSeconds),
                   DurationOverflowException);
      ASSERT_THROW(DayTimeDuration(DayTimeDuration::Type::Negative, randDay,
                                   randHour, randMinute, randSeconds),
                   DurationOverflowException);
    } else {
      const auto& dv2 =
          DayTimeDuration(DayTimeDuration::Type::Positive, randDay, randHour,
                          randMinute, randSeconds)
              .getValues();
      const auto& dv3 =
          DayTimeDuration(DayTimeDuration::Type::Negative, randDay, randHour,
                          randMinute, randSeconds)
              .getValues();
      EXPECT_EQ3(dv1.days_, dv2.days_, dv3.days_);
      EXPECT_EQ3(dv1.hours_, dv2.hours_, dv3.hours_);
      EXPECT_EQ3(dv1.minutes_, dv2.minutes_, dv3.minutes_);
      EXPECT_EQ3(dv1.seconds_, dv2.seconds_, dv3.seconds_);
    }
  }
}

//______________________________________________________________________________
TEST(DayTimeDuration, testDayTimeDurationOverflow) {
  // test with values which should trigger a DayTimeDurationOverflow exception
  ASSERT_THROW(
      DayTimeDuration(DayTimeDuration::Type::Positive, 1048577, 59, 59, 60.00),
      DurationOverflowException);
  ASSERT_THROW(
      DayTimeDuration(DayTimeDuration::Type::Negative, 1048577, 59, 59, 60.00),
      DurationOverflowException);
  ASSERT_THROW(DayTimeDuration(DayTimeDuration::Type::Negative, 1000000,
                               1165848, 121, 61.22),
               DurationOverflowException);
  ASSERT_THROW(
      DayTimeDuration::parseXsdDayTimeDuration("P1048577DT59H59M60.00S"),
      DurationOverflowException);
  ASSERT_THROW(
      DayTimeDuration::parseXsdDayTimeDuration("-P1048577DT59H59M60.00S"),
      DurationOverflowException);
  ASSERT_THROW(
      DayTimeDuration::parseXsdDayTimeDuration("P1000000DT11346848H121M61.22S"),
      DurationOverflowException);
}

//______________________________________________________________________________
ad_utility::SlowRandomIntGenerator randomSign{0, 1};
ad_utility::SlowRandomIntGenerator randomDay{0, 1048575};
ad_utility::SlowRandomIntGenerator randomHour{0, 23};
ad_utility::SlowRandomIntGenerator randomMinute{0, 59};
ad_utility::RandomDoubleGenerator randomSecond{0, 59.9990};

DayTimeDuration getRandomDayTimeDuration() {
  return randomSign() == 0
             ? DayTimeDuration{DayTimeDuration::Type::Negative, randomDay(),
                               randomHour(), randomMinute(), randomSecond()}
             : DayTimeDuration{DayTimeDuration::Type::Positive, randomDay(),
                               randomHour(), randomMinute(), randomSecond()};
}

//______________________________________________________________________________
auto compareDurationLess = [](DayTimeDuration& d1,
                              DayTimeDuration& d2) -> bool {
  if (d1.isPositive() != d2.isPositive()) {
    return d1.isPositive() < d2.isPositive();
  }
  // We have to differentiate here w.r.t. the sign, if the duration d1 is signed
  // negative, larger days, hours,... mean that the duration becomes smaller.
  // With a positive (signed) duration, smaller number for days, hours,...
  // imply a smaller duration. (when comparing to duration d2).
  if (d1.isPositive()) {
    if (d1.getDays() != d2.getDays()) {
      return d1.getDays() < d2.getDays();
    }
    if (d1.getHours() != d2.getHours()) {
      return d1.getHours() < d2.getHours();
    }
    if (d1.getMinutes() != d1.getMinutes()) {
      return d1.getMinutes() < d2.getMinutes();
    }
    if (d1.getSeconds() != d2.getSeconds()) {
      return d1.getSeconds() < d2.getSeconds();
    }
  } else {
    if (d1.getDays() != d2.getDays()) {
      return d1.getDays() > d2.getDays();
    }
    if (d1.getHours() != d2.getHours()) {
      return d1.getHours() > d2.getHours();
    }
    if (d1.getMinutes() != d1.getMinutes()) {
      return d1.getMinutes() > d2.getMinutes();
    }
    if (d1.getSeconds() != d2.getSeconds()) {
      return d1.getSeconds() > d2.getSeconds();
    }
  }
  // When we reach this point here the values are equal.
  return false;
};

std::vector<DayTimeDuration> getRandomDayTimeDurations(size_t n) {
  std::vector<DayTimeDuration> durations;
  durations.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    durations.push_back(getRandomDayTimeDuration());
  }
  return durations;
}

void testSorting(std::vector<DayTimeDuration> durations) {
  auto durationsCopy = durations;
  std::sort(durations.begin(), durations.end());
  std::sort(durationsCopy.begin(), durationsCopy.end(), compareDurationLess);
  ASSERT_EQ(durations, durationsCopy);
}

//______________________________________________________________________________
TEST(DayTimeDuration, testOrderOnBytes) {
  auto durations = getRandomDayTimeDurations(1000);
  testSorting(durations);
}

//______________________________________________________________________________
TEST(DateYearOrDuration, testDayTimeDurationFromDate) {
  Date::TimeZone tz;
  std::vector<DateYearOrDuration> dateOrLargeYearDurations;
  std::vector<DayTimeDuration> dayTimeDurations;

  // add them in an descending order
  int min = -23;
  for (int max = 23; max > min; --max) {
    tz = max;
    DateYearOrDuration dateOrLargeYear =
        DateYearOrDuration(Date{2024, 7, 6, 14, 45, 2.00, tz});
    auto optDateOrLargeYear =
        DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear);
    ASSERT_TRUE(optDateOrLargeYear.has_value());
    dateOrLargeYearDurations.push_back(optDateOrLargeYear.value());
    dayTimeDurations.push_back(optDateOrLargeYear.value().getDayTimeDuration());
  }

  // test the sorting on DayTimeDurations which have been created from
  // Date::TimeZone values
  testSorting(dayTimeDurations);

  // Sort DayTimeDurations and DateOrLargeYears build from DayTimeDurations in
  // ascending order.
  std::sort(dayTimeDurations.begin(), dayTimeDurations.end());
  std::sort(dateOrLargeYearDurations.begin(), dateOrLargeYearDurations.end());

  // Check that sorting on the operation <=> yields the correct order w.r.t.
  // each other (on underlying timezone/hour value)
  for (size_t i = 0; i < dayTimeDurations.size(); i++) {
    int dayTimeDurationHour = dayTimeDurations[i].getHours();
    int dayTimeDurationHourFromDateOrLargeYear =
        dateOrLargeYearDurations[i].getDayTimeDuration().getHours();
    ASSERT_EQ(dayTimeDurationHour, dayTimeDurationHourFromDateOrLargeYear);
  }
}

TEST(DayTimeDuration, testFromTimezoneToString) {
  Date::TimeZone tz = 12;
  DateYearOrDuration dateOrLargeYear =
      DateYearOrDuration(Date{2024, 7, 6, 14, 45, 2.00, tz});
  DayTimeDuration duration =
      DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear)
          .value()
          .getDayTimeDuration();
  EXPECT_EQ(duration.toStringAndType().first, "PT12H");
  tz = 0;
  dateOrLargeYear = DateYearOrDuration(Date{2024, 7, 6, 14, 45, 2.00, tz});
  duration = DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear)
                 .value()
                 .getDayTimeDuration();
  EXPECT_EQ(duration.toStringAndType().first, "PT0S");
  tz = Date::TimeZoneZ{};
  dateOrLargeYear = DateYearOrDuration(Date{2024, 7, 6, 14, 45, 2.00, tz});
  duration = DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear)
                 .value()
                 .getDayTimeDuration();
  EXPECT_EQ(duration.toStringAndType().first, "PT0S");
  tz = -12;
  dateOrLargeYear = DateYearOrDuration(Date{2024, 7, 6, 14, 45, 2.00, tz});
  duration = DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear)
                 .value()
                 .getDayTimeDuration();
  EXPECT_EQ(duration.toStringAndType().first, "-PT12H");
  tz = Date::NoTimeZone{};
  dateOrLargeYear = DateYearOrDuration(Date{2024, 7, 6, 14, 45, 2.00, tz});
  EXPECT_FALSE(DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear)
                   .has_value());
  dateOrLargeYear = DateYearOrDuration(10000, DateYearOrDuration::Type::Year);
  EXPECT_FALSE(DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear)
                   .has_value());
  duration = DayTimeDuration::parseXsdDayTimeDuration("-P9999D");
  ASSERT_EQ(duration.toStringAndType().first, "-P9999D");
}
