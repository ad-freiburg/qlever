//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>

#include "./util/GTestHelpers.h"
#include "util/Date.h"
#include "util/Random.h"

using ad_utility::source_location;

SlowRandomIntGenerator yearGenerator{-9999, 9999};
SlowRandomIntGenerator monthGenerator{1, 12};
SlowRandomIntGenerator dayGenerator{1, 31};
SlowRandomIntGenerator hourGenerator{0, 23};
SlowRandomIntGenerator minuteGenerator{0, 59};
RandomDoubleGenerator secondGenerator{0, 59.9999};
SlowRandomIntGenerator timezoneGenerator{-23, 23};

TEST(Date, Size) {
  ASSERT_EQ(sizeof(Date), 8);
  ASSERT_EQ(7, Date::numUnusedBits);
}

TEST(Date, SetAndExtract) {
  for (int i = 0; i < 3000; ++i) {
    auto year = yearGenerator();
    auto month = monthGenerator();
    auto day = dayGenerator();
    auto hour = hourGenerator();
    auto minute = minuteGenerator();
    auto second = secondGenerator();
    auto timezone = timezoneGenerator();

    Date date(year, month, day, hour, minute, second, timezone);

    ASSERT_EQ(year, date.getYear());
    ASSERT_EQ(month, date.getMonth());
    ASSERT_EQ(day, date.getDay());
    ASSERT_EQ(hour, date.getHour());
    ASSERT_EQ(minute, date.getMinute());
    ASSERT_NEAR(second, date.getSecond(), 0.001);
    ASSERT_EQ(Date::Timezone{timezone}, date.getTimezone());

    Date date2 = Date::fromBits(date.toBits());
    ASSERT_EQ(date, date2);

    ASSERT_EQ(year, date2.getYear());
    ASSERT_EQ(month, date2.getMonth());
    ASSERT_EQ(day, date2.getDay());
    ASSERT_EQ(hour, date2.getHour());
    ASSERT_EQ(minute, date2.getMinute());
    ASSERT_NEAR(second, date2.getSecond(), 0.002);
    ASSERT_EQ(Date::Timezone{timezone}, date2.getTimezone());
  }
}

Date getRandomDate() {
  auto year = yearGenerator();
  auto month = monthGenerator();
  auto day = dayGenerator();
  auto hour = hourGenerator();
  auto minute = minuteGenerator();
  auto second = secondGenerator();
  auto timezone = timezoneGenerator();

  return {year, month, day, hour, minute, second, timezone};
}

TEST(Date, RangeChecks) {
  Date date = getRandomDate();
  ASSERT_NO_THROW(date.setYear(-9999));
  ASSERT_NO_THROW(date.setYear(9999));
  Date dateCopy = date;
  ASSERT_THROW(date.setYear(-10000), DateOutOfRangeException);
  ASSERT_THROW(date.setYear(10000), DateOutOfRangeException);
  // Strong exception guarantee: If the setters throw an exception, then the
  // `Date` remains unchanged.
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setMonth(1));
  ASSERT_NO_THROW(date.setMonth(12));
  dateCopy = date;
  ASSERT_THROW(date.setMonth(0), DateOutOfRangeException);
  ASSERT_THROW(date.setMonth(13), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setDay(1));
  ASSERT_NO_THROW(date.setDay(31));
  dateCopy = date;
  ASSERT_THROW(date.setDay(0), DateOutOfRangeException);
  ASSERT_THROW(date.setDay(32), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setHour(0));
  ASSERT_NO_THROW(date.setHour(23));
  dateCopy = date;
  ASSERT_THROW(date.setHour(-1), DateOutOfRangeException);
  ASSERT_THROW(date.setHour(24), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setMinute(0));
  ASSERT_NO_THROW(date.setMinute(59));
  dateCopy = date;
  ASSERT_THROW(date.setMinute(-1), DateOutOfRangeException);
  ASSERT_THROW(date.setMinute(60), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setSecond(0.0));
  ASSERT_NO_THROW(date.setSecond(59.999));
  dateCopy = date;
  ASSERT_THROW(date.setSecond(-0.1), DateOutOfRangeException);
  ASSERT_THROW(date.setSecond(60.0), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setTimezone(-23));
  ASSERT_NO_THROW(date.setTimezone(23));
  dateCopy = date;
  ASSERT_THROW(date.setTimezone(-24), DateOutOfRangeException);
  ASSERT_THROW(date.setTimezone(24), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);
}

auto dateLessComparator = [](Date a, Date b) -> bool {
  if (a.getYear() != b.getYear()) {
    return a.getYear() < b.getYear();
  }
  if (a.getMonth() != b.getMonth()) {
    return a.getMonth() < b.getMonth();
  }
  if (a.getDay() != b.getDay()) {
    return a.getDay() < b.getDay();
  }
  if (a.getHour() != b.getHour()) {
    return a.getHour() < b.getHour();
  }
  if (a.getMinute() != b.getMinute()) {
    return a.getMinute() < b.getMinute();
  }
  if (a.getSecond() != b.getSecond()) {
    return a.getSecond() < b.getSecond();
  }
  return a.getTimezoneAsInternalIntForTesting() <
         b.getTimezoneAsInternalIntForTesting();
};

std::vector<Date> getRandomDates(size_t n) {
  std::vector<Date> dates;
  dates.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    dates.push_back(getRandomDate());
  }
  return dates;
}

void testSorting(std::vector<Date> dates) {
  auto datesCopy = dates;
  std::sort(dates.begin(), dates.end());
  std::sort(datesCopy.begin(), datesCopy.end(), dateLessComparator);
  ASSERT_EQ(dates, datesCopy);
}

TEST(Date, OrderRandomValues) {
  auto dates = getRandomDates(100);
  testSorting(std::move(dates));

  auto randomYear = yearGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
  }
  testSorting(dates);

  randomYear = yearGenerator();
  auto randomMonth = monthGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
    date.setMonth(randomMonth);
  }
  testSorting(dates);

  randomYear = yearGenerator();
  randomMonth = monthGenerator();
  auto randomDay = dayGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
    date.setMonth(randomMonth);
    date.setDay(randomDay);
  }
  testSorting(dates);

  randomYear = yearGenerator();
  randomMonth = monthGenerator();
  randomDay = dayGenerator();
  auto randomHour = hourGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
    date.setMonth(randomMonth);
    date.setDay(randomDay);
    date.setHour(randomHour);
  }
  testSorting(dates);

  randomYear = yearGenerator();
  randomMonth = monthGenerator();
  randomDay = dayGenerator();
  randomHour = hourGenerator();
  auto randomMinute = minuteGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
    date.setMonth(randomMonth);
    date.setDay(randomDay);
    date.setHour(randomHour);
    date.setMinute(randomMinute);
  }
  testSorting(dates);

  randomYear = yearGenerator();
  randomMonth = monthGenerator();
  randomDay = dayGenerator();
  randomHour = hourGenerator();
  randomMinute = minuteGenerator();
  auto randomSecond = secondGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
    date.setMonth(randomMonth);
    date.setDay(randomDay);
    date.setHour(randomHour);
    date.setMinute(randomMinute);
    date.setSecond(randomSecond);
  }
  testSorting(dates);

  randomYear = yearGenerator();
  randomMonth = monthGenerator();
  randomDay = dayGenerator();
  randomHour = hourGenerator();
  randomMinute = minuteGenerator();
  randomSecond = secondGenerator();
  auto randomTimezone = timezoneGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
    date.setMonth(randomMonth);
    date.setDay(randomDay);
    date.setHour(randomHour);
    date.setMinute(randomMinute);
    date.setSecond(randomSecond);
    date.setTimezone(randomTimezone);
  }
  testSorting(dates);
}

namespace {
    auto testDatetimeImpl(auto parseFunction, std::string_view input, int year, int month, int day,
                      int hour, int minute = 0, double second = 0.0,
                      Date::Timezone timezone = 0) {
        ASSERT_NO_THROW(std::invoke(parseFunction, input));
        DateOrLargeYear dateLarge = std::invoke(parseFunction, input);
        EXPECT_TRUE(dateLarge.isDate());
        EXPECT_EQ(dateLarge.getYear(), year);
        auto d = dateLarge.getDate();
        EXPECT_EQ(year, d.getYear());
        EXPECT_EQ(month, d.getMonth());
        EXPECT_EQ(day, d.getDay());
        EXPECT_EQ(hour, d.getHour());
        EXPECT_EQ(minute, d.getMinute());
        EXPECT_NEAR(second, d.getSecond(), 0.001);
        EXPECT_EQ(timezone, d.getTimezone());
    }
    auto testDatetime(std::string_view input, int year, int month, int day,
                  int hour, int minute = 0, double second = 0.0,
                  Date::Timezone timezone = 0) {
        return testDatetimeImpl(DateOrLargeYear::parseXsdDatetime, input, year, month, day, hour, minute, second, timezone);
}

auto testDate(std::string_view input, int year, int month, int day,
              Date::Timezone timezone = 0,
              source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateOrLargeYear::parseXsdDate, input, year, month, day, 0, 0, 0, timezone);
}

auto testYear(std::string_view input, int year, Date::Timezone timezone = 0,
              source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateOrLargeYear::parseGYear, input, year, 1, 1, 0, 0, 0, timezone);
}

auto testYearMonth(std::string_view input, int year, int month,
                   Date::Timezone timezone = 0,
                   source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateOrLargeYear::parseGYearMonth, input, year, month, 1, 0, 0, 0, timezone);
}
}  // namespace

TEST(Date, parseDateTime) {
  testDatetime("2034-12-24T02:12:42.34+12:00", 2034, 12, 24, 2, 12, 42.34, 12);
  testDatetime("2034-12-24T02:12:42.34-03:00", 2034, 12, 24, 2, 12, 42.34, -3);
  testDatetime("2034-12-24T02:12:42.34Z", 2034, 12, 24, 2, 12, 42.34,
               Date::TimezoneZ{});
  testDatetime("2034-12-24T02:12:42.34", 2034, 12, 24, 2, 12, 42.34,
               Date::NoTimezone{});
  testDatetime("-2034-12-24T02:12:42.34", -2034, 12, 24, 2, 12, 42.34,
               Date::NoTimezone{});
}

TEST(Date, parseDate) {
  testDate("2034-12-24+12:00", 2034, 12, 24, 12);
  testDate("2034-12-24-03:00", 2034, 12, 24, -3);
  testDate("2034-12-24Z", 2034, 12, 24, Date::TimezoneZ{});
  testDate("2034-12-24", 2034, 12, 24, Date::NoTimezone{});
  testDate("-2034-12-24", -2034, 12, 24, Date::NoTimezone{});
}

TEST(Date, parseYearMonth) {
  testYearMonth("2034-12+12:00", 2034, 12, 12);
  testYearMonth("2034-12-03:00", 2034, 12, -3);
  testYearMonth("2034-12Z", 2034, 12, Date::TimezoneZ{});
  testYearMonth("2034-12", 2034, 12, Date::NoTimezone{});
  testYearMonth("-2034-12", -2034, 12, Date::NoTimezone{});
}

TEST(Date, parseYear) {
  testYear("2034+12:00", 2034, 12);
  testYear("2034-03:00", 2034, -3);
  testYear("2034Z", 2034, Date::TimezoneZ{});
  testYear("2034", 2034, Date::NoTimezone{});
  testYear("-2034", -2034, Date::NoTimezone{});
}
