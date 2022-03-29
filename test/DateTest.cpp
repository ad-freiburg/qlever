//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>

#include "../src/util/Date.h"
#include "../src/util/Random.h"

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
    auto seconds = secondGenerator();
    auto timezone = timezoneGenerator();

    Date date(year, month, day, hour, minute, seconds, timezone);

    ASSERT_EQ(year, date.year());
    ASSERT_EQ(month, date.month());
    ASSERT_EQ(day, date.day());
    ASSERT_EQ(hour, date.hour());
    ASSERT_EQ(minute, date.minute());
    ASSERT_NEAR(seconds, date.second(), 0.002);
    ASSERT_EQ(timezone, date.timezone());

    Date date2 = Date::fromBits(date.toBits());
    ASSERT_EQ(date, date2);

    ASSERT_EQ(year, date2.year());
    ASSERT_EQ(month, date2.month());
    ASSERT_EQ(day, date2.day());
    ASSERT_EQ(hour, date2.hour());
    ASSERT_EQ(minute, date2.minute());
    ASSERT_NEAR(seconds, date2.second(), 0.002);
    ASSERT_EQ(timezone, date2.timezone());
  }
}

Date getRandomDate() {
  auto year = yearGenerator();
  auto month = monthGenerator();
  auto day = dayGenerator();
  auto hour = hourGenerator();
  auto minute = minuteGenerator();
  auto seconds = secondGenerator();
  auto timezone = timezoneGenerator();

  return {year, month, day, hour, minute, seconds, timezone};
}

TEST(Date, RangeChecks) {
  Date date = getRandomDate();
  date.setYear(-9999);
  date.setYear(9999);
  Date dateCopy = date;
  ASSERT_THROW(date.setYear(-10000), DateOutOfRangeException);
  ASSERT_THROW(date.setYear(10000), DateOutOfRangeException);
  // Strong exception guarantee: If the setters throw an exception, then the
  // `Date` remains unchanged.
  ASSERT_EQ(date, dateCopy);

  date.setMonth(1);
  date.setMonth(12);
  dateCopy = date;
  ASSERT_THROW(date.setMonth(0), DateOutOfRangeException);
  ASSERT_THROW(date.setMonth(13), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  date.setDay(1);
  date.setDay(31);
  dateCopy = date;
  ASSERT_THROW(date.setDay(0), DateOutOfRangeException);
  ASSERT_THROW(date.setDay(32), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  date.setHour(0);
  date.setHour(23);
  dateCopy = date;
  ASSERT_THROW(date.setHour(-1), DateOutOfRangeException);
  ASSERT_THROW(date.setHour(24), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  date.setMinute(0);
  date.setMinute(59);
  dateCopy = date;
  ASSERT_THROW(date.setMinute(-1), DateOutOfRangeException);
  ASSERT_THROW(date.setMinute(60), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  date.setSeconds(0.0);
  date.setSeconds(59.999);
  dateCopy = date;
  ASSERT_THROW(date.setSeconds(-0.1), DateOutOfRangeException);
  ASSERT_THROW(date.setSeconds(60.0), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  date.setTimezone(-23);
  date.setTimezone(23);
  dateCopy = date;
  ASSERT_THROW(date.setTimezone(-24), DateOutOfRangeException);
  ASSERT_THROW(date.setTimezone(24), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);
}

auto dateComparator = [](Date a, Date b) -> bool {
  if (a.year() != b.year()) {
    return a.year() < b.year();
  }
  if (a.month() != b.month()) {
    return a.month() < b.month();
  }
  if (a.day() != b.day()) {
    return a.day() < b.day();
  }
  if (a.hour() != b.hour()) {
    return a.hour() < b.hour();
  }
  if (a.minute() != b.minute()) {
    return a.minute() < b.minute();
  }
  if (a.second() != b.second()) {
    return a.second() < b.second();
  }
  return a.timezone() < b.timezone();
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
  std::sort(datesCopy.begin(), datesCopy.end(), dateComparator);
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
    date.setSeconds(randomSecond);
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
    date.setSeconds(randomSecond);
    date.setTimezone(randomTimezone);
  }
  testSorting(dates);
}
