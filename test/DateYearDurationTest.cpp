//  Copyright 2022-2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> (2022)
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de> (2024)

#include <gtest/gtest.h>

#include <bitset>

#include "./util/GTestHelpers.h"
#include "global/Constants.h"
#include "parser/TokenizerCtre.h"
#include "parser/TurtleParser.h"
#include "util/DateYearDuration.h"
#include "util/Random.h"

using ad_utility::source_location;

ad_utility::SlowRandomIntGenerator yearGenerator{-9999, 9999};
ad_utility::SlowRandomIntGenerator monthGenerator{1, 12};
ad_utility::SlowRandomIntGenerator dayGenerator{1, 31};
ad_utility::SlowRandomIntGenerator hourGenerator{0, 23};
ad_utility::SlowRandomIntGenerator minuteGenerator{0, 59};
ad_utility::RandomDoubleGenerator secondGenerator{0, 59.9999};
ad_utility::SlowRandomIntGenerator timeZoneGenerator{-23, 23};

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
    auto timeZone = timeZoneGenerator();

    Date date(year, month, day, hour, minute, second, timeZone);

    ASSERT_EQ(year, date.getYear());
    ASSERT_EQ(month, date.getMonth());
    ASSERT_EQ(day, date.getDay());
    ASSERT_EQ(hour, date.getHour());
    ASSERT_EQ(minute, date.getMinute());
    ASSERT_NEAR(second, date.getSecond(), 0.001);
    ASSERT_EQ(Date::TimeZone{timeZone}, date.getTimeZone());

    Date date2 = Date::fromBits(date.toBits());
    ASSERT_EQ(date, date2);

    ASSERT_EQ(year, date2.getYear());
    ASSERT_EQ(month, date2.getMonth());
    ASSERT_EQ(day, date2.getDay());
    ASSERT_EQ(hour, date2.getHour());
    ASSERT_EQ(minute, date2.getMinute());
    ASSERT_NEAR(second, date2.getSecond(), 0.002);
    ASSERT_EQ(Date::TimeZone{timeZone}, date2.getTimeZone());
  }
}

Date getRandomDate() {
  auto year = yearGenerator();
  auto month = monthGenerator();
  auto day = dayGenerator();
  auto hour = hourGenerator();
  auto minute = minuteGenerator();
  auto second = secondGenerator();
  auto timeZone = timeZoneGenerator();

  return {year, month, day, hour, minute, second, timeZone};
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
  ASSERT_NO_THROW(date.setMonth(0));
  ASSERT_NO_THROW(date.setMonth(12));
  dateCopy = date;
  ASSERT_THROW(date.setMonth(-1), DateOutOfRangeException);
  ASSERT_THROW(date.setMonth(13), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setDay(1));
  ASSERT_NO_THROW(date.setDay(0));
  ASSERT_NO_THROW(date.setDay(31));
  dateCopy = date;
  ASSERT_THROW(date.setDay(-1), DateOutOfRangeException);
  ASSERT_THROW(date.setDay(32), DateOutOfRangeException);
  ASSERT_EQ(date, dateCopy);

  ASSERT_NO_THROW(date.setHour(0));
  ASSERT_NO_THROW(date.setHour(-1));
  ASSERT_NO_THROW(date.setHour(23));
  dateCopy = date;
  ASSERT_THROW(date.setHour(-2), DateOutOfRangeException);
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

  ASSERT_NO_THROW(date.setTimeZone(-23));
  ASSERT_NO_THROW(date.setTimeZone(23));
  dateCopy = date;
  ASSERT_THROW(date.setTimeZone(-24), DateOutOfRangeException);
  ASSERT_THROW(date.setTimeZone(24), DateOutOfRangeException);
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
  return a.getTimeZoneAsInternalIntForTesting() <
         b.getTimeZoneAsInternalIntForTesting();
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
  auto randomTimeZone = timeZoneGenerator();
  dates = getRandomDates(100);
  for (auto& date : dates) {
    date.setYear(randomYear);
    date.setMonth(randomMonth);
    date.setDay(randomDay);
    date.setHour(randomHour);
    date.setMinute(randomMinute);
    date.setSecond(randomSecond);
    date.setTimeZone(randomTimeZone);
  }
  testSorting(dates);
}

namespace {
// Test that `parseFunction(input)` results in a `DateOrLargeYear` object that
// stores a `Date` with the given xsd `type` and the given `year, month, ... ,
// timeZone`. Also test that the result of this parsing, when converted back to
// a string, yields `input` again.
auto testDatetimeImpl(auto parseFunction, std::string_view input,
                      const char* type, int year, int month, int day, int hour,
                      int minute = 0, double second = 0.0,
                      Date::TimeZone timeZone = 0) {
  ASSERT_NO_THROW(std::invoke(parseFunction, input));
  DateYearOrDuration dateLarge = std::invoke(parseFunction, input);
  EXPECT_TRUE(dateLarge.isDate());
  EXPECT_EQ(dateLarge.getYear(), year);
  auto d = dateLarge.getDate();
  EXPECT_EQ(year, d.getYear());
  EXPECT_EQ(month, d.getMonth());
  EXPECT_EQ(day, d.getDay());
  EXPECT_EQ(hour, d.getHour());
  EXPECT_EQ(minute, d.getMinute());
  EXPECT_NEAR(second, d.getSecond(), 0.001);
  EXPECT_EQ(timeZone, d.getTimeZone());
  const auto& [literal, outputType] = d.toStringAndType();
  EXPECT_EQ(literal, input);
  EXPECT_STREQ(type, outputType);

  TripleComponent parsedAsTurtle =
      TurtleStringParser<TokenizerCtre>::parseTripleObject(
          absl::StrCat("\"", input, "\"^^<", type, ">"));
  auto optionalId = parsedAsTurtle.toValueIdIfNotString();
  ASSERT_TRUE(optionalId.has_value());
  ASSERT_TRUE(optionalId.value().getDatatype() == Datatype::Date);
  ASSERT_EQ(optionalId.value().getDate(), dateLarge);
}

// Specialization of `testDatetimeImpl` for parsing `xsd:dateTime`.
auto testDatetime(std::string_view input, int year, int month, int day,
                  int hour, int minute = 0, double second = 0.0,
                  Date::TimeZone timeZone = 0) {
  return testDatetimeImpl(DateYearOrDuration::parseXsdDatetime, input,
                          XSD_DATETIME_TYPE, year, month, day, hour, minute,
                          second, timeZone);
}

// Specialization of `testDatetimeImpl` for parsing `xsd:date`.
auto testDate(std::string_view input, int year, int month, int day,
              Date::TimeZone timeZone = 0,
              source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateYearOrDuration::parseXsdDate, input,
                          XSD_DATE_TYPE, year, month, day, -1, 0, 0, timeZone);
}

// Specialization of `testDatetimeImpl` for parsing `xsd:gYear`.
auto testYear(std::string_view input, int year, Date::TimeZone timeZone = 0,
              source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateYearOrDuration::parseGYear, input, XSD_GYEAR_TYPE,
                          year, 0, 0, -1, 0, 0, timeZone);
}

// Specialization of `testDatetimeImpl` for parsing `xsd:gYearMonth`.
auto testYearMonth(std::string_view input, int year, int month,
                   Date::TimeZone timeZone = 0,
                   source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateYearOrDuration::parseGYearMonth, input,
                          XSD_GYEARMONTH_TYPE, year, month, 0, -1, 0, 0,
                          timeZone);
}
}  // namespace

TEST(Date, parseDateTime) {
  testDatetime("2034-12-24T02:12:42.340+12:00", 2034, 12, 24, 2, 12, 42.34, 12);
  testDatetime("2034-12-24T02:12:42.342-03:00", 2034, 12, 24, 2, 12, 42.342,
               -3);
  testDatetime("2034-12-24T02:12:42.340Z", 2034, 12, 24, 2, 12, 42.34,
               Date::TimeZoneZ{});
  testDatetime("2034-12-24T02:12:42.341", 2034, 12, 24, 2, 12, 42.341,
               Date::NoTimeZone{});
  testDatetime("-2034-12-24T02:12:42.340", -2034, 12, 24, 2, 12, 42.34,
               Date::NoTimeZone{});
  testDatetime("-2034-12-24T02:12:42", -2034, 12, 24, 2, 12, 42.0,
               Date::NoTimeZone{});
  testDatetime("-2034-12-24T02:12:42Z", -2034, 12, 24, 2, 12, 42.0,
               Date::TimeZoneZ{});
}

TEST(Date, parseDate) {
  testDate("2034-12-24+12:00", 2034, 12, 24, 12);
  testDate("2034-12-24-03:00", 2034, 12, 24, -3);
  testDate("2034-12-24Z", 2034, 12, 24, Date::TimeZoneZ{});
  testDate("2034-12-24", 2034, 12, 24, Date::NoTimeZone{});
  testDate("-2034-12-24", -2034, 12, 24, Date::NoTimeZone{});
}

TEST(Date, parseYearMonth) {
  testYearMonth("2034-12+12:00", 2034, 12, 12);
  testYearMonth("2034-12-03:00", 2034, 12, -3);
  testYearMonth("2034-12Z", 2034, 12, Date::TimeZoneZ{});
  testYearMonth("2034-12", 2034, 12, Date::NoTimeZone{});
  testYearMonth("-2034-12", -2034, 12, Date::NoTimeZone{});
}

TEST(Date, parseYear) {
  testYear("2034+12:00", 2034, 12);
  testYear("2034-03:00", 2034, -3);
  testYear("2034Z", 2034, Date::TimeZoneZ{});
  testYear("2034", 2034, Date::NoTimeZone{});
  testYear("-2034", -2034, Date::NoTimeZone{});
}

TEST(Date, timeZoneWithMinutes) {
  auto d = DateYearOrDuration::parseGYear("2034+01:13");
  // `1:13` as a timeZone is silently rounded down to `1`.
  ASSERT_EQ(std::get<int>(d.getDate().getTimeZone()), 1);
}

namespace {
// Test that `parseFunction(input)` results in a `DateOrLargeYear` object that
// stores a large year with the given xsd `type` and the given `year. Also test
// that the result of this parsing, when converted back to a string, yields
// `input` again.
auto testLargeYearImpl(auto parseFunction, std::string_view input,
                       const char* type, DateYearOrDuration::Type typeEnum,
                       int64_t year,
                       std::optional<std::string> actualOutput = std::nullopt) {
  ASSERT_NO_THROW(std::invoke(parseFunction, input));
  DateYearOrDuration dateLarge = std::invoke(parseFunction, input);
  ASSERT_FALSE(dateLarge.isDate());
  EXPECT_EQ(dateLarge.getYear(), year);
  ASSERT_EQ(dateLarge.getType(), typeEnum);
  const auto& [literal, outputType] = dateLarge.toStringAndType();
  if (!actualOutput.has_value()) {
    EXPECT_EQ(literal, input);
  } else {
    EXPECT_EQ(literal, actualOutput.value());
  }
  EXPECT_STREQ(type, outputType);

  TripleComponent parsedAsTurtle =
      TurtleStringParser<TokenizerCtre>::parseTripleObject(
          absl::StrCat("\"", input, "\"^^<", type, ">"));
  auto optionalId = parsedAsTurtle.toValueIdIfNotString();
  ASSERT_TRUE(optionalId.has_value());
  ASSERT_TRUE(optionalId.value().getDatatype() == Datatype::Date);
  ASSERT_EQ(optionalId.value().getDate(), dateLarge);
}

// Specialization of `testLargeYearImpl` for `xsd:dateTime`
auto testLargeYearDatetime(
    std::string_view input, int64_t year,
    std::optional<std::string> actualOutput = std::nullopt) {
  return testLargeYearImpl(
      DateYearOrDuration::parseXsdDatetime, input, XSD_DATETIME_TYPE,
      DateYearOrDuration::Type::DateTime, year, std::move(actualOutput));
}

// Specialization of `testLargeYearImpl` for `xsd:date`
auto testLargeYearDate(std::string_view input, int64_t year,
                       std::optional<std::string> actualOutput = std::nullopt) {
  return testLargeYearImpl(DateYearOrDuration::parseXsdDate, input,
                           XSD_DATE_TYPE, DateYearOrDuration::Type::Date, year,
                           std::move(actualOutput));
}

// Specialization of `testLargeYearImpl` for `xsd:gYearMonth`
auto testLargeYearGYearMonth(
    std::string_view input, int64_t year,
    std::optional<std::string> actualOutput = std::nullopt) {
  return testLargeYearImpl(
      DateYearOrDuration::parseGYearMonth, input, XSD_GYEARMONTH_TYPE,
      DateYearOrDuration::Type::YearMonth, year, std::move(actualOutput));
}

// Specialization of `testLargeYearImpl` for `xsd:gYear`
auto testLargeYearGYear(
    std::string_view input, int64_t year,
    std::optional<std::string> actualOutput = std::nullopt) {
  return testLargeYearImpl(DateYearOrDuration::parseGYear, input,
                           XSD_GYEAR_TYPE, DateYearOrDuration::Type::Year, year,
                           std::move(actualOutput));
}
}  // namespace

TEST(Date, parseLargeYear) {
  testLargeYearGYear("2039481726", 2039481726);
  testLargeYearGYear("-2039481726", -2039481726);

  testLargeYearGYearMonth("2039481726-01", 2039481726);
  testLargeYearGYearMonth("-2039481726-01", -2039481726);

  testLargeYearDate("2039481726-01-01", 2039481726);
  testLargeYearDate("-2039481726-01-01", -2039481726);

  testLargeYearDatetime("2039481726-01-01T00:00:00", 2039481726);
  testLargeYearDatetime("-2039481726-01-01T00:00:00", -2039481726);
}

TEST(Date, parseLargeYearCornerCases) {
  // If the date is too low or too high, a warning is printed and the year is
  // clipped to the min or max value that is representable.
  testLargeYearGYear(std::to_string(std::numeric_limits<int64_t>::max()),
                     DateYearOrDuration::maxYear,
                     std::to_string(DateYearOrDuration::maxYear));
  testLargeYearGYear(std::to_string(std::numeric_limits<int64_t>::min()),
                     DateYearOrDuration::minYear,
                     std::to_string(DateYearOrDuration::minYear));

  // When the year has more than four digits, then the information about the
  // date and time is lost.
  testLargeYearGYearMonth("2039481726-03", 2039481726, "2039481726-01");
  testLargeYearGYearMonth("-2039481726-07", -2039481726, "-2039481726-01");

  testLargeYearDate("2039481726-03-01", 2039481726, "2039481726-01-01");
  testLargeYearDate("-2039481726-02-05", -2039481726, "-2039481726-01-01");

  testLargeYearDatetime("2039481726-01-01T12:00:00", 2039481726,
                        "2039481726-01-01T00:00:00");
  testLargeYearDatetime("2039481726-01-01T00:13:00", 2039481726,
                        "2039481726-01-01T00:00:00");
  testLargeYearDatetime("-2039481726-01-01T00:00:14", -2039481726,
                        "-2039481726-01-01T00:00:00");
}

TEST(Date, parseErrors) {
  using D = DateYearOrDuration;
  using E = DateParseException;
  ASSERT_THROW(D::parseGYear("1994-12"), E);
  ASSERT_THROW(D::parseGYear("Kartoffelsalat"), E);
  ASSERT_THROW(D::parseGYearMonth("1994"), E);
  ASSERT_THROW(D::parseGYearMonth("Kartoffelsalat"), E);
  ASSERT_THROW(D::parseXsdDate("1994-##-##"), E);
  ASSERT_THROW(D::parseXsdDate("Kartoffelsalat"), E);
  ASSERT_THROW(D::parseXsdDatetime("1994-12-13"), E);
  ASSERT_THROW(D::parseXsdDate("Kartoffelsalat"), E);
}

TEST(DateYearOrDuration, AssertionFailures) {
  // These values are out of range.
  ASSERT_ANY_THROW(DateYearOrDuration(std::numeric_limits<int64_t>::min(),
                                      DateYearOrDuration::Type::Year));
  ASSERT_ANY_THROW(DateYearOrDuration(std::numeric_limits<int64_t>::max(),
                                      DateYearOrDuration::Type::Year));

  // These years have to be stored as a `Date`, not as a large year.
  ASSERT_ANY_THROW(DateYearOrDuration(-9998, DateYearOrDuration::Type::Year));
  ASSERT_ANY_THROW(DateYearOrDuration(2021, DateYearOrDuration::Type::Year));

  // Calling `getDate` on an object that is stored as a large year.
  DateYearOrDuration d{123456, DateYearOrDuration::Type::Year};
  ASSERT_ANY_THROW(d.getDate());
}

TEST(DateYearOrDuration, Order) {
  DateYearOrDuration d1{-12345, DateYearOrDuration::Type::Year};
  DateYearOrDuration d2{Date{2022, 7, 16}};
  DateYearOrDuration d3{12345, DateYearOrDuration::Type::Year};
  DateYearOrDuration d4{
      DayTimeDuration{DayTimeDuration::Type::Positive, 0, 23, 23, 62.44}};
  DateYearOrDuration d5{
      DayTimeDuration{DayTimeDuration::Type::Positive, 1, 24, 23, 62.44}};
  DateYearOrDuration d6{
      DayTimeDuration{DayTimeDuration::Type::Negative, 1, 24, 23, 62.44}};
  DateYearOrDuration d7{
      DayTimeDuration{DayTimeDuration::Type::Negative, 1, 25, 23, 62.44}};

  ASSERT_EQ(d1, d1);
  ASSERT_EQ(d2, d2);
  ASSERT_EQ(d3, d3);
  ASSERT_EQ(d4, d4);
  ASSERT_EQ(d5, d5);
  ASSERT_EQ(d6, d6);
  ASSERT_EQ(d7, d7);
  ASSERT_LT(d1, d2);
  ASSERT_LT(d2, d3);
  ASSERT_LT(d1, d3);
  ASSERT_LT(d4, d5);
  ASSERT_LT(d6, d5);
  ASSERT_LT(d7, d4);
  ASSERT_LT(d7, d6);
}

// SECTION TEST DURATION
//______________________________________________________________________________
TEST(DayTimeDuration, sizeInitDayTimeDuration) {
  ASSERT_EQ(sizeof(DayTimeDuration), 8);
  ASSERT_EQ(DayTimeDuration::numUnusedBits, 16);
  DayTimeDuration defaultDuration{};
  ASSERT_TRUE(defaultDuration.isPositive());
  EXPECT_NEAR(defaultDuration.getSeconds(), 0.00, 0.001);
  ASSERT_EQ(defaultDuration.getMinutes(), 0);
  ASSERT_EQ(defaultDuration.getHours(), 0);
  ASSERT_EQ(defaultDuration.getDays(), 0);
}

//______________________________________________________________________________
TEST(DayTimeDuration, setAndGetValues) {
  ad_utility::SlowRandomIntGenerator randomDay{0, 1048575};
  ad_utility::SlowRandomIntGenerator randomHour{0, 23};
  ad_utility::SlowRandomIntGenerator randomMinute{0, 59};
  ad_utility::RandomDoubleGenerator randomSecond{0, 59.9999};
  DayTimeDuration::Type positive = DayTimeDuration::Type::Positive;
  DayTimeDuration::Type negative = DayTimeDuration::Type::Negative;
  size_t i = 0, max = 3333;
  while (i < max) {
    auto seconds = randomSecond();
    auto minutes = randomMinute();
    auto days = randomDay();
    auto hours = randomHour();
    DayTimeDuration duration1{positive, days, hours, minutes, seconds};
    DayTimeDuration duration2{negative, days, hours, minutes, seconds};
    EXPECT_NEAR(duration1.getSeconds(), seconds, 0.001);
    EXPECT_NEAR(duration2.getSeconds(), seconds, 0.001);
    ASSERT_TRUE(duration1.isPositive());
    ASSERT_FALSE(duration2.isPositive());
    ASSERT_EQ(duration1.getDays(), days);
    ASSERT_EQ(duration2.getDays(), days);
    ASSERT_EQ(duration1.getHours(), hours);
    ASSERT_EQ(duration2.getHours(), hours);
    ASSERT_EQ(duration1.getMinutes(), minutes);
    ASSERT_EQ(duration2.getMinutes(), minutes);

    // Basic comparison
    // duration1 is set to positive, duration2 to negative, this should
    // always hold.
    ASSERT_TRUE(duration1 == duration1);
    ASSERT_TRUE(duration2 == duration2);
    ASSERT_TRUE(duration2 < duration1);
    ASSERT_FALSE(duration2 == duration1);
    i++;
  }
}

//______________________________________________________________________________
TEST(DayTimeDuration, checkParseAndGetStringForSpecialValues) {
  DayTimeDuration duration0 =
      DayTimeDuration::parseXsdDayTimeDuration("P0DT0H0M0S");
  EXPECT_EQ(duration0.toStringAndType().first, "PT0S");
  EXPECT_STREQ(duration0.toStringAndType().second, XSD_DAYTIME_DURATION_TYPE);
  duration0 = DayTimeDuration::parseXsdDayTimeDuration("PT0H0M0S");
  EXPECT_EQ(duration0.toStringAndType().first, "PT0S");
  EXPECT_STREQ(duration0.toStringAndType().second, XSD_DAYTIME_DURATION_TYPE);
  duration0 = DayTimeDuration::parseXsdDayTimeDuration("PT0H0.00S");
  EXPECT_EQ(duration0.toStringAndType().first, "PT0S");
  EXPECT_STREQ(duration0.toStringAndType().second, XSD_DAYTIME_DURATION_TYPE);
  duration0 = DayTimeDuration::parseXsdDayTimeDuration("PT0S");
  EXPECT_EQ(duration0.toStringAndType().first, "PT0S");
  EXPECT_STREQ(duration0.toStringAndType().second, XSD_DAYTIME_DURATION_TYPE);

  // Test w.r.t. maximum values where we don't expect given the current bounds a
  // normalization effect yet.
  DayTimeDuration dMax =
      DayTimeDuration::parseXsdDayTimeDuration("P1048575DT23H59M59.999S");
  ASSERT_EQ(dMax.toStringAndType().first, "P1048575DT23H59M59.999S");
  EXPECT_STREQ(dMax.toStringAndType().second, XSD_DAYTIME_DURATION_TYPE);
  DayTimeDuration dMin =
      DayTimeDuration::parseXsdDayTimeDuration("-P1048575DT23H59M59.999S");
  ASSERT_EQ(dMin.toStringAndType().first, "-P1048575DT23H59M59.999S");
  EXPECT_STREQ(dMin.toStringAndType().second, XSD_DAYTIME_DURATION_TYPE);

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
TEST(DayTimeDuration, checkToAndFromBits) {
  auto d1 = DayTimeDuration(DayTimeDuration::Type::Positive, 1, 23, 23, 59.99);
  auto bits = d1.toBits();
  d1 = DayTimeDuration::fromBits(bits);
  DayTimeDuration::DurationValue dv = d1.getValues();
  ASSERT_EQ(dv.days, 1);
  ASSERT_EQ(dv.hours, 23);
  ASSERT_EQ(dv.minutes, 23);
  EXPECT_NEAR(dv.seconds, 59.99, 0.001);

  auto d2 =
      DayTimeDuration(DayTimeDuration::Type::Negative, 1048574, 3, 0, 0.99);
  bits = d2.toBits();
  d2 = DayTimeDuration::fromBits(bits);
  dv = d2.getValues();
  ASSERT_EQ(dv.days, 1048574);
  ASSERT_EQ(dv.hours, 3);
  ASSERT_EQ(dv.minutes, 0);
  EXPECT_NEAR(dv.seconds, 0.99, 0.001);
}

//______________________________________________________________________________
TEST(DayTimeDuration, DurationOverflowException) {
  try {
    [[maybe_unused]] auto d1 =
        DayTimeDuration(DayTimeDuration::Type::Positive, 643917423, 4, 7, 1);
    FAIL() << "DurationOverflowException was expected.";
  } catch (const DurationOverflowException& e) {
    EXPECT_STREQ(e.what(),
                 "Overflow exception raised by DayTimeDuration, please provide "
                 "smaller values for xsd:dayTimeDuration.");
  }
}

//______________________________________________________________________________
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
  ad_utility::SlowRandomIntGenerator randomDay{1000000, maxDays};
  ad_utility::SlowRandomIntGenerator randomHour{22, 1000000};
  ad_utility::SlowRandomIntGenerator randomMinute{55, 1000000};
  ad_utility::RandomDoubleGenerator randomSecond{58.999, 99999.999};

  size_t i = 0, max = 1024;
  while (i < max) {
    int randDay = randomDay();
    int randHour = randomHour();
    int randMinute = randomMinute();
    int randSeconds = randomSecond();

    DayTimeDuration::DurationValue dv1 =
        toAndFromMilliseconds(randDay, randHour, randMinute, randSeconds);

    if (dv1.days > maxDays) {
      ASSERT_THROW(DayTimeDuration(DayTimeDuration::Type::Positive, randDay,
                                   randHour, randMinute, randSeconds),
                   DurationOverflowException);
    } else {
      const auto& dv2 =
          DayTimeDuration(DayTimeDuration::Type::Positive, randDay, randHour,
                          randMinute, randSeconds)
              .getValues();
      ASSERT_EQ(dv1.days, dv2.days);
      ASSERT_EQ(dv1.hours, dv2.hours);
      ASSERT_EQ(dv1.minutes, dv2.minutes);
      EXPECT_NEAR(dv1.seconds, dv2.seconds, 0.001);
    }
    i++;
  }
}

//______________________________________________________________________________
TEST(DayTimeDuration, checkParseAndGetString) {
  // Set the lower limit to 1, 0's will be mostly (some special cases are
  // tested directly above) ignored when constructing a xsd:dayTimeDuration
  // string.
  ad_utility::SlowRandomIntGenerator randomDay{1, 1048575};
  ad_utility::SlowRandomIntGenerator randomHour{1, 23};
  ad_utility::SlowRandomIntGenerator randomMinute{1, 59};
  ad_utility::RandomDoubleGenerator randomSecond{1, 59.9999};

  static constexpr ctll::fixed_string dayTimePattern =
      "(?<negation>-?)P((?<days>\\d+)D)?(T((?<hours>\\d+)H)?((?<"
      "minutes>\\d+)M)?((?<seconds>\\d+(\\.\\d+)?)S)?)?";

  // Helper which adjusts the random seconds (decimal places) before string
  // concatenation.
  const auto handleSecondsStr = [](double seconds) {
    std::ostringstream strStream;
    strStream << std::fixed << std::setprecision(5) << seconds;
    return strStream.str();
  };

  size_t i = 0, max = 256;
  while (i < max) {
    int randDay = randomDay(), randHour = randomHour(),
        randMinute = randomMinute();
    double randSec = randomSecond();
    auto xsdDuration =
        absl::StrCat("P", randDay, "DT", randHour, "H", randMinute, "M",
                     handleSecondsStr(randSec), "S");
    DayTimeDuration d = DayTimeDuration::parseXsdDayTimeDuration(xsdDuration);
    // Given that the seconds value is object to a rounding procedure, we have
    // to test over matching, and thus test with EXPECT_NEAR.
    auto match = ctre::match<dayTimePattern>(d.toStringAndType().first);
    EXPECT_EQ(match.get<"days">().to_number(), randDay);
    EXPECT_EQ(match.get<"hours">().to_number(), randHour);
    EXPECT_EQ(match.get<"minutes">().to_number(), randMinute);
    EXPECT_NEAR(std::strtod(match.get<"seconds">().data(), nullptr), randSec,
                0.001);
    i++;
  }

  i = 0, max = 256;
  while (i < max) {
    auto randSec = randomSecond();
    auto xsdDuration =
        absl::StrCat("-P0DT0H0M", handleSecondsStr(randSec), "S");
    DayTimeDuration d = DayTimeDuration::parseXsdDayTimeDuration(xsdDuration);
    auto match = ctre::match<dayTimePattern>(d.toStringAndType().first);
    ASSERT_TRUE(match);
    EXPECT_NEAR(std::strtod(match.get<"seconds">().data(), nullptr), randSec,
                0.001);
    i++;
  }

  i = 0, max = 256;
  while (i < max) {
    auto randDay = randomDay();
    auto xsdDuration = absl::StrCat("-P", randDay, "DT0M0S");
    auto xsdRes = absl::StrCat("-P", randDay, "D");
    DayTimeDuration d = DayTimeDuration::parseXsdDayTimeDuration(xsdDuration);
    ASSERT_EQ(d.toStringAndType().first, xsdRes);
    i++;
  }

  i = 0, max = 256;
  while (i < max) {
    auto randDay = randomDay();
    auto randHour = randomHour();
    auto xsdDuration = absl::StrCat("-P", randDay, "DT", randHour, "H0M3.0S");
    auto xsdRes = absl::StrCat("-P", randDay, "DT", randHour, "H3S");
    DayTimeDuration d = DayTimeDuration::parseXsdDayTimeDuration(xsdDuration);
    ASSERT_EQ(d.toStringAndType().first, xsdRes);
    i++;
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
ad_utility::RandomDoubleGenerator randomSecond{0, 59.9999};

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
  int min = -24, max = 23;
  while (max > min) {
    tz = max;
    DateYearOrDuration dateOrLargeYear =
        DateYearOrDuration(Date{2024, 7, 6, 14, 45, 2.00, tz});
    auto optDateOrLargeYear =
        DateYearOrDuration::xsdDayTimeDurationFromDate(dateOrLargeYear);
    ASSERT_TRUE(optDateOrLargeYear.has_value());
    dateOrLargeYearDurations.push_back(optDateOrLargeYear.value());
    dayTimeDurations.push_back(optDateOrLargeYear.value().getDayTimeDuration());
    max--;
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
