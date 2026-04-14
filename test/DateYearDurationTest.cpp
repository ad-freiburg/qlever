//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>

#include "./util/GTestHelpers.h"
#include "global/Constants.h"
#include "parser/RdfParser.h"
#include "parser/TokenizerCtre.h"
#include "util/DateYearDuration.h"
#include "util/Random.h"

using ad_utility::source_location;

namespace {

ad_utility::SlowRandomIntGenerator yearGenerator{-9999, 9999};
ad_utility::SlowRandomIntGenerator monthGenerator{1, 12};
ad_utility::SlowRandomIntGenerator dayGenerator{1, 31};
ad_utility::SlowRandomIntGenerator hourGenerator{0, 23};
ad_utility::SlowRandomIntGenerator minuteGenerator{0, 59};
ad_utility::RandomDoubleGenerator secondGenerator{0, 59.9999};
ad_utility::SlowRandomIntGenerator timeZoneGenerator{-23, 23};

auto encodedIriManager = []() -> const EncodedIriManager* {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
};
}  // namespace

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

namespace {
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
}  // namespace

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

namespace {
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

// This function is used to test the subtraction/addition operation of
// `DateYearOrDuration`objects.
void testOperation(DateYearOrDuration expected,
                   std::optional<DateYearOrDuration> result) {
  ASSERT_TRUE(result);
  EXPECT_TRUE(result.value().isDayTimeDuration());
  EXPECT_EQ(expected, result.value());
}
}  // namespace

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
template <typename F>
auto testDatetimeImpl(F parseFunction, std::string_view input, const char* type,
                      int year, int month, int day, int hour, int minute = 0,
                      double second = 0.0, Date::TimeZone timeZone = 0) {
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
      RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
          absl::StrCat("\"", input, "\"^^<", type, ">"));
  auto optionalId = parsedAsTurtle.toValueIdIfNotString(encodedIriManager());
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
              source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateYearOrDuration::parseXsdDate, input,
                          XSD_DATE_TYPE, year, month, day, -1, 0, 0, timeZone);
}

// Specialization of `testDatetimeImpl` for parsing `xsd:gYear`.
auto testYear(std::string_view input, int year, Date::TimeZone timeZone = 0,
              source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  return testDatetimeImpl(DateYearOrDuration::parseGYear, input, XSD_GYEAR_TYPE,
                          year, 0, 0, -1, 0, 0, timeZone);
}

// Specialization of `testDatetimeImpl` for parsing `xsd:gYearMonth`.
auto testYearMonth(std::string_view input, int year, int month,
                   Date::TimeZone timeZone = 0,
                   source_location l = AD_CURRENT_SOURCE_LOC()) {
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
  testDatetime("-0100-12-31T02:12:42Z", -100, 12, 31, 2, 12, 42.0,
               Date::TimeZoneZ{});
}

TEST(Date, parseDate) {
  testDate("2034-12-24+12:00", 2034, 12, 24, 12);
  testDate("2034-12-24-03:00", 2034, 12, 24, -3);
  testDate("2034-12-24Z", 2034, 12, 24, Date::TimeZoneZ{});
  testDate("2034-12-24", 2034, 12, 24, Date::NoTimeZone{});
  testDate("-2034-12-24", -2034, 12, 24, Date::NoTimeZone{});
  testDate("-0100-12-31", -100, 12, 31, Date::NoTimeZone{});
}

TEST(Date, parseYearMonth) {
  testYearMonth("2034-12+12:00", 2034, 12, 12);
  testYearMonth("2034-12-03:00", 2034, 12, -3);
  testYearMonth("2034-12Z", 2034, 12, Date::TimeZoneZ{});
  testYearMonth("2034-12", 2034, 12, Date::NoTimeZone{});
  testYearMonth("-2034-12", -2034, 12, Date::NoTimeZone{});
  testYearMonth("-0100-12", -100, 12, Date::NoTimeZone{});
}

TEST(Date, parseYear) {
  testYear("2034+12:00", 2034, 12);
  testYear("2034-03:00", 2034, -3);
  testYear("2034Z", 2034, Date::TimeZoneZ{});
  testYear("2034", 2034, Date::NoTimeZone{});
  testYear("-2034", -2034, Date::NoTimeZone{});
  testYear("-0100", -100, Date::NoTimeZone{});
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
template <typename F>
auto testLargeYearImpl(F parseFunction, std::string_view input,
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
      RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
          absl::StrCat("\"", input, "\"^^<", type, ">"));
  auto optionalId = parsedAsTurtle.toValueIdIfNotString(encodedIriManager());
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

#ifndef REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
TEST(Date, toEpoch) {
  {
    using namespace std::chrono;
    auto ns = [](sys_time<nanoseconds> v) {
      return v.time_since_epoch().count();
    };

    Date date = Date(1970, 1, 1, 0, 0, 0);
    sys_time<std::chrono::nanoseconds> timestamp =
        sys_time<std::chrono::nanoseconds>{nanoseconds{0}};
    auto result = date.toEpoch();
    ASSERT_TRUE(result);
    EXPECT_EQ(ns(timestamp), ns(result.value()));

    date = Date(1969, 12, 31, 23, 59, 20);
    timestamp = sys_time<nanoseconds>{seconds{-40}};
    ASSERT_TRUE(date.toEpoch());
    EXPECT_EQ(ns(timestamp), ns(date.toEpoch().value()));

    date = Date(1970, 1, 1, 1, 1, 1);
    timestamp = sys_time<nanoseconds>{seconds{3661}};
    ASSERT_TRUE(date.toEpoch());
    EXPECT_EQ(ns(timestamp), ns(date.toEpoch().value()));

    date = Date(1970, 1, 1, 0, 0, 20.235);
    auto second = duration<double>{20.235};
    timestamp = sys_time<nanoseconds>{duration_cast<nanoseconds>(second)};
    ASSERT_TRUE(date.toEpoch());
    EXPECT_NEAR(ns(timestamp), ns(date.toEpoch().value()), 500000);

    date = Date(1999, 2, 1, 8, 15, 13.098);
    second = duration<double>{13.098};
    timestamp = sys_time<nanoseconds>{seconds{917856900}} +
                duration_cast<nanoseconds>(second);
    ASSERT_TRUE(date.toEpoch());
    EXPECT_NEAR(ns(timestamp), ns(date.toEpoch().value()), 500000);

    // Test invalid `Date`.
    date = Date(1970, 11, 31, 13, 24, 24);
    ASSERT_FALSE(date.toEpoch());
    date = Date(2021, 2, 29, 9, 1, 23);
    ASSERT_FALSE(date.toEpoch());
  }
  {
    using namespace std::chrono;
    // Test different timezones.
    Date date1 = Date(1999, 10, 11, 10, 5, 30);  // UTC.
    for (int i = 1; i < 24; i++) {
      Date date2 = Date(1999, 10, 11, 10, 5, 30, i);  // UTC + i.
      // Difference in hours is converted to ns to be compared.
      long long expected = static_cast<long long>(i) * 60 * 60 * 1'000'000'000;
      EXPECT_EQ(expected,
                (date1.toEpoch().value() - date2.toEpoch().value()).count());
      date2 = Date(1999, 10, 11, 10, 5, 30, -i);  // UTC - i
      EXPECT_EQ(-expected,
                (date1.toEpoch().value() - date2.toEpoch().value()).count());
    }
  }
}

// _____________________________________________________________________________
TEST(Date, Subtraction) {
  // Invalid `Date`s.
  Date date1 = Date(1970, 11, 31);
  Date date2 = Date(2021, 2, 29);

  // Valid `Date`s.
  Date date3 = Date(1986, 6, 24);
  Date date4 = Date(1986, 6, 22);

  EXPECT_FALSE(date1 - date2);
  EXPECT_FALSE(date1 - date3);
  EXPECT_FALSE(date4 - date2);

  ASSERT_TRUE((date3 - date4).has_value());
  ASSERT_TRUE((date4 - date3).has_value());
  EXPECT_EQ(DayTimeDuration(DayTimeDuration::Type::Positive, 2),
            (date3 - date4).value());
  EXPECT_EQ(DayTimeDuration(DayTimeDuration::Type::Negative, 2),
            (date4 - date3).value());
}
#endif

// _____________________________________________________________________________
TEST(Date, getTimeZoneOffsetToUTCInHours) {
  Date date = Date(1970, 1, 1, 0, 0, 0);  // No `TimeZone` given.
  ASSERT_EQ(0, date.getTimeZoneOffsetToUTCInHours());
  date = Date(1989, 2, 3, 14, 4, 5, Date::TimeZoneZ{});  // UTC.
  ASSERT_EQ(0, date.getTimeZoneOffsetToUTCInHours());

  for (int i = 1; i < 24; i++) {
    date = Date(1989, 2, 3, 14, 4, 5, i);  // UTC + i.
    ASSERT_EQ(i, date.getTimeZoneOffsetToUTCInHours());
    date = Date(1989, 2, 3, 14, 4, 5, -i);  // UTC - i.
    ASSERT_EQ(-i, date.getTimeZoneOffsetToUTCInHours());
  }
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

// _____________________________________________________________________________
TEST(DateYearOrDuration, Hashing) {
  DateYearOrDuration d1{
      DayTimeDuration{DayTimeDuration::Type::Positive, 0, 23, 23, 62.44}};
  DateYearOrDuration d2{
      DayTimeDuration{DayTimeDuration::Type::Positive, 1, 24, 23, 62.44}};
  ad_utility::HashSet<DateYearOrDuration> set{d1, d2};
  EXPECT_THAT(set, ::testing::UnorderedElementsAre(d1, d2));
}

// _____________________________________________________________________________
TEST(DateYearOrDuration, isLongYear) {
  DateYearOrDuration year =
      DateYearOrDuration(12'000, DateYearOrDuration::Type::Year);
  EXPECT_TRUE(year.isLongYear());
  year = DateYearOrDuration(-10'000, DateYearOrDuration::Type::Year);
  EXPECT_TRUE(year.isLongYear());

  year = DateYearOrDuration(Date(9999, 1, 1));
  EXPECT_FALSE(year.isLongYear());
  year = DateYearOrDuration(Date(-9999, 1, 1));
  EXPECT_FALSE(year.isLongYear());
}

#ifndef REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
TEST(DateYearOrDuration, Subtraction) {
  {
    // Test for `Date`-subtraction.
    DateYearOrDuration test1 = DateYearOrDuration(Date(2012, 12, 24));
    DateYearOrDuration test2 = DateYearOrDuration(Date(2012, 12, 1));
    testOperation(DateYearOrDuration(
                      DayTimeDuration(DayTimeDuration::Type::Positive, 23)),
                  test1 - test2);
    testOperation(DateYearOrDuration(
                      DayTimeDuration(DayTimeDuration::Type::Negative, 23)),
                  test2 - test1);

    test1 = DateYearOrDuration(Date(2012, 12, 24));
    test2 = DateYearOrDuration(Date(2010, 12, 24));
    testOperation(DateYearOrDuration(
                      DayTimeDuration(DayTimeDuration::Type::Positive, 731)),
                  test1 - test2);
    testOperation(DateYearOrDuration(
                      DayTimeDuration(DayTimeDuration::Type::Negative, 731)),
                  test2 - test1);

    test2 = DateYearOrDuration(Date(1979, 3, 13));
    testOperation(DateYearOrDuration(
                      DayTimeDuration(DayTimeDuration::Type::Positive, 12340)),
                  test1 - test2);

    test1 = DateYearOrDuration(Date(1868, 5, 16));
    testOperation(DateYearOrDuration(
                      DayTimeDuration(DayTimeDuration::Type::Negative, 40477)),
                  test1 - test2);
  }
  {
    // Test invalid `Date`s.
    DateYearOrDuration date1 = DateYearOrDuration(Date(1970, 11, 31));
    DateYearOrDuration date2 = DateYearOrDuration(Date(2021, 2, 29));
    EXPECT_FALSE(date1 - date2);

    DateYearOrDuration date3 = DateYearOrDuration(Date(1980, 9, 13));
    EXPECT_FALSE(date1 - date3);
    EXPECT_FALSE(date2 - date3);
  }
  {
    // Test for `DateTime`-subtraction.
    DateYearOrDuration date1 =
        DateYearOrDuration(Date(2012, 12, 22, 12, 6, 12));
    DateYearOrDuration date2 =
        DateYearOrDuration(Date(2012, 12, 20, 15, 15, 59));
    // Expected `DayTimeDuration` of 1d20h50min13sec.
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 1, 20, 50, 13)),
                  date1 - date2);
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 1, 20, 50, 13)),
                  date2 - date1);

    date2 = DateYearOrDuration(Date(2010, 1, 13, 10, 32, 15));
    // Expected `DayTimeDuration` of 1074d1h33min57sec.
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 1074, 1, 33, 57)),
                  date1 - date2);

    // `Date` - `DateTime`
    date1 = DateYearOrDuration(Date(2012, 12, 22, 0, 0, 0, 0));
    date2 = DateYearOrDuration(Date(2012, 12, 20, 13, 50, 59));
    // Expected `DayTimeDuration` of 1d10h9min1sec.
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 1, 10, 9, 1)),
                  date1 - date2);
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 1, 10, 9, 1)),
                  date2 - date1);
  }
  {
    // Test previous bug where days/hours/minutes passed were negative.
    // Two `Date`s with same day.
    DateYearOrDuration date1 = DateYearOrDuration(Date(2021, 01, 23, 21, 0, 0));
    DateYearOrDuration date2 = DateYearOrDuration(Date(2021, 01, 23, 23, 0, 0));
    // Expected `DayTimeDuration` of 0d2h0min0sec.
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 0, 2, 0, 0)),
                  date1 - date2);

    // Two `Date`s with same day and hour.
    date1 = DateYearOrDuration(Date(2021, 01, 23, 22, 10, 0));
    date2 = DateYearOrDuration(Date(2021, 01, 23, 22, 30, 0));
    // Expected `DayTimeDuration` of 0d0h20min0sec.
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 0, 0, 20, 0)),
                  date1 - date2);

    // Two `Date`s with same day, hour and minute.
    date1 = DateYearOrDuration(Date(2021, 01, 23, 22, 10, 03));
    date2 = DateYearOrDuration(Date(2021, 01, 23, 22, 10, 43));
    // Expected `DayTimeDuration` of 0d0h0min40sec.
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 0, 0, 0, 40)),
                  date1 - date2);
  }
  {
    // Test `DayTimeDuration`s between UTC and other `TimeZone`s.
    for (int i = 0; i < 24; i++) {
      DateYearOrDuration date1 =
          DateYearOrDuration(Date(2021, 01, 23, 20, 10, 33));
      DateYearOrDuration date2 =
          DateYearOrDuration(Date(2021, 01, 23, 20, 10, 33, i));
      // Expected positive/negative `DayTimeDuration` of i hours.
      testOperation(DateYearOrDuration(DayTimeDuration(
                        DayTimeDuration::Type::Positive, 0, i, 0, 0)),
                    date1 - date2);

      date2 = DateYearOrDuration(Date(2021, 01, 23, 20, 10, 33, -i));
      testOperation(DateYearOrDuration(DayTimeDuration(
                        DayTimeDuration::Type::Negative, 0, i, 0, 0)),
                    date1 - date2);
    }
    // Two `Date`s with same time, but different `TimeZone`s.
    DateYearOrDuration date1 =
        DateYearOrDuration(Date(1989, 01, 23, 20, 10, 33, 2));
    DateYearOrDuration date2 =
        DateYearOrDuration(Date(1989, 01, 23, 15, 10, 33, -3));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 0, 0, 0, 0)),
                  date1 - date2);

    // `TimeZone`s causing different days.
    date1 = DateYearOrDuration(Date(1989, 01, 23, 20, 10, 33, -1));
    date2 = DateYearOrDuration(Date(1989, 01, 24, 3, 10, 33, 2));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 0, 4, 0, 0)),
                  date1 - date2);

    date1 = DateYearOrDuration(Date(1989, 01, 26, 0, 0, 0, -10));
    date2 = DateYearOrDuration(Date(1989, 01, 26, 0, 0, 0, 12));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 0, 22, 0, 0)),
                  date1 - date2);

    date2 = DateYearOrDuration(Date(1989, 01, 26, 0, 0, 0, 14));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 1, 0, 0, 0)),
                  date1 - date2);
  }
  {
    // Test for `DayTimeDuration` subtraction.
    DateYearOrDuration duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 25, 0, 0, 0));
    DateYearOrDuration duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 5, 0, 0, 0)),
                  duration1 - duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 25, 0, 0, 0));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 45, 0, 0, 0)),
                  duration1 - duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 25, 0, 0, 0));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 45, 0, 0, 0)),
                  duration1 - duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 25, 0, 0, 0));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 5, 0, 0, 0)),
                  duration1 - duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 25, 0, 0, 0));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 40, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 15, 0, 0, 0)),
                  duration1 - duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 40, 23, 8, 54));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 40, 20, 3, 40));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 0, 3, 5, 14)),
                  duration1 - duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 40, 3, 8, 54));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 41, 20, 3, 40));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 1, 16, 54, 46)),
                  duration1 - duration2);
  }
  {
    // Test for `Date` - `DayTimeDuration` subtraction.
    DateYearOrDuration date =
        DateYearOrDuration(Date(1989, 01, 23, 20, 10, 33));
    DateYearOrDuration duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 0, 20, 10, 33));
    std::optional<DateYearOrDuration> result = date - duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(1989, 01, 23, 0, 0, 0)), result.value());

    duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 0, 3, 49, 27));
    result = date - duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(1989, 01, 24, 0, 0, 0)), result.value());

    duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 30, 20, 10, 33));
    result = date - duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(1988, 12, 24, 0, 0, 0)), result.value());

    date = DateYearOrDuration(Date(2000, 4, 18, 20, 10, 0, 2));  // UTC + 2
    duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 0, 10, 10, 0));
    result = date - duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(2000, 4, 18, 10, 0, 0, 2)),
              result.value());

    date = DateYearOrDuration(Date(2000, 4, 18, 20, 10, 0, -4));  // UTC - 4
    result = date - duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(2000, 4, 18, 10, 0, 0, -4)),
              result.value());

    date = DateYearOrDuration(Date(2004, 5, 16));  // Date without time.
    duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 10, 0, 0, 0));
    result = date - duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(2004, 5, 6, 0, 0, 0)), result.value());
  }
  {
    // Test for `LargeYear` - `LargeYear`.
    DateYearOrDuration year1 =
        DateYearOrDuration(22'000, DateYearOrDuration::Type::Year);
    DateYearOrDuration year2 =
        DateYearOrDuration(11'000, DateYearOrDuration::Type::Year);

    std::optional<DateYearOrDuration> result = year1 - year2;
    ASSERT_TRUE(result);
    EXPECT_TRUE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(11'000, DateYearOrDuration::Type::Year),
              result.value());

    year2 = DateYearOrDuration(42'000, DateYearOrDuration::Type::Year);
    result = year1 - year2;
    ASSERT_TRUE(result);
    EXPECT_TRUE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(-20'000, DateYearOrDuration::Type::Year),
              result.value());

    year2 = DateYearOrDuration(20'000, DateYearOrDuration::Type::Year);
    result = year1 - year2;
    ASSERT_TRUE(result);
    EXPECT_FALSE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(Date(2000, 1, 1)), result.value());

    year2 = DateYearOrDuration(24'000, DateYearOrDuration::Type::Year);
    result = year1 - year2;
    ASSERT_TRUE(result);
    EXPECT_FALSE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(Date(-2000, 1, 1)), result.value());
  }
  {
    // Test invalid subtractions.
    DateYearOrDuration date =
        DateYearOrDuration(Date(1989, 10, 20, 20, 10, 33));
    DateYearOrDuration duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 0, 30, 10, 33));
    ASSERT_FALSE(duration - date);

    // Invalid `Date`.
    date = DateYearOrDuration(Date(1989, 02, 30, 20, 10, 33));
    duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 0, 20, 10, 33));
    ASSERT_FALSE(date - duration);
  }
}

// _____________________________________________________________________________
TEST(DateYearOrDuration, Addition) {
  {
    // Test for `DayTimeDuration` addition.
    DateYearOrDuration duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 25, 0, 0, 0));
    DateYearOrDuration duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 45, 0, 0, 0)),
                  duration1 + duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 25, 0, 0, 0));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 5, 0, 0, 0)),
                  duration1 + duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 25, 0, 0, 0));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 5, 0, 0, 0)),
                  duration1 + duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 25, 0, 0, 0));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Negative, 20, 0, 0, 0));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Negative, 45, 0, 0, 0)),
                  duration1 + duration2);

    duration1 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 40, 23, 8, 54));
    duration2 = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 40, 20, 3, 40));
    testOperation(DateYearOrDuration(DayTimeDuration(
                      DayTimeDuration::Type::Positive, 81, 19, 12, 34)),
                  duration1 + duration2);
  }
  {
    // Test for `Date` + `DayTimeDuration`.
    DateYearOrDuration date =
        DateYearOrDuration(Date(1989, 01, 23, 20, 10, 33));
    DateYearOrDuration duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 0, 2, 10, 13));
    std::optional<DateYearOrDuration> result = date + duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(1989, 01, 23, 22, 20, 46)),
              result.value());

    duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 30, 2, 10, 13));
    result = date + duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(1989, 2, 22, 22, 20, 46)),
              result.value());

    date = DateYearOrDuration(Date(2000, 4, 18, 20, 10, 0, 2));  // UTC + 2
    duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 0, 10, 10, 0));
    result = date + duration;
    ASSERT_TRUE(result);
    EXPECT_EQ(DateYearOrDuration(Date(2000, 4, 19, 6, 20, 0, 2)),
              result.value());

    date = DateYearOrDuration(Date(2000, 4, 18, 20, 10, 0, -4));  // UTC - 4
    result = date + duration;
    EXPECT_EQ(DateYearOrDuration(Date(2000, 4, 19, 6, 20, 0, -4)),
              result.value());
  }
  {
    // Test for `LargeYear` addition.
    DateYearOrDuration year1 =
        DateYearOrDuration(22'000, DateYearOrDuration::Type::Year);
    DateYearOrDuration year2 =
        DateYearOrDuration(11'000, DateYearOrDuration::Type::Year);

    std::optional<DateYearOrDuration> result = year1 + year2;
    ASSERT_TRUE(result);
    EXPECT_TRUE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(33'000, DateYearOrDuration::Type::Year),
              result.value());

    year1 = DateYearOrDuration(-30'000, DateYearOrDuration::Type::Year);
    result = year1 + year2;
    ASSERT_TRUE(result);
    EXPECT_TRUE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(-19'000, DateYearOrDuration::Type::Year),
              result.value());

    year1 = DateYearOrDuration(-15'000, DateYearOrDuration::Type::Year);
    result = year1 + year2;
    ASSERT_TRUE(result);
    EXPECT_FALSE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(Date(-4000, 1, 1)), result.value());

    year2 = DateYearOrDuration(20'000, DateYearOrDuration::Type::Year);
    result = year1 + year2;
    ASSERT_TRUE(result);
    EXPECT_FALSE(result.value().isLongYear());
    EXPECT_EQ(DateYearOrDuration(Date(5000, 1, 1)), result.value());
  }
  {
    // Test invalid subtractions.
    // Invalid `Date`.
    DateYearOrDuration date =
        DateYearOrDuration(Date(1989, 02, 30, 20, 10, 33));
    DateYearOrDuration duration = DateYearOrDuration(
        DayTimeDuration(DayTimeDuration::Type::Positive, 0, 20, 10, 33));
    EXPECT_FALSE(date + duration);

    // `Date` + `Date`
    DateYearOrDuration date1 =
        DateYearOrDuration(Date(2012, 12, 22, 12, 6, 12));
    DateYearOrDuration date2 =
        DateYearOrDuration(Date(2012, 12, 20, 15, 15, 59));
    EXPECT_FALSE(date1 + date2);
  }
}
#endif
