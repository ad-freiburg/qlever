//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <chrono>
#include <iostream>

#include "util/Concepts.h"
#include "util/GTestHelpers.h"
#include "util/ParseableDuration.h"

using ad_utility::ParseableDuration;
using namespace std::chrono_literals;
using namespace std::chrono;
using ::testing::HasSubstr;

template <typename DurationType>
auto fromString(const std::string& str) {
  return ParseableDuration<DurationType>::fromString(str);
}

// _____________________________________________________________________________
TEST(ParseableDuration, testBasicSerialization) {
  ParseableDuration duration1{1ns};
  ParseableDuration duration2{1us};
  ParseableDuration duration3{1ms};
  ParseableDuration duration4{1s};
  ParseableDuration duration5{1min};
  ParseableDuration duration6{1h};

  EXPECT_EQ(duration1.toString(), "1ns");
  EXPECT_EQ(duration2.toString(), "1us");
  EXPECT_EQ(duration3.toString(), "1ms");
  EXPECT_EQ(duration4.toString(), "1s");
  EXPECT_EQ(duration5.toString(), "1min");
  EXPECT_EQ(duration6.toString(), "1h");
}

// _____________________________________________________________________________
TEST(ParseableDuration, testFailBit) {
  {
    std::istringstream is{"12345"};
    ParseableDuration<seconds> duration;
    is >> duration;
    EXPECT_EQ(is.rdstate() & std::ios_base::failbit, std::ios_base::failbit);
  }
  {
    std::istringstream is{"ms"};
    ParseableDuration<seconds> duration;
    is >> duration;
    EXPECT_EQ(is.rdstate() & std::ios_base::failbit, std::ios_base::failbit);
  }
  // Valid case should not set fail bit
  {
    std::istringstream is{"1ms"};
    ParseableDuration<seconds> duration;
    is >> duration;
    EXPECT_EQ(is.rdstate() & std::ios_base::failbit, 0);
  }
}

// _____________________________________________________________________________
TEST(ParseableDuration, testBasicParsing) {
  EXPECT_EQ(1ns, fromString<nanoseconds>("1ns"));
  EXPECT_EQ(1us, fromString<microseconds>("1us"));
  EXPECT_EQ(1ms, fromString<milliseconds>("1ms"));
  EXPECT_EQ(1s, fromString<seconds>("1s"));
  EXPECT_EQ(1min, fromString<minutes>("1min"));
  EXPECT_EQ(1h, fromString<hours>("1h"));

  EXPECT_EQ(-1ns, fromString<nanoseconds>("-1ns"));
  EXPECT_EQ(-1us, fromString<microseconds>("-1us"));
  EXPECT_EQ(-1ms, fromString<milliseconds>("-1ms"));
  EXPECT_EQ(-1s, fromString<seconds>("-1s"));
  EXPECT_EQ(-1min, fromString<minutes>("-1min"));
  EXPECT_EQ(-1h, fromString<hours>("-1h"));

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(fromString<seconds>("1234"),
                                        HasSubstr("1234"), std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(fromString<seconds>("s"),
                                        HasSubstr("s"), std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(fromString<seconds>("    "),
                                        HasSubstr("    "), std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(fromString<seconds>("onesecond"),
                                        HasSubstr("onesecond"),
                                        std::runtime_error);
}

// _____________________________________________________________________________
TEST(ParseableDuration, testParsingWithWhitespace) {
  EXPECT_EQ(1ns, fromString<nanoseconds>(" 1 ns "));
  EXPECT_EQ(1us, fromString<microseconds>("  1  us  "));
  EXPECT_EQ(1ms, fromString<milliseconds>("1    ms"));
  EXPECT_EQ(1s, fromString<seconds>("1s    "));
  EXPECT_EQ(1min, fromString<minutes>(" 1min   "));
  EXPECT_EQ(1h, fromString<hours>(" \r\n\t\v1h        "));

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(fromString<seconds>("1 n s"),
                                        HasSubstr("1 n s"), std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(fromString<seconds>(" 1 m i n "),
                                        HasSubstr(" 1 m i n "),
                                        std::runtime_error);
}

// _____________________________________________________________________________
TEST(ParseableDuration, testParsingConversion) {
  EXPECT_EQ(0us, fromString<microseconds>("1ns"));
  EXPECT_EQ(0ms, fromString<milliseconds>("1us"));
  EXPECT_EQ(0s, fromString<seconds>("1ms"));
  EXPECT_EQ(0min, fromString<minutes>("1s"));
  EXPECT_EQ(0h, fromString<hours>("1min"));

  EXPECT_EQ(1000ns, fromString<nanoseconds>("1us"));
  EXPECT_EQ(1000us, fromString<microseconds>("1ms"));
  EXPECT_EQ(1000ms, fromString<milliseconds>("1s"));
  EXPECT_EQ(60s, fromString<seconds>("1min"));
  EXPECT_EQ(60min, fromString<minutes>("1h"));
}

// _____________________________________________________________________________
TEST(ParseableDuration, testComparisonOperators) {
  EXPECT_EQ(ParseableDuration{1ms}, ParseableDuration{1ms});
  EXPECT_LT(ParseableDuration{0ms}, ParseableDuration{1ms});
  EXPECT_GT(ParseableDuration{1ms}, ParseableDuration{0ms});
  EXPECT_GE(ParseableDuration{0ms}, ParseableDuration{0ms});
  EXPECT_LE(ParseableDuration{0ms}, ParseableDuration{0ms});
}
