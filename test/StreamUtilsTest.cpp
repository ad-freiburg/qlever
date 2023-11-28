//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>

#include "util/Concepts.h"
#include "util/StreamUtils.h"

using ad_utility::ParseableDuration;
using namespace std::chrono_literals;
using namespace std::chrono;

// _____________________________________________________________________________
std::string toString(ad_utility::Streamable auto streamable) {
  std::ostringstream os;
  os << streamable;
  return std::move(os).str();
}

// _____________________________________________________________________________
template <typename DurationType>
ParseableDuration<DurationType> fromString(const std::string& str) {
  std::istringstream is{str};
  ParseableDuration<DurationType> duration;
  is >> duration;
  return duration;
}

// _____________________________________________________________________________
TEST(ParseableDuration, testBasicSerialization) {
  ParseableDuration duration1{1ns};
  ParseableDuration duration2{1us};
  ParseableDuration duration3{1ms};
  ParseableDuration duration4{1s};
  ParseableDuration duration5{1min};
  ParseableDuration duration6{1h};

  EXPECT_EQ(toString(duration1), "1ns");
  EXPECT_EQ(toString(duration2), "1us");
  EXPECT_EQ(toString(duration3), "1ms");
  EXPECT_EQ(toString(duration4), "1s");
  EXPECT_EQ(toString(duration5), "1min");
  EXPECT_EQ(toString(duration6), "1h");
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
TEST(ParseableDuration, testForwardingConstructor) {
  ParseableDuration<seconds> duration{1};

  EXPECT_EQ(toString(duration), "1s");
}
