// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 -  Johannes Kalmbach <kalmbacj@informatik.uni-freiburg.de>, UFR
// 2026 -  Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "gmock/gmock.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Parameters.h"

using namespace ad_utility;
using namespace memory_literals;
using namespace detail::parameterShortNames;
using namespace detail::parameterSerializers;
using namespace std::chrono_literals;

// Basic test, if the parameter for `MemorySize` works.
TEST(Parameters, MemorySizeParameter) {
  // Compare a given `MemorySizeParameter` with a given `MemorySize`.
  auto compareWithMemorySize = [](const auto& parameter,
                                  const MemorySize& expectedValue) {
    ASSERT_EQ(expectedValue.getBytes(), parameter.get().getBytes());
    ASSERT_STREQ(expectedValue.asString().c_str(),
                 parameter.toString().c_str());
  };

  MemorySizeParameter m(6_GB, "sixGB");
  compareWithMemorySize(m, 6_GB);
  m.set(6_MB);
  compareWithMemorySize(m, 6_MB);
  m.setFromString("6 TB");
  compareWithMemorySize(m, 6_TB);
}

// _____________________________________________________________________________
TEST(Parameter, verifyParameterConstraint) {
  Parameter<size_t, szt, toString> parameter{42, "test"};

  EXPECT_NO_THROW(parameter.set(1337));

  // Check constraint is tested for existing value
  EXPECT_THROW(parameter.setParameterConstraint(
                   [](const auto& value, std::string_view name) {
                     EXPECT_EQ(value, 1337);
                     EXPECT_EQ(name, "test");
                     throw std::runtime_error{"Test"};
                   }),
               std::runtime_error);

  // Assert constraint was not set
  EXPECT_NO_THROW(parameter.set(0));
  EXPECT_EQ(parameter.get(), 0);

  parameter.setParameterConstraint([](const auto& value, std::string_view) {
    if (value != 0) {
      throw std::runtime_error{"Test"};
    }
  });

  EXPECT_THROW(parameter.set(1), std::runtime_error);
  EXPECT_EQ(parameter.get(), 0);
}

// _____________________________________________________________________________
TEST(Parameter, verifyDurationParameterSerializationWorks) {
  DurationParameter<std::chrono::seconds> durationParameter{0s, "zeroSeconds"};
  EXPECT_EQ(durationParameter.toString(), "0s");

  durationParameter.setFromString("10s");
  EXPECT_EQ(durationParameter.get(), 10s);
}

// _____________________________________________________________________________
TEST(Parameters, SpaceSeparatedStrings) {
  SpaceSeparatedStrings s{{"abc", "def"}, "example"};
  EXPECT_THAT(s.get(), ::testing::ElementsAre("abc", "def"));
  EXPECT_EQ(s.toString(), "abc def");
  s.setFromString("xyz");
  EXPECT_THAT(s.get(), ::testing::ElementsAre("xyz"));
}
