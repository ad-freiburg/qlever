//
// Created by johannes on 07.06.21.
//

#include <gtest/gtest.h>

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
