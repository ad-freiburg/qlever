//
// Created by johannes on 07.06.21.
//

#include <gtest/gtest.h>

#include "util/MemorySize/MemorySize.h"
#include "util/Parameters.h"

using namespace ad_utility;
using namespace memory_literals;
using namespace detail::parameterShortNames;
using namespace std::chrono_literals;

DECLARE_PAREMETER(FloatParameterTag, "Float");
DECLARE_PAREMETER(IntParameterTag, "SizeT");
DECLARE_PAREMETER(DoubleParameterTag, "Double");
DECLARE_PAREMETER(BoolParameterTag, "Bool");
DECLARE_PAREMETER(BoolParameter2Tag, "Bool2");

TEST(Parameters, First) {
  using FloatParameter = Float<FloatParameterTag>;
  using IntParameter = SizeT<IntParameterTag>;
  using DoubleParameter = Double<DoubleParameterTag>;
  using BoolParameter = Bool<BoolParameterTag>;
  using BoolParameter2 =
      Bool<BoolParameter2Tag>;  // to test both results of toString

  Parameters pack2(FloatParameter{2.0f}, IntParameter{3ull},
                   DoubleParameter{42.1}, BoolParameter{true},
                   BoolParameter2{true});
  ASSERT_EQ(3ul, pack2.get<IntParameterTag>());
  ASSERT_FLOAT_EQ(2.0, pack2.get<FloatParameterTag>());
  ASSERT_DOUBLE_EQ(42.1, pack2.get<DoubleParameterTag>());
  ASSERT_TRUE(pack2.get<BoolParameterTag>());

  pack2.set<FloatParameterTag>(42.0);
  ASSERT_EQ(3ul, pack2.get<IntParameterTag>());
  ASSERT_FLOAT_EQ(42.0, pack2.get<FloatParameterTag>());

  pack2.set<DoubleParameterTag>(16.2);
  ASSERT_DOUBLE_EQ(16.2, pack2.get<DoubleParameterTag>());

  pack2.set<IntParameterTag>(134);
  ASSERT_EQ(pack2.get<IntParameterTag>(), 134);

  pack2.set<BoolParameterTag>(false);
  ASSERT_FALSE(pack2.get<BoolParameterTag>());

  ASSERT_THROW(pack2.set("NoKey", "24.1"), std::runtime_error);
  // TODO<joka921>: Make this unit test work.
  // ASSERT_THROW(pack2.set(FloatParameterTag, "24.nofloat1"),
  // std::runtime_error);

  auto map = pack2.toMap();
  ASSERT_EQ(5ul, map.size());
  ASSERT_EQ("134", map.at("SizeT"));
  ASSERT_EQ("42.000000", map.at("Float"));
  ASSERT_EQ("16.200000", map.at("Double"));
  ASSERT_EQ("false", map.at("Bool"));
  ASSERT_EQ("true", map.at("Bool2"));
}

DECLARE_PAREMETER(MemoryParameterTag, "Memory");
// Basic test, if the parameter for `MemorySize` works.
TEST(Parameters, MemorySizeParameter) {
  // Compare a given `MemorySizeParameter` with a given `MemorySize`.
  auto compareWithMemorySize = [](const auto& parameter,
                                  const MemorySize& expectedValue) {
    ASSERT_EQ(expectedValue.getBytes(), parameter.get().getBytes());
    ASSERT_STREQ(expectedValue.asString().c_str(),
                 parameter.toString().c_str());
  };

  MemorySizeParameter<MemoryParameterTag> m(6_GB);
  compareWithMemorySize(m, 6_GB);
  m.set(6_MB);
  compareWithMemorySize(m, 6_MB);
  m.setFromString("6 TB");
  compareWithMemorySize(m, 6_TB);

  // Test, if it works with `Parameters`.
  Parameters pack(Float<FloatParameterTag>{2.0f}, SizeT<IntParameterTag>{3ull},
                  Double<DoubleParameterTag>{42.1},
                  MemorySizeParameter<MemoryParameterTag>{6_GB});
  ASSERT_EQ(pack.get<MemoryParameterTag>().getBytes(), (6_GB).getBytes());
  pack.set("Memory", "6 MB");
  ASSERT_EQ(pack.get<MemoryParameterTag>().getBytes(), (6_MB).getBytes());
}

DECLARE_PAREMETER(StringParameterTag, "String");
DECLARE_PAREMETER(DurationParameterTag, "Duration");

// Basic test, if the concept works.
TEST(Parameters, ParameterConcept) {
  // Test the parameter short names.
  static_assert(IsParameter<Float<FloatParameterTag>>);
  static_assert(IsParameter<Double<DoubleParameterTag>>);
  static_assert(IsParameter<SizeT<IntParameterTag>>);
  static_assert(IsParameter<String<StringParameterTag>>);
  static_assert(IsParameter<MemorySizeParameter<MemoryParameterTag>>);
  static_assert(IsParameter<Bool<BoolParameterTag>>);
  static_assert(IsParameter<
                DurationParameter<std::chrono::seconds, DurationParameterTag>>);

  // Test some other random types.
  static_assert(!IsParameter<std::string>);
  static_assert(!IsParameter<ParameterName>);
}

DECLARE_PAREMETER(TestParameterTag, "test");
// _____________________________________________________________________________
TEST(Parameter, verifyParameterConstraint) {
  Parameter<size_t, szt, toString, TestParameterTag> parameter{42};

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

DECLARE_PAREMETER(SecondsParameterTag, "Seconds");
// _____________________________________________________________________________
TEST(Parameter, verifyDurationParameterSerializationWorks) {
  DurationParameter<std::chrono::seconds, SecondsParameterTag>
      durationParameter{0s};
  EXPECT_EQ(durationParameter.toString(), "0s");

  durationParameter.setFromString("10s");
  EXPECT_EQ(durationParameter.get(), 10s);
}
