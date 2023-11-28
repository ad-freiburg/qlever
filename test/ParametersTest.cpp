//
// Created by johannes on 07.06.21.
//

#include <gtest/gtest.h>

#include "../src/util/Parameters.h"
#include "util/MemorySize/MemorySize.h"
using namespace ad_utility;
using namespace memory_literals;

using namespace detail::parameterShortNames;
using namespace memory_literals;

TEST(Parameters, First) {
  using FloatParameter = Float<"Float">;
  using IntParameter = SizeT<"SizeT">;
  using DoubleParameter = Double<"Double">;
  using BoolParameter = Bool<"Bool">;
  using BoolParameter2 = Bool<"Bool2">;  // to test both results of toString

  Parameters pack2(FloatParameter{2.0f}, IntParameter{3ull},
                   DoubleParameter{42.1}, BoolParameter{true},
                   BoolParameter2{true});
  ASSERT_EQ(3ul, pack2.get<"SizeT">());
  ASSERT_FLOAT_EQ(2.0, pack2.get<"Float">());
  ASSERT_DOUBLE_EQ(42.1, pack2.get<"Double">());
  ASSERT_TRUE(pack2.get<"Bool">());

  pack2.set("Float", "42.0");
  ASSERT_EQ(3ul, pack2.get<"SizeT">());
  ASSERT_FLOAT_EQ(42.0, pack2.get<"Float">());

  pack2.set("Double", "16.2");
  ASSERT_DOUBLE_EQ(16.2, pack2.get<"Double">());

  pack2.set("SizeT", "134");
  ASSERT_EQ(pack2.get<"SizeT">(), 134);

  pack2.set("Bool", "false");
  ASSERT_FALSE(pack2.get<"Bool">());

  ASSERT_THROW(pack2.set("NoKey", "24.1"), std::runtime_error);
  // TODO<joka921>: Make this unit test work.
  // ASSERT_THROW(pack2.set("Float", "24.nofloat1"), std::runtime_error);

  auto map = pack2.toMap();
  ASSERT_EQ(5ul, map.size());
  ASSERT_EQ("134", map.at("SizeT"));
  ASSERT_EQ("42.000000", map.at("Float"));
  ASSERT_EQ("16.200000", map.at("Double"));
  ASSERT_EQ("false", map.at("Bool"));
  ASSERT_EQ("true", map.at("Bool2"));
}

// Basic test, if the parameter for `MemorySize` works.
TEST(Parameters, MemorySizeParameter) {
  // Compare a given `MemorySizeParameter` with a given `MemorySize`.
  auto compareWithMemorySize = [](const auto& parameter,
                                  const MemorySize& expectedValue) {
    ASSERT_EQ(expectedValue.getBytes(), parameter.get().getBytes());
    ASSERT_STREQ(expectedValue.asString().c_str(),
                 parameter.toString().c_str());
  };

  MemorySizeParameter<"Memory"> m(6_GB);
  compareWithMemorySize(m, 6_GB);
  m.set(6_MB);
  compareWithMemorySize(m, 6_MB);
  m.setFromString("6 TB");
  compareWithMemorySize(m, 6_TB);

  // Test, if it works with `Parameters`.
  Parameters pack(Float<"Float">{2.0f}, SizeT<"SizeT">{3ull},
                  Double<"Double">{42.1}, MemorySizeParameter<"Memory">{6_GB});
  ASSERT_EQ(pack.get<"Memory">().getBytes(), (6_GB).getBytes());
  pack.set("Memory", "6 MB");
  ASSERT_EQ(pack.get<"Memory">().getBytes(), (6_MB).getBytes());
}

// Basic test, if the concept works.
TEST(Parameters, ParameterConcept) {
  // Test the parameter short names.
  static_assert(IsParameter<Float<"Float">>);
  static_assert(IsParameter<Double<"Double">>);
  static_assert(IsParameter<SizeT<"SizeT">>);
  static_assert(IsParameter<String<"String">>);
  static_assert(IsParameter<MemorySizeParameter<"MemorySizeParameter">>);
  static_assert(IsParameter<Bool<"Bool">>);

  // Test some other random types.
  static_assert(!IsParameter<std::string>);
  static_assert(!IsParameter<ParameterName>);
}
