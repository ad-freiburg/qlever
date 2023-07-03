//
// Created by johannes on 07.06.21.
//

#include <gtest/gtest.h>

#include "../src/util/Parameters.h"
using namespace ad_utility;

using namespace detail::parameterShortNames;

TEST(Parameters, First) {
  using FloatParameter = Float<"Float">;
  using IntParameter = SizeT<"SizeT">;
  using DoubleParameter = Double<"Double">;

  Parameters pack2(FloatParameter{2.0f}, IntParameter{3ull},
                   DoubleParameter{42.1});
  ASSERT_EQ(3ul, pack2.get<"SizeT">());
  ASSERT_FLOAT_EQ(2.0, pack2.get<"Float">());
  ASSERT_DOUBLE_EQ(42.1, pack2.get<"Double">());

  pack2.set("Float", "42.0");
  ASSERT_EQ(3ul, pack2.get<"SizeT">());
  ASSERT_FLOAT_EQ(42.0, pack2.get<"Float">());

  pack2.set("Double", "16.2");
  ASSERT_DOUBLE_EQ(16.2, pack2.get<"Double">());

  pack2.set("SizeT", "134");
  ASSERT_EQ(pack2.get<"SizeT">(), 134);

  ASSERT_THROW(pack2.set("NoKey", "24.1"), std::runtime_error);
  // TODO<joka921>: Make this unit test work.
  // ASSERT_THROW(pack2.set("Float", "24.nofloat1"), std::runtime_error);

  auto map = pack2.toMap();
  ASSERT_EQ(3ul, map.size());
  ASSERT_EQ("134", map.at("SizeT"));
  ASSERT_EQ("42.000000", map.at("Float"));
  ASSERT_EQ("16.200000", map.at("Double"));
}
