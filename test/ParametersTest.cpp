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

  Parameters pack2(FloatParameter{2.0f}, IntParameter{3ull});
  ASSERT_EQ(3ul, pack2.get<"SizeT">());
  ASSERT_FLOAT_EQ(2.0, pack2.get<"Float">());

  pack2.set("Float", "42.0");
  ASSERT_EQ(3ul, pack2.get<"SizeT">());
  ASSERT_FLOAT_EQ(42.0, pack2.get<"Float">());
  ASSERT_THROW(pack2.set("NoKey", "24.1"), std::runtime_error);
  // TODO<joka921>: Make this unit test work.
  // ASSERT_THROW(pack2.set("Float", "24.nofloat1"), std::runtime_error);

  auto map = pack2.toMap();
  ASSERT_EQ(2ul, map.size());
  ASSERT_EQ("3", map.at("SizeT"));
  ASSERT_EQ("42.000000", map.at("Float"));
}
