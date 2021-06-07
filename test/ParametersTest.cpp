//
// Created by johannes on 07.06.21.
//

#include <gtest/gtest.h>
#include "../src/util/Parameters.h"

TEST(Parameters, First){
  using FloatParameter = Parameter<float, "Float">;
  using IntParameter = Parameter<int, "Int">;

  using ParamPack = Parameters<FloatParameter, IntParameter>;
  ParamPack pack{};
  Parameters pack2{FloatParameter{2.0}, IntParameter{3}};
  ASSERT_EQ(0, pack.get<"Int">());
 ASSERT_FLOAT_EQ(0.0, pack.get<"Float">());
  ASSERT_EQ(3, pack2.get<"Int">());
  ASSERT_FLOAT_EQ(2.0, pack2.get<"Float">());
}