// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include "util/ValueIdentity.h"

using namespace ad_utility;
using namespace ad_utility::use_value_identity;

struct Functor {
  template <typename... CompileTimeValues, typename... RuntimeValues>
  constexpr int operator()(CompileTimeValues... compile_time_values,
                           RuntimeValues... runtime_values) const {
    constexpr int compile_time_values_sum =
        (0 + ... + compile_time_values.value);
    int runtime_sum = (0 + ... + runtime_values);
    return compile_time_values_sum + runtime_sum;
  }
};

TEST(ApplyAsValueIdentity, FunctorInvocation) {
  constexpr ApplyAsValueIdentity apply{Functor{}};

  constexpr int result_single_arg = apply.operator()<5, 10, 15>(20);
  constexpr int result_double_arg = apply.operator()<5, 10, 15>(20, 25);
  EXPECT_EQ(result_single_arg, 50);
  EXPECT_EQ(result_double_arg, 75);
}
