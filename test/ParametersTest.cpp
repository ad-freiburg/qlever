// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>

#include "../src/util/Parameters.h"
using namespace ad_utility;

using namespace detail::parameterShortNames;
using FloatParameter = Float<"Float">;
using DoubleParameter = Double<"Double">;
using SizeTParameter = SizeT<"SizeT">;
using StringParameter = String<"String">;
using BoolParameter = Bool<"Bool">;

template <ParameterName name, typename T>
void testUpdate(auto& parameters, T initialValue, T compileTimeUpdate,
                std::string compileTimeUpdateExpected,
                std::string validRuntimeUpdate, T runtimeUpdateExpected,
                std::optional<std::string> invalidRuntimeUpdate) {
  ASSERT_TRUE(parameters.getKeys().contains(name));
  ASSERT_EQ(initialValue, parameters.template get<name>());
  parameters.template set<name>(compileTimeUpdate);
  auto map = parameters.toMap();
  ASSERT_EQ(compileTimeUpdateExpected, map.at(name));

  ASSERT_EQ(compileTimeUpdate, parameters.template get<name>());
  parameters.set(name, validRuntimeUpdate);
  ASSERT_EQ(runtimeUpdateExpected, parameters.template get<name>());
  if (invalidRuntimeUpdate.has_value()) {
    ASSERT_THROW(parameters.set(name, invalidRuntimeUpdate.value()),
                 std::runtime_error);
    ASSERT_EQ(runtimeUpdateExpected, parameters.template get<name>());
  }
}
TEST(Parameters, GetAndSet) {
  Parameters parameters(FloatParameter{2.0f}, SizeTParameter{3ull},
                        DoubleParameter{-4.2e-3}, BoolParameter{false},
                        Bool<"Bool2">{true}, StringParameter{"someString"});

  ASSERT_THROW(parameters.set("NoKey", "24.1"), std::runtime_error);

  // TODO<joka921>: Make this unit test work.
  // ASSERT_THROW(parameters.set("Float", "24.nofloat1"), std::runtime_error);
  testUpdate<"Float">(parameters, 2.0f, 42.0f, "42.000000", "-183E5", -183.0e5f,
                      "notAFloat");

  testUpdate<"SizeT">(parameters, 3ull, 42ull, "42", "84", 84ull, "notAnInt");

  testUpdate<"Bool">(parameters, false, true, "true", "false", false, "maybe");
  testUpdate<"Bool2">(parameters, true, false, "false", "1", true, "fAlSe");

  testUpdate<"String">(parameters, "someString", "stringUpdated",
                       "stringUpdated", "stringUpdatedAgain",
                       "stringUpdatedAgain", std::nullopt);

  testUpdate<"Double">(parameters, -4.2e-3, 12.493, "12.493000", "-0.2e4",
                       -0.2e4, "notADouble");

  ASSERT_EQ(6ul, parameters.toMap().size());
  ASSERT_EQ(6ul, parameters.getKeys().size());
}

TEST(Parameters, OnUpdateAction) {
  std::string value;
  int counter = 0;
  auto action = [&](const std::string& s) {
    value = s;
    ++counter;
  };
  Parameters parameters{StringParameter{"initialValue"}};
  parameters.setOnUpdateAction<"String">(action);
  ASSERT_EQ(counter, 1);
  ASSERT_EQ(value, "initialValue");
  parameters.set<"String">("firstUpdate");
  ASSERT_EQ(value, "firstUpdate");
  ASSERT_EQ(counter, 2);
  ASSERT_EQ(parameters.get<"String">(), "firstUpdate");
  parameters.set("String", "secondUpdate");
  ASSERT_EQ(value, "secondUpdate");
  ASSERT_EQ(counter, 3);
  ASSERT_EQ(parameters.get<"String">(), "secondUpdate");
}

TEST(Parameters, Move) {
  using FloatParameter = Float<"Float">;
  using IntParameter = SizeT<"SizeT">;
  Parameters parameters(FloatParameter{2.0f}, IntParameter{3ull});
  auto parameters2 = std::move(parameters);
}