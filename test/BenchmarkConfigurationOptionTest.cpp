// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (May of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <utility>
#include <variant>

#include "../benchmark/infrastructure/BenchmarkConfigurationOption.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/json.h"

// Easier usage.
using ConfigurationOption = ad_benchmark::BenchmarkConfigurationOption;

/*
Check if the creation of configuration options, their direct setting and the getter works as
intended.
*/
TEST(BenchmarkConfigurationOptionTest, CreateSetAndTest) {
  /*
  Checks, if the `ConfigurationOption::getValue` only works with the actual type of the value in the
  configuration option. All the other types should cause an exception.
  */
  auto otherGettersDontWork = []<ConfigurationOption::ValueTypeIndexes WorkingTypeIndex>(
                                  const ConfigurationOption& option) {
    ad_utility::ConstexprForLoop(
        std::make_index_sequence<std::variant_size_v<ConfigurationOption::ValueType>>{},
        [&option]<size_t index>() {
          /*
          Make sure, that the type at position `index` of type `ValueType` causes an exception
          to be thrown by the getter.
          */
          if constexpr (index != WorkingTypeIndex) {
            ASSERT_ANY_THROW(
                (option.getValue<
                    std::variant_alternative_t<index, ConfigurationOption::ValueType>>()));
            ASSERT_ANY_THROW(
                (option.getDefaultValue<
                    std::variant_alternative_t<index, ConfigurationOption::ValueType>>()));
          } else {
            ASSERT_NO_THROW((option.getValue<
                             std::variant_alternative_t<index, ConfigurationOption::ValueType>>()));
          }
        });
  };

  /*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
  auto setAndTest = [&otherGettersDontWork]<
      ConfigurationOption::ValueTypeIndexes typeIndex,
      typename Type = std::variant_alternative_t<typeIndex, ConfigurationOption::ValueType>>(
      ConfigurationOption & option, const Type& valueToSetTo) {
    // Do we even have the right type for this option?
    ASSERT_EQ(typeIndex, option.getActualValueType());

    ASSERT_FALSE(option.wasSetAtRuntime());

    option.setValue(valueToSetTo);

    ASSERT_TRUE(option.hasValue() && option.wasSetAtRuntime());
    ASSERT_EQ(valueToSetTo, option.getValue<Type>());

    // Make sure, that the other getters don't work.
    otherGettersDontWork.template operator()<typeIndex>(option);
  };

  /*
  Run a normal test case of creating a configuration option, checking it and
  setting it. With or without a default value.
  */
  auto testCaseWithDefault =
      [
        &setAndTest, &otherGettersDontWork
      ]<ConfigurationOption::ValueTypeIndexes typeIndex,
        typename Type = std::variant_alternative_t<typeIndex, ConfigurationOption::ValueType>>(
          const Type& defaultValue, const Type& valueToSetTo) {
    ConfigurationOption option("With default", "", typeIndex, defaultValue);

    // Can we use the default value correctly?
    ASSERT_TRUE(option.hasValue() && option.hasDefaultValue());
    ASSERT_EQ(defaultValue, option.getDefaultValue<Type>());
    ASSERT_EQ(defaultValue, option.getValue<Type>());
    otherGettersDontWork.template operator()<typeIndex>(option);

    setAndTest.template operator()<typeIndex>(option, valueToSetTo);

    // Is the default value unchanged?
    ASSERT_TRUE(option.hasDefaultValue());
    ASSERT_EQ(defaultValue, option.getDefaultValue<Type>());
  };

  auto testCaseWithoutDefault =
      [&setAndTest]<ConfigurationOption::ValueTypeIndexes typeIndex,
                    typename Type =
                        std::variant_alternative_t<typeIndex, ConfigurationOption::ValueType>>(
          const Type& valueToSetTo) {
    ConfigurationOption option("Without default", "", typeIndex);

    // Make sure, that we truly don't have a value, that can be gotten.
    ASSERT_TRUE(!option.hasValue() && !option.hasDefaultValue());
    ad_utility::ConstexprForLoop(
        std::make_index_sequence<std::variant_size_v<ConfigurationOption::ValueType>>{},
        [&option]<size_t index>() {
          ASSERT_ANY_THROW(
              (option
                   .getValue<std::variant_alternative_t<index, ConfigurationOption::ValueType>>()));
          ASSERT_ANY_THROW((option.getDefaultValue<
                            std::variant_alternative_t<index, ConfigurationOption::ValueType>>()));
        });

    setAndTest.template operator()<typeIndex>(option, valueToSetTo);

    // Is it still the case, that we don't have a default value?
    ASSERT_TRUE(!option.hasDefaultValue());
    ASSERT_ANY_THROW(option.getDefaultValue<Type>());
  };

  // Do a test case for every possible type.
  testCaseWithDefault.template operator()<ConfigurationOption::boolean>(false, true);
  testCaseWithoutDefault.template operator()<ConfigurationOption::boolean>(true);

  testCaseWithDefault.template operator()<ConfigurationOption::string>(std::string{"unset"},
                                                                       std::string{"set"});
  testCaseWithoutDefault.template operator()<ConfigurationOption::string>(std::string{"set"});

  testCaseWithDefault.template operator()<ConfigurationOption::integer>(40, 42);
  testCaseWithoutDefault.template operator()<ConfigurationOption::integer>(42);

  testCaseWithDefault.template operator()<ConfigurationOption::floatingPoint>(40.5, 42.5);
  testCaseWithoutDefault.template operator()<ConfigurationOption::floatingPoint>(42.5);

  testCaseWithDefault.template operator()<ConfigurationOption::booleanList>({false, true},
                                                                            {true, true});
  testCaseWithoutDefault.template operator()<ConfigurationOption::booleanList>({true, true});

  testCaseWithDefault.template operator()<ConfigurationOption::stringList>(
      {std::string{"First string"}, std::string{"Second string"}},
      {std::string{"Second string"}, std::string{"Second string"}});
  testCaseWithoutDefault.template operator()<ConfigurationOption::stringList>(
      {std::string{"Second string"}, std::string{"Second string"}});

  testCaseWithDefault.template operator()<ConfigurationOption::integerList>({40, 42}, {42, 42});
  testCaseWithoutDefault.template operator()<ConfigurationOption::integerList>({42, 42});

  testCaseWithDefault.template operator()<ConfigurationOption::floatingPointList>({40.8, 42.8},
                                                                                  {42.8, 42.8});
  testCaseWithoutDefault.template operator()<ConfigurationOption::floatingPointList>({42.8, 42.8});
}

// The form of a generic test case for the test `SetValueWithJson`.
template <typename T>
struct JsonTestCase {
  const T interpretedJson;
  const nlohmann::json json;
};

/*
`ConfigurationOption::setValueWithJson` interprets the given json as the type of the configuration
option. This tests, if this works correctly.
*/
TEST(BenchmarkConfigurationOptionTest, SetValueWithJson) {
  // The test cases for parsing json.
  auto getTestCase = []<ConfigurationOption::ValueTypeIndexes typeIndex>() {
    using TypeIndexes = ConfigurationOption::ValueTypeIndexes;

    if constexpr (typeIndex == TypeIndexes::boolean) {
      return JsonTestCase<bool>{true, nlohmann::json::parse(R"--(true)--")};
    } else if constexpr (typeIndex == TypeIndexes::string) {
      return JsonTestCase<std::string>{std::string{"set"}, nlohmann::json::parse(R"--("set")--")};
    } else if constexpr (typeIndex == TypeIndexes::integer) {
      return JsonTestCase<int>{42, nlohmann::json::parse(R"--(42)--")};
    } else if constexpr (typeIndex == TypeIndexes::floatingPoint) {
      return JsonTestCase<double>{42.5, nlohmann::json::parse(R"--(42.5)--")};
    } else if constexpr (typeIndex == TypeIndexes::booleanList) {
      return JsonTestCase<std::vector<bool>>{std::vector{true, true},
                                             nlohmann::json::parse(R"--([true, true])--")};
    } else if constexpr (typeIndex == TypeIndexes::stringList) {
      return JsonTestCase<std::vector<std::string>>{
          std::vector{std::string{"str"}, std::string{"str"}},
          nlohmann::json::parse(R"--(["str", "str"])--")};
    } else if constexpr (typeIndex == TypeIndexes::integerList) {
      return JsonTestCase<std::vector<int>>{std::vector{42, 42},
                                            nlohmann::json::parse(R"--([42, 42])--")};
    } else if constexpr (typeIndex == TypeIndexes::floatingPointList) {
      return JsonTestCase<std::vector<double>>{std::vector{42.8, 42.8},
                                               nlohmann::json::parse(R"--([42.8, 42.8])--")};
    }
  };

  // Add one to every element of a `std::index_sequence`.
  constexpr auto addOneToIndexSequence =
      []<size_t... Indexes>(const std::index_sequence<Indexes...>&) {
        return std::index_sequence<(Indexes + 1)...>{};
      };
  // The index sequence for `ConfigurationOption::ValueTypIndexes`.
  constexpr auto valueTypeIndexesSequence = addOneToIndexSequence(
      std::make_index_sequence<std::variant_size_v<ConfigurationOption::ValueType> - 1>{});

  /*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
  auto doTestCase =
      [
        &getTestCase, &valueTypeIndexesSequence
      ]<size_t typeIndex,
        typename Type = std::variant_alternative_t<typeIndex, ConfigurationOption::ValueType>>() {
    constexpr auto typeIndexAsEnum = static_cast<ConfigurationOption::ValueTypeIndexes>(typeIndex);

    ConfigurationOption option("t", "", typeIndexAsEnum);

    const auto& currentTest = getTestCase.template operator()<typeIndexAsEnum>();

    option.setValueWithJson(currentTest.json);

    // Is it set correctly?
    ASSERT_TRUE(option.hasValue());
    ASSERT_EQ(currentTest.interpretedJson, option.getValue<Type>());

    // Does the setter cause an exception, when given any json, that can't be interpreted as the
    // wanted type?
    ad_utility::ConstexprForLoop(valueTypeIndexesSequence, [&getTestCase, &option]<size_t index>() {
      if constexpr (typeIndexAsEnum != index) {
        ASSERT_ANY_THROW(option.setValueWithJson(
            getTestCase
                .template operator()<static_cast<ConfigurationOption::ValueTypeIndexes>(index)>()
                .json););
      }
    });

    ASSERT_ANY_THROW(option.setValueWithJson(
        nlohmann::json::parse(R"--("the value is in here " : [true, 4, 4.2])--")));
  };

  // Do the test case for every possible type.
  ad_utility::ConstexprForLoop(valueTypeIndexesSequence, doTestCase);
}
