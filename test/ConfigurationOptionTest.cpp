// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (May of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "util/ConfigurationManager/ConfigurationOption.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/json.h"

// Easier usage.
using ConfigurationOption = ad_benchmark::BenchmarkConfigurationOption;

/*
Not all identifiers are allowed for configuration options.
*/
TEST(BenchmarkConfigurationOptionTest, ConstructorException) {
  // No name.
  ASSERT_ANY_THROW(ad_benchmark::makeBenchmarkConfigurationOption<bool>("", ""));

  // Names with spaces.
  ASSERT_ANY_THROW(ad_benchmark::makeBenchmarkConfigurationOption<bool>("Option 1", ""));
}

/*
@brief Call the function with `T` of `std::optional<T>` for each of the
variantss in `ad_benchmark::BenchmarkConfigurationOption::ValueType` as template
parameter.

@tparam Function The loop body should be a templated function, with one
`typename` template argument and no more. It also shouldn't take any function
arguments. Should be passed per deduction.
*/
template <typename Function>
static void doForTypeInValueType(Function function) {
  ad_utility::ConstexprForLoop(
      std::make_index_sequence<std::variant_size_v<ConfigurationOption::ValueType>>{},
      [&function]<size_t index, typename IndexType = std::variant_alternative_t<
                                    index, ConfigurationOption::ValueType>::value_type>() {
        function.template operator()<IndexType>();
      });
}

/*
Check if the creation of configuration options, their direct setting and the
getter works as intended.
*/
TEST(BenchmarkConfigurationOptionTest, CreateSetAndTest) {
  /*
  Checks, if the `ConfigurationOption::getValue` only works with the actual type
  of the value in the configuration option. All the other types should cause an
  exception.
  */
  auto otherGettersDontWork = []<typename WorkingType>(const ConfigurationOption& option) {
    doForTypeInValueType([&option]<typename CurrentType>() {
      if constexpr (!std::is_same_v<WorkingType, CurrentType>) {
        ASSERT_ANY_THROW((option.getValue<CurrentType>()));
        ASSERT_ANY_THROW((option.getDefaultValue<CurrentType>()));
      } else {
        ASSERT_NO_THROW((option.getValue<CurrentType>()));
      }
    });
  };

  /*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
  auto setAndTest = [&otherGettersDontWork]<typename Type>(ConfigurationOption& option,
                                                           const Type& valueToSetTo) {
    // Do we even have the right type for this option?
    ASSERT_EQ(ConfigurationOption::ValueType{std::optional<Type>{}}.index(),
              option.getActualValueType());

    ASSERT_FALSE(option.wasSetAtRuntime());

    option.setValue(valueToSetTo);

    ASSERT_TRUE(option.hasValue() && option.wasSetAtRuntime());
    ASSERT_EQ(valueToSetTo, option.getValue<Type>());

    // Make sure, that the other getters don't work.
    otherGettersDontWork.template operator()<Type>(option);
  };

  /*
  Run a normal test case of creating a configuration option, checking it and
  setting it. With or without a default value.
  */
  auto testCaseWithDefault = [&setAndTest, &otherGettersDontWork]<typename Type>(
                                 const Type& defaultValue, const Type& valueToSetTo) {
    ConfigurationOption option{
        ad_benchmark::makeBenchmarkConfigurationOption<Type>("With_default", "", defaultValue)};

    // Can we use the default value correctly?
    ASSERT_TRUE(option.hasValue() && option.hasDefaultValue());
    ASSERT_EQ(defaultValue, option.getDefaultValue<Type>());
    ASSERT_EQ(defaultValue, option.getValue<Type>());
    otherGettersDontWork.template operator()<Type>(option);

    setAndTest.template operator()<Type>(option, valueToSetTo);

    // Is the default value unchanged?
    ASSERT_TRUE(option.hasDefaultValue());
    ASSERT_EQ(defaultValue, option.getDefaultValue<Type>());
  };

  auto testCaseWithoutDefault = [&setAndTest]<typename Type>(const Type& valueToSetTo) {
    ConfigurationOption option{
        ad_benchmark::makeBenchmarkConfigurationOption<Type>("Without_default", "")};

    // Make sure, that we truly don't have a value, that can be gotten.
    ASSERT_TRUE(!option.hasValue() && !option.hasDefaultValue());
    ad_utility::ConstexprForLoop(
        std::make_index_sequence<std::variant_size_v<ConfigurationOption::ValueType>>{},
        [&option]<size_t index, typename IndexType = std::variant_alternative_t<
                                    index, ConfigurationOption::ValueType>::value_type>() {
          ASSERT_ANY_THROW((option.getValue<IndexType>()));
          ASSERT_ANY_THROW((option.getDefaultValue<IndexType>()));
        });

    setAndTest.template operator()<Type>(option, valueToSetTo);

    // Is it still the case, that we don't have a default value?
    ASSERT_TRUE(!option.hasDefaultValue());
    ASSERT_ANY_THROW(option.getDefaultValue<Type>());
  };

  // Do a test case for every possible type.
  testCaseWithDefault.template operator()<bool>(false, true);
  testCaseWithoutDefault.template operator()<bool>(true);

  testCaseWithDefault.template operator()<std::string>(std::string{"unset"}, std::string{"set"});
  testCaseWithoutDefault.template operator()<std::string>(std::string{"set"});

  testCaseWithDefault.template operator()<int>(40, 42);
  testCaseWithoutDefault.template operator()<int>(42);

  testCaseWithDefault.template operator()<float>(float{40.5}, float{42.5});
  testCaseWithoutDefault.template operator()<float>(float{42.5});

  testCaseWithDefault.template operator()<std::vector<bool>>(std::vector{false, true},
                                                             {true, true});
  testCaseWithoutDefault.template operator()<std::vector<bool>>(std::vector{true, true});

  testCaseWithDefault.template operator()<std::vector<std::string>>(
      std::vector{std::string{"First string"}, std::string{"Second string"}},
      std::vector{std::string{"Second string"}, std::string{"Second string"}});
  testCaseWithoutDefault.template operator()<std::vector<std::string>>(
      std::vector{std::string{"Second string"}, std::string{"Second string"}});

  testCaseWithDefault.template operator()<std::vector<int>>(std::vector{40, 42},
                                                            std::vector{42, 42});
  testCaseWithoutDefault.template operator()<std::vector<int>>(std::vector{42, 42});

  testCaseWithDefault.template operator()<std::vector<float>>(std::vector<float>{40.8, 42.8},
                                                              std::vector<float>{42.8, 42.8});
  testCaseWithoutDefault.template operator()<std::vector<float>>(std::vector<float>{42.8, 42.8});
}

// `BenchmarkConfigurationOption` should always throw an exception, when created
// like this.
TEST(BenchmarkConfigurationOptionTest, ExceptionOnCreation) {
  // No identifier.
  ASSERT_ANY_THROW(ad_benchmark::makeBenchmarkConfigurationOption<bool>("", ""););
}

// The form of a generic test case for the test `SetValueWithJson`.
template <typename T>
struct JsonTestCase {
  const T interpretedJson;
  const nlohmann::json json;
};

/*
`ConfigurationOption::setValueWithJson` interprets the given json as the type of
the configuration option. This tests, if this works correctly.
*/
TEST(BenchmarkConfigurationOptionTest, SetValueWithJson) {
  // The test cases for parsing json.
  auto getTestCase = []<typename Type>() {
    if constexpr (std::is_same_v<Type, bool>) {
      return JsonTestCase<bool>{true, nlohmann::json::parse(R"--(true)--")};
    } else if constexpr (std::is_same_v<Type, std::string>) {
      return JsonTestCase<std::string>{std::string{"set"}, nlohmann::json::parse(R"--("set")--")};
    } else if constexpr (std::is_same_v<Type, int>) {
      return JsonTestCase<int>{42, nlohmann::json::parse(R"--(42)--")};
    } else if constexpr (std::is_same_v<Type, float>) {
      return JsonTestCase<float>{42.5, nlohmann::json::parse(R"--(42.5)--")};
    } else if constexpr (std::is_same_v<Type, std::vector<bool>>) {
      return JsonTestCase<std::vector<bool>>{std::vector{true, true},
                                             nlohmann::json::parse(R"--([true, true])--")};
    } else if constexpr (std::is_same_v<Type, std::vector<std::string>>) {
      return JsonTestCase<std::vector<std::string>>{
          std::vector{std::string{"str"}, std::string{"str"}},
          nlohmann::json::parse(R"--(["str", "str"])--")};
    } else if constexpr (std::is_same_v<Type, std::vector<int>>) {
      return JsonTestCase<std::vector<int>>{std::vector{42, 42},
                                            nlohmann::json::parse(R"--([42, 42])--")};
    } else if constexpr (std::is_same_v<Type, std::vector<float>>) {
      return JsonTestCase<std::vector<float>>{std::vector<float>{42.8, 42.8},
                                              nlohmann::json::parse(R"--([42.8, 42.8])--")};
    }
  };

  /*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
  auto doTestCase = [&getTestCase]<typename Type>() {
    ConfigurationOption option{ad_benchmark::makeBenchmarkConfigurationOption<Type>("t", "")};

    const auto& currentTest = getTestCase.template operator()<Type>();

    option.setValueWithJson(currentTest.json);

    // Is it set correctly?
    ASSERT_TRUE(option.hasValue());
    ASSERT_EQ(currentTest.interpretedJson, option.getValue<Type>());

    // Does the setter cause an exception, when given any json, that can't be
    // interpreted as the wanted type?
    doForTypeInValueType([&getTestCase, &option]<typename CurrentType>() {
      if constexpr (!std::is_same_v<Type, CurrentType>) {
        ASSERT_ANY_THROW(
            option.setValueWithJson(getTestCase.template operator()<CurrentType>().json););
      }
    });

    ASSERT_ANY_THROW(option.setValueWithJson(
        nlohmann::json::parse(R"--("the value is in here " : [true, 4, 4.2])--")));
  };

  // Do the test case for every possible type.
  doForTypeInValueType(doTestCase);
}

// Just testing the visit functions.
TEST(BenchmarkConfigurationOptionTest, Visit) {
  /*
  Creates a visitor for `ad_benchmark::BenchmarkConfigurationOption::visit...`,
  that looks, if the the given value and the value in the internal variant, are
  equal
  */
  auto createComparisonVisitor = []<typename ComparatorType>(const ComparatorType& comparator) {
    return [comparator]<typename T>(const std::optional<T>& val) {
      if constexpr (std::is_same_v<T, ComparatorType>) {
        /*
        Remember: The visitor is not allowed to be misformed and a
        comparison between two types without a valid comparision function is
        always misformed.
        */
        ASSERT_TRUE(val.has_value());
        ASSERT_EQ(comparator, val.value());
      } else {
        ASSERT_TRUE(false);
      }
    };
  };

  /*
  Set the value of a configuration option and check, that the visit reads the
  correct value.
  */
  auto setAndTest = [&createComparisonVisitor]<typename Type>(ConfigurationOption& option,
                                                              const Type& valueToSetTo) {
    option.setValue(valueToSetTo);

    option.visitValue(createComparisonVisitor(valueToSetTo));
  };

  /*
  Values for the test cases. One for usage as a default value and the other for
  setting a configuration optioner, when we have a default value.
  */
  auto testValues = []<typename Type>() {
    if constexpr (std::is_same_v<Type, bool>) {
      return std::make_pair(true, false);
    } else if constexpr (std::is_same_v<Type, std::string>) {
      return std::make_pair(std::string{"set1"}, std::string{"set2"});
    } else if constexpr (std::is_same_v<Type, int>) {
      return std::make_pair(int{42}, int{43});
    } else if constexpr (std::is_same_v<Type, float>) {
      return std::make_pair(float{42.5}, float{6.8});
    } else if constexpr (std::is_same_v<Type, std::vector<bool>>) {
      return std::make_pair(std::vector{true, true}, std::vector{false, false});
    } else if constexpr (std::is_same_v<Type, std::vector<std::string>>) {
      return std::make_pair(std::vector{std::string{"str1"}, std::string{"str1"}},
                            std::vector{std::string{"str2"}, std::string{"str2"}});
    } else if constexpr (std::is_same_v<Type, std::vector<int>>) {
      return std::make_pair(std::vector<int>{42, 42}, std::vector<int>{44, 43});
    } else if constexpr (std::is_same_v<Type, std::vector<float>>) {
      return std::make_pair(std::vector<float>{42.8, 42.8}, std::vector<float>{52.8, 42.9});
    }
  };

  /*
  Run a normal test case of creating a configuration option, checking it and
  setting it. With or without a default value.
  */
  auto testCaseWithDefault = [&createComparisonVisitor, &setAndTest, &testValues]<typename Type>() {
    const auto& values = testValues.template operator()<Type>();

    ConfigurationOption option{
        ad_benchmark::makeBenchmarkConfigurationOption<Type>("With_default", "", values.first)};

    option.visitValue(createComparisonVisitor(values.first));
    option.visitDefaultValue(createComparisonVisitor(values.first));

    setAndTest.template operator()<Type>(option, values.second);

    // Is the default value unchanged?
    option.visitDefaultValue(createComparisonVisitor(values.first));
  };

  auto testCaseWithoutDefault = [&setAndTest, &testValues]<typename Type>() {
    const auto& values = testValues.template operator()<Type>();

    ConfigurationOption option{
        ad_benchmark::makeBenchmarkConfigurationOption<Type>("Without_default", "")};

    // Make sure, that we truly don't have a value, that can be gotten.
    option.visitValue([](const auto& val) { ASSERT_FALSE(val.has_value()); });
    option.visitDefaultValue([](const auto& val) { ASSERT_FALSE(val.has_value()); });

    setAndTest.template operator()<Type>(option, values.first);

    // Is it still the case, that we don't have a default value?
    option.visitDefaultValue([](const auto& val) { ASSERT_FALSE(val.has_value()); });
  };

  doForTypeInValueType(testCaseWithDefault);
  doForTypeInValueType(testCaseWithoutDefault);
}
