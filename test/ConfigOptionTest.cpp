// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (May of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "../test/util/ConfigOptionHelpers.h"
#include "../test/util/GTestHelpers.h"
#include "../test/util/ValidatorFunctionHelpers.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_utility {
/*
Not all identifiers are allowed for configuration options.
*/
TEST(ConfigOptionTest, ConstructorException) {
  bool notUsed;

  // No name.
  ASSERT_THROW(ConfigOption("", "", &notUsed),
               ad_utility::NotValidShortHandNameException);

  // Names with spaces.
  ASSERT_THROW(ConfigOption("Option 1", "", &notUsed),
               ad_utility::NotValidShortHandNameException);

  // The variable pointer is a null pointer.
  int* ptr = nullptr;
  ASSERT_THROW(ConfigOption("Option", "", ptr),
               ad_utility::ConfigOptionConstructorNullPointerException);
}

/*
The form of a generic test case, for when a value gets converted to it's string,
or json, representation. Other the other way around.
*/
template <typename T>
struct ConversionTestCase {
  const T value;
  const nlohmann::json jsonRepresentation;
};

/*
@brief Return a `ConversionTestCase` case for the given type.
*/
template <typename T>
ConversionTestCase<T> getConversionTestCase() {
  if constexpr (std::is_same_v<T, bool>) {
    return ConversionTestCase<bool>{
        true,
        nlohmann::json::parse(R"--(true)--"),
    };
  } else if constexpr (std::is_same_v<T, std::string>) {
    return ConversionTestCase<std::string>{
        std::string{"set"}, nlohmann::json::parse(R"--("set")--")};
  } else if constexpr (std::is_same_v<T, int>) {
    return ConversionTestCase<int>{-42, nlohmann::json::parse(R"--(-42)--")};
  } else if constexpr (std::is_same_v<T, size_t>) {
    return ConversionTestCase<size_t>{42, nlohmann::json::parse(R"--(42)--")};
  } else if constexpr (std::is_same_v<T, float>) {
    return ConversionTestCase<float>{42.5,
                                     nlohmann::json::parse(R"--(42.5)--")};
  } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
    return ConversionTestCase<std::vector<bool>>{
        std::vector{true, true}, nlohmann::json::parse(R"--([true, true])--")};
  } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
    return ConversionTestCase<std::vector<std::string>>{
        std::vector{std::string{"str"}, std::string{"str"}},
        nlohmann::json::parse(R"--(["str", "str"])--")};
  } else if constexpr (std::is_same_v<T, std::vector<int>>) {
    return ConversionTestCase<std::vector<int>>{
        std::vector{-42, 42}, nlohmann::json::parse(R"--([-42, 42])--")};
  } else if constexpr (std::is_same_v<T, std::vector<size_t>>) {
    return ConversionTestCase<std::vector<size_t>>{
        std::vector{42uL, 42uL}, nlohmann::json::parse(R"--([42, 42])--")};
  } else {
    // Must be a vector of floats.
    static_assert(std::is_same_v<T, std::vector<float>>);

    return ConversionTestCase<std::vector<float>>{
        {42.5f, 42.5f}, nlohmann::json::parse(R"--([42.5, 42.5])--")};
  }
}

/*
Check if the creation of configuration options, their direct setting and the
getter works as intended.
*/
TEST(ConfigOptionTest, CreateSetAndTest) {
  /*
  Checks, if the `ConfigOption::getValue` only works with the actual type
  of the value in the configuration option. All the other types should cause an
  exception.
  */
  auto otherGettersDontWork =
      []<typename WorkingType>(const ConfigOption& option) {
        doForTypeInConfigOptionValueType([&option]<typename CurrentType>() {
          if (option.wasSet()) {
            if constexpr (!std::is_same_v<WorkingType, CurrentType>) {
              ASSERT_THROW((option.getValue<CurrentType>()),
                           ad_utility::ConfigOptionGetWrongTypeException);
            } else {
              ASSERT_NO_THROW((option.getValue<CurrentType>()));
            }

            ASSERT_NO_THROW(option.getValueAsJson());
            ASSERT_NO_THROW(option.getValueAsString());

          } else {
            ASSERT_THROW((option.getValue<CurrentType>()),
                         ad_utility::ConfigOptionValueNotSetException);
            ASSERT_ANY_THROW(option.getValueAsJson());
            ASSERT_ANY_THROW(option.getValueAsString());
          }

          if (option.hasDefaultValue()) {
            if constexpr (!std::is_same_v<WorkingType, CurrentType>) {
              ASSERT_THROW((option.getDefaultValue<CurrentType>()),
                           ad_utility::ConfigOptionGetWrongTypeException);
            } else {
              ASSERT_NO_THROW((option.getDefaultValue<CurrentType>()));
            }

            ASSERT_FALSE(option.getDefaultValueAsJson().is_null());
            ASSERT_NE("None", option.getDefaultValueAsString());
          } else {
            ASSERT_THROW((option.getDefaultValue<CurrentType>()),
                         ad_utility::ConfigOptionValueNotSetException);
            ASSERT_TRUE(option.getDefaultValueAsJson().is_null());
            ASSERT_EQ("None", option.getDefaultValueAsString());
          }
        });
      };

  /*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
  auto setAndTest = [&otherGettersDontWork]<typename Type>(
                        ConfigOption& option, Type* variablePointer,
                        const ConversionTestCase<Type>& toSetTo) {
    ASSERT_FALSE(option.wasSetAtRuntime());

    option.setValue(toSetTo.value);

    ASSERT_TRUE(option.wasSet() && option.wasSetAtRuntime());
    ASSERT_EQ(toSetTo.value, option.getValue<Type>());
    ASSERT_EQ(toSetTo.value, *variablePointer);

    // Make sure, that the other getters don't work.
    otherGettersDontWork.template operator()<Type>(option);
  };

  /*
  Run a normal test case of creating a configuration option, checking it and
  setting it. With or without a default value.
  */
  auto testCaseWithDefault = [&setAndTest,
                              &otherGettersDontWork]<typename Type>(
                                 const ConversionTestCase<Type>& toSetTo) {
    // Every configuration option keeps updating an external variable with
    // the value, that it itself holds. This is the one.
    Type configurationOptionValue;

    // The default value.
    const ConversionTestCase<Type> defaultCase{getConversionTestCase<Type>()};

    ConfigOption option{ConfigOption("With_default", "",
                                     &configurationOptionValue,
                                     std::optional{defaultCase.value})};

    // Can we use the default value correctly?
    ASSERT_TRUE(option.wasSet() && option.hasDefaultValue());
    ASSERT_EQ(defaultCase.value, option.getDefaultValue<Type>());
    ASSERT_EQ(defaultCase.value, option.getValue<Type>());
    ASSERT_EQ(defaultCase.value, configurationOptionValue);
    otherGettersDontWork.template operator()<Type>(option);

    setAndTest.template operator()<Type>(option, &configurationOptionValue,
                                         toSetTo);

    // Is the default value unchanged?
    ASSERT_TRUE(option.hasDefaultValue());
    ASSERT_EQ(defaultCase.value, option.getDefaultValue<Type>());
    ASSERT_EQ(defaultCase.jsonRepresentation, option.getDefaultValueAsJson());
  };

  auto testCaseWithoutDefault =
      [&otherGettersDontWork,
       &setAndTest]<typename Type>(const ConversionTestCase<Type>& toSetTo) {
        // Every configuration option keeps updating an external variable with
        // the value, that it itself holds. This is the one.
        Type configurationOptionValue;

        ConfigOption option{
            ConfigOption("Without_default", "", &configurationOptionValue)};

        // Make sure, that we truly don't have a default value, that can be
        // gotten.
        ASSERT_TRUE(!option.wasSet() && !option.hasDefaultValue());
        ASSERT_THROW(option.getDefaultValue<Type>(),
                     ad_utility::ConfigOptionValueNotSetException);
        ASSERT_TRUE(option.getDefaultValueAsJson().empty());
        otherGettersDontWork.template operator()<Type>(option);

        setAndTest.template operator()<Type>(option, &configurationOptionValue,
                                             toSetTo);

        // Is it still the case, that we don't have a default value?
        ASSERT_TRUE(!option.hasDefaultValue());
        ASSERT_THROW(option.getDefaultValue<Type>(),
                     ad_utility::ConfigOptionValueNotSetException);
        ASSERT_TRUE(option.getDefaultValueAsJson().empty());
        ASSERT_EQ("None", option.getDefaultValueAsString());
      };

  // Do a test case for every possible type.
  testCaseWithDefault(
      ConversionTestCase<bool>{false, nlohmann::json::parse(R"--(false)--")});
  testCaseWithoutDefault(
      ConversionTestCase<bool>{false, nlohmann::json::parse(R"--(false)--")});

  testCaseWithDefault(ConversionTestCase<std::string>{
      "unset", nlohmann::json::parse(R"--("unset")--")});
  testCaseWithoutDefault(ConversionTestCase<std::string>{
      "unset", nlohmann::json::parse(R"--("unset")--")});

  testCaseWithDefault(
      ConversionTestCase<int>{-40, nlohmann::json::parse(R"--(-40)--")});
  testCaseWithoutDefault(
      ConversionTestCase<int>{-40, nlohmann::json::parse(R"--(-40)--")});

  testCaseWithDefault(
      ConversionTestCase<size_t>{40uL, nlohmann::json::parse(R"--(40)--")});
  testCaseWithoutDefault(
      ConversionTestCase<size_t>{40uL, nlohmann::json::parse(R"--(40)--")});

  testCaseWithDefault(
      ConversionTestCase<float>{40.5, nlohmann::json::parse(R"--(40.5)--")});
  testCaseWithoutDefault(
      ConversionTestCase<float>{40.5, nlohmann::json::parse(R"--(40.5)--")});

  testCaseWithDefault(ConversionTestCase<std::vector<bool>>{
      std::vector{false, true}, nlohmann::json::parse(R"--([false, true])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<bool>>{
      std::vector{false, true}, nlohmann::json::parse(R"--([false, true])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<std::string>>{
      std::vector<std::string>{"str1", "str2"},
      nlohmann::json::parse(R"--(["str1", "str2"])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<std::string>>{
      std::vector<std::string>{"str1", "str2"},
      nlohmann::json::parse(R"--(["str1", "str2"])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<int>>{
      {-40, 41}, nlohmann::json::parse(R"--([-40, 41])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<int>>{
      {-40, 41}, nlohmann::json::parse(R"--([-40, 41])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<size_t>>{
      {40uL, 41uL}, nlohmann::json::parse(R"--([40, 41])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<size_t>>{
      {40uL, 41uL}, nlohmann::json::parse(R"--([40, 41])--")});

  testCaseWithDefault(ConversionTestCase<std::vector<float>>{
      {40.7, 40.913}, nlohmann::json::parse(R"--([40.7, 40.913])--")});
  testCaseWithoutDefault(ConversionTestCase<std::vector<float>>{
      {40.7, 40.913}, nlohmann::json::parse(R"--([40.7, 40.913])--")});
}

// `ConfigOption` should always throw an exception, when created
// like this.
TEST(ConfigOptionTest, ExceptionOnCreation) {
  // No identifier.
  bool notUsed;
  ASSERT_THROW(ConfigOption("", "", &notUsed);
               , ad_utility::NotValidShortHandNameException);
}

// Test, if a config option can only be set to values of the same type, as it is
// meant to hold.
TEST(ConfigOptionTest, SetValueException) {
  // Try every type combination.
  doForTypeInConfigOptionValueType([]<typename WorkingType>() {
    WorkingType notUsed{getConversionTestCase<WorkingType>().value};
    ConfigOption option("option", "", &notUsed);

    doForTypeInConfigOptionValueType([&option, &notUsed]<typename T>() {
      if constexpr (std::is_same_v<T, WorkingType>) {
        ASSERT_NO_THROW(option.setValue(notUsed));
      } else {
        ASSERT_THROW(option.setValue(getConversionTestCase<T>().value),
                     ad_utility::ConfigOptionSetWrongTypeException);
      }
    });
  });
}

/*
`ConfigOption::setValueWithJson` interprets the given json as the type of
the configuration option. This tests, if this works correctly.
*/
TEST(ConfigOptionTest, SetValueWithJson) {
  /*
  Set the value of a configuration option and check, that it was set
  correctly.
  */
  auto doTestCase = []<typename Type>() {
    // Every configuration option keeps updating an external variable with the
    // value, that it itself holds. This is the one.
    Type configurationOptionValue;

    ConfigOption option{ConfigOption("t", "", &configurationOptionValue)};

    const auto& currentTest = getConversionTestCase<Type>();

    option.setValueWithJson(currentTest.jsonRepresentation);

    // Is it set correctly?
    ASSERT_TRUE(option.wasSet());
    ASSERT_EQ(currentTest.value, option.getValue<Type>());
    ASSERT_EQ(currentTest.value, configurationOptionValue);

    // Does the setter cause an exception, when given any json, that can't be
    // interpreted as the wanted type?
    doForTypeInConfigOptionValueType([&option]<typename CurrentType>() {
      if constexpr (!std::is_same_v<Type, CurrentType> &&
                    !(std::is_same_v<Type, int> &&
                      std::is_same_v<
                          CurrentType,
                          size_t>)&&!(std::is_same_v<Type, std::vector<int>> &&
                                      std::is_same_v<CurrentType,
                                                     std::vector<size_t>>)) {
        ASSERT_THROW(option.setValueWithJson(
            getConversionTestCase<CurrentType>().jsonRepresentation);
                     , ad_utility::ConfigOptionSetWrongJsonTypeException);
      }
    });

    ASSERT_ANY_THROW(option.setValueWithJson(nlohmann::json::parse(
        R"--("the value is in here " : [true, 4, 4.2])--")));
  };

  // Do the test case for every possible type.
  doForTypeInConfigOptionValueType(doTestCase);
}

// Test, if there is a dummy value for any type, that a `ConfigOption` can hold.
TEST(ConfigOptionTest, DummyValueExistence) {
  doForTypeInConfigOptionValueType([]<typename T>() {
    T notUsed;
    ConfigOption option("option", "", &notUsed);

    ASSERT_FALSE(option.getDummyValueAsJson().is_null());
    ASSERT_NE("None", option.getDummyValueAsString());
    ASSERT_NE("", option.getDummyValueAsString());
  });
}

TEST(ConfigOptionTest, AddValidator) {
  /*
  Very simple check for the `set`-functions of `configOption`. Simply checks, if
  the valid value can be set without exception and if the not valid one throws
  an exception with the wanted message.
  */
  auto checkSet = []<typename T>(ConfigOption& option, const T& validValue,
                                 const T& notValidValue,
                                 std::string_view expectedErrorMessage) {
    ASSERT_NO_THROW(option.setValue(validValue));
    AD_EXPECT_THROW_WITH_MESSAGE(
        option.setValue(notValidValue),
        ::testing::ContainsRegex(expectedErrorMessage));

    // Convert the values to json representation.
    nlohmann::json validValueAsJson(validValue);
    nlohmann::json notValidValueAsJson(notValidValue);

    ASSERT_NO_THROW(option.setValueWithJson(validValueAsJson));
    AD_EXPECT_THROW_WITH_MESSAGE(
        option.setValueWithJson(notValidValueAsJson),
        ::testing::ContainsRegex(expectedErrorMessage));
  };

  // Once again, we use `doForTypeInConfigOptionValueType` and need to create a
  // function for executing a test.
  auto doTest = [&checkSet]<typename Type>() {
    Type var{};
    ConfigOption option("Test", "", &var);

    // Single validator.
    option.addValidator(
        generateSingleParameterValidatorFunction<Type>(1),
        absl::StrCat(ConfigOption::availableTypesToString<Type>(),
                     " validator 1"));
    // Using the invariant of our function generator, to create valid and none
    // valid values for the test.
    checkSet(option, createDummyValueForValidator<Type>(0),
             createDummyValueForValidator<Type>(1),
             absl::StrCat(ConfigOption::availableTypesToString<Type>(),
                          " validator 1"));

    // Multiple validators.
    constexpr size_t NUMBER_OF_VALIDATORS{50};
    for (size_t i = 2; i < NUMBER_OF_VALIDATORS + 2; i++) {
      /*
      Special handeling for `bool`, because it only has two values and that
      doesn't really work with our test for multiple validators, which uses the
      invariant property of our lambda generation function.
      */
      if constexpr (std::is_same_v<Type, bool>) {
        option.addValidator(
            generateSingleParameterValidatorFunction<Type>(i * 2 + 1),
            absl::StrCat(ConfigOption::availableTypesToString<Type>(),
                         " validator ", i * 2 + 1));
        checkSet(option, createDummyValueForValidator<Type>(0),
                 createDummyValueForValidator<Type>(1),
                 absl::StrCat(ConfigOption::availableTypesToString<Type>(),
                              " validator 1"));
      } else {
        option.addValidator(
            generateSingleParameterValidatorFunction<Type>(i),
            absl::StrCat(ConfigOption::availableTypesToString<Type>(),
                         " validator ", i));

        // Using the invariant of our function generator, to create valid and
        // none valid values for all added validators.
        for (size_t validatorNumber = 1; validatorNumber <= i;
             validatorNumber++) {
          checkSet(option, createDummyValueForValidator<Type>(i + 1),
                   createDummyValueForValidator<Type>(validatorNumber),
                   absl::StrCat(ConfigOption::availableTypesToString<Type>(),
                                " validator ", validatorNumber));
        }
      }
    }
  };

  // Test everything.
  doForTypeInConfigOptionValueType(doTest);
}

// Most errors, when adding a validator function, are found at compile time, but
// a few are not.
TEST(ConfigOptionTest, AddValidatorExceptions) {
  // Creates a configuration option of the given type and checks, if adding a
  // valid validator, which takes the wrong type of argument, doens't work.
  auto doTest = []<typename Type>() {
    Type var{};
    ConfigOption option("Test", "", &var);

    // Try adding a validator for every type, that configuration options
    // support.
    doForTypeInConfigOptionValueType([&option]<typename T>() {
      const std::string validatorName =
          absl::StrCat(ConfigOption::availableTypesToString<T>(), " validator");
      auto validatorFunction = [](const T&) { return true; };

      if constexpr (std::regular_invocable<decltype(validatorFunction),
                                           const Type&>) {
        // Right type shouldn't throw any erros. Note: Implicit conversion makes
        // this a bit more complicated.
        ASSERT_NO_THROW(option.addValidator(validatorFunction, validatorName));
      } else {
        // Wrong type should throw an erro.
        AD_EXPECT_THROW_WITH_MESSAGE(
            option.addValidator(validatorFunction, validatorName),
            ::testing::ContainsRegex(
                "Adding of validator to configuration option 'Test' failed."));
      }
    });
  };

  doForTypeInConfigOptionValueType(doTest);
}
}  // namespace ad_utility
