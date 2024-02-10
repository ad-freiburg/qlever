// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (September of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <concepts>
#include <functional>
#include <optional>
#include <tuple>
#include <unordered_set>

#include "../test/util/GTestHelpers.h"
#include "../test/util/TypeTraitsTestHelpers.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/ConfigManager/Validator.h"
#include "util/ValidatorHelpers.h"

namespace ad_utility {

TEST(ValidatorConceptTest, ValidatorConcept) {
  // Lambda function types for easier test creation.
  using SingleIntValidatorFunction = decltype([](const int&) { return true; });
  using DoubleIntValidatorFunction =
      decltype([](const int&, const int&) { return true; });

  // Valid function.
  static_assert(ValidatorFunction<SingleIntValidatorFunction, int>);
  static_assert(ValidatorFunction<DoubleIntValidatorFunction, int, int>);

  // The number of parameter types is wrong.
  static_assert(!ValidatorFunction<SingleIntValidatorFunction>);
  static_assert(!ValidatorFunction<SingleIntValidatorFunction, int, int>);
  static_assert(!ValidatorFunction<DoubleIntValidatorFunction>);
  static_assert(
      !ValidatorFunction<DoubleIntValidatorFunction, int, int, int, int>);

  // Function is valid, but the parameter types are of the wrong object type.
  static_assert(
      !ValidatorFunction<SingleIntValidatorFunction, std::vector<bool>>);
  static_assert(!ValidatorFunction<SingleIntValidatorFunction, std::string>);
  static_assert(
      !ValidatorFunction<DoubleIntValidatorFunction, std::vector<bool>, int>);
  static_assert(
      !ValidatorFunction<DoubleIntValidatorFunction, int, std::vector<bool>>);
  static_assert(!ValidatorFunction<DoubleIntValidatorFunction,
                                   std::vector<bool>, std::vector<bool>>);
  static_assert(
      !ValidatorFunction<DoubleIntValidatorFunction, std::string, int>);
  static_assert(
      !ValidatorFunction<DoubleIntValidatorFunction, int, std::string>);
  static_assert(
      !ValidatorFunction<DoubleIntValidatorFunction, std::string, std::string>);

  // The given function is not valid.

  // The parameter types of the function are wrong, but the return type is
  // correct.
  static_assert(!ValidatorFunction<decltype([](int&) { return true; }), int>);
  static_assert(!ValidatorFunction<decltype([](int&&) { return true; }), int>);
  static_assert(
      !ValidatorFunction<decltype([](const int&&) { return true; }), int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ValidatorFunction<decltype([](FirstParameter, SecondParameter) {
                                 return true;
                               }),
                               int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper);

  // Parameter types are correct, but return type is wrong.
  static_assert(!ValidatorFunction<decltype([](int n) { return n; }), int>);
  static_assert(
      !ValidatorFunction<decltype([](const int n) { return n; }), int>);
  static_assert(
      !ValidatorFunction<decltype([](const int& n) { return n; }), int>);

  auto validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ValidatorFunction<decltype([](FirstParameter n, SecondParameter) {
                                 return n;
                               }),
                               int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper),
      int, const int, const int&>(
      validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper);

  // Both the parameter types and the return type is wrong.
  static_assert(!ValidatorFunction<decltype([](int& n) { return n; }), int>);
  static_assert(!ValidatorFunction<decltype([](int&& n) { return n; }), int>);
  static_assert(
      !ValidatorFunction<decltype([](const int&& n) { return n; }), int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ValidatorFunction<decltype([](FirstParameter n, SecondParameter) {
                                 return n;
                               }),
                               int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper);
}

TEST(ExceptionValidatorConceptTest, ExceptionValidatorConcept) {
  // Lambda function types for easier test creation.
  using SingleIntExceptionValidatorFunction =
      decltype([](const int&) { return std::optional<ErrorMessage>{}; });
  using DoubleIntExceptionValidatorFunction =
      decltype([](const int&, const int&) {
        return std::optional<ErrorMessage>{};
      });

  // Valid function.
  static_assert(
      ExceptionValidatorFunction<SingleIntExceptionValidatorFunction, int>);
  static_assert(ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                           int, int>);

  // The number of parameter types is wrong.
  static_assert(
      !ExceptionValidatorFunction<SingleIntExceptionValidatorFunction>);
  static_assert(!ExceptionValidatorFunction<SingleIntExceptionValidatorFunction,
                                            int, int>);
  static_assert(
      !ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction>);
  static_assert(!ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                            int, int, int, int>);

  // Function is valid, but the parameter types are of the wrong object type.
  static_assert(!ExceptionValidatorFunction<SingleIntExceptionValidatorFunction,
                                            std::vector<bool>>);
  static_assert(!ExceptionValidatorFunction<SingleIntExceptionValidatorFunction,
                                            std::string>);
  static_assert(!ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                            std::vector<bool>, int>);
  static_assert(!ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                            int, std::vector<bool>>);
  static_assert(
      !ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                  std::vector<bool>, std::vector<bool>>);
  static_assert(!ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                            std::string, int>);
  static_assert(!ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                            int, std::string>);
  static_assert(!ExceptionValidatorFunction<DoubleIntExceptionValidatorFunction,
                                            std::string, std::string>);

  // The given function is not valid.

  // The parameter types of the function are wrong, but the return type is
  // correct.
  static_assert(
      !ExceptionValidatorFunction<
          decltype([](int&) { return std::optional<ErrorMessage>{}; }), int>);
  static_assert(
      !ExceptionValidatorFunction<
          decltype([](int&&) { return std::optional<ErrorMessage>{}; }), int>);
  static_assert(
      !ExceptionValidatorFunction<decltype([](const int&&) {
                                    return std::optional<ErrorMessage>{};
                                  }),
                                  int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(!ExceptionValidatorFunction<
                      decltype([](FirstParameter, SecondParameter) {
                        return std::optional<ErrorMessage>{};
                      }),
                      int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper);

  // Parameter types are correct, but return type is wrong.
  static_assert(
      !ExceptionValidatorFunction<decltype([](int n) { return n; }), int>);
  static_assert(
      !ExceptionValidatorFunction<decltype([](const int n) { return n; }),
                                  int>);
  static_assert(
      !ExceptionValidatorFunction<decltype([](const int& n) { return n; }),
                                  int>);

  auto validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ExceptionValidatorFunction<
                decltype([](FirstParameter n, SecondParameter) { return n; }),
                int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper),
      int, const int, const int&>(
      validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper);

  // Both the parameter types and the return type is wrong.
  static_assert(
      !ExceptionValidatorFunction<decltype([](int& n) { return n; }), int>);
  static_assert(
      !ExceptionValidatorFunction<decltype([](int&& n) { return n; }), int>);
  static_assert(
      !ExceptionValidatorFunction<decltype([](const int&& n) { return n; }),
                                  int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ValidatorFunction<decltype([](FirstParameter n, SecondParameter) {
                                 return n;
                               }),
                               int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper);
}

/*
@brief Does the test for the constructors.

@param addAlwaysValidValidatorFunction A function, that generates a
`ConfigOptionValidatorManager` for the given `ConstConfigOptionProxy<bool>`,
which manages a validator, that implements the logical `and` on the transformed
`ConstConfigOptionProxy<bool>`, whose error message is the given error message
and whose description is the given description. The function signature should
look like this: `void func(std::string errorMessage, std::string descriptor,
auto translationFunction, std::same_as<ConstConfigOptionProxy<bool>> auto...
args)`.
*/
void doConstructorTest(
    auto generateValidatorManager,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "doConstructorTest")};

  // This helper function checks, if the given `ConfigOptionValidatorManager`
  // checks the given `ConfigOption`.
  auto checkContainsOptions =
      [](const ConfigOptionValidatorManager& manager,
         const ad_utility::HashSet<const ConfigOption*>& expectedConfigOption) {
        ASSERT_EQ(manager.configOptionToBeChecked(), expectedConfigOption);
      };

  // This translation function returns the internal value of the configuration
  // option.
  auto getValueTranslation = [](ConstConfigOptionProxy<bool> p) {
    return p.getConfigOption().getValue<bool>();
  };

  // This translation function returns true, iff., the given configuration
  // option was set.
  auto wasSetTranslation = [](ConstConfigOptionProxy<bool> p) {
    return p.getConfigOption().wasSet();
  };

  // Options for the validators.
  bool bool1;
  bool bool2;
  ConfigOption opt1("Option1", "", &bool1);
  ConfigOption opt2("Option2", "", &bool2);
  ConstConfigOptionProxy<bool> proxy1(opt1);
  ConstConfigOptionProxy<bool> proxy2(opt2);

  const std::string singleArgumentValidatorDescriptor =
      "This is the validator with a single argument.";
  // Single argument validator with `wasSetTranslation`.
  ConfigOptionValidatorManager
      singleArgumentValidatorWithWasSetTranslatorManager{
          generateValidatorManager(
              "singleArgumentValidatorWithwasSetTranslation",
              singleArgumentValidatorDescriptor, wasSetTranslation, proxy1)};

  // Single argument validator with `getValueTranslation`.
  ConfigOptionValidatorManager
      singleArgumentValidatorWithGetValueTranslatorManager{
          generateValidatorManager(
              "singleArgumentValidatorWithgetValueTranslation",
              singleArgumentValidatorDescriptor, getValueTranslation, proxy1)};

  // The options, both single argument validators should be checking.
  const ad_utility::HashSet<const ConfigOption*> singleArgumentValidatorOptions{
      &opt1};

  // Check the single argument validators.
  ASSERT_STREQ(
      singleArgumentValidatorDescriptor.data(),
      singleArgumentValidatorWithWasSetTranslatorManager.getDescription()
          .data());
  checkContainsOptions(singleArgumentValidatorWithWasSetTranslatorManager,
                       singleArgumentValidatorOptions);
  ASSERT_STREQ(
      singleArgumentValidatorDescriptor.data(),
      singleArgumentValidatorWithGetValueTranslatorManager.getDescription()
          .data());
  checkContainsOptions(singleArgumentValidatorWithGetValueTranslatorManager,
                       singleArgumentValidatorOptions);
  AD_EXPECT_THROW_WITH_MESSAGE(
      singleArgumentValidatorWithWasSetTranslatorManager.checkValidator(),
      ::testing::ContainsRegex("singleArgumentValidatorWithwasSetTranslation"));
  opt1.setValue(true);
  ASSERT_NO_THROW(
      singleArgumentValidatorWithGetValueTranslatorManager.checkValidator());
  ASSERT_NO_THROW(
      singleArgumentValidatorWithWasSetTranslatorManager.checkValidator());
  opt1.setValue(false);
  AD_EXPECT_THROW_WITH_MESSAGE(
      singleArgumentValidatorWithGetValueTranslatorManager.checkValidator(),
      ::testing::ContainsRegex(
          "singleArgumentValidatorWithgetValueTranslation"));
  ASSERT_NO_THROW(
      singleArgumentValidatorWithWasSetTranslatorManager.checkValidator());

  const std::string doubleArgumentValidatorDescriptor =
      "This is the validator with two arguments.";
  // Double argument validator with `wasSetTranslation`
  ConfigOptionValidatorManager
      doubleArgumentValidatorWithWasSetTranlatorManager{
          generateValidatorManager(
              "doubleArgumentValidatorManagerWithwasSetTranslation",
              doubleArgumentValidatorDescriptor, wasSetTranslation, proxy1,
              proxy2)};

  // Double argument validator with `getValueTranslation`
  ConfigOptionValidatorManager
      doubleArgumentValidatorWithGetValueTranslatorManager{
          generateValidatorManager(
              "doubleArgumentValidatorManagerWithgetValueTranslation",
              doubleArgumentValidatorDescriptor, getValueTranslation, proxy1,
              proxy2)};

  // The options, both double argument validators should be checking.
  const ad_utility::HashSet<const ConfigOption*> doubleArgumentValidatorOptions{
      &opt1, &opt2};

  // Check the double argument validators.
  ASSERT_STREQ(
      doubleArgumentValidatorDescriptor.data(),
      doubleArgumentValidatorWithGetValueTranslatorManager.getDescription()
          .data());
  checkContainsOptions(doubleArgumentValidatorWithGetValueTranslatorManager,
                       doubleArgumentValidatorOptions);
  ASSERT_STREQ(
      doubleArgumentValidatorDescriptor.data(),
      doubleArgumentValidatorWithWasSetTranlatorManager.getDescription()
          .data());
  checkContainsOptions(doubleArgumentValidatorWithWasSetTranlatorManager,
                       doubleArgumentValidatorOptions);
  AD_EXPECT_THROW_WITH_MESSAGE(
      doubleArgumentValidatorWithWasSetTranlatorManager.checkValidator(),
      ::testing::ContainsRegex(
          "doubleArgumentValidatorManagerWithwasSetTranslation"));
  opt1.setValue(true);
  opt2.setValue(true);
  ASSERT_NO_THROW(
      doubleArgumentValidatorWithGetValueTranslatorManager.checkValidator());
  ASSERT_NO_THROW(
      doubleArgumentValidatorWithWasSetTranlatorManager.checkValidator());
  opt1.setValue(false);
  AD_EXPECT_THROW_WITH_MESSAGE(
      doubleArgumentValidatorWithGetValueTranslatorManager.checkValidator(),
      ::testing::ContainsRegex(
          "doubleArgumentValidatorManagerWithgetValueTranslation"));
  opt2.setValue(false);
  AD_EXPECT_THROW_WITH_MESSAGE(
      doubleArgumentValidatorWithGetValueTranslatorManager.checkValidator(),
      ::testing::ContainsRegex(
          "doubleArgumentValidatorManagerWithgetValueTranslation"));
  opt1.setValue(true);
  AD_EXPECT_THROW_WITH_MESSAGE(
      doubleArgumentValidatorWithGetValueTranslatorManager.checkValidator(),
      ::testing::ContainsRegex(
          "doubleArgumentValidatorManagerWithgetValueTranslation"));
}

TEST(ConfigOptionValidatorManagerTest, ExceptionValidatorConstructor) {
  doConstructorTest(
      [](std::string errorMessage, std::string descriptor,
         const auto& translationFunction,
         std::same_as<ConstConfigOptionProxy<bool>> auto... args) {
        return ConfigOptionValidatorManager(
            [errorMessage =
                 std::move(errorMessage)](const std::same_as<bool> auto... b)
                -> std::optional<ErrorMessage> {
              if ((b && ...)) {
                return std::nullopt;
              } else {
                return ErrorMessage{errorMessage};
              };
            },
            std::move(descriptor), translationFunction, args...);
      });
}

TEST(ConfigOptionValidatorManagerTest, ValidatorConstructor) {
  doConstructorTest(
      [](std::string errorMessage, std::string descriptor,
         const auto& translationFunction,
         std::same_as<ConstConfigOptionProxy<bool>> auto... args) {
        return ConfigOptionValidatorManager(
            [](const std::same_as<bool> auto... b) { return (b && ...); },
            std::move(errorMessage), std::move(descriptor), translationFunction,
            args...);
      });
}

// Rather basic test, if things behave as wanted with the helper function.
TEST(ValidatorConceptTest, TransformValidatorIntoExceptionValidator) {
  // Helper function, that check, if a given validator behaves as
  // wanted, before and after being transformed into an exception validator.
  auto checkValidator = []<typename... ParameterTypes>(
                            ValidatorFunction<ParameterTypes...> auto func,
                            const std::tuple<ParameterTypes...>& validValues,
                            const std::tuple<ParameterTypes...>& nonValidValues,
                            ad_utility::source_location l =
                                ad_utility::source_location::current()) {
    // For generating better messages, when failing a test.
    auto trace{generateLocationTrace(l, "checkValidator")};

    ASSERT_TRUE(std::apply(func, validValues));
    ASSERT_FALSE(std::apply(func, nonValidValues));

    // Transform and check.
    auto transformedFunc =
        transformValidatorIntoExceptionValidator<ParameterTypes...>(func,
                                                                    "test");
    static_assert(ExceptionValidatorFunction<decltype(transformedFunc),
                                             ParameterTypes...>);
    ASSERT_STREQ(std::apply(transformedFunc, nonValidValues)
                     .value_or(ErrorMessage{""})
                     .getMessage()
                     .c_str(),
                 "test");
    ASSERT_FALSE(std::apply(transformedFunc, validValues).has_value());
  };

  // Test with a few generated validators..
  checkValidator(generateDummyNonExceptionValidatorFunction<bool>(0),
                 std::make_tuple(false), std::make_tuple(true));
  checkValidator(generateDummyNonExceptionValidatorFunction<int>(0),
                 std::make_tuple(createDummyValueForValidator<int>(1)),
                 std::make_tuple(createDummyValueForValidator<int>(0)));
  checkValidator(
      generateDummyNonExceptionValidatorFunction<float, size_t,
                                                 std::vector<std::string>>(0),
      std::make_tuple(
          createDummyValueForValidator<float>(1),
          createDummyValueForValidator<size_t>(1),
          createDummyValueForValidator<std::vector<std::string>>(1)),
      std::make_tuple(
          createDummyValueForValidator<float>(0),
          createDummyValueForValidator<size_t>(0),
          createDummyValueForValidator<std::vector<std::string>>(0)));
}
}  // namespace ad_utility
