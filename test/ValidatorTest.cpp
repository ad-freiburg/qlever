// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (September of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <concepts>
#include <functional>
#include <optional>
#include <tuple>

#include "../test/util/GTestHelpers.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/ConfigManager/Validator.h"
#include "util/ValidatorHelpers.h"

namespace ad_utility {

/*
@brief Call the given template function with the cartesian product of the
parameter type list with itself, as template parameters. For example: If given
`<int, const int>`, then the function will be called as `Func<int, int>`,
`Func<int, const int>`, `Func<const int, int>` and `Func<const int, const int>`.
*/
template <typename Func, typename... Parameter>
constexpr void passCartesianPorductToLambda(Func func) {
  (
      [&func]<typename T>() {
        (func.template operator()<T, Parameter>(), ...);
      }.template operator()<Parameter>(),
      ...);
}

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
which manages a validator, that implements the logical `and` on those bools,
whose error message is the given error message and whose description is the
given description. The function signature should look like this: `void
func(std::string errorMessage, std::string descriptor,
std::same_as<ConstConfigOptionProxy<bool>> auto... args)`.
*/
void doConstructorTest(
    auto generateValidatorManager,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "doConstructorTest")};

  // Options for the validators.
  bool bool1;
  bool bool2;
  ConfigOption opt1("Option1", "", &bool1, std::make_optional(true));
  ConfigOption opt2("Option2", "", &bool2, std::make_optional(true));
  ConstConfigOptionProxy<bool> proxy1(opt1);
  ConstConfigOptionProxy<bool> proxy2(opt2);

  // Single argument validator.
  const std::string singleArgumentValidatorDescriptor =
      "This is the validator with a single argument.";
  ConfigOptionValidatorManager singleArgumentValidatorManager{
      generateValidatorManager("singleArgumentValidator",
                               singleArgumentValidatorDescriptor, proxy1)};
  ASSERT_STREQ(singleArgumentValidatorDescriptor.data(),
               singleArgumentValidatorManager.getDescription().data());
  ASSERT_NO_THROW(singleArgumentValidatorManager.checkValidator());
  opt1.setValue(false);
  AD_EXPECT_THROW_WITH_MESSAGE(
      singleArgumentValidatorManager.checkValidator(),
      ::testing::ContainsRegex("singleArgumentValidator"));
  ASSERT_ANY_THROW(singleArgumentValidatorManager.checkValidator());

  // Double argument validator.
  const std::string doubleArgumentValidatorDescriptor =
      "This is the validator with two arguments.";
  ConfigOptionValidatorManager doubleArgumentValidatorManager{
      generateValidatorManager("doubleArgumentValidatorManager",
                               doubleArgumentValidatorDescriptor, proxy1,
                               proxy2)};
  ASSERT_STREQ(doubleArgumentValidatorDescriptor.data(),
               doubleArgumentValidatorManager.getDescription().data());
  opt1.setValue(true);
  ASSERT_NO_THROW(doubleArgumentValidatorManager.checkValidator());
  opt1.setValue(false);
  AD_EXPECT_THROW_WITH_MESSAGE(
      doubleArgumentValidatorManager.checkValidator(),
      ::testing::ContainsRegex("doubleArgumentValidator"));
  opt2.setValue(false);
  AD_EXPECT_THROW_WITH_MESSAGE(
      doubleArgumentValidatorManager.checkValidator(),
      ::testing::ContainsRegex("doubleArgumentValidator"));
  opt1.setValue(true);
  AD_EXPECT_THROW_WITH_MESSAGE(
      doubleArgumentValidatorManager.checkValidator(),
      ::testing::ContainsRegex("doubleArgumentValidator"));
}

TEST(ConfigOptionValidatorManagerTest, ExceptionValidatorConstructor) {
  doConstructorTest(
      [](std::string errorMessage, std::string descriptor,
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
            std::move(descriptor),
            [](ConstConfigOptionProxy<bool> p) {
              return p.getConfigOption().getValue<bool>();
            },
            args...);
      });
}

TEST(ConfigOptionValidatorManagerTest, ValidatorConstructor) {
  doConstructorTest(
      [](std::string errorMessage, std::string descriptor,
         std::same_as<ConstConfigOptionProxy<bool>> auto... args) {
        return ConfigOptionValidatorManager(
            [](const std::same_as<bool> auto... b) { return (b && ...); },
            std::move(errorMessage), std::move(descriptor),
            [](ConstConfigOptionProxy<bool> p) {
              return p.getConfigOption().getValue<bool>();
            },
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
