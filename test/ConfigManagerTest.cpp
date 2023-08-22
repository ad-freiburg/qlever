// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstddef>
#include <type_traits>
#include <vector>

#include "./util/ConfigOptionHelpers.h"
#include "./util/GTestHelpers.h"
#include "gtest/gtest.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/json.h"

using namespace std::string_literals;

namespace ad_utility {

/*
@brief Checks, if the given configuration option was set correctly.

@param externalVariable The variable, that the given configuration option writes
to.
@param wasSet Was the given configuration option set?
@param wantedValue The value, that the configuration option should have been set
to.
*/
template <typename T>
void checkOption(ConstConfigOptionProxy<T> option, const T& externalVariable,
                 const bool wasSet, const T& wantedValue) {
  ASSERT_EQ(wasSet, option.getConfigOption().wasSet());

  if (wasSet) {
    ASSERT_EQ(wantedValue, option.getConfigOption().template getValue<T>());
    ASSERT_EQ(wantedValue, externalVariable);
  }
}

/*
The exceptions for adding configuration options.
*/
TEST(ConfigManagerTest, CreateConfigurationOptionExceptionTest) {
  ad_utility::ConfigManager config{};

  // Configuration options for testing.
  int notUsed;
  config.addOption({"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s},
                   "", &notUsed, 42);

  // Trying to add a configuration option with the same name at the same
  // place, should cause an error.
  ASSERT_THROW(config.addOption(
      {"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s}, "", &notUsed,
      42);
               , ad_utility::ConfigManagerOptionPathAlreadyinUseException);

  /*
  An empty vector that should cause an exception.
  Reason: The last key is used as the name for the to be created `ConfigOption`.
  An empty vector doesn't work with that.
  */
  ASSERT_ANY_THROW(
      config.addOption(std::vector<std::string>{}, "", &notUsed, 42););

  /*
  Trying to add a configuration option with a path containing strings with
  spaces should cause an error.
  Reason: A string with spaces in it, can't be read by the short hand
  configuration grammar. Ergo, you can't set values, with such paths per short
  hand, which we don't want.
  */
  ASSERT_THROW(config.addOption({"Shared part"s, "Sense_of_existence"s}, "",
                                &notUsed, 42);
               , ad_utility::NotValidShortHandNameException);
}

TEST(ConfigManagerTest, ParseConfig) {
  ad_utility::ConfigManager config{};

  // Adding the options.
  int firstInt;
  int secondInt;
  int thirdInt;

  decltype(auto) optionZero =
      config.addOption({"depth_0"s, "Option_0"s},
                       "Must be set. Has no default value.", &firstInt);
  decltype(auto) optionOne =
      config.addOption({"depth_0"s, "depth_1"s, "Option_1"s},
                       "Must be set. Has no default value.", &secondInt);
  decltype(auto) optionTwo =
      config.addOption("Option_2", "Has a default value.", &thirdInt, 2);

  // Does the option with the default already have a value?
  checkOption<int>(optionTwo, thirdInt, true, 2);

  // The other two should never have set the variable, that the internal pointer
  // points to.
  checkOption<int>(optionZero, firstInt, false, 2);
  checkOption<int>(optionOne, secondInt, false, 2);

  // The json for testing `parseConfig`. Sets all of the configuration
  // options.
  const nlohmann::json testJson(nlohmann::json::parse(R"--({
"depth_0": {
  "Option_0": 10,
  "depth_1": {
    "Option_1": 11
  }
},
"Option_2": 12
})--"));

  // Set and check.
  config.parseConfig(testJson);

  checkOption<int>(optionZero, firstInt, true, 10);
  checkOption<int>(optionOne, secondInt, true, 11);
  checkOption<int>(optionTwo, thirdInt, true, 12);
}

TEST(ConfigManagerTest, ParseConfigExceptionTest) {
  ad_utility::ConfigManager config{};

  // Add one option with default and one without.
  int notUsedInt;
  std::vector<int> notUsedVector;
  config.addOption({"depth_0"s, "Without_default"s},
                   "Must be set. Has no default value.", &notUsedInt);
  config.addOption({"depth_0"s, "With_default"s},
                   "Must not be set. Has default value.", &notUsedVector,
                   {40, 41});

  // Should throw an exception, if we don't set all options, that must be set.
  ASSERT_THROW(config.parseConfig(nlohmann::json::parse(R"--({})--")),
               ad_utility::ConfigOptionWasntSetException);

  // Should throw an exception, if we try set an option, that isn't there.
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"depth_0":{"Without_default":42, "with_default" : [39]}})--")),
      ::testing::ContainsRegex(R"('/depth_0/with_default')"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"depth_0":{"Without_default":42, "test_string" : "test"}})--")),
      ::testing::ContainsRegex(R"('/depth_0/test_string')"));

  /*
  Should throw an exception, if we try set an option with a value, that we
  already know, can't be valid, regardless of the actual internal type of the
  configuration option. That is, it's neither an array, or a primitive.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"depth_0":{"Without_default":42, "With_default" : {"value" : 4}}})--")),
      ::testing::ContainsRegex(R"('/depth_0/With_default/value')"));

  // Parsing with a non json object literal is not allowed.
  ASSERT_THROW(
      config.parseConfig(nlohmann::json(nlohmann::json::value_t::array)),
      ConfigManagerParseConfigNotJsonObjectLiteralException);
  ASSERT_THROW(
      config.parseConfig(nlohmann::json(nlohmann::json::value_t::boolean)),
      ConfigManagerParseConfigNotJsonObjectLiteralException);
  ASSERT_THROW(
      config.parseConfig(nlohmann::json(nlohmann::json::value_t::null)),
      ConfigManagerParseConfigNotJsonObjectLiteralException);
  ASSERT_THROW(
      config.parseConfig(nlohmann::json(nlohmann::json::value_t::number_float)),
      ConfigManagerParseConfigNotJsonObjectLiteralException);
  ASSERT_THROW(config.parseConfig(
                   nlohmann::json(nlohmann::json::value_t::number_integer)),
               ConfigManagerParseConfigNotJsonObjectLiteralException);
  ASSERT_THROW(config.parseConfig(
                   nlohmann::json(nlohmann::json::value_t::number_unsigned)),
               ConfigManagerParseConfigNotJsonObjectLiteralException);
  ASSERT_THROW(
      config.parseConfig(nlohmann::json(nlohmann::json::value_t::string)),
      ConfigManagerParseConfigNotJsonObjectLiteralException);
}

TEST(ConfigManagerTest, ParseShortHandTest) {
  ad_utility::ConfigManager config{};

  // Add integer options.
  int somePositiveNumberInt;
  decltype(auto) somePositiveNumber = config.addOption(
      "somePositiveNumber", "Must be set. Has no default value.",
      &somePositiveNumberInt);
  int someNegativNumberInt;
  decltype(auto) someNegativNumber = config.addOption(
      "someNegativNumber", "Must be set. Has no default value.",
      &someNegativNumberInt);

  // Add integer list.
  std::vector<int> someIntegerlistIntVector;
  decltype(auto) someIntegerlist =
      config.addOption("someIntegerlist", "Must be set. Has no default value.",
                       &someIntegerlistIntVector);

  // Add floating point options.
  float somePositiveFloatingPointFloat;
  decltype(auto) somePositiveFloatingPoint = config.addOption(
      "somePositiveFloatingPoint", "Must be set. Has no default value.",
      &somePositiveFloatingPointFloat);
  float someNegativFloatingPointFloat;
  decltype(auto) someNegativFloatingPoint = config.addOption(
      "someNegativFloatingPoint", "Must be set. Has no default value.",
      &someNegativFloatingPointFloat);

  // Add floating point list.
  std::vector<float> someFloatingPointListFloatVector;
  decltype(auto) someFloatingPointList = config.addOption(
      "someFloatingPointList", "Must be set. Has no default value.",
      &someFloatingPointListFloatVector);

  // Add boolean options.
  bool boolTrueBool;
  decltype(auto) boolTrue = config.addOption(
      "boolTrue", "Must be set. Has no default value.", &boolTrueBool);
  bool boolFalseBool;
  decltype(auto) boolFalse = config.addOption(
      "boolFalse", "Must be set. Has no default value.", &boolFalseBool);

  // Add boolean list.
  std::vector<bool> someBooleanListBoolVector;
  decltype(auto) someBooleanList =
      config.addOption("someBooleanList", "Must be set. Has no default value.",
                       &someBooleanListBoolVector);

  // Add string option.
  std::string myNameString;
  decltype(auto) myName = config.addOption(
      "myName", "Must be set. Has no default value.", &myNameString);

  // Add string list.
  std::vector<std::string> someStringListStringVector;
  decltype(auto) someStringList =
      config.addOption("someStringList", "Must be set. Has no default value.",
                       &someStringListStringVector);

  // Add option with deeper level.
  std::vector<int> deeperIntVector;
  decltype(auto) deeperIntVectorOption =
      config.addOption({"depth"s, "here"s, "list"s},
                       "Must be set. Has no default value.", &deeperIntVector);

  // This one will not be changed, in order to test, that options, that are
  // not set at run time, are not changed.
  int noChangeInt;
  decltype(auto) noChange = config.addOption("No_change", "", &noChangeInt, 10);

  // Set those.
  config.parseConfig(ad_utility::ConfigManager::parseShortHand(
      R"--(somePositiveNumber : 42, someNegativNumber : -42, someIntegerlist : [40, 41], somePositiveFloatingPoint : 4.2, someNegativFloatingPoint : -4.2, someFloatingPointList : [4.1, 4.2], boolTrue : true, boolFalse : false, someBooleanList : [true, false, true], myName : "Bernd", someStringList : ["t1", "t2"], depth : { here : {list : [7,8]}})--"));

  checkOption<int>(somePositiveNumber, somePositiveNumberInt, true, 42);
  checkOption<int>(someNegativNumber, someNegativNumberInt, true, -42);

  checkOption<std::vector<int>>(someIntegerlist, someIntegerlistIntVector, true,
                                std::vector{40, 41});

  checkOption<float>(somePositiveFloatingPoint, somePositiveFloatingPointFloat,
                     true, 4.2f);
  checkOption<float>(someNegativFloatingPoint, someNegativFloatingPointFloat,
                     true, -4.2f);

  checkOption<std::vector<float>>(someFloatingPointList,
                                  someFloatingPointListFloatVector, true,
                                  {4.1f, 4.2f});

  checkOption<bool>(boolTrue, boolTrueBool, true, true);
  checkOption<bool>(boolFalse, boolFalseBool, true, false);

  checkOption<std::vector<bool>>(someBooleanList, someBooleanListBoolVector,
                                 true, std::vector{true, false, true});

  checkOption<std::string>(myName, myNameString, true, std::string{"Bernd"});

  checkOption<std::vector<std::string>>(someStringList,
                                        someStringListStringVector, true,
                                        std::vector<std::string>{"t1", "t2"});

  checkOption<std::vector<int>>(deeperIntVectorOption, deeperIntVector, true,
                                std::vector{7, 8});

  // Is the "No Change" unchanged?
  checkOption<int>(noChange, noChangeInt, true, 10);

  // Multiple key value pairs with the same key are not allowed.
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::ConfigManager::parseShortHand(
                                   R"(complicatedKey:42, complicatedKey:43)");
                               , ::testing::ContainsRegex("'complicatedKey'"));

  // Final test: Is there an exception, if we try to parse the wrong syntax?
  ASSERT_ANY_THROW(ad_utility::ConfigManager::parseShortHand(
      R"--({"myName" : "Bernd")})--"));
  ASSERT_ANY_THROW(
      ad_utility::ConfigManager::parseShortHand(R"--("myName" = "Bernd";)--"));
}

TEST(ConfigManagerTest, PrintConfigurationDocExistence) {
  ConfigManager config{};

  // Can you print an empty one?
  ASSERT_NO_THROW(config.printConfigurationDoc(false));
  ASSERT_NO_THROW(config.printConfigurationDoc(true));

  // Can you print a non-empty one?
  int notUsed;
  config.addOption("WithDefault", "", &notUsed, 42);
  config.addOption("WithoutDefault", "", &notUsed);
  ASSERT_NO_THROW(config.printConfigurationDoc(false));
  ASSERT_NO_THROW(config.printConfigurationDoc(true));
}

/*
@brief Quick check, if `parseConfig` works with the registered validators.

@param jsonWithValidValues When parsing this, no exceptions should be thrown.
@param jsonWithNonValidValues When parsing this, an exception should be thrown.
@param containedInExpectedErrorMessage What kind of text should be contained in
the exception thrown for `jsonWithNonValidValues`. Validators have custom
exception messages, so they should be identifiable.
*/
void checkValidator(ConfigManager& manager,
                    const nlohmann::json& jsonWithValidValues,
                    const nlohmann::json& jsonWithNonValidValues,
                    std::string_view containedInExpectedErrorMessage) {
  ASSERT_NO_THROW(manager.parseConfig(jsonWithValidValues));
  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.parseConfig(jsonWithNonValidValues),
      ::testing::ContainsRegex(containedInExpectedErrorMessage));
}

// Human readable examples for `addValidator`.
TEST(ConfigManagerTest, HumanReadableAddValidator) {
  ConfigManager m{};

  // The number of the option should be in a range. We define the range by using
  // two validators.
  int someInt;
  decltype(auto) numberInRangeOption =
      m.addOption("numberInRange", "", &someInt);
  m.addValidator([](const int& num) { return num <= 100; },
                 "'numberInRange' must be <=100.", numberInRangeOption);
  m.addValidator([](const int& num) { return num > 49; },
                 "'numberInRange' must be >=50.", numberInRangeOption);
  checkValidator(m, nlohmann::json::parse(R"--({"numberInRange" : 60})--"),
                 nlohmann::json::parse(R"--({"numberInRange" : 101})--"),
                 "'numberInRange' must be <=100.");
  checkValidator(m, nlohmann::json::parse(R"--({"numberInRange" : 60})--"),
                 nlohmann::json::parse(R"--({"numberInRange" : 42})--"),
                 "'numberInRange' must be >=50.");

  // Only one of the bools should be true.
  bool boolOne;
  decltype(auto) boolOneOption = m.addOption("boolOne", "", &boolOne, false);
  bool boolTwo;
  decltype(auto) boolTwoOption = m.addOption("boolTwo", "", &boolTwo, false);
  bool boolThree;
  decltype(auto) boolThreeOption =
      m.addOption("boolThree", "", &boolThree, false);
  m.addValidator(
      [](bool one, bool two, bool three) {
        return (one && !two && !three) || (!one && two && !three) ||
               (!one && !two && three);
      },
      "Exactly one bool must be choosen.", boolOneOption, boolTwoOption,
      boolThreeOption);
  checkValidator(
      m,
      nlohmann::json::parse(
          R"--({"numberInRange" : 60, "boolOne": true, "boolTwo": false, "boolThree": false})--"),
      nlohmann::json::parse(
          R"--({"numberInRange" : 60, "boolOne": true, "boolTwo": true, "boolThree": false})--"),
      "Exactly one bool must be choosen.");
}

// Human readable examples for `addOptionValidator`.
TEST(ConfigManagerTest, HumanReadableAddOptionValidator) {
  // Check, if all the options have a default value.
  ConfigManager mAllWithDefault;
  int firstInt;
  decltype(auto) firstOption =
      mAllWithDefault.addOption("firstOption", "", &firstInt, 10);
  mAllWithDefault.addOptionValidator(
      [](const ConfigOption& opt) { return opt.hasDefaultValue(); },
      "Every option must have a default value.", firstOption);
  ASSERT_NO_THROW(mAllWithDefault.parseConfig(
      nlohmann::json::parse(R"--({"firstOption": 4})--")));
  int secondInt;
  decltype(auto) secondOption =
      mAllWithDefault.addOption("secondOption", "", &secondInt);
  mAllWithDefault.addOptionValidator(
      [](const ConfigOption& opt1, const ConfigOption& opt2) {
        return opt1.hasDefaultValue() && opt2.hasDefaultValue();
      },
      "Every option must have a default value.", firstOption, secondOption);
  ASSERT_ANY_THROW(mAllWithDefault.parseConfig(
      nlohmann::json::parse(R"--({"firstOption": 4, "secondOption" : 7})--")));

  // We want, that all options names start with the letter `d`.
  ConfigManager mFirstLetter;
  decltype(auto) correctLetter =
      mFirstLetter.addOption("dValue", "", &firstInt);
  mFirstLetter.addOptionValidator(
      [](const ConfigOption& opt) {
        return opt.getIdentifier().starts_with('d');
      },
      "Every option name must start with the letter d.", correctLetter);
  ASSERT_NO_THROW(
      mFirstLetter.parseConfig(nlohmann::json::parse(R"--({"dValue": 4})--")));
  decltype(auto) wrongLetter = mFirstLetter.addOption("value", "", &secondInt);
  mFirstLetter.addOptionValidator(
      [](const ConfigOption& opt1, const ConfigOption& opt2) {
        return opt1.getIdentifier().starts_with('d') &&
               opt2.getIdentifier().starts_with('d');
      },
      "Every option name must start with the letter d.", correctLetter,
      wrongLetter);
  ASSERT_ANY_THROW(mFirstLetter.parseConfig(
      nlohmann::json::parse(R"--({"dValue": 4, "Value" : 7})--")));
}

/*
@brief Generate a value of the given type. Used for generating test values in
cooperation with `generateSingleParameterValidatorFunction`, while keeping the
invariant of `generateValidatorFunction` true.
`variant` slightly changes the returned value.
*/
template <typename Type>
Type createDummyValueForValidator(size_t variant) {
  if constexpr (std::is_same_v<Type, bool>) {
    return variant % 2;
  } else if constexpr (std::is_same_v<Type, std::string>) {
    // Create a string counting up to `variant`.
    std::ostringstream stream;
    for (size_t i = 0; i <= variant; i++) {
      stream << i;
    }
    return stream.str();
  } else if constexpr (std::is_same_v<Type, int> ||
                       std::is_same_v<Type, size_t>) {
    // Return uneven numbers.
    return static_cast<Type>(variant) * 2 + 1;
  } else if constexpr (std::is_same_v<Type, float>) {
    return 43.70137416518735163649172636491957f * static_cast<Type>(variant);
  } else {
    // Must be a vector and we can go recursive.
    AD_CORRECTNESS_CHECK(ad_utility::isVector<Type>);
    Type vec;
    vec.reserve(variant + 1);
    for (size_t i = 0; i <= variant; i++) {
      vec.push_back(createDummyValueForValidator<typename Type::value_type>(i));
    }
    return vec;
  }
};

/*
@brief For easily creating single parameter validator functions, that compare
given values to values created using the `createDummyValueForValidator`
function.

The following invariant should always be true, except for `bool`: A validator
function with `variant` number $x$ returns false, when given
`createDummyValue(x)`. Otherwise, it always returns true.

@tparam ParameterType The parameter type for the parameter of the
function.

@param variant Changes the generated function slightly. Allows the easier
creation of multiple different validator functions. For more information,
what the exact difference is, see the code.
*/
template <typename ParameterType>
auto generateSingleParameterValidatorFunction(size_t variant) {
  if constexpr (std::is_same_v<ParameterType, bool> ||
                std::is_same_v<ParameterType, std::string> ||
                std::is_same_v<ParameterType, int> ||
                std::is_same_v<ParameterType, size_t> ||
                std::is_same_v<ParameterType, float>) {
    return [compareTo = createDummyValueForValidator<ParameterType>(variant)](
               const ParameterType& n) { return n != compareTo; };
  } else {
    // Must be a vector and we can go recursive.
    AD_CORRECTNESS_CHECK(ad_utility::isVector<ParameterType>);

    // Just call the validator function for the non-vector version of
    // the types on the elements of the vector.
    return [variant](const ParameterType& v) {
      if (v.size() != variant + 1) {
        return true;
      }

      for (size_t i = 0; i < variant + 1; i++) {
        if (generateSingleParameterValidatorFunction<
                typename ParameterType::value_type>(i)(v.at(i))) {
          return true;
        }
      }

      return false;
    };
  }
};

TEST(ConfigManagerTest, AddValidator) {
  /*
  @brief Call the given lambda with all possible combinations of types in
  `ConfigOption::AvailableTypes` for a template function with `n` template
  parameters.
  For example: With 2 template parameters that would be `<bool, int>`, `<int,
  bool>`, `<bool, string>`, `<string, bool>`, etc.

  @tparam NumTemplateParameter The number of template parameter for your lambda.
  @tparam Ts Please ignore, that is for passing via recursive algorithm.

  @param func Your lambda function, that will be called.
  @param callGivenLambdaWithAllCombinationsOfTypes Please ignore, that is for
  passing via recursive algorithm.
  */
  auto callGivenLambdaWithAllCombinationsOfTypes =
      []<size_t NumTemplateParameter, typename... Ts>(
          auto&& func, auto&& callGivenLambdaWithAllCombinationsOfTypes) {
        if constexpr (NumTemplateParameter == 0) {
          func.template operator()<Ts...>();
        } else {
          doForTypeInConfigOptionValueType(
              [&callGivenLambdaWithAllCombinationsOfTypes,
               &func]<typename T>() {
                callGivenLambdaWithAllCombinationsOfTypes
                    .template operator()<NumTemplateParameter - 1, T, Ts...>(
                        AD_FWD(func),
                        AD_FWD(callGivenLambdaWithAllCombinationsOfTypes));
              });
        }
      };

  /*
  @brief Adjust `variant` argument for `createDummyValueForValidator` and
  `generateSingleParameterValidatorFunction`.
  The bool type in those helper functions needs special handeling, because it
  only has two values and can't fulfill the invariant, that
  `createDummyValueForValidator` and `generateSingleParameterValidatorFunction`
  should fulfill.

  @tparam T Same `T` as for `createDummyValueForValidator` and
  `generateSingleParameterValidatorFunction`.
  */
  auto adjustVariantArgument =
      []<typename T>(size_t variantThatNeedsPossibleAdjustment) -> size_t {
    if constexpr (std::is_same_v<T, bool>) {
      /*
      Even numbers for `variant` always result in true, regardless if
      passed to `createDummyValueForValidator`, or
      `generateSingleParameterValidatorFunction`.
      */
      return variantThatNeedsPossibleAdjustment * 2 + 1;
    } else {
      return variantThatNeedsPossibleAdjustment;
    }
  };

  /*
  @brief Generate an informative validator name in the form of `Config manager
  validator<x> y`. With `x` being the list of function argument types and `y` an
  unqiue number id.

  @tparam Ts The types of the function arguments of the validator function.

  @param id An identification number for identifying specific validator
  functions.
  */
  auto generateValidatorName = []<typename... Ts>(size_t id) {
    return absl::StrCat(
        "Config manager validator<",
        lazyStrJoin(std::array{ConfigOption::availableTypesToString<Ts>()...},
                    ", "),
        "> ", id);
  };

  /*
  @brief Generate and add a validator, which follows the invariant of
  `generateSingleParameterValidatorFunction` and was named via
  `generateValidatorName`, to the given config manager.

  @tparam Ts The parameter types for the validator functions.

  @param variant See `generateSingleParameterValidatorFunction`.
  @param m The config manager, to which the validator will be added.
  @param validatorArguments The values of the configuration options will be
  passed as arguments to the validator function, in the same order as given
  here.
  */
  auto addValidatorToConfigManager =
      [&generateValidatorName, &adjustVariantArgument ]<typename... Ts>(
          size_t variant, ConfigManager & m,
          ConstConfigOptionProxy<Ts>... validatorArguments)
          requires(sizeof...(Ts) == sizeof...(validatorArguments)) {
    // Add the new validator
    m.addValidator(
        [variant, &adjustVariantArgument](const Ts&... args) {
          return (generateSingleParameterValidatorFunction<Ts>(
                      adjustVariantArgument.template operator()<Ts>(variant))(
                      args) ||
                  ...);
        },
        generateValidatorName.template operator()<Ts...>(variant),
        validatorArguments...);
  };

  /*
  @brief Test all validator functions generated by `addValidatorToConfigManager`
  for a given range of `variant` and a specific configuration of `Ts`. Note:
  This is for specificly for testing the validators generated by calling
  `addValidatorToConfigManager` with a all values in `[variantStart,
  variantEnd)` as `variant` and all other arguments unchanged.

  @tparam Ts The parameter types, that `addValidatorToConfigManager` was called
  with.

  @param variantStart, variantEnd The range of variant values,
  `addValidatorToConfigManager` was called with.
  @param m The config manager, to which the validators were added.
  @param defaultValues The values for all the configuration options, that will
  not be checked via the validator.
  @param configOptionPaths Paths to the configuration options, who were  given
  to `addValidatorToConfigManager`.
  */
  auto testGeneratedValidatorsOfConfigManager =
      [&generateValidatorName, &adjustVariantArgument ]<typename... Ts>(
          size_t variantStart, size_t variantEnd, ConfigManager & m,
          const nlohmann::json& defaultValues,
          const std::same_as<
              nlohmann::json::json_pointer> auto&... configOptionPaths)
          requires(sizeof...(Ts) == sizeof...(configOptionPaths)) {
    // Using the invariant of our function generator, to create valid
    // and none valid values for all added validators.
    for (size_t validatorNumber = variantStart; validatorNumber < variantEnd;
         validatorNumber++) {
      nlohmann::json validJson(defaultValues);
      ((validJson[configOptionPaths] = createDummyValueForValidator<Ts>(
            adjustVariantArgument.template operator()<Ts>(variantEnd) + 1)),
       ...);

      nlohmann::json invalidJson(defaultValues);
      ((invalidJson[configOptionPaths] = createDummyValueForValidator<Ts>(
            adjustVariantArgument.template operator()<Ts>(validatorNumber))),
       ...);

      /*
      If we have only bools, than we can't use the invariant of the
      generated validators to go 'through' the added validators via invalid
      values.
      Instead, we can only ever fail the first added validator, because they
      are all identical.
      */
      if constexpr ((std::is_same_v<bool, Ts> && ...)) {
        checkValidator(m, validJson, invalidJson,
                       generateValidatorName.template operator()<Ts...>(0));
      } else {
        checkValidator(
            m, validJson, invalidJson,
            generateValidatorName.template operator()<Ts...>(validatorNumber));
      }
    }
  };

  /*
  @brief Does the validator tests for given config manager, by adding validators
  generated via `addValidatorToConfigManager` and testing them via
  `testGeneratedValidatorsOfConfigManager`.

  @tparam Ts The parameter types for the validator functions.

  @param m The config manager, to which validators will be added and on which
  `parseConfig` will be called. Note, that those validators will **not** be
  deleted and that the given config manager should have zero already existing
  validators.
  @param defaultValues The values for all the configuration options, that will
  not be checked via the validator.
  @param validatorArguments As list of pairs, that contain a json pointer to the
  position of the configuration option in the configuration manager and a proxy
  to the `ConfigOption` object itself. The described configuration options will
  be passed as arguments to the validator function, in the same order as given
  here.
  */
  auto doTestNoValidatorInSubManager =
      [&addValidatorToConfigManager, &
       testGeneratedValidatorsOfConfigManager ]<typename... Ts>(
          ConfigManager & m, const nlohmann::json& defaultValues,
          const std::pair<nlohmann::json::json_pointer,
                          ConstConfigOptionProxy<Ts>>&... validatorArguments)
          requires(sizeof...(Ts) == sizeof...(validatorArguments)) {
    // How many validators are to be added?
    constexpr size_t NUMBER_OF_VALIDATORS{5};

    for (size_t i = 0; i < NUMBER_OF_VALIDATORS; i++) {
      // Add a new validator
      addValidatorToConfigManager.template operator()<Ts...>(
          i, m, validatorArguments.second...);

      // Test all the added validators.
      testGeneratedValidatorsOfConfigManager.template operator()<Ts...>(
          0, i + 1, m, defaultValues, validatorArguments.first...);
    }
  };

  // Does all tests for single parameter validators for a given type.
  auto doSingleParameterTests =
      [&doTestNoValidatorInSubManager]<typename Type>() {
        // Variables needed for configuration options.
        Type firstVar;

        ConfigManager mNoSub;
        decltype(auto) mNoSubOption =
            mNoSub.addOption("someValue", "", &firstVar);
        doTestNoValidatorInSubManager.template operator()<Type>(
            mNoSub, nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(nlohmann::json::json_pointer("/someValue"),
                           mNoSubOption));
      };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<1>(
      doSingleParameterTests, callGivenLambdaWithAllCombinationsOfTypes);

  // Does all tests for validators with two parameter types for the given type
  // combination.
  auto doDoubleParameterTests =
      [&doTestNoValidatorInSubManager]<typename Type1, typename Type2>() {
        // Variables needed for configuration options.
        Type1 firstVar;
        Type2 secondVar;

        ConfigManager mNoSub;
        decltype(auto) mNoSubOption1 =
            mNoSub.addOption("someValue1", "", &firstVar);
        decltype(auto) mNoSubOption2 =
            mNoSub.addOption("someValue2", "", &secondVar);
        doTestNoValidatorInSubManager.template operator()<Type1, Type2>(
            mNoSub, nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(nlohmann::json::json_pointer("/someValue1"),
                           mNoSubOption1),
            std::make_pair(nlohmann::json::json_pointer("/someValue2"),
                           mNoSubOption2));
      };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<2>(
      doDoubleParameterTests, callGivenLambdaWithAllCombinationsOfTypes);

  // Testing, if validators with different parameter types work, when added to
  // the same config manager.
  auto doDifferentParameterTests = [&addValidatorToConfigManager,
                                    &generateValidatorName]<typename Type1,
                                                            typename Type2>() {
    // Variables for config options.
    Type1 var1;
    Type2 var2;

    /*
    @brief Helper function, that checks all combinations of valid/invalid values
    for two single argument parameter validator functions.

    @tparam T1, T2 Function parameter type for the first and the second
    validator.

    @param m The config manager, on which `parseConfig` will be called on.
    @param validator1, validator2 A pair consisting of the path to the
    configuration option, that the validator is looking at, and the `variant`
    value, that was used to generate the validator function with
    `addValidatorToConfigManager`.
    */
    auto checkAllValidAndInvalidValueCombinations =
        [&generateValidatorName]<typename T1, typename T2>(
            ConfigManager& m,
            const std::pair<nlohmann::json::json_pointer, size_t>& validator1,
            const std::pair<nlohmann::json::json_pointer, size_t>& validator2) {
          /*
          Input for `parseConfig`. One contains values, that are valid for all
          validators, the other contains values, that are valid for all
          validators except one.
          */
          nlohmann::json validValueJson(nlohmann::json::value_t::object);
          nlohmann::json invalidValueJson(nlohmann::json::value_t::object);

          // Add the valid values.
          validValueJson[validator1.first] =
              createDummyValueForValidator<Type1>(validator1.second + 1);
          validValueJson[validator2.first] =
              createDummyValueForValidator<Type2>(validator2.second + 1);

          // Value for `validator1` is invalid. Value for `validator2` is valid.
          invalidValueJson[validator1.first] =
              createDummyValueForValidator<Type1>(validator1.second);
          invalidValueJson[validator2.first] =
              createDummyValueForValidator<Type2>(validator2.second + 1);
          checkValidator(m, validValueJson, invalidValueJson,
                         generateValidatorName.template operator()<Type1>(
                             validator1.second));

          // Value for `validator1` is valid. Value for `validator2` is invalid.
          invalidValueJson[validator1.first] =
              createDummyValueForValidator<Type1>(validator1.second + 1);
          invalidValueJson[validator2.first] =
              createDummyValueForValidator<Type2>(validator2.second);
          checkValidator(m, validValueJson, invalidValueJson,
                         generateValidatorName.template operator()<Type2>(
                             validator2.second));
        };

    ConfigManager mNoSub;
    decltype(auto) mNoSubOption1 = mNoSub.addOption("someValue1", "", &var1);
    decltype(auto) mNoSubOption2 = mNoSub.addOption("someValue2", "", &var2);
    addValidatorToConfigManager.template operator()<Type1>(1, mNoSub,
                                                           mNoSubOption1);
    addValidatorToConfigManager.template operator()<Type2>(1, mNoSub,
                                                           mNoSubOption2);
    checkAllValidAndInvalidValueCombinations.template operator()<Type1, Type2>(
        mNoSub, std::make_pair(nlohmann::json::json_pointer("/someValue1"), 1),
        std::make_pair(nlohmann::json::json_pointer("/someValue2"), 1));
  };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<2>(
      doDifferentParameterTests, callGivenLambdaWithAllCombinationsOfTypes);
}

TEST(ConfigManagerTest, AddValidatorException) {
  /*
  Test, if there is an exception, when we give `addValidator` configuration
  options, that are not contained in the corresponding configuration manager.
  */
  auto doValidatorParameterNotInConfigManagerTest = []<typename T>() {
    // Variable for the configuration options.
    T var{};

    // Dummy validator function.
    auto validatorDummyFunction = [](const T&) { return true; };

    /*
    @brief Check, if a call to the `addValidator` function behaves as wanted.
    */
    auto checkAddValidatorBehavior =
        [&validatorDummyFunction](ConfigManager& m,
                                  ConstConfigOptionProxy<T> validOption,
                                  ConstConfigOptionProxy<T> notValidOption) {
          ASSERT_NO_THROW(
              m.addValidator(validatorDummyFunction, "", validOption));
          AD_EXPECT_THROW_WITH_MESSAGE(
              m.addValidator(validatorDummyFunction,
                             notValidOption.getConfigOption().getIdentifier(),
                             notValidOption),
              ::testing::ContainsRegex(
                  notValidOption.getConfigOption().getIdentifier()));
        };

    // An outside configuration option.
    ConfigOption outsideOption("outside", "", &var);
    ConstConfigOptionProxy<T> outsideOptionProxy(outsideOption);

    ConfigManager mNoSub;
    decltype(auto) mNoSubOption = mNoSub.addOption("someOption", "", &var);
    checkAddValidatorBehavior(mNoSub, mNoSubOption, outsideOptionProxy);
  };

  doForTypeInConfigOptionValueType(doValidatorParameterNotInConfigManagerTest);
}

TEST(ConfigManagerTest, AddOptionValidator) {
  // Generate a lambda, that requires all the given configuration option to have
  // the wanted string as the representation of their value.
  auto generateValueAsStringComparison =
      [](std::string_view valueStringRepresentation) {
        return [wantedString = std::string(valueStringRepresentation)](
                   const auto&... options) {
          return ((options.getValueAsString() == wantedString) && ...);
        };
      };

  // Variables for configuration options.
  int firstVar;
  int secondVar;

  ConfigManager managerWithNoSubManager;
  decltype(auto) managerWithNoSubManagerOption1 =
      managerWithNoSubManager.addOption("someOption1", "", &firstVar);
  managerWithNoSubManager.addOptionValidator(
      generateValueAsStringComparison("10"), "someOption1",
      managerWithNoSubManagerOption1);
  checkValidator(managerWithNoSubManager,
                 nlohmann::json::parse(R"--({"someOption1" : 10})--"),
                 nlohmann::json::parse(R"--({"someOption1" : 1})--"),
                 "someOption1");
  decltype(auto) managerWithNoSubManagerOption2 =
      managerWithNoSubManager.addOption("someOption2", "", &secondVar);
  managerWithNoSubManager.addOptionValidator(
      generateValueAsStringComparison("10"), "Both options",
      managerWithNoSubManagerOption1, managerWithNoSubManagerOption2);
  checkValidator(
      managerWithNoSubManager,
      nlohmann::json::parse(R"--({"someOption1" : 10, "someOption2" : 10})--"),
      nlohmann::json::parse(R"--({"someOption1" : 10, "someOption2" : 1})--"),
      "Both options");
}

TEST(ConfigManagerTest, AddOptionValidatorException) {
  // Variable for the configuration options.
  int var;

  // Dummy validator function.
  auto validatorDummyFunction = [](const ConfigOption&) { return true; };

  /*
  @brief Check, if a call to the `addOptionValidator` function behaves as
  wanted.
  */
  auto checkAddOptionValidatorBehavior =
      [&validatorDummyFunction]<typename T>(
          ConfigManager& m, ConstConfigOptionProxy<T> validOption,
          ConstConfigOptionProxy<T> notValidOption) {
        ASSERT_NO_THROW(
            m.addOptionValidator(validatorDummyFunction, "", validOption));
        AD_EXPECT_THROW_WITH_MESSAGE(
            m.addOptionValidator(
                validatorDummyFunction,
                notValidOption.getConfigOption().getIdentifier(),
                notValidOption),
            ::testing::ContainsRegex(
                notValidOption.getConfigOption().getIdentifier()));
      };

  // An outside configuration option.
  ConfigOption outsideOption("outside", "", &var);
  ConstConfigOptionProxy<int> outsideOptionProxy(outsideOption);

  ConfigManager mNoSub;
  decltype(auto) mNoSubOption = mNoSub.addOption("someOption", "", &var);
  checkAddOptionValidatorBehavior(mNoSub, mNoSubOption, outsideOptionProxy);
}

TEST(ConfigManagerTest, ContainsOption) {
  /*
  @brief Verify, that the given configuration options are (not) contained in the
  given configuration manager.

  @param optionsAndWantedStatus A list of configuration options together with
  the information, if they should be contained in `m`. If true, it should be, if
  false, it shouldn't.
  */
  using ContainmentStatusVector =
      std::vector<std::pair<const ConfigOption*, bool>>;
  auto checkContainmentStatus =
      [](const ConfigManager& m,
         const ContainmentStatusVector& optionsAndWantedStatus) {
        std::ranges::for_each(
            optionsAndWantedStatus,
            [&m](const ContainmentStatusVector::value_type& p) {
              if (p.second) {
                ASSERT_TRUE(m.containsOption(*p.first));
              } else {
                ASSERT_FALSE(m.containsOption(*p.first));
              }
            });
      };

  // Variable for the configuration options.
  int var;

  // Outside configuration option.
  const ConfigOption outsideOption("OutsideOption", "", &var);

  ConfigManager m;
  checkContainmentStatus(m, {{&outsideOption, false}});
  decltype(auto) topManagerOption = m.addOption("TopLevel", "", &var);
  checkContainmentStatus(m, {{&outsideOption, false},
                             {&topManagerOption.getConfigOption(), true}});
}

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

TEST(ConfigManagerTest, ValidatorConcept) {
  // Lambda function types for easier test creation.
  using SingleIntValidatorFunction = decltype([](const int&) { return true; });
  using DoubleIntValidatorFunction =
      decltype([](const int&, const int&) { return true; });

  // Valid function.
  static_assert(ad_utility::Validator<SingleIntValidatorFunction, int>);
  static_assert(ad_utility::Validator<DoubleIntValidatorFunction, int, int>);

  // The number of parameter types is wrong.
  static_assert(!ad_utility::Validator<SingleIntValidatorFunction>);
  static_assert(!ad_utility::Validator<SingleIntValidatorFunction, int, int>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction>);
  static_assert(
      !ad_utility::Validator<DoubleIntValidatorFunction, int, int, int, int>);

  // Function is valid, but the parameter types are of the wrong object type.
  static_assert(
      !ad_utility::Validator<SingleIntValidatorFunction, std::vector<bool>>);
  static_assert(
      !ad_utility::Validator<SingleIntValidatorFunction, std::string>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction,
                                       std::vector<bool>, int>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction, int,
                                       std::vector<bool>>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction,
                                       std::vector<bool>, std::vector<bool>>);
  static_assert(
      !ad_utility::Validator<DoubleIntValidatorFunction, std::string, int>);
  static_assert(
      !ad_utility::Validator<DoubleIntValidatorFunction, int, std::string>);
  static_assert(!ad_utility::Validator<DoubleIntValidatorFunction, std::string,
                                       std::string>);

  // The given function is not valid.

  // The parameter types of the function are wrong, but the return type is
  // correct.
  static_assert(
      !ad_utility::Validator<decltype([](int&) { return true; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](int&&) { return true; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int&&) { return true; }), int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ad_utility::Validator<
                decltype([](FirstParameter, SecondParameter) { return true; }),
                int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeRightTestHelper);

  // Parameter types are correct, but return type is wrong.
  static_assert(!ad_utility::Validator<decltype([](int n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int& n) { return n; }), int>);

  auto validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ad_utility::Validator<decltype([](FirstParameter n,
                                               SecondParameter) { return n; }),
                                   int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper),
      int, const int, const int&>(
      validParameterButFunctionParameterRightAndReturnTypeWrongTestHelper);

  // Both the parameter types and the return type is wrong.
  static_assert(
      !ad_utility::Validator<decltype([](int& n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](int&& n) { return n; }), int>);
  static_assert(
      !ad_utility::Validator<decltype([](const int&& n) { return n; }), int>);

  auto validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper =
      []<typename FirstParameter, typename SecondParameter>() {
        static_assert(
            !ad_utility::Validator<decltype([](FirstParameter n,
                                               SecondParameter) { return n; }),
                                   int, int>);
      };
  passCartesianPorductToLambda<
      decltype(validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper),
      int&, int&&, const int&&>(
      validParameterButFunctionParameterWrongAndReturnTypeWrongTestHelper);
}
}  // namespace ad_utility
