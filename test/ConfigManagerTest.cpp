// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstddef>
#include <type_traits>
#include <vector>

#include "./util/GTestHelpers.h"
#include "gtest/gtest.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/json.h"

namespace ad_utility {

/*
@brief Checks, if the given configuration option was set correctly.

@param externalVariable The variable, that the given configuration option writes
to.
@param wasSet Was the given configuration option set?
@param wantedValue The value, that the configuration option should have been set
to.
*/
template <typename T, typename DecayedT = std::decay_t<T>>
void checkOption(const ad_utility::ConfigOption& option,
                 const DecayedT& externalVariable, const bool wasSet,
                 const DecayedT& wantedValue) {
  ASSERT_EQ(wasSet, option.wasSet());

  if (wasSet) {
    ASSERT_EQ(wantedValue, option.getValue<DecayedT>());
    ASSERT_EQ(wantedValue, externalVariable);
  }
}

TEST(ConfigManagerTest, GetConfigurationOptionByNestedKeysTest) {
  ad_utility::ConfigManager config{};

  // Configuration options for testing.
  int notUsed;

  config.createConfigOption(
      {"Shared_part", "Unique_part_1", "Sense_of_existence"}, "", &notUsed,
      std::optional{42});

  config.createConfigOption(
      {"Shared_part", "Unique_part_2", "Sense_of_existence"}, "", &notUsed);

  // Where those two options added?
  ASSERT_EQ(config.configurationOptions_.size(), 2);

  checkOption<int>(config.getConfigurationOptionByNestedKeys(
                       {"Shared_part", "Unique_part_1", "Sense_of_existence"}),
                   notUsed, true, 42);
  checkOption<int>(config.getConfigurationOptionByNestedKeys(
                       {"Shared_part", "Unique_part_2", "Sense_of_existence"}),
                   notUsed, false, 42);

  // Trying to get a configuration option, that does not exist, should cause
  // an exception.
  ASSERT_THROW(
      config.getConfigurationOptionByNestedKeys({"Shared_part", "Getsbourgh"}),
      ad_utility::NoConfigOptionFoundException);
}

/*
The exceptions for adding configuration options.
*/
TEST(ConfigManagerTest, CreateConfigurationOptionExceptionTest) {
  ad_utility::ConfigManager config{};

  // Configuration options for testing.
  int notUsed;
  config.createConfigOption<int>(
      {"Shared_part", "Unique_part_1", "Sense_of_existence"}, "", &notUsed, 42);

  // Trying to add a configuration option with the same name at the same
  // place, should cause an error.
  ASSERT_THROW(config.createConfigOption<int>(
      {"Shared_part", "Unique_part_1", "Sense_of_existence"}, "", &notUsed, 42);
               , ad_utility::ConfigManagerOptionPathAlreadyinUseException);

  /*
  An empty vector that should cause an exception.
  Reason: The last key is used as the name for the to be created `ConfigOption`.
  An empty vector doesn't work with that.
  */
  ASSERT_ANY_THROW(config.createConfigOption<int>(std::vector<std::string>{},
                                                  "", &notUsed, 42););

  /*
  Trying to add a configuration option with a path containing strings with
  spaces should cause an error.
  Reason: A string with spaces in it, can't be read by the short hand
  configuration grammar. Ergo, you can't set values, with such paths per short
  hand, which we don't want.
  */
  ASSERT_THROW(config.createConfigOption<int>(
      std::vector<std::string>{"Shared part", "Sense_of_existence"}, "",
      &notUsed, 42);
               , ad_utility::NotValidShortHandNameException);
}

TEST(ConfigManagerTest, ParseConfig) {
  ad_utility::ConfigManager config{};

  // Adding the options.
  int firstInt;
  int secondInt;
  int thirdInt;

  config.createConfigOption<int>(
      std::vector<std::string>{"depth_0", "Option_0"},
      "Must be set. Has no default value.", &firstInt);
  config.createConfigOption<int>({"depth_0", "depth_1", "Option_1"},
                                 "Must be set. Has no default value.",
                                 &secondInt);
  config.createConfigOption<int>("Option_2", "Has a default value.", &thirdInt,
                                 2);

  // For easier access to the options.
  auto getOption = [&config](const size_t& optionNumber) {
    if (optionNumber == 0) {
      return config.getConfigurationOptionByNestedKeys({"depth_0", "Option_0"});
    } else if (optionNumber == 1) {
      return config.getConfigurationOptionByNestedKeys(
          {"depth_0", "depth_1", "Option_1"});
    } else {
      return config.getConfigurationOptionByNestedKeys({"Option_2"});
    }
  };

  // Does the option with the default already have a value?
  checkOption<int>(getOption(2), thirdInt, true, 2);

  // The other two should never have set the variable, that the internal pointer
  // points to.
  checkOption<int>(getOption(0), firstInt, false, 2);
  checkOption<int>(getOption(1), secondInt, false, 2);

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

  checkOption<int>(getOption(0), firstInt, true, 10);
  checkOption<int>(getOption(1), secondInt, true, 11);
  checkOption<int>(getOption(2), thirdInt, true, 12);
}

TEST(ConfigManagerTest, ParseConfigExceptionTest) {
  ad_utility::ConfigManager config{};

  // Add one option with default and one without.
  int notUsedInt;
  std::vector<int> notUsedVector;
  config.createConfigOption<int>(
      std::vector<std::string>{"depth_0", "Without_default"},
      "Must be set. Has no default value.", &notUsedInt);
  config.createConfigOption<std::vector<int>>(
      std::vector<std::string>{"depth_0", "With_default"},
      "Must not be set. Has default value.", &notUsedVector,
      std::vector{40, 41});

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
  config.createConfigOption<int>("somePositiveNumber",
                                 "Must be set. Has no default value.",
                                 &somePositiveNumberInt);
  int someNegativNumberInt;
  config.createConfigOption<int>("someNegativNumber",
                                 "Must be set. Has no default value.",
                                 &someNegativNumberInt);

  // Add integer list.
  std::vector<int> someIntegerlistIntVector;
  config.createConfigOption<std::vector<int>>(
      "someIntegerlist", "Must be set. Has no default value.",
      &someIntegerlistIntVector);

  // Add floating point options.
  float somePositiveFloatingPointFloat;
  config.createConfigOption<float>("somePositiveFloatingPoint",
                                   "Must be set. Has no default value.",
                                   &somePositiveFloatingPointFloat);
  float someNegativFloatingPointFloat;
  config.createConfigOption<float>("someNegativFloatingPoint",
                                   "Must be set. Has no default value.",
                                   &someNegativFloatingPointFloat);

  // Add floating point list.
  std::vector<float> someFloatingPointListFloatVector;
  config.createConfigOption<std::vector<float>>(
      "someFloatingPointList", "Must be set. Has no default value.",
      &someFloatingPointListFloatVector);

  // Add boolean options.
  bool boolTrueBool;
  config.createConfigOption<bool>(
      "boolTrue", "Must be set. Has no default value.", &boolTrueBool);
  bool boolFalseBool;
  config.createConfigOption<bool>(
      "boolFalse", "Must be set. Has no default value.", &boolFalseBool);

  // Add boolean list.
  std::vector<bool> someBooleanListBoolVector;
  config.createConfigOption<std::vector<bool>>(
      "someBooleanList", "Must be set. Has no default value.",
      &someBooleanListBoolVector);

  // Add string option.
  std::string myNameString;
  config.createConfigOption<std::string>(
      "myName", "Must be set. Has no default value.", &myNameString);

  // Add string list.
  std::vector<std::string> someStringListStringVector;
  config.createConfigOption<std::vector<std::string>>(
      "someStringList", "Must be set. Has no default value.",
      &someStringListStringVector);

  // Add option with deeper level.
  std::vector<int> deeperIntVector;
  config.createConfigOption<std::vector<int>>(
      {"depth", "here", "list"}, "Must be set. Has no default value.",
      &deeperIntVector);

  // This one will not be changed, in order to test, that options, that are
  // not set at run time, are not changed.
  int noChangeInt;
  config.createConfigOption<int>("No_change", "", &noChangeInt, 10);

  // Set those.
  config.parseConfig(ad_utility::ConfigManager::parseShortHand(
      R"--(somePositiveNumber : 42, someNegativNumber : -42, someIntegerlist : [40, 41], somePositiveFloatingPoint : 4.2, someNegativFloatingPoint : -4.2, someFloatingPointList : [4.1, 4.2], boolTrue : true, boolFalse : false, someBooleanList : [true, false, true], myName : "Bernd", someStringList : ["t1", "t2"], depth : { here : {list : [7,8]}})--"));

  checkOption<int>(
      config.getConfigurationOptionByNestedKeys({"somePositiveNumber"}),
      somePositiveNumberInt, true, 42);
  checkOption<int>(
      config.getConfigurationOptionByNestedKeys({"someNegativNumber"}),
      someNegativNumberInt, true, -42);

  checkOption<std::vector<int>>(
      config.getConfigurationOptionByNestedKeys({"someIntegerlist"}),
      someIntegerlistIntVector, true, std::vector{40, 41});

  checkOption<float>(
      config.getConfigurationOptionByNestedKeys({"somePositiveFloatingPoint"}),
      somePositiveFloatingPointFloat, true, 4.2f);
  checkOption<float>(
      config.getConfigurationOptionByNestedKeys({"someNegativFloatingPoint"}),
      someNegativFloatingPointFloat, true, -4.2f);

  checkOption<std::vector<float>>(
      config.getConfigurationOptionByNestedKeys({"someFloatingPointList"}),
      someFloatingPointListFloatVector, true, {4.1f, 4.2f});

  checkOption<bool>(config.getConfigurationOptionByNestedKeys({"boolTrue"}),
                    boolTrueBool, true, true);
  checkOption<bool>(config.getConfigurationOptionByNestedKeys({"boolFalse"}),
                    boolFalseBool, true, false);

  checkOption<std::vector<bool>>(
      config.getConfigurationOptionByNestedKeys({"someBooleanList"}),
      someBooleanListBoolVector, true, std::vector{true, false, true});

  checkOption<std::string>(
      config.getConfigurationOptionByNestedKeys({"myName"}), myNameString, true,
      std::string{"Bernd"});

  checkOption<std::vector<std::string>>(
      config.getConfigurationOptionByNestedKeys({"someStringList"}),
      someStringListStringVector, true, std::vector<std::string>{"t1", "t2"});

  checkOption<std::vector<int>>(
      config.getConfigurationOptionByNestedKeys({"depth", "here", "list"}),
      deeperIntVector, true, std::vector{7, 8});

  // Is the "No Change" unchanged?
  checkOption<int>(config.getConfigurationOptionByNestedKeys({"No_change"}),
                   noChangeInt, true, 10);

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
  config.createConfigOption<int>("WithDefault", "", &notUsed, 42);
  config.createConfigOption<int>("WithoutDefault", "", &notUsed);
  ASSERT_NO_THROW(config.printConfigurationDoc(false));
  ASSERT_NO_THROW(config.printConfigurationDoc(true));
}
}  // namespace ad_utility
