// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstddef>
#include <vector>

#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/json.h"

namespace ad_utility {
TEST(ConfigManagerTest, GetConfigurationOptionByNestedKeysTest) {
  ad_utility::ConfigManager config{};

  // 'Compare' two configuration options
  auto compareConfigurationOptions = []<typename T>(
                                         const ad_utility::ConfigOption& a,
                                         const ad_utility::ConfigOption& b) {
    ASSERT_EQ(a.hasSetDereferencedVariablePointer(),
              b.hasSetDereferencedVariablePointer());

    if (a.hasSetDereferencedVariablePointer()) {
      ASSERT_EQ(a.getValue<T>(), b.getValue<T>());
      ASSERT_EQ(a.getValueAsString(), b.getValueAsString());
    }
  };

  // Configuration options for testing.
  int notUsed;

  config.createConfigOption(
      {"Shared_part", "Unique_part_1", "Sense_of_existence"}, "", &notUsed,
      std::optional{42});
  const ad_utility::ConfigOption& withDefault{
      ad_utility::ConfigOption("Sense_of_existence", "", &notUsed, 42)};

  config.createConfigOption(
      {"Shared_part", "Unique_part_2", size_t{3}, "Sense_of_existence"}, "",
      &notUsed);
  const ad_utility::ConfigOption& withoutDefault{
      ad_utility::ConfigOption("Sense_of_existence", "", &notUsed)};

  // Where those two options added?
  ASSERT_EQ(config.getConfigurationOptions().size(), 2);

  compareConfigurationOptions.template operator()<int>(
      withDefault, config.getConfigurationOptionByNestedKeys(
                       {"Shared_part", "Unique_part_1", "Sense_of_existence"}));
  compareConfigurationOptions.template operator()<int>(
      withoutDefault,
      config.getConfigurationOptionByNestedKeys(
          {"Shared_part", "Unique_part_2", size_t{3}, "Sense_of_existence"}));

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
  If the first key for the path given is a number, that should cause an
  exception.
  Reason: We want our `tree` (a `nlohmann::json` object) to be a json
  object literal, so that user can easier find things. Ordering your options
  by just giving them numbers, would be bad practice, so we should prevent it.
  */
  ASSERT_ANY_THROW(config.createConfigOption<int>(
      {size_t{0}, "Shared_part", "Sense_of_existence"}, "", &notUsed, 42););
  ASSERT_ANY_THROW(config.createConfigOption<int>(
      {size_t{3}, "Shared_part", "Sense_of_existence"}, "", &notUsed, 42););

  /*
  If the last key for the path given isn't a string, that should cause an
  exception.
  Reason: The last key is used as the name for the to be created `ConfigOption`.
  A number, obviously, doesn't qualify.
  */
  ASSERT_ANY_THROW(config.createConfigOption<int>(
      {"Shared_part", "Sense_of_existence", size_t{3}}, "", &notUsed, 42););

  /*
  An empty vector that should cause an exception.
  Reason: The last key is used as the name for the to be created `ConfigOption`.
  An empty vector doesn't work with that.
  */
  ASSERT_ANY_THROW(config.createConfigOption<int>(
      ad_utility::ConfigManager::VectorOfKeysForJson{}, "", &notUsed, 42););

  /*
  Trying to add a configuration option with a path containing strings with
  spaces should cause an error.
  Reason: A string with spaces in it, can't be read by the short hand
  configuration grammar. Ergo, you can't set values, with such paths per short
  hand, which we don't want.
  */
  ASSERT_THROW(config.createConfigOption<int>(
      ad_utility::ConfigManager::VectorOfKeysForJson{"Shared part",
                                                     "Sense_of_existence"},
      "", &notUsed, 42);
               , ad_utility::NotValidShortHandNameException);
}

TEST(ConfigManagerTest, ParseConfig) {
  ad_utility::ConfigManager config{};

  // Adding the options.
  int firstInt;
  int secondInt;
  int thirdInt;

  config.createConfigOption<int>(
      ad_utility::ConfigManager::VectorOfKeysForJson{"depth_0", "Option_0"},
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

  // For easier checking.
  auto checkOption = [](const ad_utility::ConfigOption& option,
                        const auto& content) {
    ASSERT_TRUE(option.hasSetDereferencedVariablePointer());
    ASSERT_EQ(content, option.getValue<std::decay_t<decltype(content)>>());
  };

  // Does the option with the default already have a value?
  checkOption(getOption(2), 2);

  // The other two should never have set the variable, that the internal pointer
  // points to.
  ASSERT_FALSE(getOption(0).hasSetDereferencedVariablePointer());
  ASSERT_FALSE(getOption(1).hasSetDereferencedVariablePointer());

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

  checkOption(getOption(0), 10);
  checkOption(getOption(1), 11);
  checkOption(getOption(2), 12);
}

TEST(ConfigManagerTest, ParseConfigExceptionTest) {
  ad_utility::ConfigManager config{};

  // Add one option with default and one without.
  int notUsedInt;
  std::vector<int> notUsedVector;
  config.createConfigOption<int>(
      ad_utility::ConfigManager::VectorOfKeysForJson{"depth_0",
                                                     "Without_default"},
      "Must be set. Has no default value.", &notUsedInt);
  config.createConfigOption<std::vector<int>>(
      ad_utility::ConfigManager::VectorOfKeysForJson{"depth_0", "With_default"},
      "Must not be set. Has default value.", &notUsedVector,
      std::vector{40, 41});

  // Should throw an exception, if we don't set all options, that must be set.
  ASSERT_THROW(config.parseConfig(nlohmann::json::parse(R"--({})--")),
               ad_utility::ConfigOptionWasntSetException);

  // Should throw an exception, if we try set an option, that isn't there.
  ASSERT_THROW(
      config.parseConfig(nlohmann::json::parse(
          R"--({"depth 0":{"Without_default":42, "with_default" : [39]}})--")),
      ad_utility::NoConfigOptionFoundException);
  ASSERT_THROW(
      config.parseConfig(nlohmann::json::parse(
          R"--({"depth 0":{"Without_default":42, "test_string" : "test"}})--")),
      ad_utility::NoConfigOptionFoundException);
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
      {"depth", size_t{0}, "list"}, "Must be set. Has no default value.",
      &deeperIntVector);

  // This one will not be changed, in order to test, that options, that are
  // not set at run time, are not changed.
  int noChangeInt;
  config.createConfigOption<int>("No_change", "", &noChangeInt, 10);

  // Set those.
  config.parseConfig(ad_utility::ConfigManager::parseShortHand(
      R"--(somePositiveNumber : 42, someNegativNumber : -42, someIntegerlist : [40, 41], somePositiveFloatingPoint : 4.2, someNegativFloatingPoint : -4.2, someFloatingPointList : [4.1, 4.2], boolTrue : true, boolFalse : false, someBooleanList : [true, false, true], myName : "Bernd", someStringList : ["t1", "t2"], depth : [{list : [7,8]}])--"));

  // Check, if an option was set to the value, you wanted.
  auto checkOption =
      [&config](const auto& content,
                const ad_utility::ConfigManager::VectorOfKeysForJson& keys) {
        const auto& option = config.getConfigurationOptionByNestedKeys(keys);
        ASSERT_TRUE(option.hasSetDereferencedVariablePointer());
        ASSERT_EQ(content,
                  option.template getValue<std::decay_t<decltype(content)>>());
      };

  checkOption(static_cast<int>(42), {"somePositiveNumber"});
  checkOption(static_cast<int>(-42), {"someNegativNumber"});

  checkOption(std::vector{40, 41}, {"someIntegerlist"});

  checkOption(static_cast<float>(4.2), {"somePositiveFloatingPoint"});
  checkOption(static_cast<float>(-4.2), {"someNegativFloatingPoint"});

  checkOption(std::vector<float>{4.1, 4.2}, {"someFloatingPointList"});

  checkOption(true, {"boolTrue"});
  checkOption(false, {"boolFalse"});

  checkOption(std::vector{true, false, true}, {"someBooleanList"});

  checkOption(std::string{"Bernd"}, {"myName"});

  checkOption(std::vector<std::string>{"t1", "t2"}, {"someStringList"});

  checkOption(std::vector{7, 8}, {"depth", size_t{0}, "list"});

  // Is the "No Change" unchanged?
  checkOption(static_cast<int>(10), {"No_change"});

  // Multiple key value pairs with the same key are not allowed.
  ASSERT_THROW(ad_utility::ConfigManager::parseShortHand(R"(a:42, a:43)");
               , ToJsonConfigShorthandVisitor::
                     ConfigShortHandVisitorKeyCollisionException);

  // Final test: Is there an exception, if we try to parse the wrong syntax?
  ASSERT_ANY_THROW(ad_utility::ConfigManager::parseShortHand(
      R"--({"myName" : "Bernd")})--"));
  ASSERT_ANY_THROW(
      ad_utility::ConfigManager::parseShortHand(R"--("myName" = "Bernd";)--"));
}

TEST(ConfigManagerTest, getListOfNotChangedConfigOptionsWithDefaultValues) {
  ConfigManager manager{};

  ASSERT_EQ(0,
            manager.getListOfNotChangedConfigOptionsWithDefaultValues().size());
  ASSERT_EQ(
      std::string{},
      manager.getListOfNotChangedConfigOptionsWithDefaultValuesAsString());

  // Create a new `ConfigOption`, that will be on the list and one, that won't
  // be.
  int notUsedInt;
  manager.createConfigOption<int>("onList", "", &notUsedInt, 4);
  manager.createConfigOption<int>("notOnList", "", &notUsedInt);

  ASSERT_EQ(1,
            manager.getListOfNotChangedConfigOptionsWithDefaultValues().size());

  const ConfigOption firstEntry =
      manager.getListOfNotChangedConfigOptionsWithDefaultValues().front();
  ASSERT_EQ("onList", firstEntry.getIdentifier());
  ASSERT_EQ(4, firstEntry.getDefaultValue<int>());
  ASSERT_EQ(4, firstEntry.getValue<int>());

  ASSERT_NE(
      std::string{},
      manager.getListOfNotChangedConfigOptionsWithDefaultValuesAsString());

  // Changing `onList` via parsing should remove it from the list.
  manager.parseConfig(
      nlohmann::json::parse(R"--({"onList":10, "notOnList":27})--"));

  ASSERT_EQ(0,
            manager.getListOfNotChangedConfigOptionsWithDefaultValues().size());
  ASSERT_EQ(
      std::string{},
      manager.getListOfNotChangedConfigOptionsWithDefaultValuesAsString());
}
}  // namespace ad_utility
