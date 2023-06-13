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

TEST(ConfigManagerTest, GetConfigurationOptionByNestedKeysTest) {
  ad_utility::ConfigManager config{};

  // 'Compare' two configuration options
  auto compareConfigurationOptions = []<typename T>(
                                         const ad_utility::ConfigOption& a,
                                         const ad_utility::ConfigOption& b) {
    ASSERT_EQ(a.hasValue(), b.hasValue());

    if (a.hasValue()) {
      ASSERT_EQ(a.getValue<T>(), b.getValue<T>());
      ASSERT_EQ(a.getValueAsString(), b.getValueAsString());
    }
  };

  // Configuration options for testing.
  int notUsed;

  config.createConfigOption(
      {"Shared_part", "Unique_part_1", "Sense_of_existence"}, "", &notUsed,
      std::optional{42});
  const ad_utility::ConfigOption& withDefault{ad_utility::makeConfigOption(
      "Sense_of_existence", "", &notUsed, std::optional{42})};

  config.createConfigOption(
      {"Shared_part", "Unique_part_2", size_t{3}, "Sense_of_existence"}, "",
      &notUsed);
  const ad_utility::ConfigOption& withoutDefault{
      ad_utility::makeConfigOption("Sense_of_existence", "", &notUsed)};

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
TEST(ConfigManagerTest, AddConfigurationOptionExceptionTest) {
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
  ASSERT_THROW(
      config.createConfigOption<int>(
          {size_t{0}, "Shared_part", "Sense_of_existence"}, "", &notUsed, 42);
      , ad_utility::ConfigManagerOptionPathDoesntStartWithStringException);
  ASSERT_THROW(
      config.createConfigOption<int>(
          {size_t{3}, "Shared_part", "Sense_of_existence"}, "", &notUsed, 42);
      , ad_utility::ConfigManagerOptionPathDoesntStartWithStringException);

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
  int notUsed;

  config.createConfigOption<int>(
      ad_utility::ConfigManager::VectorOfKeysForJson{"depth_0", "Option_0"},
      "Must be set. Has no default value.", &notUsed);
  config.createConfigOption<int>({"depth_0", "depth_1", "Option_1"},
                                 "Must be set. Has no default value.",
                                 &notUsed);
  config.createConfigOption<int>("Option_2", "Has a default value.", &notUsed,
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
    ASSERT_TRUE(option.hasValue());
    ASSERT_EQ(content, option.getValue<std::decay_t<decltype(content)>>());
  };

  // Does the option with the default already have a value?
  checkOption(getOption(2), 2);

  // The other two should have no value.
  ASSERT_FALSE(getOption(0).hasValue());
  ASSERT_FALSE(getOption(1).hasValue());

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
  int notUsedInt;
  config.createConfigOption<int>(
      "somePositiveNumber", "Must be set. Has no default value.", &notUsedInt);
  config.createConfigOption<int>(
      "someNegativNumber", "Must be set. Has no default value.", &notUsedInt);

  // Add integer list.
  std::vector<int> notUsedIntVector;
  config.createConfigOption<std::vector<int>>(
      "someIntegerlist", "Must be set. Has no default value.",
      &notUsedIntVector);

  // Add floating point options.
  float notUsedFloat;
  config.createConfigOption<float>("somePositiveFloatingPoint",
                                   "Must be set. Has no default value.",
                                   &notUsedFloat);
  config.createConfigOption<float>("someNegativFloatingPoint",
                                   "Must be set. Has no default value.",
                                   &notUsedFloat);

  // Add floating point list.
  std::vector<float> notUsedFloatVector;
  config.createConfigOption<std::vector<float>>(
      "someFloatingPointList", "Must be set. Has no default value.",
      &notUsedFloatVector);

  // Add boolean options.
  bool notUsedBool;
  config.createConfigOption<bool>(
      "boolTrue", "Must be set. Has no default value.", &notUsedBool);
  config.createConfigOption<bool>(
      "boolFalse", "Must be set. Has no default value.", &notUsedBool);

  // Add boolean list.
  std::vector<bool> notUsedBoolVector;
  config.createConfigOption<std::vector<bool>>(
      "someBooleanList", "Must be set. Has no default value.",
      &notUsedBoolVector);

  // Add string option.
  std::string notUsedString;
  config.createConfigOption<std::string>(
      "myName", "Must be set. Has no default value.", &notUsedString);

  // Add string list.
  std::vector<std::string> notUsedStringVector;
  config.createConfigOption<std::vector<std::string>>(
      "someStringList", "Must be set. Has no default value.",
      &notUsedStringVector);

  // Add option with deeper level.
  config.createConfigOption<std::vector<int>>(
      {"depth", size_t{0}, "list"}, "Must be set. Has no default value.",
      &notUsedIntVector);

  // This one will not be changed, in order to test, that options, that are
  // not set at run time, are not changed.
  config.createConfigOption<int>("No_change", "", &notUsedInt, 10);

  // Set those.
  config.parseConfig(ad_utility::ConfigManager::parseShortHand(
      R"--(somePositiveNumber : 42, someNegativNumber : -42, someIntegerlist : [40, 41], somePositiveFloatingPoint : 4.2, someNegativFloatingPoint : -4.2, someFloatingPointList : [4.1, 4.2], boolTrue : true, boolFalse : false, someBooleanList : [true, false, true], myName : "Bernd", someStringList : ["t1", "t2"], depth : [{list : [7,8]}])--"));

  // Check, if an option was set to the value, you wanted.
  auto checkOption =
      [&config](const auto& content,
                const ad_utility::ConfigManager::VectorOfKeysForJson& keys) {
        const auto& option = config.getConfigurationOptionByNestedKeys(keys);
        ASSERT_TRUE(option.hasValue());
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
