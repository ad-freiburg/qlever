// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>
#include <vector>

#include "../test/util/ValidatorFunctionHelpers.h"
#include "./util/GTestHelpers.h"
#include "gtest/gtest.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigOptionHelpers.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/StringUtils.h"
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

/*
The exceptions for adding sub managers.
*/
TEST(ConfigManagerTest, addSubManagerExceptionTest) {
  ad_utility::ConfigManager config{};

  // Sub manager for testing. Empty sub manager are not allowed.
  int notUsed;
  config
      .addSubManager({"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s})
      .addOption("ignore", "", &notUsed);

  // Trying to add a sub manager with the same name at the same place, should
  // cause an error.
  ASSERT_THROW(config.addSubManager(
                   {"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s}),
               ad_utility::ConfigManagerOptionPathAlreadyinUseException);

  // An empty vector that should cause an exception.
  ASSERT_ANY_THROW(config.addSubManager(std::vector<std::string>{}););

  /*
  Trying to add a sub manager with a path containing strings with
  spaces should cause an error.
  Reason: A string with spaces in it, can't be read by the short hand
  configuration grammar. Ergo, you can't set values, with such paths per
  short hand, which we don't want.
  */
  ASSERT_THROW(config.addSubManager({"Shared part"s, "Sense_of_existence"s});
               , ad_utility::NotValidShortHandNameException);
}

TEST(ConfigManagerTest, ParseConfigNoSubManager) {
  ad_utility::ConfigManager config{};

  // Adding the options.
  int firstInt;
  int secondInt;
  int thirdInt;

  const ConfigOption& optionZero =
      config.addOption({"depth_0"s, "Option_0"s},
                       "Must be set. Has no default value.", &firstInt);
  const ConfigOption& optionOne =
      config.addOption({"depth_0"s, "depth_1"s, "Option_1"s},
                       "Must be set. Has no default value.", &secondInt);
  const ConfigOption& optionTwo =
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

TEST(ConfigManagerTest, ParseConfigWithSubManager) {
  // Parse the given configManager with the given json and check, that all the
  // configOption were set correctly.
  auto parseAndCheck =
      [](const nlohmann::json& j, ConfigManager& m,
         const std::vector<std::pair<int*, int>>& wantedValues) {
        m.parseConfig(j);

        std::ranges::for_each(
            wantedValues, [](const std::pair<int*, int>& wantedValue) -> void {
              ASSERT_EQ(*wantedValue.first, wantedValue.second);
            });
      };

  // Simple manager, with only one sub manager and no recursion.
  ad_utility::ConfigManager managerWithOneSubNoRecursion{};
  ad_utility::ConfigManager& managerSteve =
      managerWithOneSubNoRecursion.addSubManager({"personal"s, "Steve"s});
  int steveId;
  managerSteve.addOption("Id", "", &steveId, 4);
  int steveInfractions;
  managerSteve.addOption("Infractions", "", &steveInfractions, 6);

  parseAndCheck(nlohmann::json::parse(R"--({
 "personal": {
   "Steve": {
     "Id": 40, "Infractions" : 60
   }
 }
 })--"),
                managerWithOneSubNoRecursion,
                {{&steveId, 40}, {&steveInfractions, 60}});

  // Adding configuration options to the top level manager.
  int amountOfPersonal;
  managerWithOneSubNoRecursion.addOption("AmountOfPersonal", "",
                                         &amountOfPersonal, 0);

  parseAndCheck(
      nlohmann::json::parse(R"--({
 "AmountOfPersonal" : 1,
 "personal": {
   "Steve": {
     "Id": 30, "Infractions" : 70
   }
 }
 })--"),
      managerWithOneSubNoRecursion,
      {{&amountOfPersonal, 1}, {&steveId, 30}, {&steveInfractions, 70}});

  // Simple manager, with multiple sub manager and no recursion.
  ad_utility::ConfigManager managerWithMultipleSubNoRecursion{};
  ad_utility::ConfigManager& managerDave =
      managerWithMultipleSubNoRecursion.addSubManager({"personal"s, "Dave"s});
  ad_utility::ConfigManager& managerJanice =
      managerWithMultipleSubNoRecursion.addSubManager({"personal"s, "Janice"s});
  int daveId;
  managerDave.addOption("Id", "", &daveId, 7);
  int janiceId;
  managerJanice.addOption("Id", "", &janiceId, 11);
  int daveInfractions;
  managerDave.addOption("Infractions", "", &daveInfractions, 1);
  int janiceInfractions;
  managerJanice.addOption("Infractions", "", &janiceInfractions, 143);

  parseAndCheck(nlohmann::json::parse(R"--({
 "personal": {
   "Dave": {
     "Id": 4, "Infractions" : 0
   },
   "Janice": {
     "Id": 0, "Infractions" : 6
   }
 }
 })--"),
                managerWithMultipleSubNoRecursion,
                {{&daveId, 4},
                 {&daveInfractions, 0},
                 {&janiceId, 0},
                 {&janiceInfractions, 6}});

  // Adding configuration options to the top level manager.
  managerWithMultipleSubNoRecursion.addOption("AmountOfPersonal", "",
                                              &amountOfPersonal, 0);

  parseAndCheck(nlohmann::json::parse(R"--({
 "AmountOfPersonal" : 1,
 "personal": {
   "Dave": {
     "Id": 6, "Infractions" : 2
   },
   "Janice": {
     "Id": 2, "Infractions" : 8
   }
 }
 })--"),
                managerWithMultipleSubNoRecursion,
                {{&amountOfPersonal, 1},
                 {&daveId, 6},
                 {&daveInfractions, 2},
                 {&janiceId, 2},
                 {&janiceInfractions, 8}});

  // Complex manager with recursion.
  ad_utility::ConfigManager managerWithRecursion{};
  ad_utility::ConfigManager& managerDepth1 =
      managerWithRecursion.addSubManager({"depth1"s});
  ad_utility::ConfigManager& managerDepth2 =
      managerDepth1.addSubManager({"depth2"s});

  ad_utility::ConfigManager& managerAlex =
      managerDepth2.addSubManager({"personal"s, "Alex"s});
  int alexId;
  managerAlex.addOption("Id", "", &alexId, 8);
  int alexInfractions;
  managerAlex.addOption("Infractions", "", &alexInfractions, 4);

  ad_utility::ConfigManager& managerPeter =
      managerDepth2.addSubManager({"personal"s, "Peter"s});
  int peterId;
  managerPeter.addOption("Id", "", &peterId, 8);
  int peterInfractions;
  managerPeter.addOption("Infractions", "", &peterInfractions, 4);

  parseAndCheck(nlohmann::json::parse(R"--({
 "depth1": {
     "depth2": {
         "personal": {
           "Alex": {
             "Id": 4, "Infractions" : 0
           },
           "Peter": {
             "Id": 0, "Infractions" : 6
           }
         }
     }
 }
 })--"),
                managerWithRecursion,
                {{&alexId, 4},
                 {&alexInfractions, 0},
                 {&peterId, 0},
                 {&peterInfractions, 6}});

  // Add an option to `managerDepth2`.
  int someOptionAtDepth2;
  managerDepth2.addOption("someOption", "", &someOptionAtDepth2, 7);

  parseAndCheck(nlohmann::json::parse(R"--({
 "depth1": {
     "depth2": {
         "someOption" : 9,
         "personal": {
           "Alex": {
             "Id": 6, "Infractions" : 2
           },
           "Peter": {
             "Id": 2, "Infractions" : 8
           }
         }
     }
 }
 })--"),
                managerWithRecursion,
                {{&someOptionAtDepth2, 9},
                 {&alexId, 6},
                 {&alexInfractions, 2},
                 {&peterId, 2},
                 {&peterInfractions, 8}});

  // Add an option to `managerDepth1`.
  int someOptionAtDepth1;
  managerDepth1.addOption("someOption", "", &someOptionAtDepth1, 10);

  parseAndCheck(nlohmann::json::parse(R"--({
 "depth1": {
     "someOption" : 3,
     "depth2": {
         "someOption" : 7,
         "personal": {
           "Alex": {
             "Id": 4, "Infractions" : 0
           },
           "Peter": {
             "Id": 0, "Infractions" : 6
           }
         }
     }
 }
 })--"),
                managerWithRecursion,
                {{&someOptionAtDepth1, 3},
                 {&someOptionAtDepth2, 7},
                 {&alexId, 4},
                 {&alexInfractions, 0},
                 {&peterId, 0},
                 {&peterInfractions, 6}});

  // Add a second sub manager to `managerDepth1`.
  int someOptionInSecondSubManagerAtDepth1;
  managerDepth1.addSubManager({"random"s})
      .addOption("someOption", "", &someOptionInSecondSubManagerAtDepth1, 1);

  parseAndCheck(nlohmann::json::parse(R"--({
 "depth1": {
     "random": {
       "someOption" : 8
     },
     "someOption" : 1,
     "depth2": {
         "someOption" : 5,
         "personal": {
           "Alex": {
             "Id": 2, "Infractions" : -2
           },
           "Peter": {
             "Id": -2, "Infractions" : 4
           }
         }
     }
 }
 })--"),
                managerWithRecursion,
                {{&someOptionInSecondSubManagerAtDepth1, 8},
                 {&someOptionAtDepth1, 1},
                 {&someOptionAtDepth2, 5},
                 {&alexId, 2},
                 {&alexInfractions, -2},
                 {&peterId, -2},
                 {&peterInfractions, 4}});
}

TEST(ConfigManagerTest, ParseConfigExceptionWithoutSubManagerTest) {
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
          R"--({"depth_0":{"Without_default":42, "with_default" :
           [39]}})--")),
      ::testing::ContainsRegex(R"('/depth_0/with_default')"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"depth_0":{"Without_default":42, "test_string" :
           "test"}})--")),
      ::testing::ContainsRegex(R"('/depth_0/test_string')"));

  /*
  Should throw an exception, if we try set an option with a value, that we
  already know, can't be valid, regardless of the actual internal type of the
  configuration option. That is, it's neither an array, or a primitive.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"depth_0":{"Without_default":42, "With_default" : {"value" :
           4}}})--")),
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

TEST(ConfigManagerTest, ParseConfigExceptionWithSubManagerTest) {
  ad_utility::ConfigManager config{};

  // Empty sub managers are not allowed.
  ad_utility::ConfigManager& m1 = config.addSubManager({"some"s, "manager"s});
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(R"--({})--")),
      ::testing::ContainsRegex(R"('/some/manager')"));
  int notUsedInt;
  config.addOption("Ignore", "Must not be set. Has default value.", &notUsedInt,
                   41);
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(R"--({})--")),
      ::testing::ContainsRegex(R"('/some/manager')"));

  // Add one option with default and one without.
  std::vector<int> notUsedVector;
  m1.addOption({"depth_0"s, "Without_default"s},
               "Must be set. Has no default value.", &notUsedInt);
  m1.addOption({"depth_0"s, "With_default"s},
               "Must not be set. Has default value.", &notUsedVector, {40, 41});

  // Should throw an exception, if we don't set all options, that must be set.
  ASSERT_THROW(config.parseConfig(nlohmann::json::parse(R"--({})--")),
               ad_utility::ConfigOptionWasntSetException);

  // Should throw an exception, if we try set an option, that isn't there.
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"some":{ "manager": {"depth_0":{"Without_default":42,
           "with_default" : [39]}}}})--")),
      ::testing::ContainsRegex(R"('/some/manager/depth_0/with_default')"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"some":{ "manager": {"depth_0":{"Without_default":42,
           "test_string" : "test"}}}})--")),
      ::testing::ContainsRegex(R"('/some/manager/depth_0/test_string')"));

  /*
  Should throw an exception, if we try set an option with a value, that we
  already know, can't be valid, regardless of the actual internal type of the
  configuration option. That is, it's neither an array, nor a primitive.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.parseConfig(nlohmann::json::parse(
          R"--({"some":{ "manager": {"depth_0":{"Without_default":42,
           "With_default" : {"value" : 4}}}}})--")),
      ::testing::ContainsRegex(
          R"('/some/manager/depth_0/With_default/value')"));

  // Repeat all those tests, but with a second sub manager added to the first
  // one.
  ad_utility::ConfigManager config2{};

  // Empty sub managers are not allowed.
  ad_utility::ConfigManager& config2m1 =
      config2.addSubManager({"some"s, "manager"s});
  ad_utility::ConfigManager& config2m2 =
      config2m1.addSubManager({"some"s, "manager"s});
  AD_EXPECT_THROW_WITH_MESSAGE(
      config2.parseConfig(nlohmann::json::parse(R"--({})--")),
      ::testing::ContainsRegex(R"('/some/manager/some/manager')"));
  config2.addOption("Ignore", "Must not be set. Has default value.",
                    &notUsedInt, 41);
  AD_EXPECT_THROW_WITH_MESSAGE(
      config2.parseConfig(nlohmann::json::parse(R"--({})--")),
      ::testing::ContainsRegex(R"('/some/manager/some/manager')"));
  config2m1.addOption("Ignore", "Must not be set. Has default value.",
                      &notUsedInt, 41);
  AD_EXPECT_THROW_WITH_MESSAGE(
      config2.parseConfig(nlohmann::json::parse(R"--({})--")),
      ::testing::ContainsRegex(R"('/some/manager/some/manager')"));

  // Add one option with default and one without.
  config2m2.addOption({"depth_0"s, "Without_default"s},
                      "Must be set. Has no default value.", &notUsedInt);
  config2m2.addOption({"depth_0"s, "With_default"s},
                      "Must not be set. Has default value.", &notUsedVector,
                      {40, 41});

  // Should throw an exception, if we don't set all options, that must be set.
  ASSERT_THROW(config2.parseConfig(nlohmann::json::parse(R"--({})--")),
               ad_utility::ConfigOptionWasntSetException);

  // Should throw an exception, if we try set an option, that isn't there.
  AD_EXPECT_THROW_WITH_MESSAGE(
      config2.parseConfig(nlohmann::json::parse(
          R"--({"some":{ "manager": {"some":{ "manager":
           {"depth_0":{"Without_default":42, "with_default" : [39]}}}}}})--")),
      ::testing::ContainsRegex(
          R"('/some/manager/some/manager/depth_0/with_default')"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      config2.parseConfig(nlohmann::json::parse(
          R"--({"some":{ "manager": {"some":{ "manager":
           {"depth_0":{"Without_default":42, "test_string" :
           "test"}}}}}})--")),
      ::testing::ContainsRegex(
          R"('/some/manager/some/manager/depth_0/test_string')"));

  /*
  Should throw an exception, if we try set an option with a value, that we
  already know, can't be valid, regardless of the actual internal type of the
  configuration option. That is, it's neither an array, nor a primitive.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config2.parseConfig(nlohmann::json::parse(
          R"--({"some":{ "manager": {"some":{ "manager":
           {"depth_0":{"Without_default":42, "With_default" : {"value" :
           4}}}}}}})--")),
      ::testing::ContainsRegex(
          R"('/some/manager/some/manager/depth_0/With_default/value')"));
}

TEST(ConfigManagerTest, ParseShortHandTest) {
  ad_utility::ConfigManager config{};

  // Add integer options.
  int somePositiveNumberInt;
  const ConfigOption& somePositiveNumber = config.addOption(
      "somePositiveNumber", "Must be set. Has no default value.",
      &somePositiveNumberInt);
  int someNegativNumberInt;
  const ConfigOption& someNegativNumber = config.addOption(
      "someNegativNumber", "Must be set. Has no default value.",
      &someNegativNumberInt);

  // Add integer list.
  std::vector<int> someIntegerlistIntVector;
  const ConfigOption& someIntegerlist =
      config.addOption("someIntegerlist", "Must be set. Has no default value.",
                       &someIntegerlistIntVector);

  // Add floating point options.
  float somePositiveFloatingPointFloat;
  const ConfigOption& somePositiveFloatingPoint = config.addOption(
      "somePositiveFloatingPoint", "Must be set. Has no default value.",
      &somePositiveFloatingPointFloat);
  float someNegativFloatingPointFloat;
  const ConfigOption& someNegativFloatingPoint = config.addOption(
      "someNegativFloatingPoint", "Must be set. Has no default value.",
      &someNegativFloatingPointFloat);

  // Add floating point list.
  std::vector<float> someFloatingPointListFloatVector;
  const ConfigOption& someFloatingPointList = config.addOption(
      "someFloatingPointList", "Must be set. Has no default value.",
      &someFloatingPointListFloatVector);

  // Add boolean options.
  bool boolTrueBool;
  const ConfigOption& boolTrue = config.addOption(
      "boolTrue", "Must be set. Has no default value.", &boolTrueBool);
  bool boolFalseBool;
  const ConfigOption& boolFalse = config.addOption(
      "boolFalse", "Must be set. Has no default value.", &boolFalseBool);

  // Add boolean list.
  std::vector<bool> someBooleanListBoolVector;
  const ConfigOption& someBooleanList =
      config.addOption("someBooleanList", "Must be set. Has no default value.",
                       &someBooleanListBoolVector);

  // Add string option.
  std::string myNameString;
  const ConfigOption& myName = config.addOption(
      "myName", "Must be set. Has no default value.", &myNameString);

  // Add string list.
  std::vector<std::string> someStringListStringVector;
  const ConfigOption& someStringList =
      config.addOption("someStringList", "Must be set. Has no default value.",
                       &someStringListStringVector);

  // Add option with deeper level.
  std::vector<int> deeperIntVector;
  const ConfigOption& deeperIntVectorOption =
      config.addOption({"depth"s, "here"s, "list"s},
                       "Must be set. Has no default value.", &deeperIntVector);

  // This one will not be changed, in order to test, that options, that are
  // not set at run time, are not changed.
  int noChangeInt;
  const ConfigOption& noChange =
      config.addOption("No_change", "", &noChangeInt, 10);

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

  ad_utility::ConfigManager& subMan =
      config.addSubManager({"Just"s, "some"s, "sub-manager"});
  subMan.addOption("WithDefault", "", &notUsed, 42);
  subMan.addOption("WithoutDefault", "", &notUsed);
  ASSERT_NO_THROW(config.printConfigurationDoc(false));
  ASSERT_NO_THROW(config.printConfigurationDoc(true));

  // Printing with an empty sub manager should never be possible.
  subMan.addSubManager({"Just"s, "some"s, "other"s, "sub-manager"});
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.printConfigurationDoc(false),
      ::testing::ContainsRegex(
          R"('/Just/some/sub-manager/Just/some/other/sub-manager')"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.printConfigurationDoc(true),
      ::testing::ContainsRegex(
          R"('/Just/some/sub-manager/Just/some/other/sub-manager')"));
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

TEST(ConfigManagerTest, ConfigOptionWithValidator) {
  // First test with no sub manager.
  ConfigManager managerWithoutSubManager{};
  int notUsedInt;
  managerWithoutSubManager.addOption("h", "", &notUsedInt)
      .addValidator([](const int& n) { return 0 <= n && n <= 24; },
                    "Validator of h");
  checkValidator(managerWithoutSubManager,
                 nlohmann::json::parse(R"--({"h" : 10})--"),
                 nlohmann::json::parse(R"--({"h" : 100})--"), "Validator of h");

  // Manager with a simple sub manager.
  ConfigManager managerWithSubManager{};
  ConfigManager& firstSubManager =
      managerWithSubManager.addSubManager({"first"s, "manager"s});
  ConfigManager& secondSubManager =
      firstSubManager.addSubManager({"second"s, "manager"s});
  secondSubManager.addOption("h", "", &notUsedInt)
      .addValidator([](const int& n) { return 0 <= n && n <= 24; },
                    "Validator of h");
  checkValidator(
      managerWithSubManager,
      nlohmann::json::parse(
          R"--({"first" : { "manager" : {"second" : { "manager" : {"h" : 10}}}}})--"),
      nlohmann::json::parse(
          R"--({"first" : { "manager" : {"second" : { "manager" : {"h" : 100}}}}})--"),
      "Validator of h");

  std::string sString;
  firstSubManager.addOption("s", "", &sString)
      .addValidator(
          [](const std::string& n) { return n.starts_with("String s"); },
          "Validator of s");
  checkValidator(
      managerWithSubManager,
      nlohmann::json::parse(
          R"--({"first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 10}}}}})--"),
      nlohmann::json::parse(
          R"--({"first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 101}}}}})--"),
      "Validator of h");
  checkValidator(
      managerWithSubManager,
      nlohmann::json::parse(
          R"--({"first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 10}}}}})--"),
      nlohmann::json::parse(
          R"--({"first" : { "manager" : {"s" : "String", "second" : { "manager" : {"h" : 10}}}}})--"),
      "Validator of s");

  std::vector<bool> boolVector;
  managerWithSubManager.addOption("b", "", &boolVector)
      .addValidator(
          [](const std::vector<bool>& b) {
            return std::ranges::any_of(b, std::identity{});
          },
          "Validator of b");
  checkValidator(
      managerWithSubManager,
      nlohmann::json::parse(
          R"--({"b" : [true], "first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 10}}}}})--"),
      nlohmann::json::parse(
          R"--({"b" : [true], "first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 101}}}}})--"),
      "Validator of h");
  checkValidator(
      managerWithSubManager,
      nlohmann::json::parse(
          R"--({"b" : [true], "first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 10}}}}})--"),
      nlohmann::json::parse(
          R"--({"b" : [true], "first" : { "manager" : {"s" : "String", "second" : { "manager" : {"h" : 10}}}}})--"),
      "Validator of s");
  checkValidator(
      managerWithSubManager,
      nlohmann::json::parse(
          R"--({"b" : [false ,false, true], "first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 10}}}}})--"),
      nlohmann::json::parse(
          R"--({"b" : [false, false, false, false], "first" : { "manager" : {"s" : "String s", "second" : { "manager" : {"h" : 10}}}}})--"),
      "Validator of b");
}

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
  @brief Build a validator with more than one parameter, out of results from
  `generateSingleParameterValidatorFunction` in such a way, that it keeps the
  invariant of `generateSingleParameterValidatorFunction`.

  @tparam Ts The parameter types for the function arguments of the validator.
  The generated function will have the arguments `const Ts&...`.

  @param variant See `generateSingleParameterValidatorFunction`.
  */
  auto generateValidator = [&adjustVariantArgument]<typename... Ts>(
                               size_t variant) {
    return [variant, &adjustVariantArgument](const Ts&... args) {
      return (
          generateSingleParameterValidatorFunction<Ts>(
              adjustVariantArgument.template operator()<Ts>(variant))(args) &&
          ...);
    };
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

  using PairPathAndPointerToConfigOption =
      std::pair<nlohmann::json::json_pointer, ConfigOption*>;

  /*
  @brief Add a validator, created via `generateValidator` and named via
  `generateValidatorName`, to the given config manager.

  @tparam Ts The parameter types for the validator functions.

  @param variant See `generateSingleParameterValidatorFunction`. Must be higher
  than 0.
  @param m The config manager, to which the validator will be added.
  @param validatorArguments The values of the configuration options will be
  passed as arguments to the validator function, in the same order as given
  here.
  */
  auto addValidatorToConfigManager =
      [&generateValidator, &generateValidatorName ]<typename... Ts>(
          size_t variant, ConfigManager & m,
          const std::same_as<ConfigOption> auto&... validatorArguments)
          requires(sizeof...(Ts) == sizeof...(validatorArguments)) {
    AD_CORRECTNESS_CHECK(variant >= 1);

    // Add the new validator
    m.addValidator<Ts...>(
        generateValidator.template operator()<Ts...>(variant),
        generateValidatorName.template operator()<Ts...>(variant),
        validatorArguments...);
  };

  /*
  @brief Test all validator functions generated by `addValidatorToConfigManager`
  for a given range of `variant` and a specific configuration of `Ts`. Note:
  This is for specificly for testing the validators generated by calling
  `addValidatorToConfigManager` with a all values in `[variantStart,
  variantEnd)` as `variant` and all other arguments unchanged. It won't work, if
  there are any other validators in the `configManager`.

  @tparam Ts The parameter types, that `addValidatorToConfigManager` was called
  with.

  @param variantStart, variantEnd The range of variant values,
  `addValidatorToConfigManager` was called with.
  @param m The config manager, to which the validators were added.
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
    AD_CORRECTNESS_CHECK(variantStart >= 1);

    // Using the invariant of our function generator, to create valid
    // and none valid values for all added validators.
    for (size_t validatorNumber = variantStart; validatorNumber < variantEnd;
         validatorNumber++) {
      nlohmann::json validJson(defaultValues);
      ((validJson[configOptionPaths] = createDummyValueForValidator<Ts>(
            adjustVariantArgument.template operator()<Ts>(variantEnd))),
       ...);

      nlohmann::json invalidJson(defaultValues);
      ((invalidJson[configOptionPaths] = createDummyValueForValidator<Ts>(
            adjustVariantArgument.template operator()<Ts>(validatorNumber) -
            1)),
       ...);

      /*
      If we have any bools, than we can't use the invariant of the
      generated validators to go 'through' the added validators via invalid
      values.
      Instead, we can only ever fail the first added validator, because they
      are all identical.
      */
      if constexpr ((std::is_same_v<bool, Ts> || ...)) {
        checkValidator(m, validJson, invalidJson,
                       generateValidatorName.template operator()<Ts...>(1));
      } else {
        checkValidator(
            m, validJson, invalidJson,
            generateValidatorName.template operator()<Ts...>(validatorNumber));
      }
    }
  };

  /*
  @brief Does the tests for config manager, where there either is no sub
  manager, or only the top manager has validators.

  @tparam Ts The parameter types for the validator functions.

  @param m The config manager, to which validators will be added and on which
  `parseConfig` will be called. Note, that those validators will **not** be
  deleted and that the given config manager should have zero already existing
  validators.
  @param defaultValues The values for all the configuration options, that will
  not be checked via the validator.
  @param validatorArguments As list of pairs, that contain a json pointer to the
  position of the configuration option in the configuration manager and a normal
  pointer to the `ConfigOption` object itself. The described configuration
  options will be passed as arguments to the validator function, in the same
  order as given here.
  */
  auto doTestNoValidatorInSubManager =
      [&addValidatorToConfigManager, &
       testGeneratedValidatorsOfConfigManager ]<typename... Ts>(
          ConfigManager & m, const nlohmann::json& defaultValues,
          const std::same_as<
              PairPathAndPointerToConfigOption> auto&... validatorArguments)
          requires(sizeof...(Ts) == sizeof...(validatorArguments)) {
    // How many validators are to be added?
    constexpr size_t NUMBER_OF_VALIDATORS{5};

    for (size_t i = 1; i < NUMBER_OF_VALIDATORS + 1; i++) {
      // Add a new validator
      addValidatorToConfigManager.template operator()<Ts...>(
          i, m, *validatorArguments.second...);

      // Test all the added validators.
      testGeneratedValidatorsOfConfigManager.template operator()<Ts...>(
          1, i + 1, m, defaultValues, validatorArguments.first...);
    }
  };

  auto doTestAlwaysValidatorInSubManager =
      [&addValidatorToConfigManager, &
       testGeneratedValidatorsOfConfigManager ]<typename... Ts>(
          ConfigManager & m, ConfigManager & subM,
          const nlohmann::json& defaultValues,
          const std::same_as<
              PairPathAndPointerToConfigOption> auto&... validatorArguments)
          requires(sizeof...(Ts) == sizeof...(validatorArguments)) {
    // How many validators are to be added to each of the managers?
    constexpr size_t NUMBER_OF_VALIDATORS{5};

    // Add validators to the sub manager and check, if parsing with the top
    // manager goes correctly.
    for (size_t i = 1; i < NUMBER_OF_VALIDATORS + 1; i++) {
      // Add a new validator
      addValidatorToConfigManager.template operator()<Ts...>(
          i, subM, *validatorArguments.second...);

      // Test all the added validators.
      testGeneratedValidatorsOfConfigManager.template operator()<Ts...>(
          1, i + 1, m, defaultValues, validatorArguments.first...);
    }

    // Now, we add additional validators to the top manager.
    for (size_t i = 1 + NUMBER_OF_VALIDATORS; i < 1 + NUMBER_OF_VALIDATORS * 2;
         i++) {
      // Add a new validator
      addValidatorToConfigManager.template operator()<Ts...>(
          i, m, *validatorArguments.second...);

      // Test all the added validators.
      testGeneratedValidatorsOfConfigManager.template operator()<Ts...>(
          1, i + 1, m, defaultValues, validatorArguments.first...);
    }
  };

  // Does all tests for single parameter validators for a given type.
  auto doSingleParameterTests =
      [&doTestNoValidatorInSubManager,
       &doTestAlwaysValidatorInSubManager]<typename Type>() {
        // Variables needed for configuration options.
        Type firstVar;

        // No sub manager.
        ConfigManager mNoSub;
        ConfigOption& mNoSubOption =
            mNoSub.addOption("someValue", "", &firstVar);
        doTestNoValidatorInSubManager.template operator()<Type>(
            mNoSub, nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(nlohmann::json::json_pointer("/someValue"),
                           &mNoSubOption));

        // With sub manager. Sub manager has no validators of its own.
        ConfigManager mSubNoValidator;
        ConfigOption& mSubNoValidatorOption =
            mSubNoValidator.addSubManager({"some"s, "manager"s})
                .addOption("someValue", "", &firstVar);
        doTestNoValidatorInSubManager.template operator()<Type>(
            mSubNoValidator, nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue"),
                &mSubNoValidatorOption));

        /*
        With sub manager.
        Covers the following cases:
        - Sub manager has validators of its own, however the manager does not.
        - Sub manager has validators of its own, as does the manager.
        */
        ConfigManager mSubWithValidator;
        ConfigManager& mSubWithValidatorSub =
            mSubWithValidator.addSubManager({"some"s, "manager"s});
        ConfigOption& mSubWithValidatorOption =
            mSubWithValidatorSub.addOption("someValue", "", &firstVar);
        doTestAlwaysValidatorInSubManager.template operator()<Type>(
            mSubWithValidator, mSubWithValidatorSub,
            nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue"),
                &mSubWithValidatorOption));
      };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<1>(
      doSingleParameterTests, callGivenLambdaWithAllCombinationsOfTypes);

  // Does all tests for validators with two parameter types for the given type
  // combination.
  auto doDoubleParameterTests = [&doTestNoValidatorInSubManager,
                                 &doTestAlwaysValidatorInSubManager]<
                                    typename Type1, typename Type2>() {
    // Variables needed for configuration options.
    Type1 firstVar;
    Type2 secondVar;

    // No sub manager.
    ConfigManager mNoSub;
    ConfigOption& mNoSubOption1 = mNoSub.addOption("someValue1", "", &firstVar);
    ConfigOption& mNoSubOption2 =
        mNoSub.addOption("someValue2", "", &secondVar);
    doTestNoValidatorInSubManager.template operator()<Type1, Type2>(
        mNoSub, nlohmann::json(nlohmann::json::value_t::object),
        std::make_pair(nlohmann::json::json_pointer("/someValue1"),
                       &mNoSubOption1),
        std::make_pair(nlohmann::json::json_pointer("/someValue2"),
                       &mNoSubOption2));

    // With sub manager. Sub manager has no validators of its own.
    ConfigManager mSubNoValidator;
    ConfigManager& mSubNoValidatorSub =
        mSubNoValidator.addSubManager({"some"s, "manager"s});
    ConfigOption& mSubNoValidatorOption1 =
        mSubNoValidatorSub.addOption("someValue1", "", &firstVar);
    ConfigOption& mSubNoValidatorOption2 =
        mSubNoValidatorSub.addOption("someValue2", "", &secondVar);
    doTestNoValidatorInSubManager.template operator()<Type1, Type2>(
        mSubNoValidator, nlohmann::json(nlohmann::json::value_t::object),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue1"),
                       &mSubNoValidatorOption1),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue2"),
                       &mSubNoValidatorOption2));

    /*
    With sub manager.
    Covers the following cases:
    - Sub manager has validators of its own, however the manager does not.
    - Sub manager has validators of its own, as does the manager.
    */
    ConfigManager mSubWithValidator;
    ConfigManager& mSubWithValidatorSub =
        mSubWithValidator.addSubManager({"some"s, "manager"s});
    ConfigOption& mSubWithValidatorOption1 =
        mSubWithValidatorSub.addOption("someValue1", "", &firstVar);
    ConfigOption& mSubWithValidatorOption2 =
        mSubWithValidatorSub.addOption("someValue2", "", &secondVar);
    doTestAlwaysValidatorInSubManager.template operator()<Type1, Type2>(
        mSubWithValidator, mSubWithValidatorSub,
        nlohmann::json(nlohmann::json::value_t::object),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue1"),
                       &mSubWithValidatorOption1),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue2"),
                       &mSubWithValidatorOption2));
  };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<2>(
      doDoubleParameterTests, callGivenLambdaWithAllCombinationsOfTypes);

  // Does all tests for validators with three parameter types, for the given
  // type combination.
  auto doTripleParameterTests = [&doTestNoValidatorInSubManager,
                                 &doTestAlwaysValidatorInSubManager]<
                                    typename Type1, typename Type2,
                                    typename Type3>() {
    // Variables needed for configuration options.
    Type1 firstVar;
    Type2 secondVar;
    Type3 thirdVar;

    // No sub manager.
    ConfigManager mNoSub;
    ConfigOption& mNoSubOption1 = mNoSub.addOption("someValue1", "", &firstVar);
    ConfigOption& mNoSubOption2 =
        mNoSub.addOption("someValue2", "", &secondVar);
    ConfigOption& mNoSubOption3 = mNoSub.addOption("someValue3", "", &thirdVar);
    doTestNoValidatorInSubManager.template operator()<Type1, Type2, Type3>(
        mNoSub, nlohmann::json(nlohmann::json::value_t::object),
        std::make_pair(nlohmann::json::json_pointer("/someValue1"),
                       &mNoSubOption1),
        std::make_pair(nlohmann::json::json_pointer("/someValue2"),
                       &mNoSubOption2),
        std::make_pair(nlohmann::json::json_pointer("/someValue3"),
                       &mNoSubOption3));

    // With sub manager. Sub manager has no validators of its own.
    ConfigManager mSubNoValidator;
    ConfigManager& mSubNoValidatorSub =
        mSubNoValidator.addSubManager({"some"s, "manager"s});
    ConfigOption& mSubNoValidatorOption1 =
        mSubNoValidatorSub.addOption("someValue1", "", &firstVar);
    ConfigOption& mSubNoValidatorOption2 =
        mSubNoValidatorSub.addOption("someValue2", "", &secondVar);
    ConfigOption& mSubNoValidatorOption3 =
        mSubNoValidatorSub.addOption("someValue3", "", &thirdVar);
    doTestNoValidatorInSubManager.template operator()<Type1, Type2, Type3>(
        mSubNoValidator, nlohmann::json(nlohmann::json::value_t::object),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue1"),
                       &mSubNoValidatorOption1),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue2"),
                       &mSubNoValidatorOption2),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue3"),
                       &mSubNoValidatorOption3));

    /*
    With sub manager.
    Covers the following cases:
    - Sub manager has validators of its own, however the manager does not.
    - Sub manager has validators of its own, as does the manager.
    */
    ConfigManager mSubWithValidator;
    ConfigManager& mSubWithValidatorSub =
        mSubWithValidator.addSubManager({"some"s, "manager"s});
    ConfigOption& mSubWithValidatorOption1 =
        mSubWithValidatorSub.addOption("someValue1", "", &firstVar);
    ConfigOption& mSubWithValidatorOption2 =
        mSubWithValidatorSub.addOption("someValue2", "", &secondVar);
    ConfigOption& mSubWithValidatorOption3 =
        mSubWithValidatorSub.addOption("someValue3", "", &thirdVar);
    doTestAlwaysValidatorInSubManager.template operator()<Type1, Type2, Type3>(
        mSubWithValidator, mSubWithValidatorSub,
        nlohmann::json(nlohmann::json::value_t::object),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue1"),
                       &mSubWithValidatorOption1),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue2"),
                       &mSubWithValidatorOption2),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue3"),
                       &mSubWithValidatorOption3));
  };

  // TODO This increases compilation time by a huge margin (around 8 minutes).
  // Take a look, if the performance can be improved.
  /*
  callGivenLambdaWithAllCombinationsOfTypes.template operator()<3>(
      doTripleParameterTests, callGivenLambdaWithAllCombinationsOfTypes);
  */
}

}  // namespace ad_utility
