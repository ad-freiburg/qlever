// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "./util/ConfigOptionHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/PrintConfigurationDocComparisonString.h"
#include "./util/ValidatorHelpers.h"
#include "gtest/gtest.h"
#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigManager/Validator.h"
#include "util/ConstexprUtils.h"
#include "util/CtreHelpers.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/StringUtils.h"
#include "util/TupleForEach.h"
#include "util/TypeTraits.h"
#include "util/json.h"

using namespace std::string_literals;

namespace ad_utility::ConfigManagerImpl {

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
TEST(ConfigManagerTest, AddConfigurationOptionExceptionTest) {
  ad_utility::ConfigManager config{};

  // Configuration options for testing.
  int notUsed;
  config.addOption({"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s},
                   "", &notUsed, 42);

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

  // Trying to add a configuration option with the same name at the same
  // place, should cause an error.
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addOption(
          {"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s}, "",
          &notUsed, 42),
      ::testing::ContainsRegex(
          R"('\[Shared_part\]\[Unique_part_1\]\[Sense_of_existence\]')"));

  /*
  Trying to add a configuration option, whose entire path is a prefix of the
  path of an already added option, should cause an exception. After all, this
  would imply, that the already existing option is part of this new option.
  Which is not supported at the moment.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addOption({"Shared_part"s, "Unique_part_1"s}, "", &notUsed, 42),
      ::testing::ContainsRegex(R"('\[Shared_part\]\[Unique_part_1\]')"));

  /*
  Trying to add a configuration option, who contains the entire path of an
  already added configuration option as prefix, should cause an exception. After
  all, this would imply, that the new option is part of the already existing
  option. Which is not supported at the moment.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addOption({"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s,
                        "Answer"s, "42"s},
                       "", &notUsed, 42),
      ::testing::ContainsRegex(
          R"('\[Shared_part\]\[Unique_part_1\]\[Sense_of_existence\]\[Answer\]\[42\]')"));

  /*
  Trying to add a configuration option, whose entire path is a prefix of the
  path of an already added sub manager, should cause an exception. After all,
  this would imply, that the sub manger is part of this new option. Which is not
  supported at the moment.
  */
  config.addSubManager({"sub"s, "manager"s})
      .addOption("someOpt"s, "", &notUsed, 42);
  AD_EXPECT_THROW_WITH_MESSAGE(config.addOption("sub"s, "", &notUsed, 42),
                               ::testing::ContainsRegex(R"('\[sub\]')"));

  /*
  Trying to add a configuration option, who contains the entire path of an
  already added sub manger as prefix, should cause an exception. After all, such
  recursive builds should have been done on `C++` level, not json level.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addOption({"sub"s, "manager"s, "someOption"s}, "", &notUsed, 42),
      ::testing::ContainsRegex(R"('\[sub\]\[manager\]\[someOption\]')"));

  /*
  Trying to add a configuration option, whose path is the path of an already
  added sub manger, should cause an exception.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addOption({"sub"s, "manager"s}, "", &notUsed, 42),
      ::testing::ContainsRegex(R"('\[sub\]\[manager\]')"));
}

/*
Cases, that caused exceptions with `addOption` in the past, even though they
shouldn't have.
*/
TEST(ConfigManagerTest, AddConfigurationOptionFalseExceptionTest) {
  /*
  First of, a short explanation, of what a path collision is.

  A path collisions is, when the path for a new config option, or sub manager,
  would cause problems with the path of an already added config option, or
  manager. More specifically, we call the following cases path collisions:
  - Same path. Makes it impossible for the user to later identify the correct
  one.
  - Prefix of the path of an already exiting option/manager. This would mean,
  that the old config option, or sub manager, are part of the new config option,
  or sub manager from the view of json. This is not allowed for a new config
  option, because there is currently no support to put config options, or sub
  managers, inside config options. For a new sub manager it's not allowed,
  because nesting should be done on the `C++` level, not on the json path level.
  - The path of an already exiting option/manager is a prefix of the new path.
  The reasons, why it's not allowed, are basically the same.

  In the past, it was possible to cause path collisions even though there
  weren't any, by having paths, which json pointer representation fullfilled the
  conditions for one of the cases. For example: It should be possible, to have
  one option under `[prefixes]` and one under
  `[prefixes-eternal]`. But, because `/prefixes` is a prefix of
  `/prefixes-eternal` a false exception was thrown.

  Which is why, we are testing the second and third case for path collisions
  with such cases here.
  */
  ad_utility::ConfigManager config{};

  // Configuration options for testing.
  int notUsed;
  config.addOption({"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s},
                   "", &notUsed, 42);

  /*
  Adding a config option, where the json pointer version of the path is a prefix
  of the json pointer version of a config option path, that is is already in
  use.
  */
  ASSERT_NO_THROW(
      config.addOption({"Shared_part"s, "Unique_part"s}, "", &notUsed, 42));

  /*
  Adding a config option and there exists an already in use config option path,
  whose json pointer version is a prefix of the json pointer version of the new
  path.
  */
  ASSERT_NO_THROW(config.addOption(
      {"Shared_part"s, "Unique_part_1"s, "Sense_of_existence_42"s}, "",
      &notUsed, 42));

  /*
  Adding a config option, where the json pointer version of the path is a prefix
  of the json pointer version of a sub manager path, that is is already in use.
  */
  config.addSubManager({"sub"s, "manager"s})
      .addOption("someOpt"s, "", &notUsed, 42);
  ASSERT_NO_THROW(config.addOption({"sub"s, "man"s}, "", &notUsed, 42));

  /*
  Adding a config option and there exists an already in use sub manager path,
  whose json pointer version is a prefix of the json pointer version of the new
  path.
  */
  ASSERT_NO_THROW(config.addOption({"sub"s, "manager4"s}, "", &notUsed, 42));
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

  // Trying to add a sub manager with the same name at the same place, should
  // cause an error.
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addSubManager(
          {"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s}),
      ::testing::ContainsRegex(
          R"('\[Shared_part\]\[Unique_part_1\]\[Sense_of_existence\]')"));

  /*
  Trying to add a sub manager, whose entire path is a prefix of the path of an
  already added sub manger, should cause an exception. After all, such recursive
  builds should have been done on `C++` level, not json level.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addSubManager({"Shared_part"s, "Unique_part_1"s}),
      ::testing::ContainsRegex(R"('\[Shared_part\]\[Unique_part_1\]')"));

  /*
  Trying to add a sub manager, whose path contains the entire path of an already
  added sub manager as prefix, should cause an exception. After all, such
  recursive builds should have been done on `C++` level, not json level.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addSubManager({"Shared_part"s, "Unique_part_1"s,
                            "Sense_of_existence"s, "Answer"s, "42"s}),
      ::testing::ContainsRegex(
          R"('\[Shared_part\]\[Unique_part_1\]\[Sense_of_existence\]\[Answer\]\[42\]')"));

  /*
  Trying to add a sub manger, whose entire path is a prefix of the path of an
  already added config option, should cause an exception. After all, such
  recursive builds should have been done on `C++` level, not json level.
  */
  config.addOption({"some"s, "option"s}, "", &notUsed);
  AD_EXPECT_THROW_WITH_MESSAGE(config.addSubManager({"some"s}),
                               ::testing::ContainsRegex(R"('\[some\]')"));

  /*
  Trying to add a sub manager, who contains the entire path of an already added
  config option as prefix, should cause an exception.
  After all, this would imply, that the sub manger is part of this option. Which
  is not supported at the moment.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addSubManager({"some"s, "option"s, "manager"s}),
      ::testing::ContainsRegex(R"('\[some\]\[option\]\[manager\]')"));

  /*
  Trying to add a sub manager, whose path is the path of an already added config
  option, should cause an exception.
  */
  AD_EXPECT_THROW_WITH_MESSAGE(
      config.addSubManager({"some"s, "option"s}),
      ::testing::ContainsRegex(R"('\[some\]\[option\]')"));
}

/*
Cases, that caused exceptions with `addSubManager` in the past, even though they
shouldn't have.
*/
TEST(ConfigManagerTest, AddSubManagerFalseExceptionTest) {
  /*
  First of, a short explanation, of what a path collision is.

  A path collisions is, when the path for a new config option, or sub manager,
  would cause problems with the path of an already added config option, or
  manager. More specifically, we call the following cases path collisions:
  - Same path. Makes it impossible for the user to later identify the correct
  one.
  - Prefix of the path of an already exiting option/manager. This would mean,
  that the old config option, or sub manager, are part of the new config option,
  or sub manager from the view of json. This is not allowed for a new config
  option, because there is currently no support to put config options, or sub
  managers, inside config options. For a new sub manager it's not allowed,
  because nesting should be done on the `C++` level, not on the json path level.
  - The path of an already exiting option/manager is a prefix of the new path.
  The reasons, why it's not allowed, are basically the same.

  In the past, it was possible to cause path collisions even though there
  weren't any, by having paths, which json pointer representation fullfilled the
  conditions for one of the cases. For example: It should be possible, to have
  one sub manager under `[prefixes]` and one under
  `[prefixes-eternal]`. But, because `/prefixes` is a prefix of
  `/prefixes-eternal` a false exception was thrown.

  Which is why, we are testing the second and third case for path collisions
  with such cases here.
  */
  ad_utility::ConfigManager config{};

  // Sub manager for testing. Empty sub manager are not allowed.
  int notUsed;
  config
      .addSubManager({"Shared_part"s, "Unique_part_1"s, "Sense_of_existence"s})
      .addOption("ignore", "", &notUsed);

  /*
  Adding a sub manager, where the json pointer version of the path is a prefix
  of the json pointer version of a sub manager path, that is is already in use.
  */
  ASSERT_NO_THROW(config.addSubManager({"Shared_part"s, "Unique_part"s})
                      .addOption("ignore", "", &notUsed));

  /*
  Adding a sub manager and there exists an already in use sub manager path,
  whose json pointer version is a prefix of the json pointer version of the new
  path.
  */
  ASSERT_NO_THROW(config
                      .addSubManager({"Shared_part"s, "Unique_part_1"s,
                                      "Sense_of_existence_42"s})
                      .addOption("ignore", "", &notUsed));

  /*
  Adding a sub manager, where the json pointer version of the path is a prefix
  of the json pointer version of a config option path, that is is already in
  use.
  */
  config.addOption({"some"s, "option"s}, "", &notUsed);
  ASSERT_NO_THROW(config.addSubManager({"some"s, "opt"s})
                      .addOption("ignore", "", &notUsed));

  /*
  Adding a sub manager and there exists an already in use config option path,
  whose json pointer version is a prefix of the json pointer version of the new
  path.
  */
  ASSERT_NO_THROW(config.addSubManager({"some"s, "options"s})
                      .addOption("ignore", "", &notUsed));
}

TEST(ConfigManagerTest, ParseConfigNoSubManager) {
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

  checkOption(somePositiveNumber, somePositiveNumberInt, true, 42);
  checkOption(someNegativNumber, someNegativNumberInt, true, -42);

  checkOption(someIntegerlist, someIntegerlistIntVector, true,
              std::vector{40, 41});

  checkOption(somePositiveFloatingPoint, somePositiveFloatingPointFloat, true,
              4.2f);
  checkOption(someNegativFloatingPoint, someNegativFloatingPointFloat, true,
              -4.2f);

  checkOption(someFloatingPointList, someFloatingPointListFloatVector, true,
              {4.1f, 4.2f});

  checkOption(boolTrue, boolTrueBool, true, true);
  checkOption(boolFalse, boolFalseBool, true, false);

  checkOption(someBooleanList, someBooleanListBoolVector, true,
              std::vector{true, false, true});

  checkOption(myName, myNameString, true, std::string{"Bernd"});

  checkOption(someStringList, someStringListStringVector, true,
              std::vector<std::string>{"t1", "t2"});

  checkOption(deeperIntVectorOption, deeperIntVector, true, std::vector{7, 8});

  // Is the "No Change" unchanged?
  checkOption(noChange, noChangeInt, true, 10);

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

// Human readable examples for `addValidator` with `Validator` functions.
TEST(ConfigManagerTest, HumanReadableAddValidator) {
  ConfigManager m{};

  // The number of the option should be in a range. We define the range by using
  // two validators.
  int someInt;
  decltype(auto) numberInRangeOption =
      m.addOption("numberInRange", "", &someInt);
  m.addValidator([](const int& num) { return num <= 100; },
                 "'numberInRange' must be <=100.", "", numberInRangeOption);
  m.addValidator([](const int& num) { return num > 49; },
                 "'numberInRange' must be >=50.", "", numberInRangeOption);
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
      "Exactly one bool must be choosen.", "", boolOneOption, boolTwoOption,
      boolThreeOption);
  checkValidator(
      m,
      nlohmann::json::parse(
          R"--({"numberInRange" : 60, "boolOne": true, "boolTwo": false, "boolThree": false})--"),
      nlohmann::json::parse(
          R"--({"numberInRange" : 60, "boolOne": true, "boolTwo": true, "boolThree": false})--"),
      "Exactly one bool must be choosen.");
}

// Human readable examples for `addOptionValidator` with `Validator` functions.
TEST(ConfigManagerTest, HumanReadableAddOptionValidator) {
  // Check, if all the options have a default value.
  ConfigManager mAllWithDefault;
  int firstInt;
  decltype(auto) firstOption =
      mAllWithDefault.addOption("firstOption", "", &firstInt, 10);
  mAllWithDefault.addOptionValidator(
      [](const ConfigOption& opt) { return opt.hasDefaultValue(); },
      "Every option must have a default value.", "", firstOption);
  ASSERT_NO_THROW(mAllWithDefault.parseConfig(
      nlohmann::json::parse(R"--({"firstOption": 4})--")));
  int secondInt;
  decltype(auto) secondOption =
      mAllWithDefault.addOption("secondOption", "", &secondInt);
  mAllWithDefault.addOptionValidator(
      [](const ConfigOption& opt1, const ConfigOption& opt2) {
        return opt1.hasDefaultValue() && opt2.hasDefaultValue();
      },
      "Every option must have a default value.", "", firstOption, secondOption);
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
      "Every option name must start with the letter d.", "", correctLetter);
  ASSERT_NO_THROW(
      mFirstLetter.parseConfig(nlohmann::json::parse(R"--({"dValue": 4})--")));
  decltype(auto) wrongLetter = mFirstLetter.addOption("value", "", &secondInt);
  mFirstLetter.addOptionValidator(
      [](const ConfigOption& opt1, const ConfigOption& opt2) {
        return opt1.getIdentifier().starts_with('d') &&
               opt2.getIdentifier().starts_with('d');
      },
      "Every option name must start with the letter d.", "", correctLetter,
      wrongLetter);
  ASSERT_ANY_THROW(mFirstLetter.parseConfig(
      nlohmann::json::parse(R"--({"dValue": 4, "Value" : 7})--")));
}

/*
@brief Generate an informative validator name in the form of `Config manager
validator<x> y`. With `x` being the list of function argument types and `y` an
unqiue number id.

@tparam Ts The types of the function arguments of the validator function.

@param id An optional identification number for identifying specific validator
functions.
*/
template <typename... Ts>
std::string generateValidatorName(std::optional<size_t> id) {
  static const std::string prefix = absl::StrCat(
      "Config manager validator<",
      lazyStrJoin(std::array{ConfigOption::availableTypesToString<Ts>()...},
                  ", "),
      ">");

  if (id.has_value()) {
    return absl::StrCat(prefix, " ", id.value());
  } else {
    return prefix;
  }
};

/*
@brief The test for adding a validator to a config manager.

@param addValidatorFunction A function, that adds a validator function to a
config manager. The function signature should look like this: `void func(size_t
variant, std::string_view validatorExceptionMessage, ConfigManager& m,
ConstConfigOptionProxy...)`. With `variant` being for the invariant of
`generateDummyNonExceptionValidatorFunction` and the rest for the generation, as
well as adding, of a new validator function.
@param l For better error messages, when the tests fail.
*/
void doValidatorTest(
    auto addValidatorFunction,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "doValidatorTest")};

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
  `generateDummyNonExceptionValidatorFunction`.
  The bool type in those helper functions needs special handeling, because it
  only has two values and can't fulfill the invariant, that
  `createDummyValueForValidator` and
  `generateDummyNonExceptionValidatorFunction` should fulfill.

  @tparam T Same `T` as for `createDummyValueForValidator` and
  `generateDummyNonExceptionValidatorFunction`.
  */
  auto adjustVariantArgument =
      []<typename... Ts>(size_t variantThatNeedsPossibleAdjustment) -> size_t {
    if constexpr ((std::is_same_v<Ts, bool> && ...)) {
      /*
      Even numbers for `variant` always result in true, regardless if
      passed to `createDummyValueForValidator`, or
      `generateDummyNonExceptionValidatorFunction`.
      */
      return variantThatNeedsPossibleAdjustment * 2 + 1;
    } else {
      return variantThatNeedsPossibleAdjustment;
    }
  };

  /*
  @brief Generate and add a validator, which follows the invariant of
  `generateDummyNonExceptionValidatorFunction` and was named via
  `generateValidatorName`, to the given config manager.

  @tparam Ts The parameter types for the validator functions.

  @param variant See `generateDummyNonExceptionValidatorFunction`.
  @param validatorExceptionMessage The message, that will be thrown, if the
  validator gets non valid input.
  @param m The config manager, to which the validator will be added.
  @param validatorArguments The values of the configuration options will be
  passed as arguments to the validator function, in the same order as given
  here.
  */
  auto addValidatorToConfigManager =
      [&adjustVariantArgument, &addValidatorFunction ]<typename... Ts>(
          size_t variant, ConfigManager & m,
          ConstConfigOptionProxy<Ts>... validatorArguments)
          requires(sizeof...(Ts) == sizeof...(validatorArguments)) {
    // Add the new validator
    addValidatorFunction(
        adjustVariantArgument.template operator()<Ts...>(variant),
        generateValidatorName<Ts...>(variant), m, validatorArguments...);
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
      [&adjustVariantArgument]<typename... Ts>(
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
      Because there is no strict evaluation order for validators, it is also
      impossible to say, which of the bool only validators will throw the
      exception, so we can't check for a specific validator either. Instead, we
      only check, if it was bool validator.
      */
      if constexpr ((std::is_same_v<bool, Ts> && ...)) {
        checkValidator(m, validJson, invalidJson,
                       generateValidatorName<Ts...>(std::nullopt));
      } else {
        checkValidator(m, validJson, invalidJson,
                       generateValidatorName<Ts...>(validatorNumber));
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

  /*
  @brief Do the tests for config manager with one sub manager. The sub manager
  always has validators added to them.

  @tparam Ts The parameter types for the validator functions.

  @param m The config manager, to which validators will be added and on which
  `parseConfig` will be called. Note, that those validators will **not** be
  deleted and that the given config manager should have zero already existing
  validators.
  @param subM The sub manager, to which validators will be added. Note, that
  those validators will
  **not** be deleted and that the given sub manager should have zero already
  existing validators.
  @param defaultValues The values for all the configuration options, that will
  not be checked via the validator.
  @param validatorArguments As list of pairs, that contain a json pointer to the
  position of the configuration option in the configuration manager and a proxy
  to the `ConfigOption` object itself. The described configuration options will
  be passed as arguments to the validator function, in the same order as given
  here.
  */
  auto doTestAlwaysValidatorInSubManager =
      [&addValidatorToConfigManager, &
       testGeneratedValidatorsOfConfigManager ]<typename... Ts>(
          ConfigManager & m, ConfigManager & subM,
          const nlohmann::json& defaultValues,
          const std::pair<nlohmann::json::json_pointer,
                          ConstConfigOptionProxy<Ts>>&... validatorArguments)
          requires(sizeof...(Ts) == sizeof...(validatorArguments)) {
    // How many validators are to be added to each of the managers?
    constexpr size_t NUMBER_OF_VALIDATORS{5};

    // Add validators to the sub manager and check, if parsing with the top
    // manager goes correctly.
    for (size_t i = 0; i < NUMBER_OF_VALIDATORS; i++) {
      // Add a new validator
      addValidatorToConfigManager.template operator()<Ts...>(
          i, subM, validatorArguments.second...);

      // Test all the added validators.
      testGeneratedValidatorsOfConfigManager.template operator()<Ts...>(
          0, i + 1, m, defaultValues, validatorArguments.first...);
    }

    // Now, we add additional validators to the top manager.
    for (size_t i = NUMBER_OF_VALIDATORS; i < NUMBER_OF_VALIDATORS * 2; i++) {
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
      [&doTestNoValidatorInSubManager,
       &doTestAlwaysValidatorInSubManager]<typename Type>() {
        // Variables needed for configuration options.
        Type firstVar;

        // No sub manager.
        ConfigManager mNoSub;
        decltype(auto) mNoSubOption =
            mNoSub.addOption("someValue", "", &firstVar);
        doTestNoValidatorInSubManager.template operator()<Type>(
            mNoSub, nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(nlohmann::json::json_pointer("/someValue"),
                           mNoSubOption));

        // With sub manager. Sub manager has no validators of its own.
        ConfigManager mSubNoValidator;
        decltype(auto) mSubNoValidatorOption =
            mSubNoValidator.addSubManager({"some"s, "manager"s})
                .addOption("someValue", "", &firstVar);
        doTestNoValidatorInSubManager.template operator()<Type>(
            mSubNoValidator, nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue"),
                mSubNoValidatorOption));

        /*
        With sub manager.
        Covers the following cases:
        - Sub manager has validators of its own, however the manager does not.
        - Sub manager has validators of its own, as does the manager.
        */
        ConfigManager mSubWithValidator;
        ConfigManager& mSubWithValidatorSub =
            mSubWithValidator.addSubManager({"some"s, "manager"s});
        decltype(auto) mSubWithValidatorOption =
            mSubWithValidatorSub.addOption("someValue", "", &firstVar);
        doTestAlwaysValidatorInSubManager.template operator()<Type>(
            mSubWithValidator, mSubWithValidatorSub,
            nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue"),
                mSubWithValidatorOption));
      };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<1>(
      doSingleParameterTests, callGivenLambdaWithAllCombinationsOfTypes);

  // Does all tests for validators with two parameter types for the given type
  // combination.
  auto doDoubleParameterTests =
      [&doTestNoValidatorInSubManager,
       &doTestAlwaysValidatorInSubManager]<typename Type1, typename Type2>() {
        // Variables needed for configuration options.
        Type1 firstVar;
        Type2 secondVar;

        // No sub manager.
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

        // With sub manager. Sub manager has no validators of its own.
        ConfigManager mSubNoValidator;
        ConfigManager& mSubNoValidatorSub =
            mSubNoValidator.addSubManager({"some"s, "manager"s});
        decltype(auto) mSubNoValidatorOption1 =
            mSubNoValidatorSub.addOption("someValue1", "", &firstVar);
        decltype(auto) mSubNoValidatorOption2 =
            mSubNoValidatorSub.addOption("someValue2", "", &secondVar);
        doTestNoValidatorInSubManager.template operator()<Type1, Type2>(
            mSubNoValidator, nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue1"),
                mSubNoValidatorOption1),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue2"),
                mSubNoValidatorOption2));

        /*
        With sub manager.
        Covers the following cases:
        - Sub manager has validators of its own, however the manager does not.
        - Sub manager has validators of its own, as does the manager.
        */
        ConfigManager mSubWithValidator;
        ConfigManager& mSubWithValidatorSub =
            mSubWithValidator.addSubManager({"some"s, "manager"s});
        decltype(auto) mSubWithValidatorOption1 =
            mSubWithValidatorSub.addOption("someValue1", "", &firstVar);
        decltype(auto) mSubWithValidatorOption2 =
            mSubWithValidatorSub.addOption("someValue2", "", &secondVar);
        doTestAlwaysValidatorInSubManager.template operator()<Type1, Type2>(
            mSubWithValidator, mSubWithValidatorSub,
            nlohmann::json(nlohmann::json::value_t::object),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue1"),
                mSubWithValidatorOption1),
            std::make_pair(
                nlohmann::json::json_pointer("/some/manager/someValue2"),
                mSubWithValidatorOption2));
      };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<2>(
      doDoubleParameterTests, callGivenLambdaWithAllCombinationsOfTypes);

  // Testing, if validators with different parameter types work, when added to
  // the same config manager.
  auto doDifferentParameterTests = [&addValidatorToConfigManager]<
                                       typename Type1, typename Type2>() {
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
        []<typename T1, typename T2>(
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
                         generateValidatorName<Type1>(validator1.second));

          // Value for `validator1` is valid. Value for `validator2` is invalid.
          invalidValueJson[validator1.first] =
              createDummyValueForValidator<Type1>(validator1.second + 1);
          invalidValueJson[validator2.first] =
              createDummyValueForValidator<Type2>(validator2.second);
          checkValidator(m, validValueJson, invalidValueJson,
                         generateValidatorName<Type2>(validator2.second));
        };

    // No sub manager.
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

    // With sub manager. Sub manager has no validators of its own.
    ConfigManager mSubNoValidator;
    ConfigManager& mSubNoValidatorSub =
        mSubNoValidator.addSubManager({"some"s, "manager"s});
    decltype(auto) mSubNoValidatorOption1 =
        mSubNoValidatorSub.addOption("someValue1", "", &var1);
    decltype(auto) mSubNoValidatorOption2 =
        mSubNoValidatorSub.addOption("someValue2", "", &var2);
    addValidatorToConfigManager.template operator()<Type1>(
        1, mSubNoValidator, mSubNoValidatorOption1);
    addValidatorToConfigManager.template operator()<Type2>(
        1, mSubNoValidator, mSubNoValidatorOption2);
    checkAllValidAndInvalidValueCombinations.template operator()<Type1, Type2>(
        mSubNoValidator,
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue1"),
                       1),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue2"),
                       1));

    // Sub manager has validators of its own, however the manager does not.
    ConfigManager mNoValidatorSubValidator;
    ConfigManager& mNoValidatorSubValidatorSub =
        mNoValidatorSubValidator.addSubManager({"some"s, "manager"s});
    decltype(auto) mNoValidatorSubValidatorOption1 =
        mNoValidatorSubValidatorSub.addOption("someValue1", "", &var1);
    decltype(auto) mNoValidatorSubValidatorOption2 =
        mNoValidatorSubValidatorSub.addOption("someValue2", "", &var2);
    addValidatorToConfigManager.template operator()<Type1>(
        1, mNoValidatorSubValidatorSub, mNoValidatorSubValidatorOption1);
    addValidatorToConfigManager.template operator()<Type2>(
        1, mNoValidatorSubValidatorSub, mNoValidatorSubValidatorOption2);
    checkAllValidAndInvalidValueCombinations.template operator()<Type1, Type2>(
        mNoValidatorSubValidator,
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue1"),
                       1),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue2"),
                       1));

    // Sub manager has validators of its own, as does the manager.
    ConfigManager mValidatorSubValidator;
    ConfigManager& mValidatorSubValidatorSub =
        mValidatorSubValidator.addSubManager({"some"s, "manager"s});
    decltype(auto) mValidatorSubValidatorOption1 =
        mValidatorSubValidatorSub.addOption("someValue1", "", &var1);
    decltype(auto) mValidatorSubValidatorOption2 =
        mValidatorSubValidatorSub.addOption("someValue2", "", &var2);
    addValidatorToConfigManager.template operator()<Type1>(
        1, mValidatorSubValidator, mValidatorSubValidatorOption1);
    addValidatorToConfigManager.template operator()<Type2>(
        1, mValidatorSubValidatorSub, mValidatorSubValidatorOption2);
    checkAllValidAndInvalidValueCombinations.template operator()<Type1, Type2>(
        mValidatorSubValidator,
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue1"),
                       1),
        std::make_pair(nlohmann::json::json_pointer("/some/manager/someValue2"),
                       1));
  };

  callGivenLambdaWithAllCombinationsOfTypes.template operator()<2>(
      doDifferentParameterTests, callGivenLambdaWithAllCombinationsOfTypes);

  // Quick test, if the exception message generated for failed validators
  // include up to date information about all used configuration options.

  // Variables for the options.
  std::string string0;
  std::string string1;

  // Default values.
  const std::string defaultValue = "This is the default value.";
  const std::string defaultExceptionMessage = "Default exception message.";

  // Add the validator and check, if the configuration option with it's current
  // value is in the exception message for failing the validator.
  auto doExceptionMessageTest =
      [&addValidatorFunction, &defaultExceptionMessage](
          size_t variantNumber, ConfigManager& managerToRunParseOn,
          ConfigManager& managerToAddValidatorTo,
          ConstConfigOptionProxy<std::string> option,
          const nlohmann::json::json_pointer& pathToOption) {
        // The value, which causes the automaticly generated validator with the
        // given variant to fail.
        const std::string& failValue =
            createDummyValueForValidator<std::string>(variantNumber);

        nlohmann::json jsonForParsing(nlohmann::json::value_t::object);
        jsonForParsing[pathToOption] = failValue;

        // Adding the validator and checking.
        std::invoke(addValidatorFunction, variantNumber,
                    defaultExceptionMessage, managerToAddValidatorTo, option);
        AD_EXPECT_THROW_WITH_MESSAGE(
            managerToRunParseOn.parseConfig(jsonForParsing),
            ::testing::ContainsRegex(
                absl::StrCat("value '\"", failValue, "\"'")));
      };

  // No sub manager.
  ConfigManager mNoSub;
  decltype(auto) mNoSubOption1 =
      mNoSub.addOption("someValue1", "", &string0, defaultValue);
  doExceptionMessageTest(10, mNoSub, mNoSub, mNoSubOption1,
                         nlohmann::json::json_pointer("/someValue1"));

  // With sub manager. Sub manager has no validators of its own.
  ConfigManager mSubNoValidator;
  ConfigManager& mSubNoValidatorSub =
      mSubNoValidator.addSubManager({"some"s, "manager"s});
  decltype(auto) mSubNoValidatorOption1 =
      mSubNoValidatorSub.addOption("someValue1", "", &string0, defaultValue);
  doExceptionMessageTest(
      10, mSubNoValidator, mSubNoValidator, mSubNoValidatorOption1,
      nlohmann::json::json_pointer("/some/manager/someValue1"));

  // Sub manager has validators of its own, however the manager does not.
  ConfigManager mNoValidatorSubValidator;
  ConfigManager& mNoValidatorSubValidatorSub =
      mNoValidatorSubValidator.addSubManager({"some"s, "manager"s});
  decltype(auto) mNoValidatorSubValidatorOption1 =
      mNoValidatorSubValidatorSub.addOption("someValue1", "", &string0,
                                            defaultValue);
  doExceptionMessageTest(
      10, mNoValidatorSubValidator, mNoValidatorSubValidatorSub,
      mNoValidatorSubValidatorOption1,
      nlohmann::json::json_pointer("/some/manager/someValue1"));

  // Sub manager has validators of its own, as does the manager.
  ConfigManager mValidatorSubValidator;
  ConfigManager& mValidatorSubValidatorSub =
      mValidatorSubValidator.addSubManager({"some"s, "manager"s});
  decltype(auto) mValidatorSubValidatorOption1 =
      mValidatorSubValidatorSub.addOption("someValue1", "", &string0,
                                          defaultValue);
  decltype(auto) mValidatorSubValidatorOption2 =
      mValidatorSubValidatorSub.addOption("someValue2", "", &string1,
                                          defaultValue);
  doExceptionMessageTest(
      4, mValidatorSubValidator, mValidatorSubValidator,
      mValidatorSubValidatorOption1,
      nlohmann::json::json_pointer("/some/manager/someValue1"));

  /*
  Reseting `mValidatorSubValidatorOption1`, so that the validator, that was
  added via `doExceptionMessageTest`, not longer fails.

  We can not use an r-value nlohmann json object for this, because there is no
  fitting constructor.
  */
  nlohmann::json jsonForReset(nlohmann::json::value_t::object);
  jsonForReset[nlohmann::json::json_pointer("/some/manager/someValue1")] =
      defaultValue;
  mValidatorSubValidator.parseConfig(jsonForReset);

  doExceptionMessageTest(
      5, mValidatorSubValidator, mValidatorSubValidatorSub,
      mValidatorSubValidatorOption2,
      nlohmann::json::json_pointer("/some/manager/someValue2"));
}

TEST(ConfigManagerTest, AddNonExceptionValidator) {
  doValidatorTest([]<typename... Ts>(size_t variant,
                                     std::string_view validatorExceptionMessage,
                                     ConfigManager& m,
                                     ConstConfigOptionProxy<Ts>... optProxy) {
    m.addValidator(generateDummyNonExceptionValidatorFunction<Ts...>(variant),
                   std::string(validatorExceptionMessage), "", optProxy...);
  });
}

TEST(ConfigManagerTest, AddExceptionValidator) {
  doValidatorTest([]<typename... Ts>(size_t variant,
                                     std::string validatorExceptionMessage,
                                     ConfigManager& m,
                                     ConstConfigOptionProxy<Ts>... optProxy) {
    m.addValidator(
        transformValidatorIntoExceptionValidator<Ts...>(
            generateDummyNonExceptionValidatorFunction<Ts...>(variant),
            std::move(validatorExceptionMessage)),
        "", optProxy...);
  });
}

/*
@brief The test for checking, if `addValidator` throws exceptions as wanted.

@param addAlwaysValidValidatorFunction A function, that adds a validator
function, which recognizes any input as valid, to a config manager. The function
signature should look like this: `void func(ConfigManager& m,
ConstConfigOptionProxy... validatorArguments)`.
@param l For better error messages, when the tests fail.
*/
void doValidatorExceptionTest(
    auto addAlwaysValidValidatorFunction,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "doValidatorExceptionTest")};

  /*
  Test, if there is an exception, when we give `addValidator` configuration
  options, that are not contained in the corresponding configuration manager.
  */
  auto doValidatorParameterNotInConfigManagerTest =
      [&addAlwaysValidValidatorFunction]<typename T>() {
        // Variable for the configuration options.
        T var{};

        /*
        @brief Check, if a call to the `addValidator` function behaves as
        wanted.
        */
        auto checkAddValidatorBehavior =
            [&addAlwaysValidValidatorFunction](
                ConfigManager& m, ConstConfigOptionProxy<T> validOption,
                ConstConfigOptionProxy<T> notValidOption) {
              ASSERT_NO_THROW(addAlwaysValidValidatorFunction(m, validOption));
              AD_EXPECT_THROW_WITH_MESSAGE(
                  addAlwaysValidValidatorFunction(m, notValidOption),
                  ::testing::ContainsRegex(
                      notValidOption.getConfigOption().getIdentifier()));
            };

        // An outside configuration option.
        ConfigOption outsideOption("outside", "", &var);
        ConstConfigOptionProxy<T> outsideOptionProxy(outsideOption);

        // No sub manager.
        ConfigManager mNoSub;
        decltype(auto) mNoSubOption = mNoSub.addOption("someOption", "", &var);
        checkAddValidatorBehavior(mNoSub, mNoSubOption, outsideOptionProxy);

        // With sub manager.
        ConfigManager mWithSub;
        decltype(auto) mWithSubOption =
            mWithSub.addOption("someTopOption", "", &var);
        ConfigManager& mWithSubSub =
            mWithSub.addSubManager({"Some"s, "manager"s});
        decltype(auto) mWithSubSubOption =
            mWithSubSub.addOption("someSubOption", "", &var);
        checkAddValidatorBehavior(mWithSub, mWithSubOption, outsideOptionProxy);
        checkAddValidatorBehavior(mWithSub, mWithSubSubOption,
                                  outsideOptionProxy);
        checkAddValidatorBehavior(mWithSubSub, mWithSubSubOption,
                                  outsideOptionProxy);
        checkAddValidatorBehavior(mWithSubSub, mWithSubSubOption,
                                  mWithSubOption);

        // With 2 sub manager.
        ConfigManager mWith2Sub;
        decltype(auto) mWith2SubOption =
            mWith2Sub.addOption("someTopOption", "", &var);
        ConfigManager& mWith2SubSub1 =
            mWith2Sub.addSubManager({"Some"s, "manager"s});
        decltype(auto) mWith2SubSub1Option =
            mWith2SubSub1.addOption("someSubOption1", "", &var);
        ConfigManager& mWith2SubSub2 =
            mWith2Sub.addSubManager({"Some"s, "other"s, "manager"s});
        decltype(auto) mWith2SubSub2Option =
            mWith2SubSub2.addOption("someSubOption2", "", &var);
        checkAddValidatorBehavior(mWith2Sub, mWith2SubOption,
                                  outsideOptionProxy);
        checkAddValidatorBehavior(mWith2Sub, mWith2SubSub1Option,
                                  outsideOptionProxy);
        checkAddValidatorBehavior(mWith2Sub, mWith2SubSub2Option,
                                  outsideOptionProxy);
        checkAddValidatorBehavior(mWith2SubSub1, mWith2SubSub1Option,
                                  outsideOptionProxy);
        checkAddValidatorBehavior(mWith2SubSub1, mWith2SubSub1Option,
                                  mWith2SubOption);
        checkAddValidatorBehavior(mWith2SubSub1, mWith2SubSub1Option,
                                  mWith2SubSub2Option);
        checkAddValidatorBehavior(mWith2SubSub2, mWith2SubSub2Option,
                                  outsideOptionProxy);
        checkAddValidatorBehavior(mWith2SubSub2, mWith2SubSub2Option,
                                  mWith2SubOption);
        checkAddValidatorBehavior(mWith2SubSub2, mWith2SubSub2Option,
                                  mWith2SubSub1Option);
      };

  doForTypeInConfigOptionValueType(doValidatorParameterNotInConfigManagerTest);
}

TEST(ConfigManagerTest, AddNonExceptionValidatorException) {
  doValidatorExceptionTest(
      []<typename... Ts>(ConfigManager& m,
                         ConstConfigOptionProxy<Ts>... validatorArguments) {
        m.addValidator([](const Ts&...) { return true; }, "", "",
                       validatorArguments...);
      });
}

TEST(ConfigManagerTest, AddExceptionValidatorException) {
  doValidatorExceptionTest(
      []<typename... Ts>(ConfigManager& m,
                         ConstConfigOptionProxy<Ts>... validatorArguments) {
        m.addValidator(
            [](const Ts&...) -> std::optional<ErrorMessage> {
              return {std::nullopt};
            },
            "", validatorArguments...);
      });
}

/*
@brief The test for `addOptionValidator`.

@param addNonExceptionValidatorFunction A function, that adds a non exception
validator function to a config manager. The function signature should look like
this: `void func(Validator validatorFunction, std::string_view
validatorExceptionMessage, ConfigManager& m, ConstConfigOptionProxy...)`.
@param l For better error messages, when the tests fail.
*/
void doAddOptionValidatorTest(
    auto addNonExceptionValidatorFunction,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "doAddOptionValidatorTest")};

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

  // Manager without a sub manager.
  ConfigManager managerWithNoSubManager;
  decltype(auto) managerWithNoSubManagerOption1 =
      managerWithNoSubManager.addOption("someOption1", "", &firstVar);
  addNonExceptionValidatorFunction(generateValueAsStringComparison("10"),
                                   "someOption1", managerWithNoSubManager,
                                   managerWithNoSubManagerOption1);
  checkValidator(managerWithNoSubManager,
                 nlohmann::json::parse(R"--({"someOption1" : 10})--"),
                 nlohmann::json::parse(R"--({"someOption1" : 1})--"),
                 "someOption1");
  decltype(auto) managerWithNoSubManagerOption2 =
      managerWithNoSubManager.addOption("someOption2", "", &secondVar);
  addNonExceptionValidatorFunction(generateValueAsStringComparison("10"),
                                   "Both options", managerWithNoSubManager,
                                   managerWithNoSubManagerOption1,
                                   managerWithNoSubManagerOption2);
  checkValidator(
      managerWithNoSubManager,
      nlohmann::json::parse(R"--({"someOption1" : 10, "someOption2" : 10})--"),
      nlohmann::json::parse(R"--({"someOption1" : 10, "someOption2" : 1})--"),
      "Both options");

  // With sub manager. Sub manager has no validators of its own.
  ConfigManager managerWithSubManagerWhoHasNoValidators;
  decltype(auto) managerWithSubManagerWhoHasNoValidatorsOption =
      managerWithSubManagerWhoHasNoValidators.addOption("someOption", "",
                                                        &firstVar, 4);
  ConfigManager& managerWithSubManagerWhoHasNoValidatorsSubManager =
      managerWithSubManagerWhoHasNoValidators.addSubManager(
          {"Sub"s, "manager"s});
  decltype(auto) managerWithSubManagerWhoHasNoValidatorsSubManagerOption =
      managerWithSubManagerWhoHasNoValidatorsSubManager.addOption(
          "someOption", "", &secondVar, 4);
  addNonExceptionValidatorFunction(
      generateValueAsStringComparison("10"), "Sub manager option",
      managerWithSubManagerWhoHasNoValidators,
      managerWithSubManagerWhoHasNoValidatorsSubManagerOption);
  checkValidator(
      managerWithSubManagerWhoHasNoValidators,
      nlohmann::json::parse(R"--({"Sub":{"manager" : {"someOption" : 10}}})--"),
      nlohmann::json::parse(R"--({"Sub":{"manager" : {"someOption" : 1}}})--"),
      "Sub manager option");
  addNonExceptionValidatorFunction(
      generateValueAsStringComparison("10"), "Both options",
      managerWithSubManagerWhoHasNoValidators,
      managerWithSubManagerWhoHasNoValidatorsSubManagerOption,
      managerWithSubManagerWhoHasNoValidatorsOption);
  checkValidator(
      managerWithSubManagerWhoHasNoValidators,
      nlohmann::json::parse(
          R"--({"someOption" : 10, "Sub":{"manager" : {"someOption" : 10}}})--"),
      nlohmann::json::parse(
          R"--({"someOption" : 1, "Sub":{"manager" : {"someOption" : 10}}})--"),
      "Both options");

  // Sub manager has validators of its own, however the manager does not.
  ConfigManager managerHasNoValidatorsButSubManagerDoes;
  ConfigManager& managerHasNoValidatorsButSubManagerDoesSubManager =
      managerHasNoValidatorsButSubManagerDoes.addSubManager(
          {"Sub"s, "manager"s});
  decltype(auto) managerHasNoValidatorsButSubManagerDoesSubManagerOption =
      managerHasNoValidatorsButSubManagerDoesSubManager.addOption(
          "someOption", "", &firstVar, 4);
  addNonExceptionValidatorFunction(
      generateValueAsStringComparison("10"), "Sub manager option",
      managerHasNoValidatorsButSubManagerDoesSubManager,
      managerHasNoValidatorsButSubManagerDoesSubManagerOption);
  checkValidator(
      managerHasNoValidatorsButSubManagerDoes,
      nlohmann::json::parse(R"--({"Sub":{"manager" : {"someOption" : 10}}})--"),
      nlohmann::json::parse(R"--({"Sub":{"manager" : {"someOption" : 1}}})--"),
      "Sub manager option");

  // Sub manager has validators of its own, as does the manager.
  ConfigManager bothHaveValidators;
  decltype(auto) bothHaveValidatorsOption =
      bothHaveValidators.addOption("someOption", "", &firstVar, 4);
  ConfigManager& bothHaveValidatorsSubManager =
      bothHaveValidators.addSubManager({"Sub"s, "manager"s});
  decltype(auto) bothHaveValidatorsSubManagerOption =
      bothHaveValidatorsSubManager.addOption("someOption", "", &secondVar, 4);
  addNonExceptionValidatorFunction(generateValueAsStringComparison("10"),
                                   "Top manager option", bothHaveValidators,
                                   bothHaveValidatorsOption);
  addNonExceptionValidatorFunction(
      generateValueAsStringComparison("20"), "Sub manager option",
      bothHaveValidatorsSubManager, bothHaveValidatorsSubManagerOption);
  checkValidator(
      bothHaveValidators,
      nlohmann::json::parse(
          R"--({"someOption" : 10, "Sub":{"manager" : {"someOption" : 20}}})--"),
      nlohmann::json::parse(
          R"--({"someOption" : 1, "Sub":{"manager" : {"someOption" : 20}}})--"),
      "Top manager option");
  checkValidator(
      bothHaveValidators,
      nlohmann::json::parse(
          R"--({"someOption" : 10, "Sub":{"manager" : {"someOption" : 20}}})--"),
      nlohmann::json::parse(
          R"--({"someOption" : 10, "Sub":{"manager" : {"someOption" : 2}}})--"),
      "Sub manager option");
}

TEST(ConfigManagerTest, AddOptionNoExceptionValidator) {
  doAddOptionValidatorTest(
      []<typename... Ts>(auto validatorFunction,
                         std::string validatorExceptionMessage,
                         ConfigManager& m, ConstConfigOptionProxy<Ts>... args) {
        m.addOptionValidator(validatorFunction, validatorExceptionMessage, "",
                             args...);
      });
}

TEST(ConfigManagerTest, AddOptionExceptionValidator) {
  doAddOptionValidatorTest(
      []<typename... Ts>(auto validatorFunction,
                         std::string validatorExceptionMessage,
                         ConfigManager& m, ConstConfigOptionProxy<Ts>... args) {
        m.addOptionValidator(
            transformValidatorIntoExceptionValidator<
                decltype(args.getConfigOption())...>(
                validatorFunction, std::move(validatorExceptionMessage)),
            "", args...);
      });
}

/*
@brief The test for checking, if `addOptionValidator` throws exceptions as
wanted.

@param addAlwaysValidValidatorFunction A function, that adds a validator
function, which recognizes any input as valid, to a config manager. The function
signature should look like this: `void func(ConfigManager& m,
ConstConfigOptionProxy... validatorArguments)`.
@param l For better error messages, when the tests fail.
*/
void doAddOptionValidatorExceptionTest(
    auto addAlwaysValidValidatorFunction,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "doValidatorExceptionTest")};

  // Variable for the configuration options.
  int var;
  /*
  @brief Check, if a call to the `addOptionValidator` function behaves as
  wanted.
  */
  auto checkAddOptionValidatorBehavior =
      [&addAlwaysValidValidatorFunction]<typename T>(
          ConfigManager& m, ConstConfigOptionProxy<T> validOption,
          ConstConfigOptionProxy<T> notValidOption) {
        ASSERT_NO_THROW(addAlwaysValidValidatorFunction(m, validOption));
        AD_EXPECT_THROW_WITH_MESSAGE(
            addAlwaysValidValidatorFunction(m, notValidOption),
            ::testing::ContainsRegex(
                notValidOption.getConfigOption().getIdentifier()));
      };

  // An outside configuration option.
  ConfigOption outsideOption("outside", "", &var);
  ConstConfigOptionProxy<int> outsideOptionProxy(outsideOption);

  // No sub manager.
  ConfigManager mNoSub;
  decltype(auto) mNoSubOption = mNoSub.addOption("someOption", "", &var);
  checkAddOptionValidatorBehavior(mNoSub, mNoSubOption, outsideOptionProxy);

  // With sub manager.
  ConfigManager mWithSub;
  decltype(auto) mWithSubOption = mWithSub.addOption("someTopOption", "", &var);
  ConfigManager& mWithSubSub = mWithSub.addSubManager({"Some"s, "manager"s});
  decltype(auto) mWithSubSubOption =
      mWithSubSub.addOption("someSubOption", "", &var);
  checkAddOptionValidatorBehavior(mWithSub, mWithSubOption, outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWithSub, mWithSubSubOption,
                                  outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWithSubSub, mWithSubSubOption,
                                  outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWithSubSub, mWithSubSubOption,
                                  mWithSubOption);

  // With 2 sub manager.
  ConfigManager mWith2Sub;
  decltype(auto) mWith2SubOption =
      mWith2Sub.addOption("someTopOption", "", &var);
  ConfigManager& mWith2SubSub1 = mWith2Sub.addSubManager({"Some"s, "manager"s});
  decltype(auto) mWith2SubSub1Option =
      mWith2SubSub1.addOption("someSubOption1", "", &var);
  ConfigManager& mWith2SubSub2 =
      mWith2Sub.addSubManager({"Some"s, "other"s, "manager"s});
  decltype(auto) mWith2SubSub2Option =
      mWith2SubSub2.addOption("someSubOption2", "", &var);
  checkAddOptionValidatorBehavior(mWith2Sub, mWith2SubOption,
                                  outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWith2Sub, mWith2SubSub1Option,
                                  outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWith2Sub, mWith2SubSub2Option,
                                  outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWith2SubSub1, mWith2SubSub1Option,
                                  outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWith2SubSub1, mWith2SubSub1Option,
                                  mWith2SubOption);
  checkAddOptionValidatorBehavior(mWith2SubSub1, mWith2SubSub1Option,
                                  mWith2SubSub2Option);
  checkAddOptionValidatorBehavior(mWith2SubSub2, mWith2SubSub2Option,
                                  outsideOptionProxy);
  checkAddOptionValidatorBehavior(mWith2SubSub2, mWith2SubSub2Option,
                                  mWith2SubOption);
  checkAddOptionValidatorBehavior(mWith2SubSub2, mWith2SubSub2Option,
                                  mWith2SubSub1Option);
}

TEST(ConfigManagerTest, AddOptionNoExceptionValidatorException) {
  doAddOptionValidatorExceptionTest(
      []<typename... Ts>(ConfigManager& m, ConstConfigOptionProxy<Ts>... args) {
        m.addOptionValidator(
            [](const decltype(args.getConfigOption())&...) { return true; }, "",
            "", args...);
      });
}

TEST(ConfigManagerTest, AddOptionExceptionValidatorException) {
  doAddOptionValidatorExceptionTest(
      []<typename... Ts>(ConfigManager& m, ConstConfigOptionProxy<Ts>... args) {
        m.addOptionValidator(
            [](const decltype(args.getConfigOption())&...)
                -> std::optional<ErrorMessage> { return {std::nullopt}; },
            "", args...);
      });
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

  // The vectors for all `ConfigManager` for the vector parameter in
  // `checkContainmentStatus`. Mainly to reduce duplication.
  ContainmentStatusVector mContainmentStatusVector{{&outsideOption, false}};
  ContainmentStatusVector subManagerDepth1Num1ContainmentStatusVector{
      {&outsideOption, false}};
  ContainmentStatusVector subManagerDepth1Num2ContainmentStatusVector{
      {&outsideOption, false}};
  ContainmentStatusVector subManagerDepth2ContainmentStatusVector{
      {&outsideOption, false}};

  // Without sub manager.
  ConfigManager m;
  checkContainmentStatus(m, mContainmentStatusVector);
  decltype(auto) topManagerOption = m.addOption("TopLevel", "", &var);
  mContainmentStatusVector.push_back(
      {&topManagerOption.getConfigOption(), true});
  subManagerDepth1Num1ContainmentStatusVector.push_back(
      {&topManagerOption.getConfigOption(), false});
  subManagerDepth1Num2ContainmentStatusVector.push_back(
      {&topManagerOption.getConfigOption(), false});
  subManagerDepth2ContainmentStatusVector.push_back(
      {&topManagerOption.getConfigOption(), false});
  checkContainmentStatus(m, mContainmentStatusVector);

  // Single sub manager.
  ConfigManager& subManagerDepth1Num1 = m.addSubManager({"subManager1"s});
  checkContainmentStatus(subManagerDepth1Num1,
                         subManagerDepth1Num1ContainmentStatusVector);
  decltype(auto) subManagerDepth1Num1Option =
      subManagerDepth1Num1.addOption("SubManager1", "", &var);
  mContainmentStatusVector.push_back(
      {&subManagerDepth1Num1Option.getConfigOption(), true});
  subManagerDepth1Num1ContainmentStatusVector.push_back(
      {&subManagerDepth1Num1Option.getConfigOption(), true});
  subManagerDepth1Num2ContainmentStatusVector.push_back(
      {&subManagerDepth1Num1Option.getConfigOption(), false});
  subManagerDepth2ContainmentStatusVector.push_back(
      {&subManagerDepth1Num1Option.getConfigOption(), false});
  checkContainmentStatus(subManagerDepth1Num1,
                         subManagerDepth1Num1ContainmentStatusVector);
  checkContainmentStatus(m, mContainmentStatusVector);

  // Second sub manager.
  ConfigManager& subManagerDepth1Num2 = m.addSubManager({"subManager2"s});
  checkContainmentStatus(subManagerDepth1Num2,
                         subManagerDepth1Num2ContainmentStatusVector);
  decltype(auto) subManagerDepth1Num2Option =
      subManagerDepth1Num2.addOption("SubManager2", "", &var);
  mContainmentStatusVector.push_back(
      {&subManagerDepth1Num2Option.getConfigOption(), true});
  subManagerDepth1Num1ContainmentStatusVector.push_back(
      {&subManagerDepth1Num2Option.getConfigOption(), false});
  subManagerDepth1Num2ContainmentStatusVector.push_back(
      {&subManagerDepth1Num2Option.getConfigOption(), true});
  subManagerDepth2ContainmentStatusVector.push_back(
      {&subManagerDepth1Num2Option.getConfigOption(), false});
  checkContainmentStatus(subManagerDepth1Num1,
                         subManagerDepth1Num1ContainmentStatusVector);
  checkContainmentStatus(m, mContainmentStatusVector);
  checkContainmentStatus(subManagerDepth1Num2,
                         subManagerDepth1Num2ContainmentStatusVector);

  // Sub manager in the second sub manager.
  ConfigManager& subManagerDepth2 =
      subManagerDepth1Num2.addSubManager({"subManagerDepth2"s});
  checkContainmentStatus(subManagerDepth2,
                         subManagerDepth2ContainmentStatusVector);
  decltype(auto) subManagerDepth2Option =
      subManagerDepth2.addOption("SubManagerDepth2", "", &var);
  mContainmentStatusVector.push_back(
      {&subManagerDepth2Option.getConfigOption(), true});
  subManagerDepth1Num1ContainmentStatusVector.push_back(
      {&subManagerDepth2Option.getConfigOption(), false});
  subManagerDepth1Num2ContainmentStatusVector.push_back(
      {&subManagerDepth2Option.getConfigOption(), true});
  subManagerDepth2ContainmentStatusVector.push_back(
      {&subManagerDepth2Option.getConfigOption(), true});
  checkContainmentStatus(subManagerDepth1Num1,
                         subManagerDepth1Num1ContainmentStatusVector);
  checkContainmentStatus(m, mContainmentStatusVector);
  checkContainmentStatus(subManagerDepth1Num2,
                         subManagerDepth1Num2ContainmentStatusVector);
  checkContainmentStatus(subManagerDepth2,
                         subManagerDepth2ContainmentStatusVector);
}

// Describes the order of configuration options and validators inside a
// configuration manager.
struct ConfigOptionsAndValidatorsOrder {
  // The order of the configuration options. Options can be identified via their
  // memory address.
  std::vector<const ConfigOption*> configOptions_;

  // The order the validators. unfortunately, there is no way, to perfectly
  // identify an instance, but the description can be used, as long as all the
  // added validators have unique descriptions.
  std::vector<std::string> validators_;

  // Appends the content of an different `ConfigOptionsAndValidatorsOrder`.
  template <typename T>
  requires isSimilar<T, ConfigOptionsAndValidatorsOrder>
  void append(T&& order) {
    appendVector(configOptions_, AD_FWD(order).configOptions_);
    appendVector(validators_, AD_FWD(order).validators_);
  }
};

/*
This is for testing, if the internal helper function
`ConfigManager::validators` sorts its return value correctly.
*/
TEST(ConfigManagerTest, ValidatorsSorting) {
  /*
  Add dummy configuration option for all supported types and dummy validators
  for them to the given `configManager`.
  @returns The order of all added configuration options and validators.
  */
  auto addConfigOptionsAndValidators = [callNum =
                                            0](ConfigManager* manager) mutable {
    ConfigOptionsAndValidatorsOrder order{};

    // We know, how many instances we will add.
    order.configOptions_.reserve(
        std::variant_size_v<ConfigOption::AvailableTypes>);
    order.validators_.reserve(
        std::variant_size_v<ConfigOption::AvailableTypes> * 2);

    // Fill the list.
    forEachTypeInTemplateType<ConfigOption::AvailableTypes>(
        [&order, &manager, &callNum]<typename T>() {
          // This variable will never be used, so this SHOULD be okay.
          T var;

          // Create the options and validators.
          decltype(auto) opt =
              manager->addOption({absl::StrCat("Option", callNum++)}, "", &var);
          order.configOptions_.push_back(&opt.getConfigOption());
          manager->addValidator([](const T&) { return true; }, "",
                                absl::StrCat("Normal validator ", callNum),
                                opt);
          order.validators_.push_back(
              absl::StrCat("Normal validator ", callNum++));
          manager->addOptionValidator(
              [](const ConfigOption&) { return true; }, "",
              absl::StrCat("Option validator ", callNum), opt);
          order.validators_.push_back(
              absl::StrCat("Option validator ", callNum++));
        });

    return order;
  };

  // Check the order of the validators inside the configuration manager.
  auto checkOrder = [](const ConfigManager& manager,
                       const ConfigOptionsAndValidatorsOrder& order,
                       ad_utility::source_location l =
                           ad_utility::source_location::current()) {
    // For generating better messages, when failing a test.
    auto trace{generateLocationTrace(l, "checkOrder")};

    ASSERT_TRUE(std::ranges::equal(
        manager.validators(true), order.validators_, {},
        [](const ConfigOptionValidatorManager& validatorManager) {
          return validatorManager.getDescription();
        }));
  };

  // First add the options, then the sub manager and then more options to the
  // top manager again.
  ConfigManager mOptionFirst;
  auto mOptionFirstOrderOfAll{addConfigOptionsAndValidators(&mOptionFirst)};
  checkOrder(mOptionFirst, mOptionFirstOrderOfAll);

  // Add the sub manager, together with its config options.
  ConfigManager& mOptionFirstSub{mOptionFirst.addSubManager({"s"})};
  auto mOptionFirstSubOrder{addConfigOptionsAndValidators(&mOptionFirstSub)};
  checkOrder(mOptionFirstSub, mOptionFirstSubOrder);
  mOptionFirstOrderOfAll.append(mOptionFirstSubOrder);
  checkOrder(mOptionFirst, mOptionFirstOrderOfAll);

  // Add config options to the top manager.
  mOptionFirstOrderOfAll.append(addConfigOptionsAndValidators(&mOptionFirst));
  checkOrder(mOptionFirstSub, mOptionFirstSubOrder);
  checkOrder(mOptionFirst, mOptionFirstOrderOfAll);

  // First add the sub manager with config options, then options to the top
  // manager and then options to the sub manager again.
  ConfigManager mSubManagerFirst;
  ConfigManager& mSubManagerFirstSub{mSubManagerFirst.addSubManager({"s"})};
  auto mSubManagerFirstSubOrder{
      addConfigOptionsAndValidators(&mSubManagerFirstSub)};
  auto mSubManagerFirstOrderOfAll{mSubManagerFirstSubOrder};
  checkOrder(mSubManagerFirst, mSubManagerFirstOrderOfAll);
  checkOrder(mSubManagerFirstSub, mSubManagerFirstSubOrder);

  // Add config options to the top manager.
  mSubManagerFirstOrderOfAll.append(
      addConfigOptionsAndValidators(&mSubManagerFirst));
  checkOrder(mSubManagerFirst, mSubManagerFirstOrderOfAll);
  checkOrder(mSubManagerFirstSub, mSubManagerFirstSubOrder);

  // Add config options to the sub manager.
  auto mSubManagerFirstSubOrder2{
      addConfigOptionsAndValidators(&mSubManagerFirstSub)};
  mSubManagerFirstOrderOfAll.append(mSubManagerFirstSubOrder2);
  mSubManagerFirstSubOrder.append(std::move(mSubManagerFirstSubOrder2));
  checkOrder(mSubManagerFirst, mSubManagerFirstOrderOfAll);
  checkOrder(mSubManagerFirstSub, mSubManagerFirstSubOrder);
}

// Small test for the helper class
// `ConfigManager::ConfigurationDocValidatorAssignment`.
TEST(ConfigManagerTest, ConfigurationDocValidatorAssignment) {
  // How many instances of `ConfigOption`s and `ConfigManager`s should be added
  // for the test?
  constexpr size_t NUM_CONFIG_OPTION = 1;
  constexpr size_t NUM_CONFIG_MANAGER = NUM_CONFIG_OPTION;

  // Generate a vector of dummy `ConfigOptionValidatorManager`. Note: They will
  // not actually work.
  auto generateDummyValidatorManager = [](const size_t numValidator) {
    // Dummy configuration option needed for validator manager constructor.
    bool b;
    ConfigOption opt("d", "", &b);
    ConstConfigOptionProxy<bool> proxy(opt);

    // Dummy translator function needed for validator manager constructor.
    auto translator{std::identity{}};

    // Dummy validator function needed for validator manager constructor.
    auto validator = [](const auto&) { return true; };

    // Create the validators.
    std::vector<ConfigOptionValidatorManager> validators;
    validators.reserve(numValidator);
    for (size_t v = 0; v < numValidator; v++) {
      validators.emplace_back(validator, "", "", translator, proxy);
    }
    return validators;
  };

  /*
  @brief Create a vector of key and `std::vector<ConfigOptionValidatorManager>`
  pairs. The size of `std::vector<ConfigOptionValidatorManager>` is random.

  @param keyFactory The constructor for the key elements. Should take no
  arguments and return a a fresh instance every time, it is invoked.
  @param numPairs How many pairs should be created?
  */
  auto createKeyAndValidatorPairVector =
      [&generateDummyValidatorManager]<typename KeyFactory>(
          const KeyFactory& keyFactory, const size_t numPairs) {
        AD_CONTRACT_CHECK(numPairs > 0);

        // The type of the keys.
        using Key = std::invoke_result_t<KeyFactory>;

        /*
        A random number generator will be used, to determine the amount of
        `ConfigOptionValidatorManager`s, that will be added for each key. (To
        make the test less uniform.)
        */
        SlowRandomIntGenerator<size_t> generatorNumValidatorPerKey(0, 15);

        // Generate the keys, together with their validators.
        std::vector<std::pair<Key, std::vector<ConfigOptionValidatorManager>>>
            keysAndValidators;
        keysAndValidators.reserve(numPairs);
        for (size_t i = 0; i < numPairs; i++) {
          keysAndValidators.emplace_back(
              std::invoke(keyFactory),
              generateDummyValidatorManager(generatorNumValidatorPerKey()));
        }

        return keysAndValidators;
      };

  // Add a vector, generated by `createKeyAndValidatorPairVector`, to the given
  // `ConfigurationDocValidatorAssignment`.
  auto addPairVector =
      []<typename T>(
          ConfigManager::ConfigurationDocValidatorAssignment* assignment,
          const std::vector<
              std::pair<T, std::vector<ConfigOptionValidatorManager>>>&
              pairVector) {
        // Simply insert all the entries.
        std::ranges::for_each(pairVector, [&assignment](const auto& pair) {
          const auto& [key, validatorVector] = pair;
          std::ranges::for_each(
              validatorVector,
              [&assignment,
               &key](const ConfigOptionValidatorManager& validator) {
                assignment->addEntryUnderKey(key, validator);
              });
        });
      };

  /*
  Test if a vector, generated by `createKeyAndValidatorPairVector`, has entries
  in `ConfigurationDocValidatorAssignment`, as described by the vector. Note:
  This will be checked via identity, not equality.
  */
  auto testPairVector =
      []<typename T>(
          const ConfigManager::ConfigurationDocValidatorAssignment& assignment,
          const std::vector<
              std::pair<T, std::vector<ConfigOptionValidatorManager>>>&
              pairVector,
          ad_utility::source_location l =
              ad_utility::source_location::current()) {
        // For generating better messages, when failing a test.
        auto trace{generateLocationTrace(l, "testPairVector")};
        std::ranges::for_each(pairVector, [&assignment](const auto& pair) {
          const auto& [key, expectedValidatorVector] = pair;

          // Are the entries under `key` the objects in the expected vector?
          auto toPointer = [](const ConfigOptionValidatorManager& x) {
            return &x;
          };
          ASSERT_TRUE(std::ranges::equal(assignment.getEntriesUnderKey(key),
                                         expectedValidatorVector, {}, toPointer,
                                         toPointer));
        });
      };

  /*
  Generate the `ConfigOption`, together with their validators. Note: We do not
  need working `ConfigOption`. As long as they exists, everythings fine.
  */
  const auto configOptionKeysAndValidators{createKeyAndValidatorPairVector(
      []() {
        bool b;
        return ConfigOption("d", "", &b);
      },
      NUM_CONFIG_OPTION)};

  /*
  Generate the `ConfigManager`, together with their validators. Note: We do not
  need working `ConfigManager`. As long as they exists, everythings fine.
  */
  const auto configManagerKeysAndValidators{createKeyAndValidatorPairVector(
      []() { return ConfigManager{}; }, NUM_CONFIG_MANAGER)};

  // Add and test the vectors.
  ConfigManager::ConfigurationDocValidatorAssignment assignment{};
  addPairVector(&assignment, configOptionKeysAndValidators);
  testPairVector(assignment, configOptionKeysAndValidators);
  addPairVector(&assignment, configManagerKeysAndValidators);
  testPairVector(assignment, configOptionKeysAndValidators);
  testPairVector(assignment, configManagerKeysAndValidators);

  // Check the behavior, if the key has nothing assigned to it and the validator
  // was never assigned to anything.
  bool b;
  ConfigOption notIncludedOpt("d", "", &b);
  ConstConfigOptionProxy<bool> notIncludedOptProxy(notIncludedOpt);
  ConfigManager notIncludedConfigManager{};
  ConfigOptionValidatorManager notIncludedValidator(
      [](const auto&) { return true; }, "", "", std::identity{},
      notIncludedOptProxy);
  ASSERT_TRUE(assignment.getEntriesUnderKey(notIncludedOpt).empty());
  ASSERT_TRUE(assignment.getEntriesUnderKey(notIncludedConfigManager).empty());
}

// A simple hard coded comparison test.
TEST(ConfigManagerTest, PrintConfigurationDocComparison) {
  // For comparing strings.
  auto assertStringEqual = [](const std::string_view a,
                              const std::string_view b,
                              ad_utility::source_location l =
                                  ad_utility::source_location::current()) {
    // For generating better messages, when failing a test.
    auto trace{generateLocationTrace(l, "assertStringEqual")};

    ASSERT_STREQ(a.data(), b.data());
  };

  // Empty config manager.
  assertStringEqual(emptyConfigManagerExpectedString,
                    ConfigManager{}.printConfigurationDoc(true));
  assertStringEqual(emptyConfigManagerExpectedString,
                    ConfigManager{}.printConfigurationDoc(false));

  // Add a default validator to the given `ConfigManager`.
  auto addDefaultValidator =
      [](ConfigManager* configManager,
         const ad_utility::isInstantiation<
             ConstConfigOptionProxy> auto&... configOptionsToBeChecked) {
        const std::string validatorDescription = absl::StrCat(
            "Validator for configuration options ",
            ad_utility::lazyStrJoin(
                std::vector{configOptionsToBeChecked.getConfigOption()
                                .getIdentifier()...},
                ", "),
            ".");
        configManager->addValidator([](const auto&...) { return true; },
                                    validatorDescription, validatorDescription,
                                    configOptionsToBeChecked...);
      };

  // Add example config options and single option validators.
  auto addDefaultExampleOptionsAndSingleOptionValidators =
      [&addDefaultValidator](ConfigManager* configManager) {
        // Add the example configuration options by calling `addOption` with all
        // valid argument combinations.
        doForTypeInConfigOptionValueType([&addDefaultValidator, &configManager]<
                                             typename OptionType>() {
          /*
          @brief Add a configuration option with the wanted characteristics to
          `configManager`.

          @param configOptionVariable The variable, that the config option will
          write to, whenever it is set.
          @param hasDescription Should the configuration option have a
          description?
          @param hasDefaultValue Should the configuration option have a default
          value?
          @param keepsDefaultValue Should the configuration option keep the
          default value, or should it be set to a different value?
          @param hasOwnValidator Should the configuration option have a
          validator, that only checks the option?
          */
          auto addOption = [&configManager, &addDefaultValidator](
                               OptionType* configOptionVariable,
                               const bool hasDescription,
                               const bool hasDefaultValue,
                               const bool keepsDefaultValue,
                               const bool hasOwnValidator) {
            // Default description.
            const std::string description{
                hasDescription
                    ? absl::StrCat(
                          "Description for type ",
                          ConfigOption::availableTypesToString<OptionType>(),
                          ".")
                    : ""};

            /*
            Generate the identifier.
            We use the string representation of the type, to make the identifier
            unique, but the helper function for that was created for a different
            usage, which leads to it creating strings, that are not valid in
            identifiers. The easiest option was to just adjust them as needed.
            */
            auto withOrWithout = [](const bool isWith,
                                    std::string_view postfix) {
              return absl::StrCat(isWith ? "With" : "Without", postfix);
            };
            const std::string identifier = absl::StrCat(
                absl::StrReplaceAll(
                    ConfigOption::availableTypesToString<OptionType>(),
                    {{" ", ""}}),
                withOrWithout(hasDescription, "Description"),
                withOrWithout(hasDefaultValue, "DefaultValue"),
                hasDefaultValue
                    ? withOrWithout(keepsDefaultValue, "KeepDefaultValue")
                    : "",
                withOrWithout(hasOwnValidator, "Validator"));

            // Create the option.
            const ConfigOption* createdOption{};
            if (hasDefaultValue) {
              createdOption =
                  &configManager
                       ->addOption(identifier, description,
                                   configOptionVariable,
                                   createDummyValueForValidator<OptionType>(0))
                       .getConfigOption();
              if (!keepsDefaultValue) {
                /*
                We have to get a bit hacky, because the setting of configuration
                option, that was created via `ConfigManager`, should not be
                possible. Only the parse function of `ConfigManager` should be
                able to do that.
                */
                const_cast<ConfigOption*>(createdOption)
                    ->setValue(createDummyValueForValidator<OptionType>(1));
              }
            } else {
              createdOption = &configManager
                                   ->addOption(identifier, description,
                                               configOptionVariable)
                                   .getConfigOption();
            }

            // Proxy object for the option
            ConstConfigOptionProxy<OptionType> proxy{*createdOption};

            // Add the validator.
            if (hasOwnValidator) {
              addDefaultValidator(configManager, proxy);
            }

            // Return the create option for further usage.
            return proxy;
          };

          // All option of a type share a variable.
          static OptionType var{};
          addOption(&var, false, false, false, false);
          addOption(&var, false, false, false, true);
          addOption(&var, false, true, true, false);
          addOption(&var, false, true, false, false);
          addOption(&var, false, true, true, true);
          addOption(&var, false, true, false, true);
          addOption(&var, true, false, false, false);
          addOption(&var, true, false, false, true);
          addOption(&var, true, true, true, false);
          addOption(&var, true, true, false, false);
          addOption(&var, true, true, true, true);
          addOption(&var, true, true, false, true);
        });
      };

  /*
  Lets create a configuration manager with a single sub manager.
  The sub manager has a validator invariant for multiple configuration options,
  the top manager only has single option validators.
  */
  ConfigManager topManager{};
  addDefaultExampleOptionsAndSingleOptionValidators(&topManager);
  ConfigManager& subManger{topManager.addSubManager({"subManager"})};
  addDefaultExampleOptionsAndSingleOptionValidators(&subManger);
  bool boolForDoubleArgumentValidatorOptions{false};
  decltype(auto) doubleArgumentValidatorFirstArgument{
      subManger.addOption("doubleArgumentValidatorFirstArgument", "",
                          &boolForDoubleArgumentValidatorOptions)};
  decltype(auto) doubleArgumentValidatorSecondArgument{
      subManger.addOption("doubleArgumentValidatorSecondArgument", "",
                          &boolForDoubleArgumentValidatorOptions)};
  addDefaultValidator(&subManger, doubleArgumentValidatorFirstArgument,
                      doubleArgumentValidatorSecondArgument);

  // Finally, check, if the expected and actual output is the same.
  assertStringEqual(exampleConfigManagerExpectedNotDetailedString,
                    topManager.printConfigurationDoc(false));
  assertStringEqual(exampleConfigManagerExpectedDetailedString,
                    topManager.printConfigurationDoc(true));
}
}  // namespace ad_utility::ConfigManagerImpl
