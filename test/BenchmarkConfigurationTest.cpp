// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"

TEST(BenchmarkConfigurationTest, GetConfigurationOptionByNestedKeysTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Configuration options for testing.
  const ad_benchmark::BenchmarkConfigurationOption& withDefault{
      ad_benchmark::BenchmarkConfigurationOption(
          "Sense_of_existence", "",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::integer,
          42)};
  const ad_benchmark::BenchmarkConfigurationOption& withoutDefault{
      ad_benchmark::BenchmarkConfigurationOption(
          "Sense_of_existence", "",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::integer,
          1)};

  // 'Compare' two benchmark configuration options
  auto compareConfigurationOptions =
      []<typename T>(const ad_benchmark::BenchmarkConfigurationOption& a,
                     const ad_benchmark::BenchmarkConfigurationOption& b) {
        ASSERT_EQ(a.hasValue(), b.hasValue());
        ASSERT_EQ(a.getValue<T>(), b.getValue<T>());
      };

  config.addConfigurationOption(withDefault, "Shared_part", "Unique_part_1");
  config.addConfigurationOption(withoutDefault, "Shared_part", "Unique_part_2",
                                3);

  // Where those two options added?
  ASSERT_EQ(config.getConfigurationOptions().size(), 2);

  compareConfigurationOptions.template operator()<int>(
      withDefault, config.getConfigurationOptionByNestedKeys(
                       "Shared_part", "Unique_part_1", "Sense_of_existence"));
  compareConfigurationOptions.template operator()<int>(
      withoutDefault,
      config.getConfigurationOptionByNestedKeys("Shared_part", "Unique_part_2",
                                                3, "Sense_of_existence"));

  // Trying to add a configuration option with the same name at the same place,
  // should cause an error.
  ASSERT_ANY_THROW(config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Sense_of_existence", "",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::integer,
          42),
      "Shared_part", "Unique_part_1"));

  /*
  Trying to add a configuration option with a path containing strings with
  spaces, or integers smaller than 0, should cause an error.
  Reason:
  - A string with spaces in it, can't be read by the short hand configuration
  grammar. Ergo, you can't set values, with such paths per short hand, which we
  don't want.

  - An integer smaller than 0, is not a valid key in json. Ergo, you can't set
  options like that with json, which we dont want. Furthermore, we use
  `nlohmann::json` to save those key paths, so we couldn't even save them.
  */
  ASSERT_ANY_THROW(config.addConfigurationOption(withDefault, "Shared part"););
  ASSERT_ANY_THROW(config.addConfigurationOption(withDefault, -4););
  ASSERT_ANY_THROW(
      config.addConfigurationOption(withDefault, "Shared part", -2););
  ASSERT_ANY_THROW(config.addConfigurationOption(
      withDefault, -10, "Somewhere over the rainbow"););

  // Trying to get a configuration option, that does not exist, should cause an
  // exception.
  ASSERT_ANY_THROW(
      config.getConfigurationOptionByNestedKeys("Shared_part", "Getsbourgh"));
}

TEST(BenchmarkConfigurationTest, SetJsonStringTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Adding the options.
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Option_0", "Must be set. Has no default value.",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
              integer),
      "depth_0");
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Option_1", "Must be set. Has no default value.",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
              integer),
      "depth_0", "depth_1");
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "Option_2", "Has a default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::integer,
      2));

  // For easier access to the options.
  auto getOption = [&config](const size_t& optionNumber) {
    if (optionNumber == 0) {
      return config.getConfigurationOptionByNestedKeys("depth_0", "Option_0");
    } else if (optionNumber == 1) {
      return config.getConfigurationOptionByNestedKeys("depth_0", "depth_1",
                                                       "Option_1");
    } else {
      return config.getConfigurationOptionByNestedKeys("Option_2");
    }
  };

  // For easier checking.
  auto checkOption =
      [](const ad_benchmark::BenchmarkConfigurationOption& option,
         const auto& content) {
        ASSERT_TRUE(option.hasValue());
        ASSERT_EQ(content, option.getValue<decltype(content)>());
      };

  // Does the option with the default already have a value?
  checkOption(getOption(2), 2);

  // The other two should have no value.
  ASSERT_FALSE(getOption(0).hasValue());
  ASSERT_FALSE(getOption(1).hasValue());

  // The json string for testing `setJsonString`. Sets all of the configuration
  // options.
  const std::string testJsonString{R"--({
"depth_0": {
  "Option_0": 10,
  "depth_1": {
    "Option_1": 11
  }
},
"Option_2": 12
})--"};

  // Set and check.
  config.setJsonString(testJsonString);

  checkOption(getOption(0), 10);
  checkOption(getOption(1), 11);
  checkOption(getOption(2), 12);
}

TEST(BenchmarkConfigurationTest, SetJsonStringExceptionTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Add one option with default and one without.
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Without_default", "Must be set. Has no default value.",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
              integer),
      "depth_0");
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "With_default", "Must not be set. Has default value.",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
              integerList,
          std::vector{40, 41}),
      "depth_0");

  // Should throw an exception, if we don't set all options, that must be set.
  ASSERT_ANY_THROW(config.setJsonString(R"--({})--"));

  // Should throw an exception, if we try set an option, that isn't there.
  ASSERT_ANY_THROW(config.setJsonString(
      R"--({"depth 0":{"Without_default":42, "with_default" : [39]}})--"));
  ASSERT_ANY_THROW(config.setJsonString(
      R"--({"depth 0":{"Without_default":42, "test_string" : "test"}})--"));
}

TEST(BenchmarkConfigurationTest, ParseShortHandTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Add integer options.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "somePositiveNumber", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::integer));
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "someNegativNumber", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::integer));

  // Add integer list.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "someIntegerlist", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
          integerList));

  // Add floating point options.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "somePositiveFloatingPoint", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
          floatingPoint));
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "someNegativFloatingPoint", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
          floatingPoint));

  // Add floating point list.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "someFloatingPointList", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
          floatingPointList));

  // Add boolean options.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "boolTrue", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::boolean));
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "boolFalse", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::boolean));

  // Add boolean list.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "someBooleanList", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
          booleanList));

  // Add string option.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "myName", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::string));

  // Add string list.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "someStringList", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
          stringList));

  // Add option with deeper level.
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "list", "Must be set. Has no default value.",
          ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::
              integerList),
      "depth", 0);

  // This one will not be changed, in order to test, that options, that are not
  // set at run time, are not changed.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "No_change", "",
      ad_benchmark::BenchmarkConfigurationOption::ValueTypeIndexes::integer,
      10));

  // Set those.
  config.setShortHand(
      R"--(somePositiveNumber : 42, someNegativNumber : -42, someIntegerlist : [40, 41], somePositiveFloatingPoint : 4.2, someNegativFloatingPoint : -4.2, someFloatingPointList : [4.1, 4.2], boolTrue : true, boolFalse : false, someBooleanList : [true, false, true], myName : "Bernd", someStringList : ["t1", "t2"], depth : [{list : [7,8]}])--");

  // Check, if an option was set to the value, you wanted.
  auto checkOption = [&config](const auto& content, const auto&... keys) {
    const auto& option = config.getConfigurationOptionByNestedKeys(keys...);
    ASSERT_TRUE(option.hasValue());
    ASSERT_EQ(content, option.template getValue<decltype(content)>());
  };

  checkOption(static_cast<int>(42), "somePositiveNumber");
  checkOption(static_cast<int>(-42), "someNegativNumber");

  checkOption(std::vector{40, 41}, "someIntegerlist");

  checkOption(static_cast<double>(4.2), "somePositiveFloatingPoint");
  checkOption(static_cast<double>(-4.2), "someNegativFloatingPoint");

  checkOption(std::vector{4.1, 4.2}, "someFloatingPointList");

  checkOption(true, "boolTrue");
  checkOption(false, "boolFalse");

  checkOption(std::vector{true, false, true}, "someBooleanList");

  checkOption(std::string{"Bernd"}, "myName");

  checkOption(std::vector<std::string>{"t1", "t2"}, "someStringList");

  checkOption(std::vector{7, 8}, "depth", 0, "list");

  // Is the "No Change" unchanged?
  checkOption(static_cast<int>(10), "No_change");

  // Multiple key value pairs with the same key are not allowed.
  ASSERT_ANY_THROW(config.setShortHand(R"(a:42, a:43)"););

  // Final test: Is there an exception, if we try to parse the wrong syntax?
  ASSERT_ANY_THROW(config.setShortHand(R"--({"myName" : "Bernd")})--"));
  ASSERT_ANY_THROW(config.setShortHand(R"--("myName" = "Bernd";)--"));
}
