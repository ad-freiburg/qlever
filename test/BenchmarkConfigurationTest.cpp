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
          "Sense of existence", "",
          ad_benchmark::BenchmarkConfigurationOption::integer, 42)};
  const ad_benchmark::BenchmarkConfigurationOption& withoutDefault{
      ad_benchmark::BenchmarkConfigurationOption(
          "Sense of existence", "",
          ad_benchmark::BenchmarkConfigurationOption::integer, 1)};

  // 'Compare' two benchmark configuration options
  auto compareConfigurationOptions =
      []<typename T>(const ad_benchmark::BenchmarkConfigurationOption& a,
                     const ad_benchmark::BenchmarkConfigurationOption& b) {
        ASSERT_EQ(a.hasValue(), b.hasValue());
        ASSERT_EQ(a.getValue<T>(), b.getValue<T>());
      };

  config.addConfigurationOption(withDefault, "Shared part", "Unique part 1");
  config.addConfigurationOption(withoutDefault, "Shared part", "Unique part 2",
                                3);

  // Where those two options added?
  ASSERT_EQ(config.getConfigurationOptions().size(), 2);

  compareConfigurationOptions.template operator()<int>(
      withDefault, config.getConfigurationOptionByNestedKeys(
                       "Shared part", "Unique part 1", "Sense of existence"));
  compareConfigurationOptions.template operator()<int>(
      withoutDefault,
      config.getConfigurationOptionByNestedKeys("Shared part", "Unique part 2",
                                                3, "Sense of existence"));

  // Trying to add a configuration option with the same name at the same place,
  // should cause an error.
  ASSERT_ANY_THROW(config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Sense of existence", "",
          ad_benchmark::BenchmarkConfigurationOption::integer, 42),
      "Shared part", "Unique part 1"));

  // Trying to get a configuration option, that does not exist, should cause an
  // exception.
  ASSERT_ANY_THROW(
      config.getConfigurationOptionByNestedKeys("Shared part", "Getsbourgh"));
}

TEST(BenchmarkConfigurationTest, SetJsonStringTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Adding the options.
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Option 0", "Must be set. Has no default value.",
          ad_benchmark::BenchmarkConfigurationOption::integer),
      "depth 0");
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Option 1", "Must be set. Has no default value.",
          ad_benchmark::BenchmarkConfigurationOption::integer),
      "depth 0", "depth 1");
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "Option 2", "Has a default value.",
      ad_benchmark::BenchmarkConfigurationOption::integer, 2));

  // For easier access to the options.
  auto getOption = [&config](const size_t& optionNumber) {
    if (optionNumber == 0) {
      return config.getConfigurationOptionByNestedKeys("depth 0", "Option 0");
    } else if (optionNumber == 1) {
      return config.getConfigurationOptionByNestedKeys("depth 0", "depth 1",
                                                       "Option 1");
    } else {
      return config.getConfigurationOptionByNestedKeys("Option 2");
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
"depth 0": {
  "Option 0": 10,
  "depth 1": {
    "Option 1": 11
  }
},
"Option 2": 12
})--"};

  // Set and check.
  config.setJsonString(testJsonString);

  checkOption(getOption(0), 10);
  checkOption(getOption(1), 11);
  checkOption(getOption(2), 12);
}

TEST(BenchmarkConfigurationTest, SetJsonStringExceptionTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Add one option.
  config.addConfigurationOption(
      ad_benchmark::BenchmarkConfigurationOption(
          "Option", "Must be set. Has no default value.",
          ad_benchmark::BenchmarkConfigurationOption::integer),
      "depth 0");

  // Should throw an exception, if we try set an option, that isn't there.
  ASSERT_ANY_THROW(config.setJsonString(R"--({"depth 0":{"option":42}})--"));

  // Should throw an exception, if we don't set all options, that must be set.
  ASSERT_ANY_THROW(config.setJsonString(R"--({})--"));
}

TEST(BenchmarkConfigurationTest, ParseShortHandTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Add integer options.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "somePositiveNumber", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::integer));
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "someNegativNumber", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::integer));

  // Add boolean options.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "boolTrue", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::boolean));
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "boolFalse", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::boolean));

  // Add string option.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "myName", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::string));

  // Add list option.
  config.addConfigurationOption(ad_benchmark::BenchmarkConfigurationOption(
      "list", "Must be set. Has no default value.",
      ad_benchmark::BenchmarkConfigurationOption::integerList));

  // Set those.
  config.setShortHand(
      R"(somePositiveNumber=42; someNegativNumber=-42; boolTrue = true; boolFalse = false; myName = "Bernd"; list = [42, -42];)");

  // Check, if an option was set to the value, you wanted.
  auto checkOption = [&config](const std::string& optionName,
                               const auto& content) {
    const auto& option = config.getConfigurationOptionByNestedKeys(optionName);
    ASSERT_TRUE(option.hasValue());
    ASSERT_EQ(content, option.getValue<decltype(content)>());
  };

  checkOption("somePositiveNumber", static_cast<int>(42));
  checkOption("someNegativNumber", static_cast<int>(-42));

  checkOption("boolTrue", true);
  checkOption("boolFalse", false);

  checkOption("myName", std::string{"Bernd"});

  checkOption("list", std::vector{42, -42});
}
