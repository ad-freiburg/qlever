// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"

// TODO Add a exception catch test for trying to json string set to anything else than an object.
// TODO Add a exception catch test for trying to json string add, without seting the configuration to anything.

TEST(BenchmarkConfigurationTest, ParseJsonTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // The json string for testing `setJsonString`.
  // The content is a modified demo from
  // `https://www.objgen.com/json?demo=true`.
  const std::string testJsonString{R"--({
  "product": "Live JSON generator",
  "version": 3.1,
  "demo": true,
  "person": {
    "id": 12345,
    "name": "John Doe",
    "phones": {
      "home": "800-123-4567",
      "mobile": "877-123-1234"
    },
    "email": [
      "jd@example.com",
      "jd@example.org"
    ]
  }
  })--"};

  // Set and checking some samples.
  config.setJsonString(testJsonString);

  auto checkSamples = [&config]() {
    ASSERT_EQ((float)3.1,
              config.getValueByNestedKeys<float>("version").value());
    ASSERT_EQ(std::string{"Live JSON generator"},
              config.getValueByNestedKeys<std::string>("product").value());
    ASSERT_EQ(true, config.getValueByNestedKeys<bool>("demo").value());
    ASSERT_EQ(
        std::string{"800-123-4567"},
        config.getValueByNestedKeys<std::string>("person", "phones", "home")
            .value());
    ASSERT_EQ(
        std::string{"jd@example.org"},
        config.getValueByNestedKeys<std::string>("person", "email", 1).value());
  };
  checkSamples();

  // After calling `setJsonString` with an empty json object, `config` should
  // be empty.
  config.setJsonString(R"({})");

  ASSERT_FALSE(config.getValueByNestedKeys<float>("version").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<std::string>("product").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<bool>("demo").has_value());
  ASSERT_FALSE(
      config.getValueByNestedKeys<std::string>("person", "phones", "home")
          .has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<std::string>("person", "email", 1)
                   .has_value());

  // Adding the `testJsonString` again.
  config.addJsonString(testJsonString);
  checkSamples();

  // Does adding things actually overwrite them?
  config.addJsonString(R"--({"product": false})--");
  ASSERT_FALSE(config.getValueByNestedKeys<bool>("product").value());
}

TEST(BenchmarkConfigurationTest, ParseShortHandTest) {
  ad_benchmark::BenchmarkConfiguration config{};

  // Used to set/add all the values, that we test and check them too.
  auto doAndCheck = [&config](const auto& function) {
    // Parse integers.
    function(R"(somePositiveNumber=42;someNegativNumber=-42;)");
    ASSERT_EQ(42,
              config.getValueByNestedKeys<int>("somePositiveNumber").value());
    ASSERT_EQ(-42,
              config.getValueByNestedKeys<int>("someNegativNumber").value());

    // Parse booleans.
    function(R"(boolTrue = true; boolFalse = false;)");
    ASSERT_TRUE(config.getValueByNestedKeys<bool>("boolTrue").value());
    ASSERT_FALSE(config.getValueByNestedKeys<bool>("boolFalse").value());

    // Parse a list of mixed literals.
    function(R"(list = {42, -42, true, false};)");
    ASSERT_EQ(42, config.getValueByNestedKeys<int>("list", 0).value());
    ASSERT_EQ(-42, config.getValueByNestedKeys<int>("list", 1).value());
    ASSERT_TRUE(config.getValueByNestedKeys<bool>("list", 2).value());
    ASSERT_FALSE(config.getValueByNestedKeys<bool>("list", 3).value());
  };

  // Do the test for set.
  doAndCheck(
      [&config](const std::string& toPass) { config.setShortHand(toPass); });

  // Reset the config to a simpler value.
  config.setShortHand(R"(myWishAverage = 1;)");
  // Everything else should have vanished. Lets take a quick sample.
  ASSERT_FALSE(
      config.getValueByNestedKeys<int>("somePositiveNumber").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<bool>("boolFalse").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<bool>("list", 2).has_value());

  // Do the test for add.
  doAndCheck(
      [&config](const std::string& toPass) { config.addShortHand(toPass); });

  // Is the `myWishAverage` still there?
  ASSERT_TRUE(config.getValueByNestedKeys<int>("myWishAverage").has_value());
  ASSERT_EQ(1, config.getValueByNestedKeys<int>("myWishAverage").value());
}
