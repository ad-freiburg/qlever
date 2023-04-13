// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"

TEST(BenchmarkConfigurationTest, ParseJsonTest){
  ad_benchmark::BenchmarkConfiguration config{};

  // The json string for parsing.
  // The content is the demo from `https://www.objgen.com/json?demo=true`.
  const std::string testJsonString{R"--({
  "product": "Live JSON generator",
  "version": 3.1,
  "releaseDate": "2014-06-25T00:00:00.000Z",
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
    ],
    "dateOfBirth": "1980-01-02T00:00:00.000Z",
    "registered": true,
    "emergencyContacts": [
      {
        "name": "Jane Doe",
        "phone": "888-555-1212",
        "relationship": "spouse"
      },
      {
        "name": "Justin Doe",
        "phone": "877-123-1212",
        "relationship": "parent"
      }
    ]
  }
  })--"};

  // Parsing and checking some samples.
  config.parseJsonString(testJsonString);

  ASSERT_EQ((float) 3.1, config.getValueByNestedKeys<float>("version").value());
  ASSERT_EQ(std::string{"Live JSON generator"},
    config.getValueByNestedKeys<std::string>("product").value());
  ASSERT_EQ(true, config.getValueByNestedKeys<bool>("demo").value());
  ASSERT_EQ(std::string{"800-123-4567"},
    config.getValueByNestedKeys<std::string>("person", "phones",
    "home").value());
  ASSERT_EQ(std::string{"jd@example.org"},
    config.getValueByNestedKeys<std::string>("person", "email", 1).value());

  // After parsing an empty json object, `config` should be empty.
  config.parseJsonString(R"({})");

  ASSERT_FALSE(config.getValueByNestedKeys<float>("version").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<std::string>("product").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<bool>("demo").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<std::string>("person", "phones",
    "home").has_value());
  ASSERT_FALSE(config.getValueByNestedKeys<std::string>("person", "email", 2).has_value());
}

TEST(BenchmarkConfigurationTest, ParseShortHandTest){
  ad_benchmark::BenchmarkConfiguration config{};

  // Parse integers.
  config.parseShortHand(R"(somePositiveNumber=42;someNegativNumber=-42;)");
  ASSERT_EQ(42, config.getValueByNestedKeys<int>("somePositiveNumber").value());
  ASSERT_EQ(-42,
    config.getValueByNestedKeys<int>("someNegativNumber").value());

  // Parse booleans.
  config.parseShortHand(R"(boolTrue = true; boolFalse = false;)");
  ASSERT_TRUE(config.getValueByNestedKeys<bool>("boolTrue").value());
  ASSERT_FALSE(config.getValueByNestedKeys<bool>("boolFalse").value());

  // Parse a list of mixed literals.
  config.parseShortHand(R"(list = {42, -42, true, false};)");
  ASSERT_EQ(42, config.getValueByNestedKeys<int>("list", 0).value());
  ASSERT_EQ(-42, config.getValueByNestedKeys<int>("list", 1).value());
  ASSERT_TRUE(config.getValueByNestedKeys<bool>("list", 2).value());
  ASSERT_FALSE(config.getValueByNestedKeys<bool>("list", 3).value());
}