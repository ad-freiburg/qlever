// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/json.h"

TEST(JsonUtilTests, JsonToTypeString) {
  // All official data types in json.
  ASSERT_EQ("array",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::array)));
  ASSERT_EQ("boolean",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::boolean)));
  ASSERT_EQ("null",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::null)));
  ASSERT_EQ(
      "number",
      jsonToTypeString(nlohmann::json(nlohmann::json::value_t::number_float)));
  ASSERT_EQ("number", jsonToTypeString(nlohmann::json(
                          nlohmann::json::value_t::number_integer)));
  ASSERT_EQ("number", jsonToTypeString(nlohmann::json(
                          nlohmann::json::value_t::number_unsigned)));
  ASSERT_EQ("object",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::object)));
  ASSERT_EQ("string",
            jsonToTypeString(nlohmann::json(nlohmann::json::value_t::string)));

  // Unofficial types shouldn't work.
  ASSERT_ANY_THROW(
      jsonToTypeString(nlohmann::json(nlohmann::json::value_t::discarded)));
}
