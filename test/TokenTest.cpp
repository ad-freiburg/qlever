// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#include <gtest/gtest.h>
#include <string>
#include "../src/parser/Tokenizer.h"

using std::string;
TEST(TokenTest, Numbers) {
  TurtleToken t;

  string integer = "+235632";
  string decimal = "-235632.23";
  ASSERT_TRUE(std::regex_match(integer, t.Integer));
  ASSERT_TRUE(std::regex_match(decimal, t.Decimal));
  ASSERT_FALSE(std::regex_match(integer, t.Decimal));

  ASSERT_TRUE(true);
}
