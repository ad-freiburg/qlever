// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/parser/RdfEscaping.h"

TEST(RdfEscapingTest, testEscapeForCsv) {
  ASSERT_EQ(RdfEscaping::escapeForCsv("abc"), "abc");
  ASSERT_EQ(RdfEscaping::escapeForCsv("a\nb\rc,d"), "\"a\nb\rc,d\"");
  ASSERT_EQ(RdfEscaping::escapeForCsv("\""), "\"\"\"\"");
  ASSERT_EQ(RdfEscaping::escapeForCsv("a\"b"), "\"a\"\"b\"");
  ASSERT_EQ(RdfEscaping::escapeForCsv("a\"\"c"), "\"a\"\"\"\"c\"");
}

TEST(RdfEscapingTest, testEscapeForTsv) {
  ASSERT_EQ(RdfEscaping::escapeForTsv("abc"), "abc");
  ASSERT_EQ(RdfEscaping::escapeForTsv("a\nb\tc"), "a\\nb c");
}
