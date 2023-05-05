// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//          Hannah bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "parser/RdfEscaping.h"

using namespace RdfEscaping;

// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForCsv) {
  ASSERT_EQ(escapeForCsv("abc"), "abc");
  ASSERT_EQ(escapeForCsv("a\nb\rc,d"), "\"a\nb\rc,d\"");
  ASSERT_EQ(escapeForCsv("\""), "\"\"\"\"");
  ASSERT_EQ(escapeForCsv("a\"b"), "\"a\"\"b\"");
  ASSERT_EQ(escapeForCsv("a\"\"c"), "\"a\"\"\"\"c\"");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForTsv) {
  ASSERT_EQ(escapeForTsv("abc"), "abc");
  ASSERT_EQ(escapeForTsv("a\nb\tc"), "a\\nb c");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, validRDFLiteralFromNormalized) {
  ASSERT_EQ(validRDFLiteralFromNormalized(R"(""\a\"")"), R"("\"\\a\\\"")");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("\b\"@en)"), R"("\\b\\"@en)");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("\c""^^<s>)"), R"("\\c\""^^<s>)");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"\nhi\r\\\""), R"("\nhi\r\\")");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizedContentFromLiteralOrIri) {
  auto f = [](std::string_view s) {
    return normalizedContentFromLiteralOrIri(std::string{s});
  };
  ASSERT_EQ(f("<bladiblu>"), "bladiblu");
  ASSERT_EQ(f("\"bladibla\""), "bladibla");
  ASSERT_EQ(f("\"bimm\"@en"), "bimm");
  ASSERT_EQ(f("\"bumm\"^^<http://www.mycustomiris.com/sometype>"), "bumm");
}
