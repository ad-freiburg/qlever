// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//          Hannah bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "rdfTypes/RdfEscaping.h"
using namespace RdfEscaping;

// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForCsv) {
  ASSERT_EQ(escapeForCsv("abc"), "abc");
  ASSERT_EQ(escapeForCsv("a\nb\rc,d"), "\"a\nb\rc,d\"");
  ASSERT_EQ(escapeForCsv("\""), "\"\"\"\"");
  ASSERT_EQ(escapeForCsv("a\"b"), "\"a\"\"b\"");
  ASSERT_EQ(escapeForCsv("a\"\"c"), "\"a\"\"\"\"c\"");
  // Regression test for https://github.com/ad-freiburg/qlever/issues/3055:
  // literals with any of the three common line-ending conventions (`\n`
  // only, `\r` only, or `\r\n`) must be wrapped in quotes, even if no other
  // special character is present.
  ASSERT_EQ(escapeForCsv("a\nb"), "\"a\nb\"");
  ASSERT_EQ(escapeForCsv("a\rb"), "\"a\rb\"");
  ASSERT_EQ(escapeForCsv("a\r\nb"), "\"a\r\nb\"");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForTsv) {
  ASSERT_EQ(escapeForTsv("abc"), "abc");
  ASSERT_EQ(escapeForTsv("a\nb\tc"), "a\\nb c");
  // Regression test for https://github.com/ad-freiburg/qlever/issues/3055:
  // a lone `\r` (the old Mac line ending) must be escaped just like `\n`,
  // otherwise the raw control character ends up unescaped in the TSV output.
  ASSERT_EQ(escapeForTsv("a\rb"), "a\\rb");
  ASSERT_EQ(escapeForTsv("a\r\nb"), "a\\r\\nb");
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

TEST(RdfEscapingTest, invalidEscapeThrows) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      normalizeRDFLiteral("\"invalid\\Escape\""),
      ::testing::HasSubstr("Unsupported escape sequence"));
}
// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeForXml) {
  ASSERT_EQ(escapeForXml("abc\n\t;"), "abc\n\t;");
  ASSERT_EQ(escapeForXml("a&b\"'c<d>"), "a&amp;b&quot;&apos;c&lt;d&gt;");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeLiteralWithQuotesToNormalizedString) {
  ASSERT_EQ(
      "Hello \" \\World",
      asStringViewUnsafe(normalizeLiteralWithQuotes(R"("Hello \" \\World")")));
  ASSERT_THROW(normalizeLiteralWithQuotes("no quotes"), ad_utility::Exception);
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeLiteralWithoutQuotesToNormalizedString) {
  ASSERT_EQ(
      "Hello \" \\World",
      asStringViewUnsafe(normalizeLiteralWithoutQuotes(R"(Hello \" \\World)")));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeIriWithBracketsToNormalizedString) {
  ASSERT_EQ("https://example.org/books/book1",
            asStringViewUnsafe(
                normalizeIriWithBrackets("<https://example.org/books/book1>")));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeIriWithoutBracketsToNormalizedString) {
  ASSERT_EQ("https://example.org/books/book1",
            asStringViewUnsafe(normalizeIriWithoutBrackets(
                "https://example.org/books/book1")));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, normalizeLanguageTagToNormalizedString) {
  ASSERT_EQ("se", asStringViewUnsafe(normalizeLanguageTag("@se")));
  ASSERT_EQ("se", asStringViewUnsafe(normalizeLanguageTag("se")));
}
