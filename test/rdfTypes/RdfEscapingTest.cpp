// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//          Hannah bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "rdfTypes/RdfEscaping.h"
using namespace RdfEscaping;

// ___________________________________________________________________________
TEST(RdfEscapingTest, hexadecimalCharactersToUtf8Codepoint) {
  using detail::hexadecimalCharactersToUtf8Codepoint;
  // Ordinary cases: one-, two-, three- and four-byte codepoints.
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("0041"), "A");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("00e4"), "ä");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("2702"), "✂");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("0001F600"), "\U0001F600");
  // Corner cases: shorter and full-length (8 hex digits) inputs are accepted.
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("41"), "A");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("1F600"), "\U0001F600");
  // An input longer than a single codepoint (more than 8 hex digits) violates
  // the contract check.
  AD_EXPECT_THROW_WITH_MESSAGE(
      hexadecimalCharactersToUtf8Codepoint("000000000"),
      ::testing::HasSubstr("Assertion `hex.size() <= 8` failed"));
}

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
