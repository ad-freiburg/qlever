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
  // Ordinary cases: one-, two-, three- and four-byte codepoints. The expected
  // values use the C++ `\u`/`\U` escapes matching the hexadecimal input (except
  // for `0041`, since `A` would be an ill-formed universal character name).
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("0041"), "A");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("00e4"), "\u00e4");
  EXPECT_EQ(hexadecimalCharactersToUtf8Codepoint("2702"), "\u2702");
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
TEST(RdfEscapingTest, unescapeLiteralWithQuotesRemoved) {
  auto f = [](std::string_view input) {
    // Append to a non-empty string to demonstrate that the result is appended.
    std::string result = "prefix:";
    unescapeLiteralWithQuotesRemoved(input, result);
    return result;
  };
  ASSERT_EQ("prefix:Hello \" \\World", f(R"("Hello \" \\World")"));
  ASSERT_EQ("prefix:Hello \" \\World", f(R"('''Hello \" \\World''')"));
  // A literal without any escape sequence is copied in a single block.
  ASSERT_EQ("prefix:Hello World", f(R"("Hello World")"));
  ASSERT_THROW(f("no quotes"), ad_utility::Exception);
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, unescapeLiteral) {
  auto f = [](std::string_view input) {
    std::string result;
    unescapeLiteral(input, result);
    return result;
  };
  ASSERT_EQ("Hello \" \\World", f(R"(Hello \" \\World)"));
  ASSERT_EQ("Hello World", f("Hello World"));
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, unescapeNewlinesAndBackslashes) {
  ASSERT_EQ(unescapeNewlinesAndBackslashes(""), "");
  ASSERT_EQ(unescapeNewlinesAndBackslashes("Hello World"), "Hello World");
  ASSERT_EQ(unescapeNewlinesAndBackslashes(R"(a\nb\\c\n)"), "a\nb\\c\n");
  // Roundtrip with `escapeNewlinesAndBackslashes`.
  std::string_view original = "line1\nline2\\\t\"end";
  ASSERT_EQ(
      unescapeNewlinesAndBackslashes(escapeNewlinesAndBackslashes(original)),
      original);

  // All other string escapes are not allowed.
  for (std::string_view input :
       {R"(a\tb)", R"(a\rb)", R"(a\bb)", R"(a\fb)", R"(a\"b)", R"(a\'b)"}) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        unescapeNewlinesAndBackslashes(input),
        ::testing::HasSubstr("String escapes like \\n or \\t are not allowed"));
  }
  // Numeric escapes are not allowed.
  for (std::string_view input : {R"(a\u00e4)", R"(a\U0001F600)"}) {
    AD_EXPECT_THROW_WITH_MESSAGE(unescapeNewlinesAndBackslashes(input),
                                 ::testing::HasSubstr("Numeric escapes like"));
  }
  // Unknown escapes and a trailing backslash violate contract checks.
  AD_EXPECT_THROW_WITH_MESSAGE(
      unescapeNewlinesAndBackslashes(R"(a\zb)"),
      ::testing::HasSubstr("Unsupported escape sequence"));
  ASSERT_THROW(unescapeNewlinesAndBackslashes("a\\"), ad_utility::Exception);
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, unescapeIrirefIntoBuffer) {
  // If there is nothing to unescape, the input itself is returned and the
  // buffer is left untouched, which tells the caller that it may keep using
  // the original string.
  {
    std::string buffer;
    std::string_view input = "<http://example.org/a>";
    std::string_view result = unescapeIriref(input, buffer);
    EXPECT_EQ(result, input);
    EXPECT_EQ(result.data(), input.data());
    EXPECT_TRUE(buffer.empty());
  }
  // Otherwise the unescaped IRI is stored in the buffer, and a view of the
  // buffer is returned.
  {
    std::string buffer;
    std::string_view result =
        unescapeIriref(R"(<http://example.org/B\u00E4>)", buffer);
    EXPECT_EQ(result, "<http://example.org/Bä>");
    EXPECT_EQ(result.data(), buffer.data());
  }
  // The buffer has to be empty, else the result of the fast path (an untouched
  // buffer) could not be distinguished from an actual unescaping.
  {
    std::string buffer = "notEmpty";
    AD_EXPECT_THROW_WITH_MESSAGE(
        unescapeIriref("<http://example.org/a>", buffer),
        ::testing::HasSubstr("buffer.empty()"));
  }
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, unescapePrefixedIri) {
  // Inputs without any escape sequence are returned unchanged.
  ASSERT_EQ(unescapePrefixedIri(""), "");
  ASSERT_EQ(unescapePrefixedIri("Q3138"), "Q3138");
  // A percent encoding is not an escape sequence and is left alone.
  ASSERT_EQ(unescapePrefixedIri("a%20b"), "a%20b");

  // A single escape sequence, at the beginning, in the middle, and at the end
  // (the last case leaves an empty remainder).
  ASSERT_EQ(unescapePrefixedIri(R"(\-abc)"), "-abc");
  ASSERT_EQ(unescapePrefixedIri(R"(ab\.cd)"), "ab.cd");
  ASSERT_EQ(unescapePrefixedIri(R"(abc\-)"), "abc-");

  // Several escape sequences, including two directly after one another.
  ASSERT_EQ(unescapePrefixedIri(R"(a\.b\-c\~d)"), "a.b-c~d");
  ASSERT_EQ(unescapePrefixedIri(R"(\$\$)"), "$$");

  // All characters that may be escaped, and nothing else.
  ASSERT_EQ(unescapePrefixedIri(R"(\_\~\.\-\!\$\&\'\(\)\*\+\,\;\=\/\?\#\@\%)"),
            R"(_~.-!$&'()*+,;=/?#@%)");
}

// ___________________________________________________________________________
TEST(RdfEscapingTest, unescapePrefixedIriInvalidEscape) {
  // A backslash at the very end has nothing to escape.
  AD_EXPECT_THROW_WITH_MESSAGE(
      unescapePrefixedIri(R"(abc\)"),
      ::testing::HasSubstr(R"(Could not unescape the prefixed iri abc\)"));
  // A backslash followed by a character that may not be escaped. Note that `z`
  // is a perfectly valid character inside a prefixed IRI, it just may not be
  // preceded by a backslash.
  AD_EXPECT_THROW_WITH_MESSAGE(
      unescapePrefixedIri(R"(ab\zc)"),
      ::testing::HasSubstr(R"(Could not unescape the prefixed iri ab\zc)"));
  // The invalid escape is only reached in the second iteration of the loop.
  // The error message reports the complete input, not just the unparsed rest.
  AD_EXPECT_THROW_WITH_MESSAGE(
      unescapePrefixedIri(R"(a\.b\zc)"),
      ::testing::HasSubstr(R"(Could not unescape the prefixed iri a\.b\zc)"));
}
