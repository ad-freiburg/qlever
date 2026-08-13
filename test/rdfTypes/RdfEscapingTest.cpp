// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//          Hannah bast <bast@cs.uni-freiburg.de>

#include <absl/strings/escaping.h>
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
// Boundary forms of `validRDFLiteralFromNormalized`. The fast path scans the
// window `[1, posLastQuote)` between the leading and the closing quote, so
// these tests exercise (a) suffixed literals, whose closing quote is NOT the
// second-to-last character and which must still take the fast path when the
// content is clean, and (b) content lengths around the SSE2 (16 bytes) and
// AVX2 (32 bytes) chunk boundaries with a special byte at the very end of the
// window (the overlapping tail load) and with no special byte at all.
TEST(RdfEscapingTest, validRDFLiteralFromNormalizedBoundaries) {
  // Empty literal and minimal forms, with and without suffix: no escaping
  // needed, must take the fast path and be returned unchanged.
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("")"), R"("")");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("x")"), R"("x")");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("x"@en)"), R"("x"@en)");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("x"^^<http://example.org/type>)"),
            R"("x"^^<http://example.org/type>)");
  ASSERT_EQ(validRDFLiteralFromNormalized(R"("hello world"@de)"),
            R"("hello world"@de)");

  // Quote inside the content (with and without a suffix): must escape.
  ASSERT_EQ(validRDFLiteralFromNormalized("\"a\"b\""), "\"a\\\"b\"");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"a\"b\"@en"), "\"a\\\"b\"@en");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"a\"b\"^^<s>"), "\"a\\\"b\"^^<s>");

  // Backslash, newline, CR inside the content: must escape.
  ASSERT_EQ(validRDFLiteralFromNormalized("\"a\\b\""), "\"a\\\\b\"");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"a\nb\"@en"), "\"a\\nb\"@en");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"a\rb\"^^<s>"), "\"a\\rb\"^^<s>");

  // Unicode content (multi-byte UTF-8, including a surrogate-pair emoji) must
  // be returned byte-identically.
  ASSERT_EQ(validRDFLiteralFromNormalized("\"h\u00e9llo w\u00f6rld\""),
            "\"h\u00e9llo w\u00f6rld\"");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"\U0001F600\""), "\"\U0001F600\"");
  ASSERT_EQ(validRDFLiteralFromNormalized("\"\U0001F600\"@en"),
            "\"\U0001F600\"@en");

  // Long clean contents (no escaping needed): lengths straddle the SSE2 and
  // AVX2 chunk boundaries, with and without a suffix. The suffix must not
  // defeat the fast path (the closing quote is excluded from the window).
  for (size_t contentLength :
       {15ul, 16ul, 17ul, 31ul, 32ul, 33ul, 63ul, 64ul, 65ul, 100ul}) {
    std::string clean = "\"" + std::string(contentLength, 'a') + "\"";
    ASSERT_EQ(validRDFLiteralFromNormalized(clean), clean);
    std::string cleanSuffixed =
        "\"" + std::string(contentLength, 'a') + "\"@en";
    ASSERT_EQ(validRDFLiteralFromNormalized(cleanSuffixed), cleanSuffixed);
    std::string cleanTyped =
        "\"" + std::string(contentLength, 'a') + "\"^^<http://example.org/t>";
    ASSERT_EQ(validRDFLiteralFromNormalized(cleanTyped), cleanTyped);
  }

  // Long contents with a special byte at the very END of the window (the last
  // content byte, right before the closing quote): this is the overlapping
  // tail load of the SIMD scan; it must be found for every chunk boundary.
  for (size_t contentLength :
       {15ul, 16ul, 17ul, 31ul, 32ul, 33ul, 63ul, 64ul, 65ul, 100ul}) {
    for (char special : {'"', '\\', '\n', '\r'}) {
      std::string literal = "\"" + std::string(contentLength - 1, 'a') +
                            std::string(1, special) + "\"";
      std::string expected = "\"" + std::string(contentLength - 1, 'a') +
                             (special == '"'    ? "\\\""
                              : special == '\\' ? "\\\\"
                              : special == '\n' ? "\\n"
                                                : "\\r") +
                             "\"";
      ASSERT_EQ(validRDFLiteralFromNormalized(literal), expected);
      // Same with a suffix.
      std::string literalSuffixed = literal + "@en";
      ASSERT_EQ(validRDFLiteralFromNormalized(literalSuffixed),
                expected + "@en");
    }
  }

  // Special byte at the very START of the window (first chunk of the scan):
  // the leading backslash must be escaped, so the expected literal carries the
  // doubled backslash.
  ASSERT_EQ(
      validRDFLiteralFromNormalized("\"\\n" + std::string(40, 'a') + "\""),
      "\"\\\\n" + std::string(40, 'a') + "\"");
}

// ___________________________________________________________________________
// Scalar reference implementations of the escape functions (mirroring the
// `absl::StrReplaceAll` transformations in `RdfEscaping.cpp`). The SIMD
// change only replaces the "is escaping needed?" detection, so for every
// input the output of the real functions must be byte-identical to these
// references.
namespace {
std::string refEscapeForCsv(std::string_view input) {
  if (input.find_first_of("\r\n\",") == std::string::npos) {
    return std::string{input};
  }
  std::string res = "\"";
  for (char c : input) {
    if (c == '"') {
      res += "\"\"";
    } else {
      res += c;
    }
  }
  res += '"';
  return res;
}

std::string refEscapeForTsv(std::string_view input) {
  if (input.find_first_of("\t\n") == std::string::npos) {
    return std::string{input};
  }
  std::string res;
  res.reserve(input.size());
  for (char c : input) {
    if (c == '\t') {
      res += ' ';
    } else if (c == '\n') {
      res += "\\n";
    } else {
      res += c;
    }
  }
  return res;
}

std::string refEscapeForXml(std::string_view input) {
  std::string res;
  res.reserve(input.size());
  for (char c : input) {
    switch (c) {
      case '&':
        res += "&amp;";
        break;
      case '"':
        res += "&quot;";
        break;
      case '<':
        res += "&lt;";
        break;
      case '>':
        res += "&gt;";
        break;
      case '\'':
        res += "&apos;";
        break;
      default:
        res += c;
    }
  }
  return res;
}

// A deterministic battery of tricky inputs: short/long, clean, one special at
// every position (in particular the last byte, which exercises the SIMD tail
// load), runs of specials, UTF-8, embedded NUL.
std::vector<std::string> trickyInputs() {
  std::vector<std::string> result;
  result.emplace_back();
  result.emplace_back("abc");
  for (size_t len : {1ul, 2ul, 15ul, 16ul, 17ul, 31ul, 32ul, 33ul, 63ul, 64ul,
                     65ul, 128ul}) {
    std::string base(len, 'a');
    result.push_back(base);
    for (size_t pos = 0; pos < len; ++pos) {
      for (char special : {'"', '\n', '\t', '\r', ',', '&', '<', '>', '\''}) {
        std::string s = base;
        s[pos] = special;
        result.push_back(s);
      }
    }
    // runs of specials in the middle
    std::string run = base;
    for (size_t i = len / 4; i < len / 4 + 8 && i < len; ++i) {
      run[i] = '"';
    }
    result.push_back(run);
  }
  // UTF-8 and control characters
  result.emplace_back("h\u00e9llo w\u00f6rld \U0001F600");
  result.emplace_back("\x01\x02\x1f\x7f");
  result.emplace_back("\"\\\n\r\t,&<>'\"");
  result.push_back(std::string("\0\0\0", 3));  // embedded NULs
  return result;
}
}  // namespace

// ___________________________________________________________________________
TEST(RdfEscapingTest, escapeFunctionsMatchScalarReferences) {
  for (const auto& input : trickyInputs()) {
    EXPECT_EQ(escapeForCsv(input), refEscapeForCsv(input))
        << "escapeForCsv input: " << absl::CEscape(input);
    EXPECT_EQ(escapeForTsv(input), refEscapeForTsv(input))
        << "escapeForTsv input: " << absl::CEscape(input);
    EXPECT_EQ(escapeForXml(input), refEscapeForXml(input))
        << "escapeForXml input: " << absl::CEscape(input);
  }
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
