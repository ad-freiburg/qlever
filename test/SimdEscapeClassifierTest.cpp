// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "./util/GTestHelpers.h"
#include "engine/SimdEscapeClassifier.h"

using namespace ad_utility::simd;

// ___________________________________________________________________________
TEST(SimdEscapeClassifierTest, scanChunk32And16Turtle) {
  // Test clean 32-byte chunk
  std::string clean32 = "01234567890123456789012345678901";
  auto maskClean =
      SimdEscapeClassifier::scanChunk32<EscapeFormat::Turtle>(clean32.data());
  EXPECT_TRUE(maskClean.isAllClean());
  EXPECT_FALSE(maskClean.hasEscape());
  EXPECT_EQ(maskClean.rawMask(), 0u);
  EXPECT_EQ(maskClean.countEscapes(), 0u);
  EXPECT_EQ(maskClean.firstEscapeIndex(), 32u);

  // Test escape at specific positions in 32-byte chunk
  for (size_t pos = 0; pos < 32; ++pos) {
    for (char escapeChar : {'"', '\\', '\n', '\r'}) {
      std::string text = clean32;
      text[pos] = escapeChar;
      auto mask =
          SimdEscapeClassifier::scanChunk32<EscapeFormat::Turtle>(text.data());
      EXPECT_TRUE(mask.hasEscape());
      EXPECT_EQ(mask.rawMask(), 1u << pos);
      EXPECT_EQ(mask.countEscapes(), 1u);
      EXPECT_EQ(mask.firstEscapeIndex(), pos);
    }
  }

  // Test multiple escapes in 32-byte chunk
  std::string multiEscape = clean32;
  multiEscape[0] = '"';
  multiEscape[5] = '\\';
  multiEscape[31] = '\n';
  auto maskMulti = SimdEscapeClassifier::scanChunk32<EscapeFormat::Turtle>(
      multiEscape.data());
  EXPECT_TRUE(maskMulti.hasEscape());
  EXPECT_EQ(maskMulti.rawMask(), (1u << 0) | (1u << 5) | (1u << 31));
  EXPECT_EQ(maskMulti.countEscapes(), 3u);
  EXPECT_EQ(maskMulti.firstEscapeIndex(), 0u);

  // Test 16-byte chunk
  std::string clean16 = "0123456789012345";
  auto mask16Clean =
      SimdEscapeClassifier::scanChunk16<EscapeFormat::Turtle>(clean16.data());
  EXPECT_TRUE(mask16Clean.isAllClean());
  EXPECT_EQ(mask16Clean.countEscapes(), 0u);

  std::string escape16 = clean16;
  escape16[15] = '"';
  auto mask16 =
      SimdEscapeClassifier::scanChunk16<EscapeFormat::Turtle>(escape16.data());
  EXPECT_TRUE(mask16.hasEscape());
  EXPECT_EQ(mask16.rawMask(), 1u << 15);
  EXPECT_EQ(mask16.firstEscapeIndex(), 15u);
}

// ___________________________________________________________________________
TEST(SimdEscapeClassifierTest, findFirstEscapeTurtle) {
  // Empty string
  EXPECT_EQ(SimdEscapeClassifier::findFirstEscape<EscapeFormat::Turtle>(""),
            std::string_view::npos);
  EXPECT_FALSE(SimdEscapeClassifier::hasEscapes<EscapeFormat::Turtle>(""));

  // Clean strings of various sizes across boundary conditions (1 to 100 bytes)
  for (size_t len = 1; len <= 100; ++len) {
    std::string clean(len, 'a');
    EXPECT_EQ(
        SimdEscapeClassifier::findFirstEscape<EscapeFormat::Turtle>(clean),
        std::string_view::npos);
    EXPECT_FALSE(SimdEscapeClassifier::hasEscapes<EscapeFormat::Turtle>(clean));
  }

  // Single escape at every possible position across various sizes
  for (size_t len : {1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100}) {
    for (size_t pos = 0; pos < len; ++pos) {
      std::string text(len, 'x');
      text[pos] = '"';
      EXPECT_EQ(
          SimdEscapeClassifier::findFirstEscape<EscapeFormat::Turtle>(text),
          pos);
      EXPECT_TRUE(SimdEscapeClassifier::hasEscapes<EscapeFormat::Turtle>(text));

      text[pos] = '\\';
      EXPECT_EQ(
          SimdEscapeClassifier::findFirstEscape<EscapeFormat::Turtle>(text),
          pos);

      text[pos] = '\n';
      EXPECT_EQ(
          SimdEscapeClassifier::findFirstEscape<EscapeFormat::Turtle>(text),
          pos);

      text[pos] = '\r';
      EXPECT_EQ(
          SimdEscapeClassifier::findFirstEscape<EscapeFormat::Turtle>(text),
          pos);
    }
  }
}

// ___________________________________________________________________________
TEST(SimdEscapeClassifierTest, scanForEscapesMasks) {
  std::string input =
      "Hello World! This is a test with \"quotes\" and \\backslash and \nnew "
      "line.";
  auto masks =
      SimdEscapeClassifier::scanForEscapes<EscapeFormat::Turtle>(input);
  size_t expectedChunks = (input.size() + 31) / 32;
  EXPECT_EQ(masks.size(), expectedChunks);

  // Check each character against the masks
  for (size_t i = 0; i < input.size(); ++i) {
    size_t chunkIdx = i / 32;
    size_t bitIdx = i % 32;
    bool isBitSet = (masks[chunkIdx] & (1u << bitIdx)) != 0;
    bool expectedEscape =
        SimdEscapeClassifier::isEscapeChar<EscapeFormat::Turtle>(input[i]);
    EXPECT_EQ(isBitSet, expectedEscape)
        << "Mismatch at byte index " << i << " ('" << input[i] << "')";
  }
}

// ___________________________________________________________________________
TEST(SimdEscapeClassifierTest, escapeForCsv) {
  EXPECT_EQ(SimdEscapeClassifier::escapeForCsv("abc"), "abc");
  EXPECT_EQ(SimdEscapeClassifier::escapeForCsv("a\nb\rc,d"), "\"a\nb\rc,d\"");
  EXPECT_EQ(SimdEscapeClassifier::escapeForCsv("\""), "\"\"\"\"");
  EXPECT_EQ(SimdEscapeClassifier::escapeForCsv("a\"b"), "\"a\"\"b\"");
  EXPECT_EQ(SimdEscapeClassifier::escapeForCsv("a\"\"c"), "\"a\"\"\"\"c\"");
  EXPECT_EQ(
      SimdEscapeClassifier::escapeForCsv("simple_text_without_special_chars"),
      "simple_text_without_special_chars");
  EXPECT_EQ(
      SimdEscapeClassifier::escapeForCsv(
          "long_string_that_exceeds_thirty_two_characters_clean_fast_path"),
      "long_string_that_exceeds_thirty_two_characters_clean_fast_path");
}

// ___________________________________________________________________________
TEST(SimdEscapeClassifierTest, escapeForTsv) {
  EXPECT_EQ(SimdEscapeClassifier::escapeForTsv("abc"), "abc");
  EXPECT_EQ(SimdEscapeClassifier::escapeForTsv("a\nb\tc"), "a\\nb c");
  EXPECT_EQ(SimdEscapeClassifier::escapeForTsv(
                "clean_tab_separated_value_test_32_bytes_long"),
            "clean_tab_separated_value_test_32_bytes_long");
  EXPECT_EQ(SimdEscapeClassifier::escapeForTsv("tab\there\nand\rhere\\too"),
            "tab here\\nand\\rhere\\\\too");
}

// ___________________________________________________________________________
TEST(SimdEscapeClassifierTest, validRDFLiteralFromNormalized) {
  EXPECT_EQ(SimdEscapeClassifier::validRDFLiteralFromNormalized(R"(""\a\"")"),
            R"("\"\\a\\\"")");
  EXPECT_EQ(SimdEscapeClassifier::validRDFLiteralFromNormalized(R"("\b\"@en)"),
            R"("\\b\\"@en)");
  EXPECT_EQ(
      SimdEscapeClassifier::validRDFLiteralFromNormalized(R"("\c""^^<s>)"),
      R"("\\c\""^^<s>)");
  EXPECT_EQ(SimdEscapeClassifier::validRDFLiteralFromNormalized("\"\nhi\r\\\""),
            R"("\nhi\r\\")");
  EXPECT_EQ(SimdEscapeClassifier::validRDFLiteralFromNormalized(
                "\"simple clean literal\""),
            "\"simple clean literal\"");
  EXPECT_EQ(SimdEscapeClassifier::validRDFLiteralFromNormalized(
                "\"This is a longer clean literal that spans multiple 32-byte "
                "SIMD blocks without any escapes.\""),
            "\"This is a longer clean literal that spans multiple 32-byte SIMD "
            "blocks without any escapes.\"");
}

// ___________________________________________________________________________
TEST(SimdEscapeClassifierTest, utf8Preservation) {
  // UTF-8 multibyte characters should not trigger false positives
  std::string utf8Text =
      "\"München, Düsseldorf, Zürich, 日本語, 🚀 Antigravity\"@de";
  EXPECT_EQ(SimdEscapeClassifier::validRDFLiteralFromNormalized(utf8Text),
            utf8Text);

  // UTF-8 multibyte characters with an escape char
  std::string utf8WithEscape = "\"München \"Düsseldorf\" Zürich\"@de";
  EXPECT_EQ(SimdEscapeClassifier::validRDFLiteralFromNormalized(utf8WithEscape),
            "\"München \\\"Düsseldorf\\\" Zürich\"@de");
}
