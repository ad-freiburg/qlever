// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/StringUtils.h"

using ad_utility::constantTimeEquals;
using ad_utility::getLowercaseUtf8;
using ad_utility::getUTF8Substring;

TEST(StringUtilsTest, getLowercaseUtf8) {
  setlocale(LC_CTYPE, "");
  ASSERT_EQ("schindler's list", getLowercaseUtf8("Schindler's List"));
  ASSERT_EQ("#+-_foo__bar++", getLowercaseUtf8("#+-_foo__Bar++"));
  ASSERT_EQ("fôéßaéé", getLowercaseUtf8("FÔÉßaéÉ"));
}

TEST(StringUtilsTest, getUTF8Substring) {
  // Works normally for strings with only single byte characters.
  ASSERT_EQ("fel", getUTF8Substring("Apfelsaft", 2, 3));
  ASSERT_EQ("saft", getUTF8Substring("Apfelsaft", 5, 4));
  // start+size > number of codepoints
  ASSERT_EQ("saft", getUTF8Substring("Apfelsaft", 5, 5));
  ASSERT_EQ("Apfelsaft", getUTF8Substring("Apfelsaft", 0, 9));
  // start+size > number of codepoints
  ASSERT_EQ("Apfelsaft", getUTF8Substring("Apfelsaft", 0, 100));
  // start > number of codepoints
  ASSERT_EQ("", getUTF8Substring("Apfelsaft", 12, 13));
  ASSERT_EQ("saft", getUTF8Substring("Apfelsaft", 5));
  ASSERT_EQ("t", getUTF8Substring("Apfelsaft", 8));
  // String with multibyte UTF-16 characters.
  ASSERT_EQ("Fl", getUTF8Substring("Flöhe", 0, 2));
  ASSERT_EQ("he", getUTF8Substring("Flöhe", 3, 2));
  // start+size > number of codepoints
  ASSERT_EQ("Apfelsaft", getUTF8Substring("Apfelsaft", 0, 100));
  ASSERT_EQ("he", getUTF8Substring("Flöhe", 3, 4));
  ASSERT_EQ("löh", getUTF8Substring("Flöhe", 1, 3));
  // UTF-32 and UTF-16 Characters.
  ASSERT_EQ("\u2702", getUTF8Substring("\u2702\U0001F605\u231A\u00A9", 0, 1));
  ASSERT_EQ("\U0001F605\u231A",
            getUTF8Substring("\u2702\U0001F605\u231A\u00A9", 1, 2));
  ASSERT_EQ("\u231A\u00A9",
            getUTF8Substring("\u2702\U0001F605\u231A\u00A9", 2, 2));
  ASSERT_EQ("\u00A9", getUTF8Substring("\u2702\U0001F605\u231A\u00A9", 3, 1));
  // start+size > number of codepoints
  ASSERT_EQ("Apfelsaft", getUTF8Substring("Apfelsaft", 0, 100));
  ASSERT_EQ("\u00A9", getUTF8Substring("\u2702\u231A\u00A9", 2, 2));
}

// It should just work like the == operator for strings, just without
// the typical short circuit optimization
TEST(StringUtilsTest, constantTimeEquals) {
  EXPECT_TRUE(constantTimeEquals("", ""));
  EXPECT_TRUE(constantTimeEquals("Abcdefg", "Abcdefg"));
  EXPECT_FALSE(constantTimeEquals("Abcdefg", "abcdefg"));
  EXPECT_FALSE(constantTimeEquals("", "Abcdefg"));
  EXPECT_FALSE(constantTimeEquals("Abcdefg", ""));
  EXPECT_FALSE(constantTimeEquals("Abc", "defg"));
}
