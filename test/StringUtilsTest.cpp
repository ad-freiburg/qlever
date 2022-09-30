// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/StringUtils.h"

using ad_utility::getLowercaseUtf8;

TEST(StringUtilsTest, getLowercaseUtf8) {
  setlocale(LC_CTYPE, "");
  ASSERT_EQ("schindler's list", getLowercaseUtf8("Schindler's List"));
  ASSERT_EQ("#+-_foo__bar++", getLowercaseUtf8("#+-_foo__Bar++"));
  ASSERT_EQ("fôéßaéé", getLowercaseUtf8("FÔÉßaéÉ"));
}

TEST(StringUtilsTest, getUTF8Substring) {
  // Works normally for strings with only single byte characters.
  ASSERT_EQ("fel", ad_utility::getUTF8Substring("Apfelsaft", 2, 3));
  ASSERT_EQ("saft", ad_utility::getUTF8Substring("Apfelsaft", 5, 4));
  // start+size > number of codepoints
  ASSERT_EQ("saft", ad_utility::getUTF8Substring("Apfelsaft", 5, 5));
  ASSERT_EQ("Apfelsaft", ad_utility::getUTF8Substring("Apfelsaft", 0, 9));
  // start+size > number of codepoints
  ASSERT_EQ("Apfelsaft", ad_utility::getUTF8Substring("Apfelsaft", 0, 100));
  // start > number of codepoints
  ASSERT_EQ("", ad_utility::getUTF8Substring("Apfelsaft", 12, 13));
  ASSERT_EQ("saft", ad_utility::getUTF8Substring("Apfelsaft", 5));
  ASSERT_EQ("t", ad_utility::getUTF8Substring("Apfelsaft", 8));
  // String with multibyte UTF-16 characters.
  ASSERT_EQ("Fl", ad_utility::getUTF8Substring("Flöhe", 0, 2));
  ASSERT_EQ("he", ad_utility::getUTF8Substring("Flöhe", 3, 2));
  // start+size > number of codepoints
  ASSERT_EQ("Apfelsaft", ad_utility::getUTF8Substring("Apfelsaft", 0, 100));
  ASSERT_EQ("he", ad_utility::getUTF8Substring("Flöhe", 3, 4));
  ASSERT_EQ("löh", ad_utility::getUTF8Substring("Flöhe", 1, 3));
  // UTF-32 and UTF-16 Characters.
  ASSERT_EQ("\u2702", ad_utility::getUTF8Substring("\u2702\u231A\u00A9", 0, 1));
  ASSERT_EQ("\u231A\u00A9",
            ad_utility::getUTF8Substring("\u2702\u231A\u00A9", 1, 2));
  ASSERT_EQ("\u00A9", ad_utility::getUTF8Substring("\u2702\u231A\u00A9", 2, 1));
  // start+size > number of codepoints
  ASSERT_EQ("Apfelsaft", ad_utility::getUTF8Substring("Apfelsaft", 0, 100));
  ASSERT_EQ("\u00A9", ad_utility::getUTF8Substring("\u2702\u231A\u00A9", 2, 2));
}
