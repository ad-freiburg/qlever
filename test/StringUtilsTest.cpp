// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <functional>
#include <ranges>
#include <sstream>
#include <string>

#include "util/Forward.h"
#include "util/Generator.h"
#include "util/StringUtils.h"

using ad_utility::constantTimeEquals;
using ad_utility::getUTF8Substring;
using ad_utility::utf8ToLower;
using ad_utility::utf8ToUpper;

TEST(StringUtilsTest, utf8ToLower) {
  EXPECT_EQ("schindler's list", utf8ToLower("Schindler's List"));
  EXPECT_EQ("#+-_foo__bar++", utf8ToLower("#+-_foo__Bar++"));
  EXPECT_EQ("fôéßaéé", utf8ToLower("FÔÉßaéÉ"));
}
TEST(StringUtilsTest, utf8ToUpper) {
  EXPECT_EQ("SCHINDLER'S LIST", utf8ToUpper("Schindler's List"));
  EXPECT_EQ("#+-_BIMM__BAMM++", utf8ToUpper("#+-_bImM__baMm++"));
  EXPECT_EQ("FÔÉSSAÉÉ", utf8ToUpper("FôéßaÉé"));
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

TEST(StringUtilsTest, listToString) {
  /*
  Do the test for all overloads of `lazyStrJoin`.
  Every overload needs it's own `range`, because ranges like, for example,
  generators, change, when read and also don't allow copying.
  */
  auto doTestForAllOverloads = [](std::string_view expectedResult,
                                  auto&& rangeForStreamOverload,
                                  auto&& rangeForStringReturnOverload,
                                  std::string_view separator) {
    ASSERT_EQ(expectedResult,
              ad_utility::lazyStrJoin(AD_FWD(rangeForStringReturnOverload),
                                      separator));

    std::ostringstream stream;
    ad_utility::lazyStrJoin(&stream, AD_FWD(rangeForStreamOverload), separator);
    ASSERT_EQ(expectedResult, stream.str());
  };

  // Vectors.
  const std::vector<int> emptyVector;
  const std::vector<int> singleValueVector{42};
  const std::vector<int> multiValueVector{40, 41, 42, 43};
  doTestForAllOverloads("", emptyVector, emptyVector, "\n");
  doTestForAllOverloads("42", singleValueVector, singleValueVector, "\n");
  doTestForAllOverloads("40,41,42,43", multiValueVector, multiValueVector, ",");
  doTestForAllOverloads("40 -> 41 -> 42 -> 43", multiValueVector,
                        multiValueVector, " -> ");

  /*
  `std::ranges::views` can cause dangling pointers, if a `std::identity` is
  called with one, that returns r-values.
  */
  /*
  TODO Do a test, where the `std::views::transform` uses an r-value vector,
  once we no longer support `gcc-11`. The compiler has a bug, where it
  doesn't allow that code, even though it's correct.
  */
  auto plus10View = std::views::transform(
      multiValueVector, [](const int& num) -> int { return num + 10; });
  doTestForAllOverloads("50,51,52,53", plus10View, plus10View, ",");

  auto identityView = std::views::transform(multiValueVector, std::identity{});
  doTestForAllOverloads("40,41,42,43", identityView, identityView, ",");

  // Test, that uses an actual `std::ranges::input_range`. That is, a range who
  // doesn't know it's own size and can only be iterated once.

  // Returns the content of a given vector, element by element.
  auto goThroughVectorGenerator =
      []<typename T>(const std::vector<T>& vec) -> cppcoro::generator<T> {
    for (T entry : vec) {
      co_yield entry;
    }
  };

  doTestForAllOverloads("", goThroughVectorGenerator(emptyVector),
                        goThroughVectorGenerator(emptyVector), "\n");
  doTestForAllOverloads("42", goThroughVectorGenerator(singleValueVector),
                        goThroughVectorGenerator(singleValueVector), "\n");
  doTestForAllOverloads("40,41,42,43",
                        goThroughVectorGenerator(multiValueVector),
                        goThroughVectorGenerator(multiValueVector), ",");
}

TEST(StringUtilsTest, addIndentation) {
  // The input strings for testing.
  static constexpr std::string_view withoutLineBreaks = "Hello\tworld!";
  static constexpr std::string_view withLineBreaks = "\nHello\nworld\n!";

  // No indentation wanted, should cause an error.
  ASSERT_ANY_THROW(ad_utility::addIndentation(withoutLineBreaks, ""));

  // Testing a few different indentation symbols.
  ASSERT_EQ("    Hello\tworld!",
            ad_utility::addIndentation(withoutLineBreaks, "    "));
  ASSERT_EQ("\tHello\tworld!",
            ad_utility::addIndentation(withoutLineBreaks, "\t"));
  ASSERT_EQ("Not Hello\tworld!",
            ad_utility::addIndentation(withoutLineBreaks, "Not "));

  ASSERT_EQ("    \n    Hello\n    world\n    !",
            ad_utility::addIndentation(withLineBreaks, "    "));
  ASSERT_EQ("\t\n\tHello\n\tworld\n\t!",
            ad_utility::addIndentation(withLineBreaks, "\t"));
  ASSERT_EQ("Not \nNot Hello\nNot world\nNot !",
            ad_utility::addIndentation(withLineBreaks, "Not "));
}
