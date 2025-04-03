// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <functional>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>

#include "../test/util/GTestHelpers.h"
#include "global/Constants.h"
#include "util/ConstexprSmallString.h"
#include "util/ConstexprUtils.h"
#include "util/Forward.h"
#include "util/Generator.h"
#include "util/StringUtils.h"
#include "util/StringUtilsImpl.h"

using ad_utility::constantTimeEquals;
using ad_utility::constexprStrCat;
using ad_utility::getUTF8Substring;
using ad_utility::strIsLangTag;
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
  `ql::ranges::views` can cause dangling pointers, if a `std::identity` is
  called with one, that returns r-values.
  */
  /*
  TODO Do a test, where the `ql::views::transform` uses an r-value vector,
  once we no longer support `gcc-11`. The compiler has a bug, where it
  doesn't allow that code, even though it's correct.
  */
  auto plus10View = ql::views::transform(
      multiValueVector, [](const int& num) -> int { return num + 10; });
  doTestForAllOverloads("50,51,52,53", plus10View, plus10View, ",");

  auto identityView = ql::views::transform(multiValueVector, std::identity{});
  doTestForAllOverloads("40,41,42,43", identityView, identityView, ",");

  // Test, that uses an actual `ql::ranges::input_range`. That is, a range who
  // doesn't know it's own size and can only be iterated once.

  // Returns the content of a given vector, element by element.
  auto goThroughVectorGenerator = [](const auto& vec)
      -> cppcoro::generator<typename std::decay_t<decltype(vec)>::value_type> {
    for (auto entry : vec) {
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

TEST(StringUtilsTest, insertThousandSeparator) {
  /*
  Do the tests, that are not exception tests, with the given arguments for
  `insertThousandSeparator`.
  */
  auto doNotExceptionTest = []<const char floatingPointSignifier>(
                                const char separatorSymbol,
                                ad_utility::source_location l =
                                    ad_utility::source_location::current()) {
    // For generating better messages, when failing a test.
    auto trace{generateLocationTrace(l, "doNotExceptionTest")};

    // For easier usage with `absl::StrCat()`.
    const std::string floatingPointSignifierString{floatingPointSignifier};

    /*
    @brief Make a comparison check, that the given string, given in pieces,
    generates the wanted string, when called with `insertThousandSeparator` with
    the arguments from `doNotExceptionTest`.

    @param stringPieces The input for `insertThousandSeparator` are those pieces
    concatenated and the expected output are those pieces concatenated with
    `separatorSymbol` between them. For example: `{"This number 4", "198."}`.
    */
    auto simpleComparisonTest =
        [&separatorSymbol](const std::vector<std::string>& stringPieces,
                           ad_utility::source_location l =
                               ad_utility::source_location::current()) {
          // For generating better messages, when failing a test.
          auto trace{generateLocationTrace(l, "simpleComparisonTest")};
          ASSERT_STREQ(
              ad_utility::lazyStrJoin(stringPieces,
                                      std::string{separatorSymbol})
                  .c_str(),
              ad_utility::insertThousandSeparator<floatingPointSignifier>(
                  ad_utility::lazyStrJoin(stringPieces, ""), separatorSymbol)
                  .c_str());
        };

    // Empty string.
    simpleComparisonTest({});

    // No numbers.
    simpleComparisonTest(
        {"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
         "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim "
         "ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut "
         "aliquip ex ea commodo consequat. Duis aute irure dolor in "
         "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
         "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in "
         "culpa qui officia deserunt mollit anim id est laborum."});

    // Only whole numbers.
    simpleComparisonTest({"1"});
    simpleComparisonTest({"21"});
    simpleComparisonTest({"321"});
    simpleComparisonTest({"4", "321"});
    simpleComparisonTest({"54", "321"});
    simpleComparisonTest({"654", "321"});
    simpleComparisonTest({"7", "654", "321"});
    simpleComparisonTest({"87", "654", "321"});
    simpleComparisonTest({"987", "654", "321"});

    // Floating points.
    simpleComparisonTest(
        {absl::StrCat("1", floatingPointSignifierString, "000")});
    simpleComparisonTest(
        {absl::StrCat("2", floatingPointSignifierString, "1")});
    simpleComparisonTest(
        {absl::StrCat("362", floatingPointSignifierString, "1")});
    simpleComparisonTest(
        {absl::StrCat("3", floatingPointSignifierString, "21")});
    simpleComparisonTest(
        {"876", absl::StrCat("703", floatingPointSignifierString, "21")});
    simpleComparisonTest(
        {absl::StrCat("3", floatingPointSignifierString,
                      "217710466665135481349068158967136466")});
    simpleComparisonTest(
        {"140", "801",
         absl::StrCat("813", floatingPointSignifierString,
                      "217710466665135481349068158967136466")});

    // Mixing numbers and normal symbols.
    simpleComparisonTest(
        {"140", "801",
         absl::StrCat("813", floatingPointSignifierString,
                      "217710466665135481349068158967136466", " 3",
                      floatingPointSignifierString,
                      "217710466665135481349068158967136466", " 876"),
         absl::StrCat("703", floatingPointSignifierString, "21 3",
                      floatingPointSignifierString, "21 362",
                      floatingPointSignifierString, "1 2",
                      floatingPointSignifierString, "1 987"),
         "654", "321 87", "654", "321 7", "654", "321 654", "321 54", "321 4",
         "321 321 21 1"});
    simpleComparisonTest(
        {absl::StrCat("Lorem ipsum dolor sit "
                      "813",
                      floatingPointSignifierString,
                      "217710466665135481349068158967136466 amet, "
                      "consectetur adipiscing elit. Quippe:  876"),
         absl::StrCat(
             "703", floatingPointSignifierString,
             "21 habes enim a rhetoribus; Bork Falli igitur possumus. Bonum "
             "integritas corporis: misera debilitas 987"),
         "654",
         "321.  Nos commodius agimus.Duo "
         "Reges : constructio interrete 42.  Quod cum dixissent, ille "
         "contra.Tuo "
         "vero id quidem, inquam, arbitratu.Omnia contraria, quos etiam "
         "insanos esse vultis.Sed haec in pueris; "});
  };
  doNotExceptionTest.template operator()<','>(' ');
  doNotExceptionTest.template operator()<'+'>('t');
  doNotExceptionTest.template operator()<'t'>('+');
  doNotExceptionTest.template operator()<'?'>('\"');
  doNotExceptionTest.template operator()<'-'>('~');

  // Set the `floatingPointSignifier` to characters, that are reserved regex
  // characters.
  auto reservedRegexCharTest = [&doNotExceptionTest]<const char... Cs>() {
    (doNotExceptionTest.template operator()<Cs>(' '), ...);
  };
  reservedRegexCharTest
      .template operator()<'.', '(', ')', '[', ']', '|', '{', '}', '*', '+',
                           '?', '^', '$', '\\', '-', '/'>();

  // Numbers as `separatorSymbol`, or `floatingPointSignifier`, are not allowed.
  auto forbiddenSymbolTest =
      [&doNotExceptionTest]<const char... floatingPointSignifiers>() {
        for (size_t separatorSymbolNum = 0; separatorSymbolNum < 10;
             separatorSymbolNum++) {
          const char separatorSymbol{absl::StrCat(separatorSymbolNum).front()};
          (
              [&doNotExceptionTest, &separatorSymbol]<const char c>() {
                ASSERT_ANY_THROW(
                    doNotExceptionTest.template operator()<c>(' '));
                ASSERT_ANY_THROW(doNotExceptionTest.template operator()<'.'>(
                    separatorSymbol));
                ASSERT_ANY_THROW(
                    doNotExceptionTest.template operator()<c>(separatorSymbol));
              }.template operator()<floatingPointSignifiers>(),
              ...);
        }
      };
  forbiddenSymbolTest
      .template operator()<'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'>();
}

TEST(StringUtilsTest, findLiteralEnd) {
  using namespace ad_utility;
  EXPECT_EQ(findLiteralEnd("nothing", "\""), std::string_view::npos);
  EXPECT_EQ(findLiteralEnd("no\"thing", "\""), 2u);
  EXPECT_EQ(findLiteralEnd("no\\\"thi\"ng", "\""), 7u);  // codespell-ignore
  EXPECT_EQ(findLiteralEnd("no\\\\\"thing", "\""), 4u);
}

TEST(StringUtilsTest, strLangTag) {
  // INVALID TAGS
  ASSERT_FALSE(strIsLangTag(""));
  ASSERT_FALSE(strIsLangTag("de-@"));
  ASSERT_FALSE(strIsLangTag("x46"));
  ASSERT_FALSE(strIsLangTag("*-DE"));
  ASSERT_FALSE(strIsLangTag("en@US"));
  ASSERT_FALSE(strIsLangTag("de_US"));
  ASSERT_FALSE(strIsLangTag("9046"));
  ASSERT_FALSE(strIsLangTag("-fr-BE-"));
  ASSERT_FALSE(strIsLangTag("de-366-?"));

  // VALID TAGS
  ASSERT_TRUE(strIsLangTag("en"));
  ASSERT_TRUE(strIsLangTag("en-US"));
  ASSERT_TRUE(strIsLangTag("es-419"));
  ASSERT_TRUE(strIsLangTag("zh-Hant-HK"));
  ASSERT_TRUE(strIsLangTag("fr-BE-1606nict"));
  ASSERT_TRUE(strIsLangTag("de-CH-x-zh"));
  ASSERT_TRUE(strIsLangTag("en"));
}

// _____________________________________________________________________________
TEST(StringUtilsTest, constexprStrCat) {
  using namespace std::string_view_literals;
  ASSERT_EQ((constexprStrCat<>()), ""sv);
  ASSERT_EQ((constexprStrCat<"">()), ""sv);
  ASSERT_EQ((constexprStrCat<"single">()), "single"sv);
  ASSERT_EQ((constexprStrCat<"", "single", "">()), "single"sv);
  ASSERT_EQ((constexprStrCat<"hello", " ", "World!">()), "hello World!"sv);
  static_assert(constexprStrCat<"hello", " ", "World!">() == "hello World!"sv);
}

// _____________________________________________________________________________
TEST(StringUtilsTest, constexprStrCatImpl) {
  // The coverage tools don't track the compile time usages of these internal
  // helper functions, so we test them manually.
  using namespace ad_utility::detail::constexpr_str_cat_impl;
  ASSERT_EQ((constexprStrCatBufferImpl<"h", "i">()),
            (std::array{'h', 'i', '\0'}));

  using C = ConstexprString;
  ASSERT_EQ((catImpl<2>(std::array{C{"h"}, C{"i"}})),
            (std::array{'h', 'i', '\0'}));
}

TEST(StringUtilsTest, truncateOperationString) {
  auto expectTruncate = [](std::string_view test, bool willTruncate,
                           ad_utility::source_location l =
                               ad_utility::source_location::current()) {
    auto tr = generateLocationTrace(l);
    const std::string truncated = ad_utility::truncateOperationString(test);
    if (willTruncate) {
      EXPECT_THAT(truncated, testing::SizeIs(MAX_LENGTH_OPERATION_ECHO));
    } else {
      EXPECT_THAT(truncated, testing::SizeIs(test.size()));
    }
    std::string_view truncated_view = truncated;
    if (willTruncate) {
      EXPECT_THAT(truncated_view.substr(0, MAX_LENGTH_OPERATION_ECHO - 3),
                  testing::Eq(test.substr(0, MAX_LENGTH_OPERATION_ECHO - 3)));
    } else {
      EXPECT_THAT(truncated, testing::Eq(test));
    }
  };
  expectTruncate(std::string(MAX_LENGTH_OPERATION_ECHO + 1000, 'f'), true);
  expectTruncate(std::string(MAX_LENGTH_OPERATION_ECHO + 1, 'f'), true);
  expectTruncate(std::string(MAX_LENGTH_OPERATION_ECHO, 'f'), false);
  expectTruncate(std::string(MAX_LENGTH_OPERATION_ECHO - 1, 'f'), false);
  expectTruncate("SELECT * WHERE { ?s ?p ?o }", false);
}
