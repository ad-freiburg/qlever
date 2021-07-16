// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "../src/util/StringUtils.h"

using std::string;
using std::vector;

namespace ad_utility {
TEST(StringUtilsTest, startWith) {
  string patternAsString = "ab";
  const char* patternAsCharP = patternAsString.c_str();
  size_t patternSize = 2;

  string matchingText1 = "abc";
  string matchingText2 = "ab";
  string mismatchText1 = "";
  string mismatchText2 = "bcd";
  string mismatchText3 = "aa";

  ASSERT_TRUE(startsWith(matchingText1, patternAsString));
  ASSERT_TRUE(startsWith(matchingText1, patternAsCharP, patternSize));
  ASSERT_TRUE(startsWith(matchingText1, patternAsCharP));

  ASSERT_TRUE(startsWith(matchingText2, patternAsString));
  ASSERT_TRUE(startsWith(matchingText2, patternAsCharP, patternSize));
  ASSERT_TRUE(startsWith(matchingText2, patternAsCharP));

  ASSERT_FALSE(startsWith(mismatchText1, patternAsString));
  ASSERT_FALSE(startsWith(mismatchText1, patternAsCharP, patternSize));
  ASSERT_FALSE(startsWith(mismatchText1, patternAsCharP));

  ASSERT_FALSE(startsWith(mismatchText2, patternAsString));
  ASSERT_FALSE(startsWith(mismatchText2, patternAsCharP, patternSize));
  ASSERT_FALSE(startsWith(mismatchText2, patternAsCharP));

  ASSERT_FALSE(startsWith(mismatchText3, patternAsString));
  ASSERT_FALSE(startsWith(mismatchText3, patternAsCharP, patternSize));
  ASSERT_FALSE(startsWith(mismatchText3, patternAsCharP));
}

TEST(StringUtilsTest, endsWith) {
  string patternAsString = "ba";
  const char* patternAsCharP = patternAsString.c_str();
  size_t patternSize = 2;

  string matchingText1 = "cba";
  string matchingText2 = "ba";
  string mismatchText1 = "";
  string mismatchText2 = "dcb";
  string mismatchText3 = "aa";

  ASSERT_TRUE(endsWith(matchingText1, patternAsString));
  ASSERT_TRUE(endsWith(matchingText1, patternAsCharP, patternSize));
  ASSERT_TRUE(endsWith(matchingText1, patternAsCharP));

  ASSERT_TRUE(endsWith(matchingText2, patternAsString));
  ASSERT_TRUE(endsWith(matchingText2, patternAsCharP, patternSize));
  ASSERT_TRUE(endsWith(matchingText2, patternAsCharP));

  ASSERT_FALSE(endsWith(mismatchText1, patternAsString));
  ASSERT_FALSE(endsWith(mismatchText1, patternAsCharP, patternSize));
  ASSERT_FALSE(endsWith(mismatchText1, patternAsCharP));

  ASSERT_FALSE(endsWith(mismatchText2, patternAsString));
  ASSERT_FALSE(endsWith(mismatchText2, patternAsCharP, patternSize));
  ASSERT_FALSE(endsWith(mismatchText2, patternAsCharP));

  ASSERT_FALSE(endsWith(mismatchText3, patternAsString));
  ASSERT_FALSE(endsWith(mismatchText3, patternAsCharP, patternSize));
  ASSERT_FALSE(endsWith(mismatchText3, patternAsCharP));
}

TEST(StringUtilsTest, getLowercaseUtf8) {
  setlocale(LC_CTYPE, "");
  ASSERT_EQ("schindler's list", getLowercaseUtf8("Schindler's List"));
  ASSERT_EQ("#+-_foo__bar++", getLowercaseUtf8("#+-_foo__Bar++"));
  ASSERT_EQ("fôéßaéé", getLowercaseUtf8("FÔÉßaéÉ"));
}

TEST(StringUtilsTest, firstCharToUpperUtf8) {
  setlocale(LC_CTYPE, "");
  ASSERT_EQ("Foo", firstCharToUpperUtf8("foo"));
  ASSERT_EQ("Foo", firstCharToUpperUtf8("Foo"));
  ASSERT_EQ("#foo", firstCharToUpperUtf8("#foo"));
  // The next test has to be deactivated due to different versions of libs
  // that use different specifications for unicode chars.
  // In one, the capital ß exists, in others it doesn't.
  //  ASSERT_EQ("ẞfoo", firstCharToUpperUtf8("ßfoo"));
  ASSERT_EQ("Éfoo", firstCharToUpperUtf8("éfoo"));
  ASSERT_EQ("Éfoo", firstCharToUpperUtf8("Éfoo"));
}

TEST(StringUtilsTest, toJson) {
  setlocale(LC_CTYPE, "");
  ASSERT_EQ("\"nothing special\"", toJson("nothing special"));
  // We can pass an optional without value to get JSON null
  ASSERT_EQ("null", toJson(std::nullopt));
  ASSERT_EQ("\"2 byte unicode: äöüß\"", toJson("2 byte unicode: äöüß"));
  ASSERT_EQ("\"Chinese: 漢字\"", toJson("Chinese: 漢字"));

  ASSERT_EQ("\"embedded \\\"quotes\\\" should work\"",
            toJson("embedded \"quotes\" should work"));
  ASSERT_EQ("\"quoteception: \\\"\\\\\\\"\\\"\"",
            toJson("quoteception: \"\\\"\""));
  ASSERT_EQ("\"tabs\\tare nice\"", toJson("tabs\tare nice"));
  ASSERT_EQ("\"multi\\nline\"", toJson("multi\nline"));
}

TEST(StringUtilsTest, split) {
  string s1 = "this\tis\tit";
  string s2 = "thisisit";
  string s3 = "this is it";

  auto v1 = split(s1, '\t');
  ASSERT_EQ(size_t(3), v1.size());
  ASSERT_EQ("this", v1[0]);
  ASSERT_EQ("is", v1[1]);
  ASSERT_EQ("it", v1[2]);
  auto v2 = split(s2, '\t');
  ASSERT_EQ(size_t(1), v2.size());
  auto v3 = split(s3, '\t');
  ASSERT_EQ(size_t(1), v3.size());
  auto v4 = split(s3, ' ');
  ASSERT_EQ(size_t(3), v4.size());
  // Splitting at every char
  string s5 = "   ";
  auto v5 = split(s5, ' ');
  // In between all and at the start and end
  ASSERT_EQ(s5.size() + 1, v5.size());
  for (const auto& val : v5) {
    ASSERT_EQ(val, "");
  }
  // and with unicode
  string s6 = "Spaß ❤ 漢字";
  auto v6 = split(s6, ' ');
  ASSERT_EQ("Spaß", v6[0]);
  ASSERT_EQ("❤", v6[1]);
  ASSERT_EQ("漢字", v6[2]);
}

TEST(StringUtilsTest, join) {
  string s1 = "this\tis\tit";

  // With char
  auto v1 = split(s1, '\t');
  auto joined1 = join(v1, '|');
  ASSERT_EQ("this|is|it", joined1);

  // With string
  auto v2 = split(s1, '\t');
  auto joined2 = join(v2, "<SEP>");
  ASSERT_EQ("this<SEP>is<SEP>it", joined2);

  // Empty list
  std::vector<std::string> v_empty = {};
  auto joined_empty = join(v_empty, "<SEP>");
  ASSERT_EQ("", joined_empty);

  // Yay for templates, it also does (somewhat useless) math
  vector<uint16_t> v3 = {3, 4, 5, 6};
  auto joined3 = join(v3, 2);
  ASSERT_EQ((3 + 2 + 4 + 2 + 5 + 2 + 6), joined3);
}

TEST(StringUtilsTest, splitAny) {
  string s1 = "this\tis\tit";
  string s2 = "thisisit";
  string s3 = "this is it";

  auto v1 = splitAny(s1, "\t");
  ASSERT_EQ(size_t(3), v1.size());
  ASSERT_EQ("this", v1[0]);
  ASSERT_EQ("is", v1[1]);
  ASSERT_EQ("it", v1[2]);
  auto v2 = splitAny(s2, "\t");
  ASSERT_EQ(size_t(1), v2.size());
  auto v3 = splitAny(s3, "\t");
  ASSERT_EQ(size_t(1), v3.size());
  auto v4 = splitAny(s3, " ");
  ASSERT_EQ(size_t(3), v4.size());

  auto v5 = splitAny(s1, "i\t");
  ASSERT_EQ(size_t(4), v5.size());
  ASSERT_EQ("th", v5[0]);
  ASSERT_EQ("s", v5[1]);
  ASSERT_EQ("s", v5[2]);
  ASSERT_EQ("t", v5[3]);

  auto v6 = splitAny(s1, "ih\t");
  ASSERT_EQ(size_t(4), v6.size());
  ASSERT_EQ("t", v6[0]);
  ASSERT_EQ("s", v6[1]);
  ASSERT_EQ("s", v6[2]);
  ASSERT_EQ("t", v6[3]);

  auto v7 = splitAny(s1, "sih\t");
  ASSERT_EQ(size_t(2), v7.size());
  ASSERT_EQ("t", v6[0]);
  ASSERT_EQ("t", v6[3]);

  auto v8 = splitAny(s1, "sih\tt");
  ASSERT_EQ(size_t(0), v8.size());
  // and with unicode
  string s9 = "Spaß ❤\t漢字";
  auto v9 = splitAny(s9, " \t");
  ASSERT_EQ("Spaß", v9[0]);
  ASSERT_EQ("❤", v9[1]);
  ASSERT_EQ("漢字", v9[2]);

  // and with a string specifying seperators
  string s10 = "semicolon;or,comma,separating;data";
  string seps = ",;";
  auto v10 = splitAny(s10, seps);
  ASSERT_EQ("semicolon", v10[0]);
  ASSERT_EQ("or", v10[1]);
  ASSERT_EQ("comma", v10[2]);
  ASSERT_EQ("separating", v10[3]);
  ASSERT_EQ("data", v10[4]);
}

TEST(StringUtilsTest, strip) {
  string s1("   abc  ");
  string s2("abc");
  string s3("abc  ");
  string s4("   abc");
  string s5("xxabcxx");
  string s6(" ");
  string s7("    ");
  string s8("");

  ASSERT_EQ("abc", strip(s1, ' '));
  ASSERT_EQ("abc", strip(s2, ' '));

  ASSERT_NE("abc", lstrip(s3, ' '));
  ASSERT_EQ("abc", rstrip(s3, ' '));
  ASSERT_NE("abc", rstrip(s4, ' '));

  ASSERT_NE("abc", strip(s5, ' '));
  ASSERT_EQ("abc", strip(s5, 'x'));

  ASSERT_EQ("", strip(s6, ' '));
  ASSERT_EQ("", strip(s7, ' '));
  ASSERT_EQ("", strip(s8, ' '));

  ASSERT_EQ("", rstrip(s6, ' '));
  ASSERT_EQ("", rstrip(s7, ' '));
  ASSERT_EQ("", rstrip(s8, ' '));

  ASSERT_EQ("", lstrip(s6, ' '));
  ASSERT_EQ("", lstrip(s7, ' '));
  ASSERT_EQ("", lstrip(s8, ' '));

  ASSERT_EQ("bc", strip("xxaxaxaxabcaaaxxx", "xa"));
  // And with unicode
  ASSERT_EQ("äcaaaxxx", lstrip("xxaxaxaxaäcaaaxxx", "xa"));
  ASSERT_EQ("äö", strip("xxaxaxaxaäöaaaxxx", "xa"));
  ASSERT_EQ("xxaxaxaxa♥", rstrip("xxaxaxaxa♥aaaxxx", "xa"));
}

TEST(StringUtilsTest, splitWs) {
  setlocale(LC_CTYPE, "");
  string s1 = "  this\nis\t  \nit  ";
  string s2 = "\n   \t  \n \t";
  string s3 = "thisisit";
  string s4 = "this is\nit";
  string s5 = "a";
  auto v1 = splitWs(s1);
  ASSERT_EQ(size_t(3), v1.size());
  ASSERT_EQ("this", v1[0]);
  ASSERT_EQ("is", v1[1]);
  ASSERT_EQ("it", v1[2]);

  auto v2 = splitWs(s2);
  ASSERT_EQ(size_t(0), v2.size());

  auto v3 = splitWs(s3);
  ASSERT_EQ(size_t(1), v3.size());
  ASSERT_EQ("thisisit", v3[0]);

  auto v4 = splitWs(s4);
  ASSERT_EQ(size_t(3), v4.size());
  ASSERT_EQ("this", v4[0]);
  ASSERT_EQ("is", v4[1]);
  ASSERT_EQ("it", v4[2]);

  auto v5 = splitWs(s5);
  ASSERT_EQ(size_t(1), v5.size());
  ASSERT_EQ("a", v5[0]);

  // and with unicode
  string s6 = "Spaß \t ❤ \n漢字  ";
  auto v6 = splitWs(s6);
  ASSERT_EQ("Spaß", v6[0]);
  ASSERT_EQ("❤", v6[1]);
  ASSERT_EQ("漢字", v6[2]);

  // unicode code point 224 has a second byte (160), that equals the space
  // character if the first bit is ignored
  // (which may happen when casting char to int).
  string s7 = "Test\u00e0test";
  auto v7 = splitWs(s7);
  ASSERT_EQ(1u, v7.size());
  ASSERT_EQ(s7, v7[0]);
}

TEST(StringUtilsTest, splitWsWithEscape) {
  setlocale(LC_CTYPE, "");
  // Test for splitting with a different opening and closing character
  string s1 = " a1 \t (this is() one) two (\t\na)()";
  auto v1 = splitWsWithEscape(s1, '(', ')');
  ASSERT_EQ(4u, v1.size());
  ASSERT_EQ("a1", v1[0]);
  ASSERT_EQ("(this is() one)", v1[1]);
  ASSERT_EQ("two", v1[2]);
  ASSERT_EQ("(\t\na)()", v1[3]);

  // Test for splitting with the same opening and closing character
  string s2 = " a1 \t 'this is' ' two' three '\t\na'' a'";
  auto v2 = splitWsWithEscape(s2, '\'', '\'');
  ASSERT_EQ(5u, v2.size());
  ASSERT_EQ("a1", v2[0]);
  ASSERT_EQ("'this is'", v2[1]);
  ASSERT_EQ("' two'", v2[2]);
  ASSERT_EQ("three", v2[3]);
  ASSERT_EQ("'\t\na'' a'", v2[4]);
}

}  // namespace ad_utility

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
