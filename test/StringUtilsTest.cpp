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
TEST(StringUtilsTest, getLowercaseUtf8) {
  setlocale(LC_CTYPE, "");
  ASSERT_EQ("schindler's list", getLowercaseUtf8("Schindler's List"));
  ASSERT_EQ("#+-_foo__bar++", getLowercaseUtf8("#+-_foo__Bar++"));
  ASSERT_EQ("fôéßaéé", getLowercaseUtf8("FÔÉßaéÉ"));
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

  ASSERT_EQ("bc", strip("xxaxaxaxabcaaaxxx", "xa"));
  // And with unicode
  ASSERT_EQ("äö", strip("xxaxaxaxaäöaaaxxx", "xa"));
  ASSERT_EQ("xxaxaxaxa♥", rstrip("xxaxaxaxa♥aaaxxx", "xa"));
}

}  // namespace ad_utility

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
