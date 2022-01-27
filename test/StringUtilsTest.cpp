// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

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
}  // namespace ad_utility
