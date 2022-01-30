// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/util/StringUtils.h"

using ad_utility::getLowercaseUtf8;

TEST(StringUtilsTest, getLowercaseUtf8) {
  setlocale(LC_CTYPE, "");
  ASSERT_EQ("schindler's list", getLowercaseUtf8("Schindler's List"));
  ASSERT_EQ("#+-_foo__bar++", getLowercaseUtf8("#+-_foo__Bar++"));
  ASSERT_EQ("fôéßaéé", getLowercaseUtf8("FÔÉßaéÉ"));
}
