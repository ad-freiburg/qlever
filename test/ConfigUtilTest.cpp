// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (June of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/json.h"

TEST(ConfigUtilTest, isNameInShortHand) {
  ASSERT_TRUE(ad_utility::isNameInShortHand("name077173dapiwl--__-d--_--allj"));
  ASSERT_FALSE(ad_utility::isNameInShortHand("name 3"));
  ASSERT_FALSE(ad_utility::isNameInShortHand("name\t3"));
  ASSERT_FALSE(ad_utility::isNameInShortHand("name\n3"));
  ASSERT_FALSE(ad_utility::isNameInShortHand(""));
}
