// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "index/KeyOrder.h"

// _____________________________________________________________________________
TEST(KeyOrder, keyOrder) {
  using namespace qlever;
  AD_EXPECT_THROW_WITH_MESSAGE(KeyOrder(0, 1, 2, 4),
                               ::testing::HasSubstr("out of range"));
  AD_EXPECT_THROW_WITH_MESSAGE(KeyOrder(0, 1, 2, 2),
                               ::testing::HasSubstr("not unique"));
  KeyOrder keyOrder{3, 0, 1, 2};
  EXPECT_THAT(keyOrder.keys(), ::testing::ElementsAre(3, 0, 1, 2));
}
