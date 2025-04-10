// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "index/KeyOrder.h"

using namespace qlever;
using namespace testing;
// _____________________________________________________________________________
TEST(KeyOrder, Constructor) {
  AD_EXPECT_THROW_WITH_MESSAGE(KeyOrder(0, 1, 2, 4), HasSubstr("out of range"));
  AD_EXPECT_THROW_WITH_MESSAGE(KeyOrder(0, 1, 2, 2), HasSubstr("not unique"));
  auto keyOrder = KeyOrder{3, 0, 1, 2};
  EXPECT_THAT(keyOrder.keys(), ElementsAre(3, 0, 1, 2));
}

// _____________________________________________________________________________
TEST(KeyOrder, Permute) {
  KeyOrder keyOrder{2, 3, 1, 0};
  std::array a{0, 1, 2, 3};
  EXPECT_THAT(keyOrder.permuteTuple(a), ElementsAre(2, 3, 1, 0));

  std::array b{0, 1, 2};
  // Not supported, as the permutation doesn't have the graph in the last
  // column.
  EXPECT_ANY_THROW(keyOrder.permuteTriple(b));

  keyOrder = KeyOrder{2, 0, 1, 3};
  EXPECT_THAT(keyOrder.permuteTriple(b), ElementsAre(2, 0, 1));
}
