//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>


#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "util/JoinAlgorithms.h"
#include "global/Id.h"
#include "./util/IdTestHelpers.h"

using namespace ad_utility;

namespace {
const Id U = Id::makeUndefined();
auto V = ad_utility::testing::VocabId;
}

TEST(JoinAlgorithms, findSmallerUndefRangesForRowsWithoutUndef) {
  using OneCol = std::array<Id, 1>;
  std::vector<OneCol> oneCol{{U}, {U}, {V(3)}, {V(7)}, {V(8)}};
  // TODO<joka921> Test here.

  std::vector<decltype(oneCol.begin())> iterators;
  OneCol c {V(3)};
  for (auto it : findSmallerUndefRangesForRowsWithoutUndef<OneCol>(c, oneCol.begin(), oneCol.end())) {
    iterators.push_back(it);
  }

  EXPECT_THAT(iterators, ::testing::ElementsAre(oneCol.begin(), oneCol.begin() + 1));

  using TwoCol = std::array<Id, 2>;
  std::vector<TwoCol> twoCols{{U, U}, {U, V(1)}, {U, V(2)}, {U, V(3)}, {U, V(3)}, {U, V(19)}, {V(1), U}, {V(3), U}, {V(3), V(3)}, {V(7), V(12)}, {V(8), U}};
  // TODO<joka921> Test here.

  std::vector<int64_t> iteratorstwo;
  TwoCol c2 {V(3), V(19)};
  for (auto it : findSmallerUndefRangesForRowsWithoutUndef<TwoCol>(c2, twoCols.begin(), twoCols.end())) {
    iteratorstwo.push_back(it - twoCols.begin());
  }

  EXPECT_THAT(iteratorstwo, ::testing::ElementsAre(0, 5, 7));

}
