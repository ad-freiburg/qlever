// Copyright 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "global/IdTriple.h"

using namespace ad_utility::testing;

TEST(IdTripleTest, constructors) {
  const std::array<Id, 3> undefIds{Id::makeUndefined(), Id::makeUndefined(),
                                   Id::makeUndefined()};
  const std::array<Id, 3> ids{Id::makeFromInt(42), VocabId(10),
                              Id::makeFromBool(false)};

  {
    auto idTriple = IdTriple();
    EXPECT_THAT(idTriple, AD_FIELD(IdTriple<>, ids_,
                                   testing::ElementsAreArray(undefIds)));
  }

  {
    auto idTriple = IdTriple(ids);
    EXPECT_THAT(idTriple,
                AD_FIELD(IdTriple<>, ids_, testing::ElementsAreArray(ids)));
  }

  {
    auto permutedTriple =
        CompressedBlockMetadata::PermutedTriple{ids[0], ids[1], ids[2]};
    auto idTriple = IdTriple(permutedTriple);
    EXPECT_THAT(idTriple,
                AD_FIELD(IdTriple<>, ids_, testing::ElementsAreArray(ids)));
  }
}

TEST(IdTripleTest, permute) {
  std::array<Id, 3> ids{VocabId(0), VocabId(1), VocabId(2)};
  // Without a payload
  {
    IdTriple<0> idTriple{ids};
    EXPECT_THAT(idTriple.permute({1, 0, 2}),
                testing::Eq(IdTriple{
                    std::array<Id, 3>{VocabId(1), VocabId(0), VocabId(2)}}));
  }

  // With a payload
  {
    IdTriple<2> idTriple(ids, {IntId(10), IntId(5)});
    EXPECT_THAT(idTriple.permute({1, 0, 2}),
                testing::Eq(IdTriple<2>({VocabId(1), VocabId(0), VocabId(2)},
                                        {IntId(10), IntId(5)})));
  }
}
