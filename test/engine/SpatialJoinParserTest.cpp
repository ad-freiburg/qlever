// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./SpatialJoinParserTestHelpers.h"

// _____________________________________________________________________________
namespace {

using namespace SpatialJoinParserTestHelpers;

// _____________________________________________________________________________
TEST(SpatialJoinParser, SpatialJoinTaskOperatorEq) {
  // Test equality operator of `SpatialJoinParseJob` helper struct
  auto point = ValueId::makeFromGeoPoint({1, 1});
  auto undef = ValueId::makeUndefined();

  SpatialJoinParseJob job1{point, 5, true, ""};
  SpatialJoinParseJob job1Copy = job1;
  SpatialJoinParseJob job2{point, 7, true, ""};
  SpatialJoinParseJob job3{point, 5, false, ""};
  SpatialJoinParseJob job4{undef, 5, true, ""};

  ASSERT_TRUE(job1 == job1);
  ASSERT_TRUE(job2 == job2);
  ASSERT_TRUE(job3 == job3);
  ASSERT_TRUE(job4 == job4);
  ASSERT_TRUE(job1 == job1Copy);

  ASSERT_FALSE(job1 == job2);
  ASSERT_FALSE(job1 == job3);
  ASSERT_FALSE(job1 == job4);
  ASSERT_FALSE(job2 == job3);
  ASSERT_FALSE(job2 == job4);
  ASSERT_FALSE(job3 == job4);
}

// _____________________________________________________________________________
TEST(SpatialJoinParser, AddValueIdToQueue) {
  ;  // TODO
}

}  // namespace
