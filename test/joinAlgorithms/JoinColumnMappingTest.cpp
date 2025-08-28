// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

#include <gmock/gmock.h>

#include "util/JoinAlgorithms/JoinColumnMapping.h"

using ad_utility::JoinColumnMapping;
using namespace ::testing;

// _____________________________________________________________________________
class JoinColumnMappingTest : public ::testing::TestWithParam<bool> {};

// _____________________________________________________________________________
TEST_P(JoinColumnMappingTest, SingleJoinColAtBeginningOfLeft) {
  bool keepJoinCols = GetParam();
  JoinColumnMapping m{{{0, 2}}, 2, 3, keepJoinCols};
  EXPECT_THAT(m.jcsLeft(), ElementsAre(0));
  EXPECT_THAT(m.jcsRight(), ElementsAre(2));
  EXPECT_THAT(m.permutationLeft(), ElementsAre(0, 1));
  EXPECT_THAT(m.permutationRight(), ElementsAre(2, 0, 1));
  if (keepJoinCols) {
    EXPECT_THAT(m.permutationResult(), ElementsAre(0, 1, 2, 3));
  } else {
    EXPECT_THAT(m.permutationResult(), ElementsAre(0, 1, 2));
  }
}

// _____________________________________________________________________________
TEST_P(JoinColumnMappingTest, SingleJoinColInMiddleOfLeft) {
  bool keepJoinCols = GetParam();
  JoinColumnMapping m{{{1, 0}}, 3, 2, keepJoinCols};
  EXPECT_THAT(m.jcsLeft(), ElementsAre(1));
  EXPECT_THAT(m.jcsRight(), ElementsAre(0));
  EXPECT_THAT(m.permutationLeft(), ElementsAre(1, 0, 2));
  EXPECT_THAT(m.permutationRight(), ElementsAre(0, 1));
  if (keepJoinCols) {
    EXPECT_THAT(m.permutationResult(), ElementsAre(1, 0, 2, 3));
  } else {
    EXPECT_THAT(m.permutationResult(), ElementsAre(0, 1, 2));
  }
}

// _____________________________________________________________________________
TEST_P(JoinColumnMappingTest, MultipleJoinCols) {
  bool keepJoinCols = GetParam();
  JoinColumnMapping m{{{2, 0}, {1, 3}}, 3, 4, keepJoinCols};
  EXPECT_THAT(m.jcsLeft(), ElementsAre(2, 1));
  EXPECT_THAT(m.jcsRight(), ElementsAre(0, 3));
  EXPECT_THAT(m.permutationLeft(), ElementsAre(2, 1, 0));
  EXPECT_THAT(m.permutationRight(), ElementsAre(0, 3, 1, 2));
  if (keepJoinCols) {
    EXPECT_THAT(m.permutationResult(), ElementsAre(2, 1, 0, 3, 4));
  } else {
    EXPECT_THAT(m.permutationResult(), ElementsAre(0, 1, 2));
  }
}

// Instantiate the test suite with true and false
INSTANTIATE_TEST_SUITE_P(JoinColumnMapping,                // Instance name
                         JoinColumnMappingTest,            // Test suite name
                         ::testing::Values(true, false));  // Parameters
