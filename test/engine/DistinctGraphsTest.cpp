//   Copyright 2026, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Mete Tolga Gonultas <mg885@email.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/IndexTestHelpers.h"
#include "engine/DistinctGraphs.h"
#include "gmock/gmock.h"
#include "rdfTypes/Variable.h"

// _____________________________________________________________________________
TEST(DistinctGraphs, getChildren){
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_THAT(dg.getChildren(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getDescriptor){
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getDescriptor(), "Distinct Graphs");
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getResultWidth){
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getResultWidth(), 1);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getMultiplicity){
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getMultiplicity(0), 1.0f);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, knownEmptyResult){
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};
  
  EXPECT_FALSE(dg.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, isDeterministic){
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};
  
  EXPECT_TRUE(dg.isDeterministic());  
}
