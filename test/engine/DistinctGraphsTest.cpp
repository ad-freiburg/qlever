// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Mete Tolga Gonultas <mg885@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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
