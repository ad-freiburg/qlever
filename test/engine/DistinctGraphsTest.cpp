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
#include "../util/OperationTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "engine/DistinctGraphs.h"
#include "engine/VariableToColumnMap.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "gmock/gmock.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/TripleComponentConversions.h"
#include "rdfTypes/Variable.h"

// _____________________________________________________________________________
TEST(DistinctGraphs, getChildren) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_THAT(dg.getChildren(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getDescriptor) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getDescriptor(), "Distinct Graphs");
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getResultWidth) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getResultWidth(), 1);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getMultiplicity) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getMultiplicity(0), 1.0f);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, knownEmptyResult) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_FALSE(dg.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, isDeterministic) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_TRUE(dg.isDeterministic());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getCostEstimate) {
  {
    auto* qec = ad_utility::testing::getQec(
        "<a> <p1> <b> . <a> <p2> <c> . <b> <p1> <c> .");
    DistinctGraphs dg{qec, Variable{"?g"}};

    EXPECT_EQ(dg.getCostEstimate(), 3u);
  }
  {
    auto* qec = ad_utility::testing::getQec("");
    DistinctGraphs dg{qec, Variable{"?g"}};

    EXPECT_EQ(dg.getCostEstimate(), 0u);
  }
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getSizeEstimateDefault) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getSizeEstimate(), MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getSizeEstimateComputed) {
  ad_utility::testing::TestIndexConfig config{
      "<a> <p> <b> <g1> . <c> <p> <d> <g2> . <e> <p> <f> <g3> ."};
  config.indexType = qlever::Filetype::NQuad;
  auto* qec = ad_utility::testing::getQec(config);
  DistinctGraphs dg{qec, Variable{"?g"}};

  dg.getResult();
  EXPECT_EQ(dg.getSizeEstimate(), 3u);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getCacheKey) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_EQ(dg.getCacheKey(), "DistinctGraphs");
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getResultSortedOn) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  EXPECT_THAT(dg.getResultSortedOn(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, clone) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  auto clone = dg.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(dg, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), dg.getDescriptor());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, computeVariableToColumnMap) {
  auto* qec = ad_utility::testing::getQec();
  DistinctGraphs dg{qec, Variable{"?g"}};

  VariableToColumnMap expected{{Variable{"?g"}, makeAlwaysDefinedColumn(0)}};
  EXPECT_EQ(dg.getExternallyVisibleVariableColumns(), expected);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, computeResultExcludesDefaultGraphByDefault) {
  auto* qec = ad_utility::testing::getQec("<a> <p1> <b> . <a> <p2> <c> .");
  DistinctGraphs dg{qec, Variable{"?g"}};

  auto result = dg.getResult();
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTableView().size(), 0u);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, computeResultReturnsDistinctGraphIds) {
  ad_utility::testing::TestIndexConfig config{
      "<a> <p> <b> <g1> . <a> <p> <c> <g2> . <b> <p> <c> <g1> ."};
  config.indexType = qlever::Filetype::NQuad;
  auto* qec = ad_utility::testing::getQec(config);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  DistinctGraphs dg{qec, Variable{"?g"}};
  auto result = dg.getResult();
  ASSERT_TRUE(result->isFullyMaterialized());

  auto column = result->idTableView().getColumn(0);
  EXPECT_THAT(std::vector<Id>(column.begin(), column.end()),
              ::testing::UnorderedElementsAre(getId("<g1>"), getId("<g2>")));
}

// _____________________________________________________________________________
TEST(DistinctGraphs,
     computeResultIncludesDefaultGraphWhenRuntimeParameterIsSet) {
  auto* qec = ad_utility::testing::getQec("<x> <p> <y> .");
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::treatDefaultGraphAsNamedGraph_>(true);

  DistinctGraphs dg{qec, Variable{"?g"}};
  auto result = dg.getResult();
  ASSERT_TRUE(result->isFullyMaterialized());

  auto defaultGraphId = toValueId(
      TripleComponent{
          ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)},
      qec->getIndex().getImpl());
  auto column = result->idTableView().getColumn(0);
  EXPECT_THAT(std::vector<Id>(column.begin(), column.end()),
              ::testing::ElementsAre(defaultGraphId.value()));
}
