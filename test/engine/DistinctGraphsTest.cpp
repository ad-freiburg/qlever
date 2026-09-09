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

namespace {

using ad_utility::testing::TestIndexConfig;

// Create a `DistinctGraphs` operation for the graph variable `?g` on a test
// index built from `config`. Without an argument, the default test index is
// used. The `QueryExecutionContext` and the `Index` of the operation remain
// accessible via `getExecutionContext()` and `getIndex()`.
DistinctGraphs makeDistinctGraphs(TestIndexConfig config = TestIndexConfig{}) {
  return DistinctGraphs{ad_utility::testing::getQec(std::move(config)),
                        Variable{"?g"}};
}

// Same as `makeDistinctGraphs`, but the index is built from NQuad input, which
// is required for an index that actually contains named graphs.
DistinctGraphs makeDistinctGraphsFromQuads(std::string nquads) {
  TestIndexConfig config{std::move(nquads)};
  config.indexType = qlever::Filetype::NQuad;
  return makeDistinctGraphs(std::move(config));
}

}  // namespace

// _____________________________________________________________________________
TEST(DistinctGraphs, getChildren) {
  auto dg = makeDistinctGraphs();

  EXPECT_THAT(dg.getChildren(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getDescriptor) {
  auto dg = makeDistinctGraphs();

  EXPECT_EQ(dg.getDescriptor(), "Distinct Graphs");
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getResultWidth) {
  auto dg = makeDistinctGraphs();

  EXPECT_EQ(dg.getResultWidth(), 1);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getMultiplicity) {
  auto dg = makeDistinctGraphs();

  EXPECT_EQ(dg.getMultiplicity(0), 1.0f);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, knownEmptyResult) {
  auto dg = makeDistinctGraphs();

  EXPECT_FALSE(dg.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, isDeterministic) {
  auto dg = makeDistinctGraphs();

  EXPECT_TRUE(dg.isDeterministic());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getCostEstimate) {
  {
    auto dg = makeDistinctGraphs(
        TestIndexConfig{"<a> <p1> <b> . <a> <p2> <c> . <b> <p1> <c> ."});

    EXPECT_EQ(dg.getCostEstimate(), 3u);
  }
  {
    auto dg = makeDistinctGraphs(TestIndexConfig{""});

    EXPECT_EQ(dg.getCostEstimate(), 0u);
  }
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getSizeEstimateComputed) {
  auto dg = makeDistinctGraphsFromQuads(
      "<a> <p> <b> <g1> . <c> <p> <d> <g2> . <e> <p> <f> <g3> .");

  dg.getResult();
  EXPECT_EQ(dg.getSizeEstimate(), 3u);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getCacheKey) {
  auto dg = makeDistinctGraphs();

  EXPECT_EQ(dg.getCacheKey(), "DistinctGraphs");
}

// _____________________________________________________________________________
TEST(DistinctGraphs, getResultSortedOn) {
  auto dg = makeDistinctGraphs();

  EXPECT_THAT(dg.getResultSortedOn(), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, clone) {
  auto dg = makeDistinctGraphs();

  auto clone = dg.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(dg, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), dg.getDescriptor());
}

// _____________________________________________________________________________
TEST(DistinctGraphs, computeVariableToColumnMap) {
  auto dg = makeDistinctGraphs();

  VariableToColumnMap expected{{Variable{"?g"}, makeAlwaysDefinedColumn(0)}};
  EXPECT_EQ(dg.getExternallyVisibleVariableColumns(), expected);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, computeResultExcludesDefaultGraphByDefault) {
  auto dg =
      makeDistinctGraphs(TestIndexConfig{"<a> <p1> <b> . <a> <p2> <c> ."});

  auto result = dg.getResult();
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTableView().size(), 0u);
}

// _____________________________________________________________________________
TEST(DistinctGraphs, computeResultReturnsDistinctGraphIds) {
  auto dg = makeDistinctGraphsFromQuads(
      "<a> <p> <b> <g1> . <a> <p> <c> <g2> . <b> <p> <c> <g1> .");
  auto getId = ad_utility::testing::makeGetId(dg.getIndex());

  auto result = dg.getResult();
  ASSERT_TRUE(result->isFullyMaterialized());

  auto column = result->idTableView().getColumn(0);
  EXPECT_THAT(std::vector<Id>(column.begin(), column.end()),
              ::testing::UnorderedElementsAre(getId("<g1>"), getId("<g2>")));
}

// _____________________________________________________________________________
TEST(DistinctGraphs,
     computeResultIncludesDefaultGraphWhenRuntimeParameterIsSet) {
  auto dg = makeDistinctGraphs(TestIndexConfig{"<x> <p> <y> ."});
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::treatDefaultGraphAsNamedGraph_>(true);

  auto result = dg.getResult();
  ASSERT_TRUE(result->isFullyMaterialized());

  auto defaultGraphId = toValueId(
      TripleComponent{
          ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)},
      dg.getIndex().getImpl());
  auto column = result->idTableView().getColumn(0);
  EXPECT_THAT(std::vector<Id>(column.begin(), column.end()),
              ::testing::ElementsAre(defaultGraphId.value()));
}
