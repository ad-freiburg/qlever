//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/EmptyPath.h"
#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "engine/QueryExecutionTree.h"
#include "index/DeltaTriples.h"

using ad_utility::testing::getQec;
using ::testing::ElementsAreArray;
using ::testing::UnorderedElementsAreArray;
using Graphs = EmptyPath::Graphs;
using Vars = std::vector<std::optional<Variable>>;

namespace {
// The knowledge graph that is used by most of the tests below. Note that the
// subjects and the objects overlap in `<a>` and `<b>`, that `<c>` only occurs
// as a subject, that `<z>` only occurs as an object, and that `<p>` occurs
// neither as a subject nor as an object.
constexpr std::string_view kg =
    "<a> <p> <b> . <b> <p> <a> . <c> <p> <z> . <a> <p> <z> .";

// The same knowledge graph, but distributed over two named graphs. `<a>` occurs
// in both graphs, `<b>` only in `<g1>`, and `<c>` and `<z>` only in `<g2>`.
constexpr std::string_view nquads =
    "<a> <p> <b> <g1> . <b> <p> <a> <g1> . <c> <p> <z> <g2> . "
    "<a> <p> <z> <g2> .";

// Create a `QueryExecutionContext` for one of the knowledge graphs above.
QueryExecutionContext* makeQec(std::string_view turtleInput,
                               bool isQuads = false) {
  ad_utility::testing::TestIndexConfig config;
  config.turtleInput = std::string{turtleInput};
  config.indexType =
      isQuads ? qlever::Filetype::NQuad : qlever::Filetype::Turtle;
  auto* qec = getQec(std::move(config));
  qec->clearCacheUnpinnedOnly();
  return qec;
}

// Return a `GraphFilter` that only allows the single graph with the given IRI.
Graphs singleGraph(std::string_view iri) {
  return Graphs::Whitelist({TripleComponent{
      TripleComponent::Iri::fromIriref(absl::StrCat("<", iri, ">"))}});
}

// Compute the result of `emptyPath` and return it as a single `IdTable`. The
// `EmptyPath` operation only supports lazy computation, so the tables that it
// yields have to be concatenated.
IdTable computeResult(EmptyPath& emptyPath) {
  auto result = emptyPath.computeResultOnlyForTesting(true);
  EXPECT_FALSE(result.isFullyMaterialized());
  return aggregateTables(result.idTables(), emptyPath.getResultWidth()).first;
}

// Create an `EmptyPath` that checks the values in the `joinColumn` of `input`
// (interpreted as a table with the given `vars`) against the knowledge graph.
EmptyPath makeExistenceCheck(QueryExecutionContext* qec,
                             std::optional<Variable> graphVariable,
                             IdTable input, Vars vars,
                             ColumnIndex joinColumn = 0,
                             bool isSortedOnJoinColumn = false) {
  auto sortedColumns = isSortedOnJoinColumn
                           ? std::vector<ColumnIndex>{joinColumn}
                           : std::vector<ColumnIndex>{};
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input), std::move(vars), false, std::move(sortedColumns));
  return EmptyPath{qec,           Variable{"?x"},   Graphs::All(),
                   graphVariable, std::move(child), joinColumn};
}
}  // namespace

// _____________________________________________________________________________
TEST(EmptyPath, allEntitiesAreReturnedSortedAndDistinct) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EmptyPath emptyPath{qec, Variable{"?x"}, Graphs::All(), std::nullopt};

  EXPECT_EQ(emptyPath.getResultWidth(), 1);
  EXPECT_THAT(emptyPath.getChildren(), ::testing::IsEmpty());
  EXPECT_THAT(emptyPath.getResultSortedOn(), ElementsAreArray({0}));
  EXPECT_FALSE(emptyPath.knownEmptyResult());

  std::vector<Id> expected{getId("<a>"), getId("<b>"), getId("<c>"),
                           getId("<z>")};
  ql::ranges::sort(expected);
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result.getColumn(0), ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(EmptyPath, allEntitiesAreFilteredByTheActiveGraphs) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EmptyPath emptyPath{qec, Variable{"?x"}, singleGraph("g1"), std::nullopt};

  std::vector<Id> expected{getId("<a>"), getId("<b>")};
  ql::ranges::sort(expected);
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result.getColumn(0), ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(EmptyPath, allEntitiesWithTheirGraphs) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EmptyPath emptyPath{qec, Variable{"?x"}, Graphs::All(), Variable{"?g"}};

  EXPECT_EQ(emptyPath.getResultWidth(), 2);
  EXPECT_THAT(emptyPath.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAre(
                  ::testing::Pair(Variable{"?x"}, makeAlwaysDefinedColumn(0)),
                  ::testing::Pair(Variable{"?g"}, makeAlwaysDefinedColumn(1))));

  auto result = computeResult(emptyPath);
  EXPECT_THAT(result, UnorderedElementsAreArray(makeIdTableFromVector(
                          {{getId("<a>"), getId("<g1>")},
                           {getId("<a>"), getId("<g2>")},
                           {getId("<b>"), getId("<g1>")},
                           {getId("<c>"), getId("<g2>")},
                           {getId("<z>"), getId("<g2>")}})));
  // The result is sorted by the entity and then by the graph.
  EXPECT_TRUE(
      ql::ranges::is_sorted(result, [](const auto& row1, const auto& row2) {
        return std::tie(row1[0], row1[1]) < std::tie(row2[0], row2[1]);
      }));
}

// _____________________________________________________________________________
TEST(EmptyPath, existenceCheckFiltersValuesThatAreNotInTheKnowledgeGraph) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto emptyPath = makeExistenceCheck(
      qec, std::nullopt,
      makeIdTableFromVector({{getId("<a>")}, {getId("<p>")}, {getId("<z>")}}),
      {Variable{"?x"}});

  EXPECT_EQ(emptyPath.getResultWidth(), 1);
  EXPECT_THAT(emptyPath.getChildren(), ::testing::SizeIs(1));

  // `<p>` only occurs as a predicate, so it is filtered out.
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result.getColumn(0),
              UnorderedElementsAreArray({getId("<a>"), getId("<z>")}));
}

// _____________________________________________________________________________
TEST(EmptyPath, existenceCheckIsFilteredByTheActiveGraphs) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{getId("<a>")}, {getId("<c>")}}),
      Vars{Variable{"?x"}});
  EmptyPath emptyPath{qec,          Variable{"?x"},   singleGraph("g1"),
                      std::nullopt, std::move(child), 0};

  // `<c>` only occurs in `<g2>`.
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result.getColumn(0), ElementsAreArray({getId("<a>")}));
}

// _____________________________________________________________________________
TEST(EmptyPath, existenceCheckCarriesOverPayloadColumns) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto emptyPath = makeExistenceCheck(
      qec, std::nullopt,
      makeIdTableFromVector({{getId("<a>"), getId("<p>"), getId("<b>")},
                             {getId("<b>"), getId("<p>"), getId("<a>")}}),
      {Variable{"?y"}, Variable{"?x"}, Variable{"?z"}}, 1);

  // The join column is dropped, the other two columns are carried over in their
  // original order.
  EXPECT_EQ(emptyPath.getResultWidth(), 3);
  EXPECT_THAT(emptyPath.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAre(
                  ::testing::Pair(Variable{"?x"}, makeAlwaysDefinedColumn(0)),
                  ::testing::Pair(Variable{"?y"}, makeAlwaysDefinedColumn(1)),
                  ::testing::Pair(Variable{"?z"}, makeAlwaysDefinedColumn(2))));

  // `<p>` is not part of the knowledge graph, so nothing survives the check.
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result, ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(EmptyPath, existenceCheckExpandsUndefValues) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto emptyPath = makeExistenceCheck(
      qec, std::nullopt,
      makeIdTableFromVector(
          {{getId("<a>"), getId("<p>")}, {Id::makeUndefined(), getId("<p>")}}),
      {Variable{"?x"}, Variable{"?y"}});
  // The join column might contain UNDEF values, so the sort order is not
  // preserved.
  EXPECT_THAT(emptyPath.getResultSortedOn(), ::testing::IsEmpty());

  // The UNDEF value matches every entity of the knowledge graph, and the
  // payload column is duplicated for each of these matches.
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result, UnorderedElementsAreArray(makeIdTableFromVector(
                          {{getId("<a>"), getId("<p>")},
                           {getId("<a>"), getId("<p>")},
                           {getId("<b>"), getId("<p>")},
                           {getId("<c>"), getId("<p>")},
                           {getId("<z>"), getId("<p>")}})));
}

// _____________________________________________________________________________
TEST(EmptyPath, existenceCheckAddsTheGraphColumn) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto emptyPath =
      makeExistenceCheck(qec, Variable{"?g"},
                         makeIdTableFromVector({{getId("<a>"), getId("<p>")},
                                                {getId("<c>"), getId("<p>")}}),
                         {Variable{"?x"}, Variable{"?y"}});

  EXPECT_EQ(emptyPath.getResultWidth(), 3);
  // `<a>` occurs in both graphs, so it is expanded to two rows, and the payload
  // column is duplicated accordingly.
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result, UnorderedElementsAreArray(makeIdTableFromVector(
                          {{getId("<a>"), getId("<g1>"), getId("<p>")},
                           {getId("<a>"), getId("<g2>"), getId("<p>")},
                           {getId("<c>"), getId("<g2>"), getId("<p>")}})));
}

// _____________________________________________________________________________
TEST(EmptyPath, undefValuesAreExpandedWithTheGraphColumnOfTheChild) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto emptyPath = makeExistenceCheck(
      qec, Variable{"?g"},
      makeIdTableFromVector({{getId("<a>"), getId("<g2>")},
                             {Id::makeUndefined(), getId("<g1>")},
                             {Id::makeUndefined(), Id::makeUndefined()}}),
      {Variable{"?x"}, Variable{"?g"}});

  // The first row is a regular existence check. The second row matches every
  // entity of `<g1>`, and the third row (which has an UNDEF graph as well)
  // matches every pair of entity and graph.
  auto result = computeResult(emptyPath);
  EXPECT_THAT(result, UnorderedElementsAreArray(makeIdTableFromVector(
                          {{getId("<a>"), getId("<g2>")},
                           {getId("<a>"), getId("<g1>")},
                           {getId("<b>"), getId("<g1>")},
                           {getId("<a>"), getId("<g1>")},
                           {getId("<a>"), getId("<g2>")},
                           {getId("<b>"), getId("<g1>")},
                           {getId("<c>"), getId("<g2>")},
                           {getId("<z>"), getId("<g2>")}})));
}

// _____________________________________________________________________________
TEST(EmptyPath, existenceCheckChecksPairsOfValueAndGraph) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto emptyPath = makeExistenceCheck(
      qec, Variable{"?g"},
      makeIdTableFromVector({{getId("<a>"), getId("<g1>")},
                             {getId("<b>"), getId("<g2>")},
                             {getId("<c>"), Id::makeUndefined()}}),
      {Variable{"?x"}, Variable{"?g"}});

  // The graph column of the child is consumed instead of being carried over.
  EXPECT_EQ(emptyPath.getResultWidth(), 2);
  // `<b>` doesn't occur in `<g2>`, so it is filtered out, and the UNDEF graph
  // of `<c>` is expanded to the graphs that `<c>` actually occurs in.
  auto result = computeResult(emptyPath);
  EXPECT_THAT(
      result,
      UnorderedElementsAreArray(makeIdTableFromVector(
          {{getId("<a>"), getId("<g1>")}, {getId("<c>"), getId("<g2>")}})));
}

// _____________________________________________________________________________
TEST(EmptyPath, sortednessIsPreservedForSortedChildrenWithoutUndef) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  {
    auto emptyPath = makeExistenceCheck(qec, std::nullopt,
                                        makeIdTableFromVector({{getId("<a>")}}),
                                        {Variable{"?x"}}, 0, true);
    EXPECT_THAT(emptyPath.getResultSortedOn(), ElementsAreArray({0}));
  }
  {
    // The child is sorted, but not on the join column.
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{getId("<a>"), getId("<b>")}}),
        Vars{Variable{"?x"}, Variable{"?y"}}, false,
        std::vector<ColumnIndex>{1});
    EmptyPath emptyPath{qec,          Variable{"?x"},   Graphs::All(),
                        std::nullopt, std::move(child), 0};
    EXPECT_THAT(emptyPath.getResultSortedOn(), ::testing::IsEmpty());
  }
  {
    // The child is sorted on the join column, but that column might contain
    // UNDEF values, which are expanded at the very end and hence break the
    // sort order.
    auto emptyPath = makeExistenceCheck(
        qec, std::nullopt,
        makeIdTableFromVector({{Id::makeUndefined()}, {getId("<a>")}}),
        {Variable{"?x"}}, 0, true);
    EXPECT_THAT(emptyPath.getResultSortedOn(), ::testing::IsEmpty());
  }
}

// _____________________________________________________________________________
TEST(EmptyPath, cacheKeyAndDescriptor) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EmptyPath withoutChild{qec, Variable{"?x"}, Graphs::All(), std::nullopt};
  EmptyPath withGraph{qec, Variable{"?x"}, Graphs::All(), Variable{"?g"}};
  EmptyPath withGraphFilter{qec, Variable{"?x"}, singleGraph("g1"),
                            std::nullopt};
  auto withChild = makeExistenceCheck(qec, std::nullopt,
                                      makeIdTableFromVector({{getId("<a>")}}),
                                      {Variable{"?x"}});

  EXPECT_EQ(withoutChild.getDescriptor(), "EmptyPath for ?x");
  EXPECT_EQ(withChild.getDescriptor(), "EmptyPath for ?x (existence check)");
  EXPECT_THAT(withoutChild.getCacheKey(), ::testing::HasSubstr("EMPTY PATH"));
  // The cache keys have to differ for all the different configurations.
  EXPECT_NE(withoutChild.getCacheKey(), withGraph.getCacheKey());
  EXPECT_NE(withoutChild.getCacheKey(), withGraphFilter.getCacheKey());
  EXPECT_NE(withoutChild.getCacheKey(), withChild.getCacheKey());
}

// _____________________________________________________________________________
TEST(EmptyPath, clone) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  {
    EmptyPath emptyPath{qec, Variable{"?x"}, Graphs::All(), std::nullopt};
    auto clone = emptyPath.clone();
    ASSERT_TRUE(clone);
    EXPECT_THAT(emptyPath, IsDeepCopy(*clone));
    EXPECT_EQ(clone->getDescriptor(), emptyPath.getDescriptor());
  }
  {
    auto emptyPath = makeExistenceCheck(qec, Variable{"?g"},
                                        makeIdTableFromVector({{getId("<a>")}}),
                                        {Variable{"?x"}});
    auto clone = emptyPath.clone();
    ASSERT_TRUE(clone);
    EXPECT_THAT(emptyPath, IsDeepCopy(*clone));
    EXPECT_EQ(clone->getCacheKey(), emptyPath.getCacheKey());
  }
}

// _____________________________________________________________________________
TEST(EmptyPath, columnOriginatesFromGraphOrUndef) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto emptyPath = makeExistenceCheck(
      qec, std::nullopt, makeIdTableFromVector({{getId("<a>"), getId("<b>")}}),
      {Variable{"?x"}, Variable{"?y"}});

  // The checked column always originates from the knowledge graph, even though
  // the values of the child don't.
  EXPECT_TRUE(emptyPath.columnOriginatesFromGraphOrUndef(Variable{"?x"}));
  EXPECT_FALSE(emptyPath.columnOriginatesFromGraphOrUndef(Variable{"?y"}));
  EXPECT_THROW(
      emptyPath.columnOriginatesFromGraphOrUndef(Variable{"?notThere"}),
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(EmptyPath, theResultHasToBeRequestedLazily) {
  auto* qec = makeQec(kg);
  EmptyPath emptyPath{qec, Variable{"?x"}, Graphs::All(), std::nullopt};
  EXPECT_THROW(emptyPath.computeResultOnlyForTesting(false),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(EmptyPath, knownEmptyResultIsDerivedFromTheChild) {
  auto* qec = makeQec(kg);
  auto emptyPath = makeExistenceCheck(
      qec, std::nullopt, IdTable{1, qec->getAllocator()}, {Variable{"?x"}});
  EXPECT_TRUE(emptyPath.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(EmptyPath, theGraphVariableMustDifferFromTheEntityVariable) {
  auto* qec = makeQec(kg);
  EXPECT_THROW((EmptyPath{qec, Variable{"?x"}, Graphs::All(), Variable{"?x"}}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(EmptyPath, deletedTriplesAreRespected) {
  // A dedicated index (and hence `QueryExecutionContext`), because the delta
  // triples below must not leak into the other tests.
  // The overload without an explicit basename derives it from the name of the
  // currently running test.
  auto index = std::make_shared<Index>(
      ad_utility::testing::makeTestIndex(std::string{kg}));
  auto getId = ad_utility::testing::makeGetId(*index);

  // `<c>` occurs in a single triple, so deleting that triple removes it from
  // the knowledge graph. This must be noticed even though the block metadata
  // (which describes the triples that are stored on disk) still contains it.
  index->deltaTriplesManager().modify<void>(
      [&getId](DeltaTriples& deltaTriples) {
        deltaTriples.deleteTriples(
            std::make_shared<ad_utility::CancellationHandle<>>(),
            {IdTriple<0>{{getId("<c>"), getId("<p>"), getId("<z>"),
                          getId(std::string{DEFAULT_GRAPH_IRI})}}});
      });

  // NOTE: The `QueryExecutionContext` has to be created after the update,
  // because it takes a snapshot of the delta triples on construction.
  QueryResultCache cache{};
  NamedResultCache namedCache{};
  auto materializedViews = std::make_shared<MaterializedViewsManager>();
  QueryExecutionContext qec{index,
                            &cache,
                            ad_utility::testing::makeAllocator(
                                ad_utility::MemorySize::megabytes(100)),
                            SortPerformanceEstimator{},
                            &namedCache,
                            materializedViews};

  EmptyPath emptyPath{&qec, Variable{"?x"}, Graphs::All(), std::nullopt};
  std::vector<Id> expected{getId("<a>"), getId("<b>"), getId("<z>")};
  ql::ranges::sort(expected);
  EXPECT_THAT(computeResult(emptyPath).getColumn(0),
              ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(EmptyPath, objectsThatPrecedeAllSubjectsAreMergedCorrectly) {
  // `<a>` only occurs as an object and is smaller than all the subjects, so the
  // objects are exhausted before the subjects. The opposite order is covered by
  // the tests that use the knowledge graph `kg` above.
  auto* qec = makeQec("<b> <p> <a> . <c> <p> <a> .");
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EmptyPath emptyPath{qec, Variable{"?x"}, Graphs::All(), std::nullopt};

  std::vector<Id> expected{getId("<a>"), getId("<b>"), getId("<c>")};
  ql::ranges::sort(expected);
  EXPECT_THAT(computeResult(emptyPath).getColumn(0),
              ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(EmptyPath, estimatesAndMultiplicities) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  const auto& index = qec->getIndex();
  {
    EmptyPath emptyPath{qec, Variable{"?x"}, Graphs::All(), std::nullopt};
    // Without a child we have to read both permutations completely, and the
    // size is the (pessimistic) upper bound for the number of distinct
    // entities.
    EXPECT_EQ(
        emptyPath.getSizeEstimate(),
        index.numDistinctSubjects().normal + index.numDistinctObjects().normal);
    EXPECT_EQ(emptyPath.getCostEstimate(), 2 * index.numTriples().normal);
    EXPECT_FLOAT_EQ(emptyPath.getMultiplicity(0), 1.0f);
  }
  {
    // The child has three columns, the second of which is the join column, so
    // the result has four columns (entity, graph, and the two payload columns).
    auto emptyPath = makeExistenceCheck(
        qec, Variable{"?g"},
        makeIdTableFromVector({{getId("<a>"), getId("<b>"), getId("<c>")}}),
        {Variable{"?y"}, Variable{"?x"}, Variable{"?z"}}, 1);
    ASSERT_EQ(emptyPath.getResultWidth(), 4);
    // The existence check can only remove rows, so the child's estimate is
    // used, and the cost is dominated by the cost of the child.
    EXPECT_EQ(emptyPath.getSizeEstimate(), 1);
    EXPECT_EQ(emptyPath.getCostEstimate(), 2);
    // `ValuesForTesting` reports a multiplicity of `42 * (column + 1)`.
    EXPECT_FLOAT_EQ(emptyPath.getMultiplicity(0), 2 * 42.0f);
    // Nothing is known about the graph column.
    EXPECT_FLOAT_EQ(emptyPath.getMultiplicity(1), 1.0f);
    EXPECT_FLOAT_EQ(emptyPath.getMultiplicity(2), 1 * 42.0f);
    EXPECT_FLOAT_EQ(emptyPath.getMultiplicity(3), 3 * 42.0f);
  }
}

// _____________________________________________________________________________
TEST(EmptyPath, theGraphColumnDoesNotOriginateFromTheKnowledgeGraph) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  EmptyPath emptyPath{qec, Variable{"?x"}, Graphs::All(), Variable{"?g"}};
  EXPECT_TRUE(emptyPath.columnOriginatesFromGraphOrUndef(Variable{"?x"}));
  // Graph names are not necessarily subjects or objects of the knowledge graph,
  // so the graph column does not qualify (just like for `IndexScan`).
  EXPECT_FALSE(emptyPath.columnOriginatesFromGraphOrUndef(Variable{"?g"}));

  // The same holds if the graph column is checked instead of added, because
  // then the values are only a subset of those of the child.
  auto withGraphChild =
      makeExistenceCheck(qec, Variable{"?g"},
                         makeIdTableFromVector({{getId("<a>"), getId("<g1>")}}),
                         {Variable{"?x"}, Variable{"?g"}});
  EXPECT_TRUE(withGraphChild.columnOriginatesFromGraphOrUndef(Variable{"?x"}));
  EXPECT_FALSE(withGraphChild.columnOriginatesFromGraphOrUndef(Variable{"?g"}));
}

// _____________________________________________________________________________
TEST(EmptyPath, fullyMaterializedChildrenAreSupported) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{getId("<a>")}, {getId("<p>")}}),
      Vars{Variable{"?x"}}, false, std::vector<ColumnIndex>{}, LocalVocab{},
      std::nullopt, true);
  EmptyPath emptyPath{qec,          Variable{"?x"},   Graphs::All(),
                      std::nullopt, std::move(child), 0};

  EXPECT_THAT(computeResult(emptyPath).getColumn(0),
              ElementsAreArray({getId("<a>")}));
}

// _____________________________________________________________________________
TEST(EmptyPath, lazyChildrenWithMultipleTablesAreSupported) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  std::vector<IdTable> tables;
  tables.push_back(makeIdTableFromVector({{getId("<a>")}, {getId("<p>")}}));
  tables.push_back(makeIdTableFromVector({{getId("<z>")}}));
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(tables), Vars{Variable{"?x"}});
  EmptyPath emptyPath{qec,          Variable{"?x"},   Graphs::All(),
                      std::nullopt, std::move(child), 0};

  EXPECT_THAT(computeResult(emptyPath).getColumn(0),
              ElementsAreArray({getId("<a>"), getId("<z>")}));
}

// _____________________________________________________________________________
TEST(EmptyPath, theJoinColumnMustBeInsideTheChild) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{getId("<a>")}}), Vars{Variable{"?x"}});
  EXPECT_THROW((EmptyPath{qec, Variable{"?x"}, Graphs::All(), std::nullopt,
                          std::move(child), 1}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(EmptyPath, theGraphColumnOfTheChildMustNotBeTheJoinColumn) {
  auto* qec = makeQec(nquads, true);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{getId("<a>")}}), Vars{Variable{"?g"}});
  EXPECT_THROW((EmptyPath{qec, Variable{"?x"}, Graphs::All(), Variable{"?g"},
                          std::move(child), 0}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(EmptyPath, theResultOfTheExistenceCheckIsYieldedInChunks) {
  auto* qec = makeQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  // A little more than the chunk size of 100'000 rows that match `<a>`,
  // followed by enough UNDEF rows to also fill a chunk with the entities of the
  // full empty path (which has four entities).
  constexpr size_t numDefined = 100'001;
  constexpr size_t numUndefined = 25'001;
  IdTable input{1, qec->getAllocator()};
  input.resize(numDefined + numUndefined);
  ql::ranges::fill(input.getColumn(0).subspan(0, numDefined), getId("<a>"));
  ql::ranges::fill(input.getColumn(0).subspan(numDefined), Id::makeUndefined());

  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input), Vars{Variable{"?x"}});
  EmptyPath emptyPath{qec,          Variable{"?x"},   Graphs::All(),
                      std::nullopt, std::move(child), 0};

  auto result = emptyPath.computeResultOnlyForTesting(true);
  ASSERT_FALSE(result.isFullyMaterialized());
  size_t numTables = 0;
  size_t numRows = 0;
  for (const auto& [table, localVocab] : result.idTables()) {
    ++numTables;
    numRows += table.numRows();
  }
  EXPECT_EQ(numRows, numDefined + 4 * numUndefined);
  EXPECT_GT(numTables, 2);
}

// _____________________________________________________________________________
TEST(EmptyPath, allEntitiesAreYieldedInChunks) {
  // More distinct entities than the chunk size of 100'000 rows, such that the
  // result is yielded in several chunks. The entities are added via delta
  // triples, because building an index with that many triples would be much too
  // slow for a unit test. See `deletedTriplesAreRespected` above for the
  // reasons behind the dedicated index and `QueryExecutionContext`.
  auto index = std::make_shared<Index>(
      ad_utility::testing::makeTestIndex(std::string{kg}));
  auto getId = ad_utility::testing::makeGetId(*index);

  constexpr size_t numEntities = 100'001;
  Id predicate = getId("<p>");
  Id graph = getId(std::string{DEFAULT_GRAPH_IRI});
  std::vector<IdTriple<0>> triples;
  triples.reserve(numEntities);
  for (size_t i : ad_utility::integerRange(numEntities)) {
    // Integers are valid subjects and objects for the index, and using them
    // avoids having to add anything to the vocabulary.
    Id entity = Id::makeFromInt(static_cast<int64_t>(i));
    triples.push_back(IdTriple<0>{{entity, predicate, entity, graph}});
  }
  index->deltaTriplesManager().modify<void>(
      [&triples](DeltaTriples& deltaTriples) {
        deltaTriples.insertTriples(
            std::make_shared<ad_utility::CancellationHandle<>>(),
            std::move(triples));
      });

  QueryResultCache cache{};
  NamedResultCache namedCache{};
  auto materializedViews = std::make_shared<MaterializedViewsManager>();
  QueryExecutionContext qec{index,
                            &cache,
                            ad_utility::testing::makeAllocator(
                                ad_utility::MemorySize::megabytes(100)),
                            SortPerformanceEstimator{},
                            &namedCache,
                            materializedViews};

  EmptyPath emptyPath{&qec, Variable{"?x"}, Graphs::All(), std::nullopt};
  auto result = emptyPath.computeResultOnlyForTesting(true);
  ASSERT_FALSE(result.isFullyMaterialized());
  size_t numTables = 0;
  size_t numRows = 0;
  for (const auto& [table, localVocab] : result.idTables()) {
    ++numTables;
    numRows += table.numRows();
  }
  // The four entities of `kg` are added to the inserted ones.
  EXPECT_EQ(numRows, numEntities + 4);
  EXPECT_GT(numTables, 1);
}
