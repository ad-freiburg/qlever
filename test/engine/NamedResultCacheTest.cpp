//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/NamedResultCache.h"

namespace {
TEST(NamedResultCache, basicWorkflow) {
  NamedResultCache cache;
  EXPECT_EQ(cache.numEntries(), 0);
  AD_EXPECT_THROW_WITH_MESSAGE(
      cache.get("query-1"),
      ::testing::HasSubstr("is not contained in the named result cache"));
  auto table = makeIdTableFromVector({{3, 7}, {9, 11}});
  auto table2 = makeIdTableFromVector({{3, 8}, {16, 11}, {39, 14}});
  using V = Variable;
  VariableToColumnMap varColMap{{V{"?x"}, makeAlwaysDefinedColumn(0)},
                                {V{"?y"}, makeAlwaysDefinedColumn(1)}};

  LocalVocab localVocab;
  localVocab.getIndexAndAddIfNotContained(
      ad_utility::triple_component::LiteralOrIri::iriref("<bliBlaBlubb>"));

  // A matcher for the local vocab
  auto matchLocalVocab =
      [&localVocab]() -> ::testing::Matcher<const LocalVocab&> {
    using namespace ::testing;
    auto get = [](const LocalVocab& vocab) {
      return vocab.getAllWordsForTesting();
    };
    return ResultOf(
        get, UnorderedElementsAreArray(localVocab.getAllWordsForTesting()));
  };

  auto qec = ad_utility::testing::getQec();
  auto getCacheValue = [&varColMap, &localVocab](const auto& table) {
    return NamedResultCache::Value{
        std::make_shared<const IdTable>(table.clone()),
        varColMap,
        {1, 0},
        localVocab.clone()};
  };
  // store something in the cache and check that it's there
  {
    cache.store("query-1", getCacheValue(table));
    EXPECT_EQ(cache.numEntries(), 1);
    auto res = cache.get("query-1");
    ASSERT_NE(res, nullptr);

    const auto& [outTable, outVarColMap, outSortedOn, outLocalVocab] = *res;
    EXPECT_THAT(*outTable, matchesIdTable(table));
    EXPECT_THAT(outVarColMap, ::testing::UnorderedElementsAreArray(varColMap));
    EXPECT_THAT(outSortedOn, ::testing::ElementsAre(1, 0));
    EXPECT_THAT(outLocalVocab, matchLocalVocab());
  }
  // overwrite with a different value
  {
    cache.store("query-1", getCacheValue(table2));
    EXPECT_EQ(cache.numEntries(), 1);
    auto res = cache.get("query-1");
    ASSERT_NE(res, nullptr);

    const auto& [outTable, outVarColMap, outSortedOn, outLocalVocab] = *res;
    EXPECT_THAT(*outTable, matchesIdTable(table2));
    EXPECT_THAT(outVarColMap, ::testing::UnorderedElementsAreArray(varColMap));
    EXPECT_THAT(outSortedOn, ::testing::ElementsAre(1, 0));
    EXPECT_THAT(outLocalVocab, matchLocalVocab());
    auto op = cache.getOperation("query-1", qec);
    EXPECT_THAT(op->computeResultOnlyForTesting().idTable(),
                matchesIdTable(table2));
  }

  AD_EXPECT_THROW_WITH_MESSAGE(
      cache.getOperation("query-2", qec),
      ::testing::HasSubstr("is not contained in the named result cache"));

  // store a second value in the cache
  {
    cache.store("query-2", getCacheValue(table2));
    EXPECT_EQ(cache.numEntries(), 2);
    auto res = cache.get("query-2");
    ASSERT_NE(res, nullptr);

    const auto& [outTable, outVarColMap, outSortedOn, outLocalVocab] = *res;
    EXPECT_THAT(*outTable, matchesIdTable(table2));
    EXPECT_THAT(outVarColMap, ::testing::UnorderedElementsAreArray(varColMap));
    EXPECT_THAT(outSortedOn, ::testing::ElementsAre(1, 0));
    EXPECT_THAT(outLocalVocab, matchLocalVocab());
  }
  // Erase only the second query, but not the first one
  {
    cache.erase("query-2");
    EXPECT_EQ(cache.numEntries(), 1);
    AD_EXPECT_THROW_WITH_MESSAGE(
        cache.getOperation("query-2", qec),
        ::testing::HasSubstr("is not contained in the named result cache"));
    EXPECT_NO_THROW(cache.getOperation("query-1", qec));
  }

  cache.clear();
  EXPECT_EQ(cache.numEntries(), 0);
  AD_EXPECT_THROW_WITH_MESSAGE(
      cache.get("query-1"),
      ::testing::HasSubstr("is not contained in the named result cache"));
}

TEST(NamedResultCache, E2E) {
  auto qec = ad_utility::testing::getQec(
      "<s> <p> <o>. <s2> <p> <o> . <s3> <p2> <o2>.");
  std::string pinnedQuery =
      "SELECT * { {?s <p> <o> } UNION {VALUES ?s { <notInVocab> }}} INTERNAL "
      "SORT BY ?s";
  qec->pinResultWithName() = "dummyQuery";
  auto qet = queryPlannerTestHelpers::parseAndPlan(pinnedQuery, qec);
  [[maybe_unused]] auto pinnedResult = qet.getResult();

  qec->pinResultWithName() = std::nullopt;
  std::string query =
      "SELECT ?s { SERVICE ql:cached-result-with-name-dummyQuery {}}";
  qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  // `false` means `not lazy` so `fully materialized`.
  auto result = qet.getResult(false);

  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  LocalVocab dummyVocab;
  auto litOrIri =
      ad_utility::triple_component::LiteralOrIri::iriref("<notInVocab>");
  auto notInVocab = Id::makeFromLocalVocabIndex(
      dummyVocab.getIndexAndAddIfNotContained(litOrIri));
  auto expected =
      makeIdTableFromVector({{notInVocab}, {getId("<s>")}, {getId("<s2>")}});
  EXPECT_THAT(result->idTable(), matchesIdTable(expected));
  EXPECT_THAT(result->localVocab().getAllWordsForTesting(),
              ::testing::ElementsAreArray(dummyVocab.getAllWordsForTesting()));
  EXPECT_THAT(result->sortedBy(), ::testing::ElementsAre(0));
  VariableToColumnMap expectedVars{
      {Variable{"?s"}, makeAlwaysDefinedColumn(0)}};
  EXPECT_THAT(qet.getVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVars));
}
}  // namespace
