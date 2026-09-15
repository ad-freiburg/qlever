//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "engine/NamedResultCache.h"
#include "index/LocalVocabEntry.h"

namespace {

// The variables of all the cached results in the tests below.
const VariableToColumnMap varColMap{
    {Variable{"?x"}, makeAlwaysDefinedColumn(0)},
    {Variable{"?y"}, makeAlwaysDefinedColumn(1)}};

// Create a cache value for the given `table`, using the `varColMap` above.
NamedResultCache::Value getCacheValue(const IdTable& table,
                                      std::vector<ColumnIndex> sortedOn = {},
                                      const LocalVocab& localVocab = {}) {
  return NamedResultCache::Value{std::make_shared<const IdTable>(table.clone()),
                                 varColMap,
                                 std::move(sortedOn),
                                 localVocab.clone(),
                                 "cache key",
                                 std::nullopt};
}

// A matcher for the message of the exception that is thrown when a result is
// not contained in the named result cache.
::testing::Matcher<const std::string&> notContainedMatcher() {
  return ::testing::HasSubstr("is not contained in the named result cache");
}

// _____________________________________________________________________________
TEST(NamedResultCache, basicWorkflow) {
  NamedResultCache cache;
  EXPECT_EQ(cache.numEntries(), 0);
  AD_EXPECT_THROW_WITH_MESSAGE(cache.get("query-1"), notContainedMatcher());
  auto table = makeIdTableFromVector({{3, 7}, {9, 11}});
  auto table2 = makeIdTableFromVector({{3, 8}, {16, 11}, {39, 14}});

  LocalVocab localVocab;
  auto* qec = ad_utility::testing::getQec();
  localVocab.getIndexAndAddIfNotContained(LocalVocabEntry::fromIriref(
      "<bliBlaBlubb>", qec->getLocalVocabContext()));

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

  auto storeValue = [&cache, &localVocab](const std::string& name,
                                          const IdTable& table) {
    cache.store(name, getCacheValue(table, {1, 0}, localVocab));
  };
  // store something in the cache and check that it's there
  {
    storeValue("query-1", table);
    EXPECT_EQ(cache.numEntries(), 1);
    auto res = cache.get("query-1");
    ASSERT_NE(res, nullptr);

    const auto& [outTable, outVarColMap, outSortedOn, outLocalVocab,
                 outCacheKey, outGeoIndex, alloc, blankNodeManager] = *res;
    EXPECT_THAT(ExplicitIdTableOperation::viewOf(outTable),
                matchesIdTable(table));
    EXPECT_THAT(outVarColMap, ::testing::UnorderedElementsAreArray(varColMap));
    EXPECT_THAT(outSortedOn, ::testing::ElementsAre(1, 0));
    EXPECT_THAT(outLocalVocab, matchLocalVocab());
  }
  // overwrite with a different value
  {
    storeValue("query-1", table2);
    EXPECT_EQ(cache.numEntries(), 1);
    auto res = cache.get("query-1");
    ASSERT_NE(res, nullptr);

    const auto& [outTable, outVarColMap, outSortedOn, outLocalVocab,
                 outCacheKey, outGeoIndex, alloc, blankNodeManager] = *res;
    EXPECT_THAT(ExplicitIdTableOperation::viewOf(outTable),
                matchesIdTable(table2));
    EXPECT_THAT(outVarColMap, ::testing::UnorderedElementsAreArray(varColMap));
    EXPECT_THAT(outSortedOn, ::testing::ElementsAre(1, 0));
    EXPECT_THAT(outLocalVocab, matchLocalVocab());
    auto op = cache.getOperation("query-1", qec);
    EXPECT_THAT(op->computeResultOnlyForTesting().idTableView(),
                matchesIdTable(table2));
  }

  AD_EXPECT_THROW_WITH_MESSAGE(cache.getOperation("query-2", qec),
                               notContainedMatcher());

  // store a second value in the cache
  {
    storeValue("query-2", table2);
    EXPECT_EQ(cache.numEntries(), 2);
    auto res = cache.get("query-2");
    ASSERT_NE(res, nullptr);

    const auto& [outTable, outVarColMap, outSortedOn, outLocalVocab,
                 outCacheKey, outGeoIndex, alloc, blankNodeManager] = *res;
    EXPECT_THAT(ExplicitIdTableOperation::viewOf(outTable),
                matchesIdTable(table2));
    EXPECT_THAT(outVarColMap, ::testing::UnorderedElementsAreArray(varColMap));
    EXPECT_THAT(outSortedOn, ::testing::ElementsAre(1, 0));
    EXPECT_THAT(outLocalVocab, matchLocalVocab());
  }
  // Erase only the second query, but not the first one
  {
    cache.erase("query-2");
    EXPECT_EQ(cache.numEntries(), 1);
    AD_EXPECT_THROW_WITH_MESSAGE(cache.getOperation("query-2", qec),
                                 notContainedMatcher());
    EXPECT_NO_THROW(cache.getOperation("query-1", qec));
  }

  cache.clear();
  EXPECT_EQ(cache.numEntries(), 0);
  AD_EXPECT_THROW_WITH_MESSAGE(cache.get("query-1"), notContainedMatcher());
}

// _____________________________________________________________________________
TEST(NamedResultCache, emptyResultInsteadOfException) {
  NamedResultCache cache;
  auto* qec = ad_utility::testing::getQec();
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::emptyResultInsteadOfExceptions_>(true);

  // The result is not contained in the cache, so we get an empty result
  // instead of an exception.
  auto op = cache.getOperation("query-1", qec);
  EXPECT_EQ(op->getResultWidth(), 0);
  EXPECT_EQ(op->sizeEstimate(), 0);
  EXPECT_TRUE(op->knownEmptyResult());
  EXPECT_THAT(op->getExternallyVisibleVariableColumns(), ::testing::IsEmpty());
  EXPECT_THAT(op->resultSortedOn(), ::testing::IsEmpty());
  const auto& result = op->computeResultOnlyForTesting();
  EXPECT_EQ(result.idTableView().numRows(), 0);
  EXPECT_EQ(result.idTableView().numColumns(), 0);

  // The cache keys of the empty results for distinct names are distinct, and
  // the name is part of the cache key.
  EXPECT_THAT(op->getCacheKey(), ::testing::HasSubstr("query-1"));
  EXPECT_NE(op->getCacheKey(),
            cache.getOperation("query-2", qec)->getCacheKey());

  // The parameter only affects `getOperation`, not `get`.
  AD_EXPECT_THROW_WITH_MESSAGE(cache.get("query-1"), notContainedMatcher());

  // A result that is contained in the cache is returned as usual.
  auto table = makeIdTableFromVector({{3, 7}, {9, 11}});
  cache.store("query-1", getCacheValue(table));
  EXPECT_THAT(cache.getOperation("query-1", qec)
                  ->computeResultOnlyForTesting()
                  .idTableView(),
              matchesIdTable(table));
}

// _____________________________________________________________________________
TEST(NamedResultCache, emptyResultInsteadOfExceptionE2E) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>. <s2> <p> <o>.");
  std::string query =
      "SELECT ?s { SERVICE ql:cached-result-with-name-notPinned {}}";

  // By default, planning the query throws, because there is no cached result
  // with the name `notPinned`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      queryPlannerTestHelpers::parseAndPlan(query, qec), notContainedMatcher());

  // With the runtime parameter set, the query is planned and yields an empty
  // result.
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::emptyResultInsteadOfExceptions_>(true);
  auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  // `false` means `not lazy` so `fully materialized`.
  auto result = qet->getResult(false);
  EXPECT_EQ(result->idTableView().numRows(), 0);
  EXPECT_THAT(qet->getVariableColumns(), ::testing::IsEmpty());

  // The empty result also propagates through the rest of the query: the
  // pattern `?s <p> <o>` alone would match two triples.
  qet = queryPlannerTestHelpers::parseAndPlan(
      "SELECT * { ?s <p> <o> . SERVICE ql:cached-result-with-name-notPinned "
      "{}}",
      qec);
  EXPECT_EQ(qet->getResult(false)->idTableView().numRows(), 0);
  EXPECT_THAT(qet->getVariableColumns(),
              ::testing::ElementsAre(::testing::Key(Variable{"?s"})));
}

// _____________________________________________________________________________
TEST(NamedResultCache, E2E) {
  auto qec = ad_utility::testing::getQec(
      "<s> <p> <o>. <s2> <p> <o> . <s3> <p2> <o2>.");
  std::string pinnedQuery =
      "SELECT * { {?s <p> <o> } UNION {VALUES ?s { <notInVocab> }}} INTERNAL "
      "SORT BY ?s";
  qec->pinResultWithName() = {"dummyQuery"};
  auto qet = queryPlannerTestHelpers::parseAndPlan(pinnedQuery, qec);
  [[maybe_unused]] auto pinnedResult = qet->getResult();

  qec->pinResultWithName() = std::nullopt;
  std::string query =
      "SELECT ?s { SERVICE ql:cached-result-with-name-dummyQuery {}}";
  qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  // `false` means `not lazy` so `fully materialized`.
  auto result = qet->getResult(false);

  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  LocalVocab dummyVocab;
  auto notInVocab = Id::makeFromLocalVocabIndex(
      dummyVocab.getIndexAndAddIfNotContained(LocalVocabEntry::fromIriref(
          "<notInVocab>", qec->getLocalVocabContext())));
  auto expected =
      makeIdTableFromVector({{notInVocab}, {getId("<s>")}, {getId("<s2>")}});
  EXPECT_THAT(result->idTableView(), matchesIdTable(expected));
  EXPECT_THAT(result->localVocab().getAllWordsForTesting(),
              ::testing::ElementsAreArray(dummyVocab.getAllWordsForTesting()));
  EXPECT_THAT(result->sortedBy(), ::testing::ElementsAre(0));
  VariableToColumnMap expectedVars{
      {Variable{"?s"}, makeAlwaysDefinedColumn(0)}};
  EXPECT_THAT(qet->getVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVars));
}
}  // namespace
