//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/NamedQueryCache.h"

namespace {
TEST(NamedQueryCache, basicWorkflow) {
  NamedQueryCache cache;
  EXPECT_EQ(cache.numEntries(), 0);
  AD_EXPECT_THROW_WITH_MESSAGE(
      cache.get("query-1"),
      ::testing::HasSubstr("was not pinned to the named query cache"));
  auto table = makeIdTableFromVector({{3, 7}, {9, 11}});
  using V = Variable;
  VariableToColumnMap varColMap{{V{"?x"}, makeAlwaysDefinedColumn(0)},
                                {V{"?y"}, makeAlwaysDefinedColumn(1)}};
  // TODO<joka921> Test with a nonempty vocab
  NamedQueryCache::Value value{std::make_shared<const IdTable>(table.clone()),
                               varColMap,
                               {1, 0},
                               LocalVocab{}};
  cache.store("query-1", std::move(value));
  EXPECT_EQ(cache.numEntries(), 1);
  auto res = cache.get("query-1");
  ASSERT_NE(res, nullptr);

  const auto& [outTable, outVarColMap, outSortedOn, outLocalVocab] = *res;
  EXPECT_THAT(*outTable, matchesIdTable(table));
  EXPECT_THAT(outVarColMap, ::testing::UnorderedElementsAreArray(varColMap));
  EXPECT_THAT(outSortedOn, ::testing::ElementsAre(1, 0));
  EXPECT_THAT(outLocalVocab, ::testing::IsEmpty());

  auto qec = ad_utility::testing::getQec();
  AD_EXPECT_THROW_WITH_MESSAGE(
      cache.getOperation("query-2", qec),
      ::testing::HasSubstr("was not pinned to the named query cache"));
  auto op = cache.getOperation("query-1", qec);

  EXPECT_THAT(op->computeResultOnlyForTesting().idTable(),
              matchesIdTable(table));
  // TODO<joka921> Test the other members, in particular the local vocab.

  cache.clear();
  EXPECT_EQ(cache.numEntries(), 0);
  AD_EXPECT_THROW_WITH_MESSAGE(
      cache.get("query-1"),
      ::testing::HasSubstr("was not pinned to the named query cache"));
}

TEST(NamedQueryCache, E2E) {
  auto qec = ad_utility::testing::getQec(
      "<s> <p> <o>. <s2> <p> <o> . <s3> <p2> <o2>.");
  std::string pinnedQuery =
      "SELECT ?s { {?s <p> <o> } UNION {BIND ( <notInVocab> as ?s)}}";
  qec->pinWithExplicitName() = "dummyQuery";
  auto qet = queryPlannerTestHelpers::parseAndPlan(pinnedQuery, qec);
  [[maybe_unused]] auto pinnedResult = qet.getResult();
  qec->pinWithExplicitName() = std::nullopt;

  squery = "SELECT * { SERVICE ql:named-cached-query-dummyQuery {}}";
}
}  // namespace
