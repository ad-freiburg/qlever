// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/functional/bind_front.h>
#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "engine/StripColumns.h"
#include "engine/ValuesForTesting.h"

namespace {
using Vars = std::vector<std::optional<Variable>>;
using namespace ad_utility::testing;
using namespace ::testing;

// _____________________________________________________________________________
template <typename IdTableOrIdTables>
StripColumns makeStrip(QueryExecutionContext* qec, IdTableOrIdTables idTable,
                       const std::set<Variable>& varsToKeep) {
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(idTable),
      Vars{Variable{"?a"}, Variable{"?b"}, Variable{"?c"}});
  return {qec, std::move(valuesTree), varsToKeep};
}

// _____________________________________________________________________________
TEST(StripColumns, basicMembers) {
  // Also test that variables that are not found in the subtree and duplicate
  // variables are silently ignored.
  auto strip = makeStrip(getQec(), makeIdTableFromVector({{1, 2, 3}}),
                         {Variable{"?b"}, Variable{"?notFoundInChild"},
                          Variable{"?c"}, Variable{"?b"}});

  EXPECT_EQ(strip.getDescriptor(), "Strip Columns");
  EXPECT_THAT(strip.getChildren(),
              ElementsAre(Pointee(AD_PROPERTY(
                  QueryExecutionTree, getRootOperation,
                  Pointee(WhenDynamicCastTo<const ValuesForTesting&>(_))))));
  EXPECT_THAT(strip.getCacheKey(),
              AllOf(HasSubstr("StripColumns"), HasSubstr("(1,2"),
                    HasSubstr("Values for testing")));

  EXPECT_EQ(strip.getResultWidth(), 2);
  EXPECT_EQ(strip.getCostEstimate(), strip.getChildren()[0]->getCostEstimate());
  EXPECT_EQ(strip.getSizeEstimate(), strip.getChildren()[0]->getSizeEstimate());

  EXPECT_EQ(strip.getMultiplicity(25), 1.0f);
  EXPECT_EQ(strip.getMultiplicity(0),
            strip.getChildren()[0]->getMultiplicity(1));
  EXPECT_FALSE(strip.knownEmptyResult());

  // Exhaustive tests for `getResultSortedOn` and for the `VariableToColumnMap`
  // can be found below.
  EXPECT_TRUE(strip.getResultSortedOn().empty());
  EXPECT_EQ(strip.getExternallyVisibleVariableColumns().size(), 2);
}

// _____________________________________________________________________________
TEST(StripColumns, computeResult) {
  using V = Variable;
  auto qec = ad_utility::testing::getQec();
  auto makeOp = [&qec]() {
    LocalVocab voc;
    voc.getIndexAndAddIfNotContained(LocalVocabEntry::iriref("<kartoffel>"));
    qec->clearCacheUnpinnedOnly();
    std::vector<IdTable> children;
    children.push_back(makeIdTableFromVector({{1, 2, 3}, {4, 5, 6}}));
    children.push_back(makeIdTableFromVector({{8, 9, 10}}));
    auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(children),
        Vars{Variable{"?a"}, Variable{"?b"}, Variable{"?c"}}, false,
        std::vector<ColumnIndex>{}, std::move(voc));
    return StripColumns{qec, std::move(valuesTree), {V{"?c"}, V{"?a"}}};
  };

  auto localVocabMatcher =
      ResultOf(std::mem_fn(&LocalVocab::getAllWordsForTesting),
               ElementsAre(LocalVocabEntry::iriref("<kartoffel>")));
  // Test materialized result.
  {
    auto strip = makeOp();
    auto res = strip.computeResultOnlyForTesting(false);
    ASSERT_TRUE(res.isFullyMaterialized());
    EXPECT_THAT(res.idTable(),
                matchesIdTableFromVector({{1, 3}, {4, 6}, {8, 10}}));
    EXPECT_THAT(res.localVocab(), localVocabMatcher);
  }
  // Test lazy result.
  {
    auto strip = makeOp();
    auto res = strip.computeResultOnlyForTesting(true);
    ASSERT_FALSE(res.isFullyMaterialized());
    std::vector<IdTable> result;
    for (auto& [table, vocab] : res.idTables()) {
      result.push_back(std::move(table));
      EXPECT_THAT(vocab, localVocabMatcher);
    }
    EXPECT_THAT(result, ElementsAre(matchesIdTableFromVector({{1, 3}, {4, 6}}),
                                    matchesIdTableFromVector({{8, 10}})));
  }
}

// _____________________________________________________________________________
TEST(StripColumns, resultSortedOnAndVarToColMap) {
  using V = Variable;
  auto qec = ad_utility::testing::getQec();
  auto alloc = ad_utility::testing::makeAllocator();

  auto a = V{"?a"};
  auto b = V{"?b"};
  auto c = V{"?c"};

  // Return a `StripColumns` operation where the child has the variables ?a ?b
  // ?c and is sorted by "?c ?a ?b". The strip operation only keeps the
  // `varsToKeep`.
  auto makeOp = [&](const std::set<Variable>& varsToKeep) {
    auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{0, Id::makeUndefined(), 3}}),
        Vars{a, b, c}, false, std::vector<ColumnIndex>{2, 0, 1});
    return StripColumns{qec, std::move(valuesTree), varsToKeep};
  };

  // Match a `result` and check that the `resultSortedOn` and the variable to
  // column mapping have the expected content.
  auto match =
      [](std::vector<ColumnIndex> sortedCols,
         std::vector<std::pair<Variable, ColumnIndexAndTypeInfo>> varToCols) {
        auto get = [](const auto& op) { return op.getResultSortedOn(); };
        auto getVars = [](const Operation& op) {
          return op.getExternallyVisibleVariableColumns();
        };
        return AllOf(ResultOf(get, ElementsAreArray(sortedCols)),
                     ResultOf(getVars, UnorderedElementsAreArray(varToCols)));
      };

  // Infrastructure to set up the variable to column mapping.
  auto makeCol = [&](Variable v, const auto& defOrUndef, size_t colIdx) {
    return std::pair{v, defOrUndef(colIdx)};
  };

  // Variables a and c are always defined, b contains an undef value.
  auto aCol = absl::bind_front(makeCol, a, makeAlwaysDefinedColumn);
  auto bCol = absl::bind_front(makeCol, b, makePossiblyUndefinedColumn);
  auto cCol = absl::bind_front(makeCol, c, makeAlwaysDefinedColumn);

  EXPECT_THAT(makeOp({a, b, c}), match({2, 0, 1}, {aCol(0), bCol(1), cCol(2)}));
  EXPECT_THAT(makeOp({a, b}), match({}, {aCol(0), bCol(1)}));
  EXPECT_THAT(makeOp({a, c}), match({1, 0}, {aCol(0), cCol(1)}));
  EXPECT_THAT(makeOp({b, c}), match({1}, {bCol(0), cCol(1)}));
}

// _____________________________________________________________________________
TEST(StripColumns, clone) {
  auto strip = makeStrip(ad_utility::testing::getQec(),
                         makeIdTableFromVector({{1, 2, 3}}), {Variable{"?b"}});
  auto clone = strip.clone();
  ASSERT_TRUE(clone != nullptr);
  EXPECT_THAT(*clone, IsDeepCopy(strip));
}

}  // namespace
