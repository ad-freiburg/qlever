//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "engine/NeutralOptional.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ValuesForTesting.h"

// _____________________________________________________________________________
TEST(NeutralOptional, getChildren) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{0, qec->getAllocator()},
      std::vector<std::optional<Variable>>{});
  NeutralOptional no{qec, child};
  EXPECT_THAT(no.getChildren(), ::testing::ElementsAre(child.get()));
}

// _____________________________________________________________________________
TEST(NeutralOptional, getCacheKey) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{0, qec->getAllocator()},
      std::vector<std::optional<Variable>>{});
  NeutralOptional no{qec, child};
  EXPECT_THAT(no.getCacheKey(),
              ::testing::AllOf(::testing::StartsWith("NeutralOptional#"),
                               ::testing::EndsWith(child->getCacheKey())));
}

// _____________________________________________________________________________
TEST(NeutralOptional, getDescriptor) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{0, qec->getAllocator()},
      std::vector<std::optional<Variable>>{});
  NeutralOptional no{qec, std::move(child)};
  EXPECT_THAT(no.getDescriptor(), ::testing::StartsWith("Optional"));
}

// _____________________________________________________________________________
TEST(NeutralOptional, getResultWidth) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{5, qec->getAllocator()},
      std::vector<std::optional<Variable>>(5, std::nullopt));
  NeutralOptional no{qec, std::move(child)};
  EXPECT_EQ(no.getResultWidth(), 5);
}

// _____________________________________________________________________________
TEST(NeutralOptional, getCostEstimate) {
  auto* qec = ad_utility::testing::getQec();
  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{0, qec->getAllocator()},
        std::vector<std::optional<Variable>>{});
    NeutralOptional no{qec, std::move(child)};
    EXPECT_EQ(no.getCostEstimate(), 1);
  }
  {
    IdTable idTable{0, qec->getAllocator()};
    idTable.resize(42);
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(idTable), std::vector<std::optional<Variable>>{});
    NeutralOptional no{qec, std::move(child)};
    EXPECT_EQ(no.getCostEstimate(), 42 * 2);
  }
}

// _____________________________________________________________________________
TEST(NeutralOptional, getSizeEstimate) {
  auto* qec = ad_utility::testing::getQec();
  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{0, qec->getAllocator()},
        std::vector<std::optional<Variable>>{});
    NeutralOptional no{qec, std::move(child)};
    EXPECT_EQ(no.getSizeEstimate(), 1);
  }
  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{0, qec->getAllocator()},
        std::vector<std::optional<Variable>>{});
    NeutralOptional no{qec, std::move(child)};
    no.applyLimitOffset({0, 0});
    EXPECT_EQ(no.getSizeEstimate(), 0);
  }
  {
    IdTable idTable{0, qec->getAllocator()};
    idTable.resize(42);
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(idTable), std::vector<std::optional<Variable>>{});
    NeutralOptional no{qec, std::move(child)};
    no.applyLimitOffset({40, 1});
    EXPECT_EQ(no.getSizeEstimate(), 40);
  }
  {
    IdTable idTable{0, qec->getAllocator()};
    idTable.resize(42);
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(idTable), std::vector<std::optional<Variable>>{});
    NeutralOptional no{qec, std::move(child)};
    EXPECT_EQ(no.getSizeEstimate(), 42);
  }
}

// _____________________________________________________________________________
TEST(NeutralOptional, getMultiplicity) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{3, qec->getAllocator()},
      std::vector<std::optional<Variable>>(3, std::nullopt));
  NeutralOptional no{qec, child};
  EXPECT_EQ(no.getMultiplicity(0), child->getMultiplicity(0));
  EXPECT_EQ(no.getMultiplicity(1), child->getMultiplicity(1));
  EXPECT_EQ(no.getMultiplicity(2), child->getMultiplicity(2));
}

// _____________________________________________________________________________
TEST(NeutralOptional, knownEmptyResult) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{0, qec->getAllocator()},
      std::vector<std::optional<Variable>>{});
  NeutralOptional no{qec, std::move(child)};
  EXPECT_FALSE(no.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(NeutralOptional, supportsLimit) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{0, qec->getAllocator()},
      std::vector<std::optional<Variable>>{});
  NeutralOptional no{qec, std::move(child)};
  EXPECT_TRUE(no.supportsLimitOffset());
}

// _____________________________________________________________________________
TEST(NeutralOptional, clone) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{0, qec->getAllocator()},
      std::vector<std::optional<Variable>>{});
  NeutralOptional no{qec, std::move(child)};
  auto clone = no.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(no, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), no.getDescriptor());
}

// _____________________________________________________________________________
TEST(NeutralOptional, getResultSortedOn) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{3, qec->getAllocator()},
      std::vector<std::optional<Variable>>(3, std::nullopt), false,
      std::vector<ColumnIndex>{1, 0, 2});
  NeutralOptional no{qec, child};
  EXPECT_EQ(no.getResultSortedOn(), child->resultSortedOn());
}

// _____________________________________________________________________________
TEST(NeutralOptional, getExternallyVisibleVariableColumns) {
  using namespace ::testing;
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{2, qec->getAllocator()},
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  NeutralOptional no{qec, std::move(child)};
  EXPECT_THAT(no.getExternallyVisibleVariableColumns(),
              UnorderedElementsAre(
                  Pair(Variable{"?a"},
                       ColumnIndexAndTypeInfo{
                           0, ColumnIndexAndTypeInfo::PossiblyUndefined}),
                  Pair(Variable{"?b"},
                       ColumnIndexAndTypeInfo{
                           1, ColumnIndexAndTypeInfo::PossiblyUndefined})));
}

// _____________________________________________________________________________
TEST(NeutralOptional, ensureEmptyResultWhenLimitCutsOffEverything) {
  auto* qec = ad_utility::testing::getQec();
  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{1, qec->getAllocator()},
        std::vector<std::optional<Variable>>{std::nullopt});
    NeutralOptional no{qec, std::move(child)};
    no.applyLimitOffset({std::nullopt, 1});

    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(false);
    EXPECT_TRUE(result.idTable().empty());
    EXPECT_TRUE(result.localVocab().empty());
  }

  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{1, qec->getAllocator()},
        std::vector<std::optional<Variable>>{std::nullopt});
    NeutralOptional no{qec, std::move(child)};
    no.applyLimitOffset({std::nullopt, 1});

    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(true);
    auto idTables = result.idTables();
    EXPECT_EQ(idTables.begin(), idTables.end());
  }
  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{1, qec->getAllocator()},
        std::vector<std::optional<Variable>>{std::nullopt});
    NeutralOptional no{qec, std::move(child)};
    no.applyLimitOffset({0});

    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(false);
    EXPECT_TRUE(result.idTable().empty());
    EXPECT_TRUE(result.localVocab().empty());
  }

  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{1, qec->getAllocator()},
        std::vector<std::optional<Variable>>{std::nullopt});
    NeutralOptional no{qec, std::move(child)};
    no.applyLimitOffset({0});

    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(true);
    auto idTables = result.idTables();
    EXPECT_EQ(idTables.begin(), idTables.end());
  }
}

// _____________________________________________________________________________
TEST(NeutralOptional, ensureSingleRowWhenChildIsEmpty) {
  auto* qec = ad_utility::testing::getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{1, qec->getAllocator()},
      std::vector<std::optional<Variable>>{std::nullopt});
  NeutralOptional no{qec, child};

  {
    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(false);
    EXPECT_EQ(result.idTable(), makeIdTableFromVector({{Id::makeUndefined()}}));
    EXPECT_TRUE(result.localVocab().empty());
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(true);
    auto idTables = result.idTables();

    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    // First table is empty from `ValuesForTesting`.
    EXPECT_TRUE(it->idTable_.empty());
    EXPECT_TRUE(it->localVocab_.empty());

    ++it;
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{Id::makeUndefined()}}));
    EXPECT_TRUE(it->localVocab_.empty());

    EXPECT_EQ(++it, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(NeutralOptional, ensureResultIsProperlyPropagated) {
  auto* qec = ad_utility::testing::getQec();
  LocalVocab localVocab{};
  localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{
      ad_utility::triple_component::Literal::fromStringRepresentation(
          "\"Test\"")});

  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1}, {2}, {3}}),
        std::vector<std::optional<Variable>>{std::nullopt}, false,
        std::vector<ColumnIndex>{}, localVocab.clone());
    NeutralOptional no{qec, child};

    {
      qec->getQueryTreeCache().clearAll();
      auto result = no.computeResultOnlyForTesting(false);
      EXPECT_EQ(result.idTable(), makeIdTableFromVector({{1}, {2}, {3}}));
      EXPECT_EQ(result.localVocab().getAllWordsForTesting(),
                localVocab.getAllWordsForTesting());
    }

    {
      qec->getQueryTreeCache().clearAll();
      auto result = no.computeResultOnlyForTesting(true);
      auto idTables = result.idTables();

      auto it = idTables.begin();
      ASSERT_NE(it, idTables.end());
      EXPECT_EQ(it->idTable_, makeIdTableFromVector({{1}, {2}, {3}}));
      EXPECT_EQ(it->localVocab_.getAllWordsForTesting(),
                localVocab.getAllWordsForTesting());

      EXPECT_EQ(++it, idTables.end());
    }
  }

  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1}, {2}, {3}}),
        std::vector<std::optional<Variable>>{std::nullopt}, false,
        std::vector<ColumnIndex>{}, localVocab.clone());
    NeutralOptional no{qec, child};
    no.applyLimitOffset({std::nullopt, 1});
    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(false);
    EXPECT_EQ(result.idTable(), makeIdTableFromVector({{2}, {3}}));
    EXPECT_EQ(result.localVocab().getAllWordsForTesting(),
              localVocab.getAllWordsForTesting());
  }

  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1}, {2}, {3}}),
        std::vector<std::optional<Variable>>{std::nullopt}, false,
        std::vector<ColumnIndex>{}, localVocab.clone());
    NeutralOptional no{qec, child};
    no.applyLimitOffset({std::nullopt, 1});
    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(true);
    auto idTables = result.idTables();

    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{2}, {3}}));
    EXPECT_EQ(it->localVocab_.getAllWordsForTesting(),
              localVocab.getAllWordsForTesting());

    EXPECT_EQ(++it, idTables.end());
  }

  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1}, {2}, {3}}),
        std::vector<std::optional<Variable>>{std::nullopt}, false,
        std::vector<ColumnIndex>{}, localVocab.clone());
    NeutralOptional no{qec, child};
    no.applyLimitOffset({2});
    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(false);
    EXPECT_EQ(result.idTable(), makeIdTableFromVector({{1}, {2}}));
    EXPECT_EQ(result.localVocab().getAllWordsForTesting(),
              localVocab.getAllWordsForTesting());
  }

  {
    auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1}, {2}, {3}}),
        std::vector<std::optional<Variable>>{std::nullopt}, false,
        std::vector<ColumnIndex>{}, localVocab.clone());
    NeutralOptional no{qec, child};
    no.applyLimitOffset({2});
    qec->getQueryTreeCache().clearAll();
    auto result = no.computeResultOnlyForTesting(true);
    auto idTables = result.idTables();

    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{1}, {2}}));
    EXPECT_EQ(it->localVocab_.getAllWordsForTesting(),
              localVocab.getAllWordsForTesting());

    EXPECT_EQ(++it, idTables.end());
  }
}
