// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/ExplicitIdTableOperation.h"
#include "engine/QueryExecutionContext.h"
#include "global/ValueId.h"
#include "parser/TripleComponent.h"

using ad_utility::testing::makeAllocator;
using ad_utility::testing::VocabId;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

namespace {

// Helper function to create a test QueryExecutionContext
QueryExecutionContext* getTestQec() { return ad_utility::testing::getQec(); }

// Helper function to create a simple test IdTable
std::shared_ptr<IdTable> createTestIdTable(size_t numRows, size_t numCols) {
  auto table = std::make_shared<IdTable>(numCols, makeAllocator());
  table->reserve(numRows);

  for (size_t row = 0; row < numRows; ++row) {
    table->emplace_back();
    for (size_t col = 0; col < numCols; ++col) {
      (*table)[row][col] = VocabId(row * numCols + col);
    }
  }

  return table;
}

// Helper function to create VariableToColumnMap
VariableToColumnMap createTestVariableMap(size_t numVars) {
  VariableToColumnMap map;
  for (size_t i = 0; i < numVars; ++i) {
    map[Variable("?var" + std::to_string(i))] = makeAlwaysDefinedColumn(i);
  }
  return map;
}

// Test fixture for ExplicitIdTableOperation tests. Parameterized over
// whether the `IdTableOrView` passed to the operation should be the owning
// `shared_ptr<const IdTable>` alternative (`GetParam() == false`) or the
// non-owning `IdTableView<0>` alternative (`GetParam() == true`), so that
// every test in this fixture exercises both cases.
class ExplicitIdTableOperationTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    qec_ = getTestQec();
    testTable_ = createTestIdTable(3, 2);
    testVariables_ = createTestVariableMap(2);
    // Avoid initializer_list assignment to sidestep a GCC 12 false positive
    // (-Werror=array-bounds).
    testSortedColumns_ = std::vector<ColumnIndex>(1, ColumnIndex{0});
    testCacheKey_ = "[dummy cache key]";
  }

  // Wrap `table` as an `ExplicitIdTableOperation::IdTableOrView`, either as
  // the owning `shared_ptr<const IdTable>` alternative, or as a non-owning
  // `IdTableView<0>` alternative, depending on `GetParam()`. The caller is
  // responsible for keeping `table` alive for as long as the returned
  // `IdTableOrView` (and anything constructed from it) is in use.
  ExplicitIdTableOperation::IdTableOrView wrapTable(
      const std::shared_ptr<IdTable>& table) const {
    if (GetParam()) {
      return table->asStaticView<0>();
    }
    return std::shared_ptr<const IdTable>{table};
  }

  QueryExecutionContext* qec_;
  std::shared_ptr<IdTable> testTable_;
  VariableToColumnMap testVariables_;
  std::vector<ColumnIndex> testSortedColumns_;
  LocalVocab testLocalVocab_;
  std::string testCacheKey_;
};

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(OwningAndView, ExplicitIdTableOperationTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           return info.param ? "View" : "Owning";
                         });

// Test trivial member functions
TEST_P(ExplicitIdTableOperationTest, TrivialGetters) {
  ExplicitIdTableOperation op(qec_, wrapTable(testTable_), testVariables_,
                              testSortedColumns_, testLocalVocab_.clone(),
                              testCacheKey_);

  // Test sizeEstimate
  EXPECT_EQ(op.sizeEstimate(), 3u);

  // Test getResultWidth
  EXPECT_EQ(op.getResultWidth(), 2u);

  // Test getCostEstimate
  EXPECT_EQ(op.getCostEstimate(), 0u);

  // Test getSizeEstimateBeforeLimit
  EXPECT_EQ(op.getSizeEstimateBeforeLimit(), 3u);

  // Test getMultiplicity
  EXPECT_EQ(op.getMultiplicity(0), 1.0f);
  EXPECT_EQ(op.getMultiplicity(1), 1.0f);

  // Test getDescriptor
  EXPECT_EQ(op.getDescriptor(), "Explicit Result");

  // Test getCacheKeyImpl
  EXPECT_EQ(op.getCacheKeyImpl(), "[dummy cache key]");

  // Test getChildren
  EXPECT_THAT(op.getChildren(), IsEmpty());

  // Test resultSortedOn
  EXPECT_THAT(op.resultSortedOn(), ElementsAre(ColumnIndex{0}));

  // Test computeVariableToColumnMap
  auto varMap = op.computeVariableToColumnMap();
  EXPECT_EQ(varMap.size(), 2u);
  EXPECT_TRUE(varMap.contains(Variable("?var0")));
  EXPECT_TRUE(varMap.contains(Variable("?var1")));
}

// _____________________________________________________________________________
TEST_P(ExplicitIdTableOperationTest, isDeterministic) {
  ExplicitIdTableOperation op(qec_, wrapTable(testTable_), testVariables_,
                              testSortedColumns_, testLocalVocab_.clone(),
                              testCacheKey_);
  EXPECT_TRUE(op.isDeterministic());
}

TEST_P(ExplicitIdTableOperationTest, KnownEmptyResult) {
  {
    auto emptyTable = std::make_shared<IdTable>(2, makeAllocator());
    ExplicitIdTableOperation op(qec_, wrapTable(emptyTable), testVariables_, {},
                                LocalVocab{}, "empty");
    EXPECT_TRUE(op.knownEmptyResult());
  }
  {
    ExplicitIdTableOperation op(qec_, wrapTable(testTable_), testVariables_, {},
                                LocalVocab{}, "empty");
    EXPECT_FALSE(op.knownEmptyResult());
  }
}

// Test computeResult functionality
TEST_P(ExplicitIdTableOperationTest, ComputeResultBasic) {
  ExplicitIdTableOperation op(qec_, wrapTable(testTable_), testVariables_,
                              testSortedColumns_, testLocalVocab_.clone(),
                              testCacheKey_);

  auto result = op.computeResult(false);

  // Check that we get back the same table
  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTableView();

  EXPECT_THAT(resultTable, matchesIdTable(*testTable_));
  // Check sorted columns are preserved
  EXPECT_THAT(result.sortedBy(), ElementsAre(ColumnIndex{0}));
}

TEST_P(ExplicitIdTableOperationTest, ComputeResultWithLaziness) {
  ExplicitIdTableOperation op(qec_, wrapTable(testTable_), testVariables_,
                              testSortedColumns_, testLocalVocab_.clone(),
                              testCacheKey_);

  // Test with requestLaziness = true (should still return materialized result)
  auto result = op.computeResult(true);

  ASSERT_TRUE(result.isFullyMaterialized());
  const auto& resultTable = result.idTableView();

  EXPECT_EQ(resultTable.numRows(), 3u);
  EXPECT_EQ(resultTable.numColumns(), 2u);
}

// _____________________________________________________________________________
TEST_P(ExplicitIdTableOperationTest, ComputeResultWithLocalVocab) {
  LocalVocab localVocab;
  LocalVocabEntry testEntry = LocalVocabEntry::fromStringRepresentation(
      "\"test_word\"", qec_->getLocalVocabContext());
  localVocab.getIndexAndAddIfNotContained(testEntry);

  ExplicitIdTableOperation op(qec_, wrapTable(testTable_), testVariables_,
                              testSortedColumns_, std::move(localVocab),
                              testCacheKey_);

  auto result = op.computeResult(false);

  // Check that local vocab is preserved
  const auto& resultLocalVocab = result.localVocab();
  auto words = resultLocalVocab.getAllWordsForTesting();
  EXPECT_TRUE(std::find(words.begin(), words.end(), testEntry) != words.end());
}

// Test cloneImpl functionality
TEST_P(ExplicitIdTableOperationTest, CloneImpl) {
  LocalVocab localVocab;
  LocalVocabEntry testEntry = LocalVocabEntry::fromStringRepresentation(
      "\"clone_test\"", qec_->getLocalVocabContext());
  localVocab.getIndexAndAddIfNotContained(testEntry);

  ExplicitIdTableOperation original(qec_, wrapTable(testTable_), testVariables_,
                                    testSortedColumns_, std::move(localVocab),
                                    testCacheKey_);

  auto cloned = original.cloneImpl();
  auto* clonedOp = dynamic_cast<ExplicitIdTableOperation*>(cloned.get());

  ASSERT_NE(clonedOp, nullptr);

  // Test that cloned operation has same properties
  EXPECT_EQ(clonedOp->sizeEstimate(), original.sizeEstimate());
  EXPECT_EQ(clonedOp->getResultWidth(), original.getResultWidth());
  EXPECT_EQ(clonedOp->getCostEstimate(), original.getCostEstimate());
  EXPECT_EQ(clonedOp->getDescriptor(), original.getDescriptor());
  EXPECT_THAT(clonedOp->resultSortedOn(), ElementsAre(ColumnIndex{0}));

  // Test that cloned operation produces same result
  auto originalResult = original.computeResult(false);
  auto clonedResult = clonedOp->computeResult(false);

  EXPECT_THAT(clonedResult.idTableView(),
              matchesIdTable(originalResult.idTableView()));

  // Test that local vocab is cloned properly
  const auto& originalLocalVocab = originalResult.localVocab();
  const auto& clonedLocalVocab = clonedResult.localVocab();
  auto originalWords = originalLocalVocab.getAllWordsForTesting();
  auto clonedWords = clonedLocalVocab.getAllWordsForTesting();
  EXPECT_TRUE(ad_utility::contains(originalWords, testEntry));
  EXPECT_TRUE(ad_utility::contains(clonedWords, testEntry));
}

TEST_P(ExplicitIdTableOperationTest, ConstructionWithSortedColumns) {
  std::vector<ColumnIndex> sortedCols = {ColumnIndex{1}, ColumnIndex{0}};
  ExplicitIdTableOperation op(qec_, wrapTable(testTable_), testVariables_,
                              sortedCols, LocalVocab{}, testCacheKey_);

  EXPECT_THAT(op.resultSortedOn(), ElementsAre(ColumnIndex{1}, ColumnIndex{0}));
}

// Test with different table sizes
TEST_P(ExplicitIdTableOperationTest, DifferentTableSizes) {
  // Test with single row
  auto singleRowTable = createTestIdTable(1, 3);
  auto singleRowVars = createTestVariableMap(3);
  ExplicitIdTableOperation singleRowOp(qec_, wrapTable(singleRowTable),
                                       singleRowVars, {}, {}, testCacheKey_);

  EXPECT_EQ(singleRowOp.sizeEstimate(), 1u);
  EXPECT_EQ(singleRowOp.getResultWidth(), 3u);
  EXPECT_FALSE(singleRowOp.knownEmptyResult());

  // Test with many rows
  auto largeTable = createTestIdTable(100, 1);
  auto largeTableVars = createTestVariableMap(1);
  ExplicitIdTableOperation largeOp(qec_, wrapTable(largeTable), largeTableVars,
                                   {}, {}, testCacheKey_);

  EXPECT_EQ(largeOp.sizeEstimate(), 100u);
  EXPECT_EQ(largeOp.getResultWidth(), 1u);
  EXPECT_FALSE(largeOp.knownEmptyResult());
}

// Test variable to column mapping
TEST_P(ExplicitIdTableOperationTest, VariableToColumnMapping) {
  VariableToColumnMap customVars;
  customVars[Variable("?subject")] = makeAlwaysDefinedColumn(0);
  customVars[Variable("?predicate")] = makeAlwaysDefinedColumn(1);

  ExplicitIdTableOperation op(qec_, wrapTable(testTable_), customVars, {}, {},
                              testCacheKey_);

  auto computedVars = op.computeVariableToColumnMap();
  EXPECT_EQ(computedVars.size(), 2u);
  EXPECT_TRUE(computedVars.contains(Variable("?subject")));
  EXPECT_TRUE(computedVars.contains(Variable("?predicate")));
  EXPECT_EQ(computedVars.at(Variable("?subject")).columnIndex_, ColumnIndex{0});
  EXPECT_EQ(computedVars.at(Variable("?predicate")).columnIndex_,
            ColumnIndex{1});
}
}  // namespace
