// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/ConstructBatchEvaluator.h"

using namespace qlever::constructExport;

namespace {

// Test fixture that sets up a small index and helper functions.
class ConstructBatchEvaluatorTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ =
      ad_utility::testing::getQec("<s> <p> <o> . <s> <q> \"hello\" .");
  const Index& index_ = qec_->getIndex();
  std::function<Id(const std::string&)> getId_ =
      ad_utility::testing::makeGetId(index_);
  LocalVocab localVocab_;

  // Create an IdTable from a vector of Id vectors.
  IdTable makeIdTable(const std::vector<std::vector<Id>>& rows) {
    size_t numCols = rows.empty() ? 0 : rows[0].size();
    IdTable table{numCols, ad_utility::testing::makeAllocator()};
    for (const auto& row : rows) {
      table.emplace_back();
      for (size_t i = 0; i < numCols; ++i) {
        table.back()[i] = row[i];
      }
    }
    return table;
  }

  // Helper to createAndEvaluateBatch a batch and return the result.
  BatchEvaluationResult createAndEvaluateBatch(
      const std::vector<size_t>& uniqueVariableColumns, const IdTable& idTable,
      const std::vector<uint64_t>& rowIndices,
      ConstructBatchEvaluator::IdCache& idCache, size_t rowOffset = 0) {
    BatchEvaluationContext ctx{idTable, localVocab_,
                               ql::span<const uint64_t>(rowIndices), rowOffset};
    return ConstructBatchEvaluator::evaluateBatch(uniqueVariableColumns, ctx,
                                                  index_, idCache);
  }
};

TEST_F(ConstructBatchEvaluatorTest, singleVariableSingleRow) {
  Id idS = getId_("<s>");
  auto idTable = makeIdTable({{idS}});
  std::vector<uint64_t> rowIndices = {0};
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = createAndEvaluateBatch({0}, idTable, rowIndices, idCache);

  ASSERT_EQ(result.numRows_, 1);
  ASSERT_EQ(result.variablesByColumn_.size(), 1);
  ASSERT_TRUE(result.variablesByColumn_.contains(0));

  const auto& val = result.getVariable(0, 0);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val.value(), "<s>");
}

TEST_F(ConstructBatchEvaluatorTest, singleVariableMultipleRows) {
  Id idS = getId_("<s>");
  Id idO = getId_("<o>");
  auto idTable = makeIdTable({{idS}, {idO}});
  std::vector<uint64_t> rowIndices = {0, 1};
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = createAndEvaluateBatch({0}, idTable, rowIndices, idCache);

  ASSERT_EQ(result.numRows_, 2);

  const auto& val0 = result.getVariable(0, 0);
  ASSERT_TRUE(val0.has_value());
  EXPECT_EQ(*val0.value(), "<s>");

  const auto& val1 = result.getVariable(0, 1);
  ASSERT_TRUE(val1.has_value());
  EXPECT_EQ(*val1.value(), "<o>");
}

TEST_F(ConstructBatchEvaluatorTest, multipleVariablesMultipleRows) {
  Id idS = getId_("<s>");
  Id idP = getId_("<p>");
  Id idO = getId_("<o>");
  Id idQ = getId_("<q>");
  auto idTable = makeIdTable({{idS, idP}, {idO, idQ}});
  std::vector<uint64_t> rowIndices = {0, 1};
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = createAndEvaluateBatch({0, 1}, idTable, rowIndices, idCache);

  ASSERT_EQ(result.numRows_, 2);
  ASSERT_EQ(result.variablesByColumn_.size(), 2);

  // Column 0, row 0: <s>
  ASSERT_TRUE(result.getVariable(0, 0).has_value());
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
  // Column 0, row 1: <o>
  ASSERT_TRUE(result.getVariable(0, 1).has_value());
  EXPECT_EQ(*result.getVariable(0, 1).value(), "<o>");
  // Column 1, row 0: <p>
  ASSERT_TRUE(result.getVariable(1, 0).has_value());
  EXPECT_EQ(*result.getVariable(1, 0).value(), "<p>");
  // Column 1, row 1: <q>
  ASSERT_TRUE(result.getVariable(1, 1).has_value());
  EXPECT_EQ(*result.getVariable(1, 1).value(), "<q>");
}

TEST_F(ConstructBatchEvaluatorTest, undefinedIdReturnsNullopt) {
  Id undefinedId = Id::makeUndefined();
  auto idTable = makeIdTable({{undefinedId}});
  std::vector<uint64_t> rowIndices = {0};
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = createAndEvaluateBatch({0}, idTable, rowIndices, idCache);

  ASSERT_EQ(result.numRows_, 1);
  const auto& val = result.getVariable(0, 0);
  EXPECT_FALSE(val.has_value());
}

TEST_F(ConstructBatchEvaluatorTest, cacheHitsTracked) {
  // Use the same ID in multiple rows to trigger cache hits.
  Id idS = getId_("<s>");
  auto idTable = makeIdTable({{idS}, {idS}, {idS}});
  std::vector<uint64_t> rowIndices = {0, 1, 2};
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = createAndEvaluateBatch({0}, idTable, rowIndices, idCache);

  ASSERT_EQ(result.numRows_, 3);
  // First lookup is a miss, remaining two are hits.
  EXPECT_EQ(idCache.stats().misses_, 1);
  EXPECT_EQ(idCache.stats().hits_, 2);

  // All three results should be the same.
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(result.getVariable(0, i).has_value());
    EXPECT_EQ(*result.getVariable(0, i).value(), "<s>");
  }
}

TEST_F(ConstructBatchEvaluatorTest, emptyBatch) {
  auto idTable = makeIdTable({});
  std::vector<uint64_t> rowIndices = {};
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = createAndEvaluateBatch({}, idTable, rowIndices, idCache);

  EXPECT_EQ(result.numRows_, 0);
  EXPECT_TRUE(result.variablesByColumn_.empty());
}

TEST_F(ConstructBatchEvaluatorTest, noVariableColumns) {
  Id idS = getId_("<s>");
  auto idTable = makeIdTable({{idS}, {idS}});
  std::vector<uint64_t> rowIndices = {0, 1};
  ConstructBatchEvaluator::IdCache idCache{1024};

  // Non-empty batch but no variable columns to createAndEvaluateBatch.
  auto result = createAndEvaluateBatch({}, idTable, rowIndices, idCache);

  EXPECT_EQ(result.numRows_, 2);
  EXPECT_TRUE(result.variablesByColumn_.empty());
}

}  // namespace
