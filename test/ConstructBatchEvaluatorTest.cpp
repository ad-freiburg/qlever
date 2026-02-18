// Copyright 2025 The QLever Authors, in particular:
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/ConstructBatchEvaluator.h"

using namespace qlever::constructExport;

namespace {

// =============================================================================
// Test fixture.
// Builds a small index from:
//   <s> <p> <o> .
//   <s> <q> "hello" .
//
// This gives us five vocabulary entries: the IRIs <s>, <p>, <o>, <q> and the
// literal "hello". The fixture provides helpers to build `IdTable`s and run the
// batch evaluator against them.
class ConstructBatchEvaluatorTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ =
      ad_utility::testing::getQec("<s> <p> <o> . <s> <q> \"hello\" .");
  const Index& index_ = qec_->getIndex();
  std::function<Id(const std::string&)> getId_ =
      ad_utility::testing::makeGetId(index_);
  LocalVocab localVocab_;

  // Build an `IdTable` from explicit rows of `Id`s. Each inner vector is one
  // row; all rows must have the same number of columns.
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

  // Evaluate the full `IdTable` (all rows) with the given variable columns in
  // one single batch.
  BatchEvaluationResult evaluateFullTable(
      const std::vector<size_t>& uniqueVariableColumns, const IdTable& idTable,
      ConstructBatchEvaluator::IdCache& idCache) {
    BatchEvaluationContext ctx{idTable, localVocab_, 0, idTable.numRows()};
    return ConstructBatchEvaluator::evaluateBatch(uniqueVariableColumns, ctx,
                                                  index_, idCache);
  }

  // Evaluate a sub-range [`firstRow`, `endRow`) of the `IdTable`.
  BatchEvaluationResult evaluateRowRange(
      const std::vector<size_t>& uniqueVariableColumns, const IdTable& idTable,
      size_t firstRow, size_t endRow,
      ConstructBatchEvaluator::IdCache& idCache) {
    BatchEvaluationContext ctx{idTable, localVocab_, firstRow, endRow};
    return ConstructBatchEvaluator::evaluateBatch(uniqueVariableColumns, ctx,
                                                  index_, idCache);
  }
};

// The simplest case: one variable column, one row. Verify that the evaluator
// resolves the id to the expected IRI string and that the result structure
// (`BatchEvaluationResult`) is correctly shaped (one column entry, one row).
TEST_F(ConstructBatchEvaluatorTest, singleVariableSingleRow) {
  auto idTable = makeIdTable({{getId_("<s>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  ASSERT_EQ(result.variablesByColumn_.size(), 1);
  ASSERT_TRUE(result.variablesByColumn_.contains(0));
  ASSERT_TRUE(result.getVariable(0, 0).has_value());
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
}

// =============================================================================
// Multiple rows in a single column
// =============================================================================

// Two rows with different IRIs in the same column. Verify that each row is
// independently resolved and that the results are in row order.
TEST_F(ConstructBatchEvaluatorTest, singleVariableMultipleRows) {
  auto idTable = makeIdTable({{getId_("<s>")}, {getId_("<o>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 2);
  ASSERT_TRUE(result.getVariable(0, 0).has_value());
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
  ASSERT_TRUE(result.getVariable(0, 1).has_value());
  EXPECT_EQ(*result.getVariable(0, 1).value(), "<o>");
}

// =============================================================================
// Multiple variable columns across multiple rows
// =============================================================================

// Two variable columns (0 and 1), two rows. This is the typical CONSTRUCT
// pattern where subject and object are both variables. Verify that all four
// (column, row) combinations are correctly resolved.
TEST_F(ConstructBatchEvaluatorTest, multipleVariablesMultipleRows) {
  //              col 0    col 1
  // row 0:       <s>      <p>
  // row 1:       <o>      <q>
  auto idTable = makeIdTable(
      {{getId_("<s>"), getId_("<p>")}, {getId_("<o>"), getId_("<q>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0, 1}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 2);
  ASSERT_EQ(result.variablesByColumn_.size(), 2);
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
  EXPECT_EQ(*result.getVariable(0, 1).value(), "<o>");
  EXPECT_EQ(*result.getVariable(1, 0).value(), "<p>");
  EXPECT_EQ(*result.getVariable(1, 1).value(), "<q>");
}

// =============================================================================
// Only a subset of columns are variable columns
// =============================================================================

// The IdTable has 3 columns, but only columns 0 and 2 are variables (column 1
// is a constant in the CONSTRUCT template and is not listed). Verify that the
// result only contains entries for the requested columns.
TEST_F(ConstructBatchEvaluatorTest, evaluatesOnlyRequestedColumns) {
  auto idTable = makeIdTable({{getId_("<s>"), getId_("<p>"), getId_("<o>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0, 2}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  ASSERT_EQ(result.variablesByColumn_.size(), 2);
  EXPECT_TRUE(result.variablesByColumn_.contains(0));
  EXPECT_TRUE(result.variablesByColumn_.contains(2));
  EXPECT_FALSE(result.variablesByColumn_.contains(1));
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
  EXPECT_EQ(*result.getVariable(2, 0).value(), "<o>");
}

// =============================================================================
// Unbound variables produce std::nullopt
// =============================================================================

// In CONSTRUCT queries, a variable can be unbound for a given row — for example
// when using OPTIONAL patterns or UNIONs where different branches bind
// different variables. The IdTable stores an undefined Id for such rows.
// The evaluator must return std::nullopt for these entries.
TEST_F(ConstructBatchEvaluatorTest, undefinedIdReturnsNullopt) {
  auto idTable = makeIdTable({{Id::makeUndefined()}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  EXPECT_FALSE(result.getVariable(0, 0).has_value());
}

// Unbound variables mixed with bound variables in the same column. Only the
// unbound rows produce nullopt; bound rows are resolved normally.
TEST_F(ConstructBatchEvaluatorTest, undefinedMixedWithValidIds) {
  auto idTable =
      makeIdTable({{getId_("<s>")}, {Id::makeUndefined()}, {getId_("<o>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 3);
  ASSERT_TRUE(result.getVariable(0, 0).has_value());
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
  EXPECT_FALSE(result.getVariable(0, 1).has_value());
  ASSERT_TRUE(result.getVariable(0, 2).has_value());
  EXPECT_EQ(*result.getVariable(0, 2).value(), "<o>");
}

// =============================================================================
// Cache behavior
// =============================================================================

// When the same Id appears in multiple rows, the first occurrence is a cache
// miss (triggers evaluateId) and subsequent occurrences are cache hits (the
// cached value is reused). Verify the cache statistics reflect this.
TEST_F(ConstructBatchEvaluatorTest, repeatedIdsCausesCacheHits) {
  Id idS = getId_("<s>");
  auto idTable = makeIdTable({{idS}, {idS}, {idS}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 3);
  // One miss for the first occurrence, two hits for the repeats.
  EXPECT_EQ(idCache.stats().misses_, 1);
  EXPECT_EQ(idCache.stats().hits_, 2);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(result.getVariable(0, i).has_value());
    EXPECT_EQ(*result.getVariable(0, i).value(), "<s>");
  }
}

// The cache is shared across calls to evaluateBatch. Verify that Ids resolved
// in a first batch are cache hits in a second batch.
TEST_F(ConstructBatchEvaluatorTest, cacheIsSharedAcrossBatches) {
  Id idS = getId_("<s>");
  Id idO = getId_("<o>");
  auto idTable = makeIdTable({{idS}, {idO}, {idS}, {idO}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  // First batch: rows [0, 2).
  auto result1 = evaluateRowRange({0}, idTable, 0, 2, idCache);
  ASSERT_EQ(result1.numRows_, 2);
  EXPECT_EQ(idCache.stats().misses_, 2);  // <s> and <o> are new
  EXPECT_EQ(idCache.stats().hits_, 0);

  // Second batch: rows [2, 4). Same Ids, so both should be cache hits.
  auto result2 = evaluateRowRange({0}, idTable, 2, 4, idCache);
  ASSERT_EQ(result2.numRows_, 2);
  EXPECT_EQ(idCache.stats().misses_, 2);  // unchanged
  EXPECT_EQ(idCache.stats().hits_, 2);    // two hits from the second batch

  // Verify values are correct in both batches.
  EXPECT_EQ(*result1.getVariable(0, 0).value(), "<s>");
  EXPECT_EQ(*result1.getVariable(0, 1).value(), "<o>");
  EXPECT_EQ(*result2.getVariable(0, 0).value(), "<s>");
  EXPECT_EQ(*result2.getVariable(0, 1).value(), "<o>");
}

// =============================================================================
// Row range: evaluating a sub-range of the IdTable
// =============================================================================

// When [firstRow_, endRow_) is a strict subset of the IdTable, only those rows
// are evaluated. The result indices are 0-based relative to firstRow_.
TEST_F(ConstructBatchEvaluatorTest, subRangeEvaluatesCorrectRows) {
  // 4 rows, but we only evaluate rows [1, 3).
  auto idTable = makeIdTable(
      {{getId_("<s>")}, {getId_("<p>")}, {getId_("<o>")}, {getId_("<q>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateRowRange({0}, idTable, 1, 3, idCache);

  ASSERT_EQ(result.numRows_, 2);
  // Row 1 of the IdTable -> result index 0.
  ASSERT_TRUE(result.getVariable(0, 0).has_value());
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<p>");
  // Row 2 of the IdTable -> result index 1.
  ASSERT_TRUE(result.getVariable(0, 1).has_value());
  EXPECT_EQ(*result.getVariable(0, 1).value(), "<o>");
}

// =============================================================================
// Literal strings
// =============================================================================

// The dataset contains the literal "hello". Verify that literals are resolved
// to their string representation (including quotes) and not treated as IRIs.
TEST_F(ConstructBatchEvaluatorTest, literalIsResolvedCorrectly) {
  auto idTable = makeIdTable({{getId_("\"hello\"")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  ASSERT_TRUE(result.getVariable(0, 0).has_value());
  EXPECT_EQ(*result.getVariable(0, 0).value(), "\"hello\"");
}

// A column with mixed IRIs and a literal. Verify that each row is resolved
// with the correct type.
TEST_F(ConstructBatchEvaluatorTest, mixedIriAndLiteralColumn) {
  auto idTable =
      makeIdTable({{getId_("<s>")}, {getId_("\"hello\"")}, {getId_("<o>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 3);
  ASSERT_TRUE(result.getVariable(0, 0).has_value());
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
  ASSERT_TRUE(result.getVariable(0, 1).has_value());
  EXPECT_EQ(*result.getVariable(0, 1).value(), "\"hello\"");
  ASSERT_TRUE(result.getVariable(0, 2).has_value());
  EXPECT_EQ(*result.getVariable(0, 2).value(), "<o>");
}

// =============================================================================
// Edge cases: empty inputs
// =============================================================================

// Empty batch (zero rows). The result should have numRows_ == 0 and no column
// entries, since there is nothing to evaluate.
TEST_F(ConstructBatchEvaluatorTest, emptyBatch) {
  auto idTable = makeIdTable({});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({}, idTable, idCache);

  EXPECT_EQ(result.numRows_, 0);
  EXPECT_TRUE(result.variablesByColumn_.empty());
}

// Non-empty IdTable but no variable columns requested. This happens when all
// positions in the CONSTRUCT template are constants. The result should reflect
// the row count but contain no column data.
TEST_F(ConstructBatchEvaluatorTest, noVariableColumns) {
  auto idTable = makeIdTable({{getId_("<s>")}, {getId_("<o>")}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  auto result = evaluateFullTable({}, idTable, idCache);

  EXPECT_EQ(result.numRows_, 2);
  EXPECT_TRUE(result.variablesByColumn_.empty());
}

// =============================================================================
// Realistic scenario: simulates a CONSTRUCT query result
// =============================================================================
//
// Simulates the IdTable that would result from:
//   CONSTRUCT { ?s <p> ?o } WHERE { ?s <p> ?o }
// against a dataset with repeated subjects. The IdTable has two variable
// columns (subject at 0, object at 1) and a constant predicate column (not
// evaluated). Multiple rows share the same subject, exercising the cache.
TEST_F(ConstructBatchEvaluatorTest, realisticConstructPattern) {
  Id idS = getId_("<s>");
  Id idP = getId_("<p>");
  Id idO = getId_("<o>");
  Id idQ = getId_("<q>");

  //              col 0 (?s)  col 1 (<p>)  col 2 (?o)
  // row 0:       <s>         <p>          <o>
  // row 1:       <s>         <p>          <q>
  // row 2:       <o>         <p>          <s>
  // row 3:       <s>         <p>          <o>    (duplicate of row 0)
  auto idTable = makeIdTable(
      {{idS, idP, idO}, {idS, idP, idQ}, {idO, idP, idS}, {idS, idP, idO}});
  ConstructBatchEvaluator::IdCache idCache{1024};

  // Only columns 0 and 2 are variables; column 1 is the constant predicate.
  auto result = evaluateFullTable({0, 2}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 4);
  ASSERT_EQ(result.variablesByColumn_.size(), 2);

  // Column 0 (?s): <s>, <s>, <o>, <s>
  EXPECT_EQ(*result.getVariable(0, 0).value(), "<s>");
  EXPECT_EQ(*result.getVariable(0, 1).value(), "<s>");
  EXPECT_EQ(*result.getVariable(0, 2).value(), "<o>");
  EXPECT_EQ(*result.getVariable(0, 3).value(), "<s>");

  // Column 2 (?o): <o>, <q>, <s>, <o>
  EXPECT_EQ(*result.getVariable(2, 0).value(), "<o>");
  EXPECT_EQ(*result.getVariable(2, 1).value(), "<q>");
  EXPECT_EQ(*result.getVariable(2, 2).value(), "<s>");
  EXPECT_EQ(*result.getVariable(2, 3).value(), "<o>");

  // Cache statistics: 4 unique Ids (<s>, <o>, <q>, <p> — but <p> is in
  // column 1 which we don't evaluate). So 3 unique Ids across columns 0 and 2.
  // Column 0: <s>(miss), <s>(hit), <o>(miss), <s>(hit) = 2 misses, 2 hits
  // Column 2: <o>(hit), <q>(miss), <s>(hit), <o>(hit)  = 1 miss,  3 hits
  // Total: 3 misses, 5 hits.
  EXPECT_EQ(idCache.stats().misses_, 3);
  EXPECT_EQ(idCache.stats().hits_, 5);
}

}  // namespace
