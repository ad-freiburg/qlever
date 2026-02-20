// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/IdTableHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/ConstructBatchEvaluator.h"

namespace {

using namespace qlever::constructExport;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Optional;
using ::testing::Pointee;

// Matcher for `std::optional<std::shared_ptr<const std::string>>`:
// asserts the optional is non-empty and the pointed-to string equals
// `expected`.
static constexpr auto evalTerm = [](const std::string& expected) {
  return Optional(Pointee(Eq(expected)));
};

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

  Id getId_(const std::string& s) const {
    return ad_utility::testing::makeGetId(index_)(s);
  }

  Id idS_ = getId_("<s>");
  Id idP_ = getId_("<p>");
  Id idO_ = getId_("<o>");
  Id idQ_ = getId_("<q>");

  LocalVocab localVocab_;

  // Evaluate all rows of the `IdTable` with the given variable columns in
  // one single batch.
  BatchEvaluationResult evaluateIdTable(
      const std::vector<size_t>& variableColumnIndices, const IdTable& idTable,
      IdCache& idCache) {
    BatchEvaluationContext ctx{idTable, 0, idTable.numRows()};
    return ConstructBatchEvaluator::evaluateBatch(variableColumnIndices, ctx,
                                                  localVocab_, index_, idCache);
  }

  // Evaluate a sub-range [`firstRow`, `endRow`) of the `IdTable`.
  BatchEvaluationResult evaluateRowRange(
      const std::vector<size_t>& variableColumnIndices, const IdTable& idTable,
      size_t firstRow, size_t endRow, IdCache& idCache) {
    BatchEvaluationContext ctx{idTable, firstRow, endRow};
    return ConstructBatchEvaluator::evaluateBatch(variableColumnIndices, ctx,
                                                  localVocab_, index_, idCache);
  }
};

// The simplest case: `IdTable` consists of one variable column, one row. Verify
// that the evaluator resolves the id to the expected IRI string and that the
// result structure (`BatchEvaluationResult`) is correctly shaped (one column
// entry, one row).
TEST_F(ConstructBatchEvaluatorTest, singleVariableSingleRow) {
  auto idTable = makeIdTableFromVector({{idS_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  ASSERT_EQ(result.variablesByColumn_.size(), 1);
  ASSERT_TRUE(result.variablesByColumn_.contains(0));
  EXPECT_THAT(result.getVariable(0, 0), evalTerm("<s>"));
}

// Two rows with different `Iri`s in the same column (i.e. with different `Iri`s
// for the same variable across different `IdTable` rows). Verify that each row
// is independently resolved and that the results for a specific variable are in
// row order.
TEST_F(ConstructBatchEvaluatorTest, singleVariableMultipleRows) {
  auto idTable = makeIdTableFromVector({{idS_}, {idO_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 2);
  EXPECT_THAT(result.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<s>"), evalTerm("<o>")));
}

// Two variable columns (0 and 1), two rows. This is the typical CONSTRUCT
// pattern where subject and object are both variables. Verify that all four
// (column, row) combinations are correctly resolved.
TEST_F(ConstructBatchEvaluatorTest, multipleVariablesMultipleRows) {
  //              col 0    col 1
  // row 0:       <s>      <p>
  // row 1:       <o>      <q>
  auto idTable = makeIdTableFromVector({{idS_, idP_}, {idO_, idQ_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0, 1}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 2);
  EXPECT_THAT(result.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<s>"), evalTerm("<o>")));
  EXPECT_THAT(result.variablesByColumn_.at(1),
              ElementsAre(evalTerm("<p>"), evalTerm("<q>")));
}

// The IdTable has 3 columns, but only columns 0 and 2 are variables (column 1
// is a constant in the CONSTRUCT template and is not listed). Verify that the
// result only contains entries for the requested columns.
TEST_F(ConstructBatchEvaluatorTest, evaluatesOnlyRequestedColumns) {
  auto idTable = makeIdTableFromVector({{idS_, idP_, idO_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0, 2}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  ASSERT_EQ(result.variablesByColumn_.size(), 2);
  EXPECT_TRUE(result.variablesByColumn_.contains(0));
  EXPECT_TRUE(result.variablesByColumn_.contains(2));
  EXPECT_FALSE(result.variablesByColumn_.contains(1));
  EXPECT_THAT(result.getVariable(0, 0), evalTerm("<s>"));
  EXPECT_THAT(result.getVariable(2, 0), evalTerm("<o>"));
}

// An unbound variable is represented in the `IdTable` as an undefined `Id`.
// Verify that the evaluator returns nullopt for such entries.
TEST_F(ConstructBatchEvaluatorTest, undefinedIdReturnsNullopt) {
  auto idTable = makeIdTableFromVector({{Id::makeUndefined()}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  EXPECT_THAT(result.getVariable(0, 0), Eq(std::nullopt));
}

// A single variable column where some rows hold a defined `Id` and one row
// holds an undefined `Id`. Verify that only the undefined row produces nullopt,
// = while the defined rows are resolved normally.
TEST_F(ConstructBatchEvaluatorTest, undefinedMixedWithValidIds) {
  auto idTable = makeIdTableFromVector({{idS_}, {Id::makeUndefined()}, {idO_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 3);
  EXPECT_THAT(result.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<s>"), Eq(std::nullopt), evalTerm("<o>")));
}

// When the same `Id` appears in multiple rows of a single variable column,
// all rows must resolve to the same string. Verify that repeated `Id`s produce
// consistent results.
TEST_F(ConstructBatchEvaluatorTest, repeatedIdsProduceConsistentResults) {
  auto idTable = makeIdTableFromVector({{idS_}, {idS_}, {idS_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 3);
  EXPECT_THAT(result.variablesByColumn_.at(0), Each(evalTerm("<s>")));
}

// The same `IdCache` instance is passed to multiple `evaluateBatch` calls.
// Verify that the evaluator produces correct results across both batches.
TEST_F(ConstructBatchEvaluatorTest,
       correctResultsWhenSameIdCacheUsedAcrossBatches) {
  auto idTable = makeIdTableFromVector({{idS_}, {idO_}, {idS_}, {idO_}});
  IdCache idCache{1024};

  // First batch: rows [0, 2).
  auto result1 = evaluateRowRange({0}, idTable, 0, 2, idCache);
  ASSERT_EQ(result1.numRows_, 2);
  EXPECT_THAT(result1.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<s>"), evalTerm("<o>")));

  // Second batch: rows [2, 4).
  auto result2 = evaluateRowRange({0}, idTable, 2, 4, idCache);
  ASSERT_EQ(result2.numRows_, 2);
  EXPECT_THAT(result2.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<s>"), evalTerm("<o>")));
}

// When [firstRow_, endRow_) is a strict subset of the `IdTable`, only those
// rows are evaluated. The result indices are 0-based relative to firstRow_.
TEST_F(ConstructBatchEvaluatorTest, subRangeEvaluatesCorrectRows) {
  // 4 rows, but we only evaluate rows [1, 3).
  auto idTable = makeIdTableFromVector({{idS_}, {idP_}, {idO_}, {idQ_}});
  IdCache idCache{1024};

  auto result = evaluateRowRange({0}, idTable, 1, 3, idCache);

  ASSERT_EQ(result.numRows_, 2);
  // Row 1 of the IdTable -> result index 0; row 2 -> result index 1.
  EXPECT_THAT(result.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<p>"), evalTerm("<o>")));
}

// The dataset contains the literal "hello". Verify that literals are resolved
// to their string representation (including quotes) and not treated as IRIs.
TEST_F(ConstructBatchEvaluatorTest, literalIsResolvedCorrectly) {
  auto idTable = makeIdTableFromVector({{getId_("\"hello\"")}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 1);
  EXPECT_THAT(result.getVariable(0, 0), evalTerm("\"hello\""));
}

// A column with mixed IRIs and a literal. Verify that each row is resolved with
// the correct type.
TEST_F(ConstructBatchEvaluatorTest, mixedIriAndLiteralColumn) {
  auto idTable = makeIdTableFromVector({{idS_}, {getId_("\"hello\"")}, {idO_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 3);
  EXPECT_THAT(
      result.variablesByColumn_.at(0),
      ElementsAre(evalTerm("<s>"), evalTerm("\"hello\""), evalTerm("<o>")));
}

// Empty batch (zero rows). The result should have `numRows_` == 0 and no column
// entries, since there is nothing to evaluate.
TEST_F(ConstructBatchEvaluatorTest, emptyBatch) {
  auto idTable = makeIdTableFromVector({});  // empty
  IdCache idCache{1024};

  auto result = evaluateIdTable({}, idTable, idCache);

  EXPECT_EQ(result.numRows_, 0);
  EXPECT_TRUE(result.variablesByColumn_.empty());
}

// Non-empty `IdTable` but no variable columns requested. This happens when all
// positions in the CONSTRUCT template are constants. The result should reflect
// the row count but contain no column data.
TEST_F(ConstructBatchEvaluatorTest, noVariableColumns) {
  auto idTable = makeIdTableFromVector({{idS_}, {idO_}});
  IdCache idCache{1024};

  auto result = evaluateIdTable({}, idTable, idCache);

  EXPECT_EQ(result.numRows_, 2);
  EXPECT_TRUE(result.variablesByColumn_.empty());
}

// Simulates the `IdTable` that would result from:
//   CONSTRUCT { ?s <p> ?o } WHERE { ?s <p> ?o }
// against a dataset with repeated subjects. The `IdTable` has two variable
// columns (subject at 0, object at 1) and a constant predicate column (not
// evaluated). Multiple rows share the same subject, exercising the cache.
TEST_F(ConstructBatchEvaluatorTest, realisticConstructPattern) {
  auto idTable = makeIdTableFromVector({
      {idS_, idP_, idO_},
      {idS_, idP_, idQ_},
      {idO_, idP_, idS_},
      {idS_, idP_, idO_}  // duplicate of row 0
  });
  IdCache idCache{1024};

  // Only columns 0 and 2 are variables; column 1 is the constant predicate.
  auto result = evaluateIdTable({0, 2}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 4);
  ASSERT_EQ(result.variablesByColumn_.size(), 2);

  // Column 0 (?s): <s>, <s>, <o>, <s>
  EXPECT_THAT(result.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<s>"), evalTerm("<s>"), evalTerm("<o>"),
                          evalTerm("<s>")));

  // Column 2 (?o): <o>, <q>, <s>, <o>
  EXPECT_THAT(result.variablesByColumn_.at(2),
              ElementsAre(evalTerm("<o>"), evalTerm("<q>"), evalTerm("<s>"),
                          evalTerm("<o>")));
}

// With a cache of size 1, every access to a different `Id` evicts the previous
// entry. Verify that the evaluator still resolves all rows correctly despite
// constant evictions.
TEST_F(ConstructBatchEvaluatorTest, cacheOfSizeOneStillProducesCorrectResults) {
  // table with 1 column and 4 rows.
  auto idTable = makeIdTableFromVector({{idS_}, {idO_}, {idP_}, {idQ_}});
  IdCache idCache{1};

  auto result = evaluateIdTable({0}, idTable, idCache);

  ASSERT_EQ(result.numRows_, 4);
  EXPECT_THAT(result.variablesByColumn_.at(0),
              ElementsAre(evalTerm("<s>"), evalTerm("<o>"), evalTerm("<p>"),
                          evalTerm("<q>")));
}

}  // namespace
