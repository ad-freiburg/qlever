// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/IdTableHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/TableWithRangeEvaluator.h"
#include "util/CancellationHandle.h"

namespace {

using namespace qlever::constructExport;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Field;
using ::testing::Pointee;

// Matcher for EvaluatedTriple: checks all three string values by pointer.
static auto matchTriple(const std::string& s, const std::string& p,
                        const std::string& o) {
  return AllOf(AD_FIELD(EvaluatedTriple, subject_,
                        Pointee(AD_FIELD(EvaluatedTermData, str, Eq(s)))),
               AD_FIELD(EvaluatedTriple, predicate_,
                        Pointee(AD_FIELD(EvaluatedTermData, str, Eq(p)))),
               AD_FIELD(EvaluatedTriple, object_,
                        Pointee(AD_FIELD(EvaluatedTermData, str, Eq(o)))));
}

// Drain all triples from a TableWithRangeEvaluator into a vector.
static std::vector<EvaluatedTriple> collectAll(TableWithRangeEvaluator& proc) {
  std::vector<EvaluatedTriple> result;
  while (auto triple = proc.get()) {
    result.push_back(std::move(*triple));
  }
  return result;
}

// =============================================================================
// Test fixture.
// Builds a small index from:
//   <s> <p> <o> .
//   <s> <q> "hello" .
//
// Provides helpers to build IdTables, templates, and TableWithRange values.
// =============================================================================

class EvaluatedTripleIteratorTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ =
      ad_utility::testing::getQec("<s> <p> <o> . <s> <q> \"hello\" .");
  const Index& index_ = qec_->getIndex();
  LocalVocab localVocab_;

  Id idS_ = ad_utility::testing::makeGetId(index_)("<s>");
  Id idP_ = ad_utility::testing::makeGetId(index_)("<p>");
  Id idO_ = ad_utility::testing::makeGetId(index_)("<o>");
  Id idQ_ = ad_utility::testing::makeGetId(index_)("<q>");

  // Create a non-cancelled CancellationHandle.
  static ad_utility::SharedCancellationHandle makeHandle() {
    return std::make_shared<
        ad_utility::SharedCancellationHandle::element_type>();
  }

  // Wrap an IdTable in a TableWithRange covering rows [start, end).
  TableWithRange makeRange(const IdTable& table, uint64_t start, uint64_t end) {
    return {TableConstRefWithVocab{table, localVocab_},
            ql::views::iota(start, end)};
  }

  // Build a PreprocessedTriple from three terms.
  static PreprocessedTriple triple(PreprocessedTerm s, PreprocessedTerm p,
                                   PreprocessedTerm o) {
    return {s, p, o};
  }

  // Shorthand constructors for the three PreprocessedTerm variants.
  static PreprocessedTerm Const(std::string v) {
    return PrecomputedConstant{std::make_shared<const EvaluatedTermData>(
        EvaluatedTermData{std::move(v), nullptr})};
  }
  static PreprocessedTerm Var(size_t col) { return PrecomputedVariable{col}; }
  static PreprocessedTerm Bnode(std::string prefix, std::string suffix) {
    return PrecomputedBlankNode{std::move(prefix), std::move(suffix)};
  }

  // Build a PreprocessedConstructTemplate.
  static PreprocessedConstructTemplate makeTemplate(
      std::vector<PreprocessedTriple> triples,
      std::vector<size_t> uniqueCols = {}) {
    return {std::move(triples), std::move(uniqueCols)};
  }
};

// =============================================================================
// Tests
// =============================================================================

// No rows in the view -> no triples emitted, regardless of the template.
TEST_F(EvaluatedTripleIteratorTest, emptyTable) {
  auto idTable = makeIdTableFromVector({});
  auto tmpl = makeTemplate({triple(Const("<s>"), Const("<p>"), Const("<o>"))});
  auto table = makeRange(idTable, 0, 0);

  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 0};

  EXPECT_TRUE(collectAll(proc).empty());
}

// All-constants template: every result row emits one identical triple,
// regardless of IdTable cell contents.
TEST_F(EvaluatedTripleIteratorTest, allConstantsYieldsOneTriplePerRow) {
  auto idTable = makeIdTableFromVector(
      {{Id::makeUndefined()}, {Id::makeUndefined()}, {Id::makeUndefined()}});
  auto tmpl = makeTemplate({triple(Const("<s>"), Const("<p>"), Const("<o>"))});
  auto table = makeRange(idTable, 0, 3);

  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 0};
  auto triples = collectAll(proc);

  EXPECT_THAT(triples, ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                                   matchTriple("<s>", "<p>", "<o>"),
                                   matchTriple("<s>", "<p>", "<o>")));
}

// Variable in subject position: correctly resolved from the IdTable column.
TEST_F(EvaluatedTripleIteratorTest, variableInSubjectResolved) {
  //              col 0
  // row 0:       <s>
  // row 1:       <o>
  auto idTable = makeIdTableFromVector({{idS_}, {idO_}});
  auto tmpl = makeTemplate({triple(Var(0), Const("<p>"), Const("<o>"))}, {0});
  auto table = makeRange(idTable, 0, 2);

  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 0};

  EXPECT_THAT(collectAll(proc), ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                                            matchTriple("<o>", "<p>", "<o>")));
}

// A row where a variable resolves to an undefined Id -> that triple is dropped.
// Rows before and after the undefined row are unaffected.
TEST_F(EvaluatedTripleIteratorTest, undefDropsTriple) {
  auto idTable = makeIdTableFromVector({{idS_}, {Id::makeUndefined()}, {idO_}});
  auto tmpl = makeTemplate({triple(Var(0), Const("<p>"), Const("<o>"))}, {0});
  auto table = makeRange(idTable, 0, 3);

  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 0};

  EXPECT_THAT(collectAll(proc), ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                                            matchTriple("<o>", "<p>", "<o>")));
}

// Multiple template triples: for each result row all template triples are
// emitted in row-major order (all triples for row 0, then row 1, ...).
TEST_F(EvaluatedTripleIteratorTest, multipleTemplateTriples) {
  auto idTable = makeIdTableFromVector({{idS_}, {idO_}});
  auto tmpl = makeTemplate({triple(Var(0), Const("<p>"), Const("<o1>")),
                            triple(Var(0), Const("<q>"), Const("<o2>"))},
                           {0});
  auto table = makeRange(idTable, 0, 2);

  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 0};

  EXPECT_THAT(collectAll(proc), ElementsAre(matchTriple("<s>", "<p>", "<o1>"),
                                            matchTriple("<s>", "<q>", "<o2>"),
                                            matchTriple("<o>", "<p>", "<o1>"),
                                            matchTriple("<o>", "<q>", "<o2>")));
}

// Blank-node row IDs combine currentRowOffset, firstRow, and the in-batch
// row index: blankNodeRowId = currentRowOffset + firstRow + rowInBatch.
TEST_F(EvaluatedTripleIteratorTest, blankNodeUsesCorrectRowId) {
  auto idTable = makeIdTableFromVector(
      {{Id::makeUndefined()}, {Id::makeUndefined()}, {Id::makeUndefined()}});
  // Template: _:u<rowId>_node <p> <o>
  auto tmpl =
      makeTemplate({triple(Bnode("_:u", "_node"), Const("<p>"), Const("<o>"))});

  // View starts at row 1 of the IdTable: firstRow = 1, numRows = 2.
  // currentRowOffset = 10.
  auto table = makeRange(idTable, 1, 3);
  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 10};

  // row 0 of batch -> blankNodeRowId = 10 + 1 + 0 = 11
  // row 1 of batch -> blankNodeRowId = 10 + 1 + 1 = 12
  EXPECT_THAT(collectAll(proc),
              ElementsAre(matchTriple("_:u11_node", "<p>", "<o>"),
                          matchTriple("_:u12_node", "<p>", "<o>")));
}

// A view starting at a non-zero index reads the correct rows from the IdTable.
TEST_F(EvaluatedTripleIteratorTest, viewSubrangeReadsCorrectRows) {
  //              col 0
  // row 0:       <s>     ← not in the view
  // row 1:       <p>     ← firstRow (view starts here)
  // row 2:       <o>
  // row 3:       <q>     ← endRow (exclusive)
  auto idTable = makeIdTableFromVector({{idS_}, {idP_}, {idO_}, {idQ_}});
  auto tmpl =
      makeTemplate({triple(Var(0), Const("<pred>"), Const("<obj>"))}, {0});
  auto table = makeRange(idTable, 1, 3);

  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 0};

  EXPECT_THAT(collectAll(proc),
              ElementsAre(matchTriple("<p>", "<pred>", "<obj>"),
                          matchTriple("<o>", "<pred>", "<obj>")));
}

// More than DEFAULT_BATCH_SIZE rows: the processor correctly crosses the
// internal batch boundary and yields triples for all rows.
TEST_F(EvaluatedTripleIteratorTest, acrossBatchBoundary) {
  constexpr size_t N = TableWithRangeEvaluator::DEFAULT_BATCH_SIZE + 1;

  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto idTable = makeIdTableFromVector(rows);
  auto tmpl = makeTemplate({triple(Var(0), Const("<p>"), Const("<o>"))}, {0});
  auto table = makeRange(idTable, 0, N);

  TableWithRangeEvaluator proc{tmpl, index_, makeHandle(), table, 0};
  auto triples = collectAll(proc);

  ASSERT_EQ(triples.size(), N);
  for (const auto& t : triples) {
    EXPECT_THAT(t, matchTriple("<s>", "<p>", "<o>"));
  }
}

// After consuming all triples in batch 0, cancelling the handle causes the
// next `get()` call (which starts batch 1) to throw.
TEST_F(EvaluatedTripleIteratorTest, cancellationThrowsBetweenBatches) {
  constexpr size_t N = TableWithRangeEvaluator::DEFAULT_BATCH_SIZE + 1;

  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto idTable = makeIdTableFromVector(rows);
  auto tmpl = makeTemplate({triple(Const("<s>"), Const("<p>"), Const("<o>"))});
  auto table = makeRange(idTable, 0, N);

  auto handle = makeHandle();
  TableWithRangeEvaluator proc{tmpl, index_, handle, table, 0};

  // Drain all DEFAULT_BATCH_SIZE triples from batch 0.
  for (size_t i = 0; i < TableWithRangeEvaluator::DEFAULT_BATCH_SIZE; ++i) {
    ASSERT_TRUE(proc.get().has_value());
  }

  // Cancel before the next `get()` triggers batch 1.
  handle->cancel(ad_utility::CancellationState::MANUAL);

  EXPECT_ANY_THROW(proc.get());
}

}  // namespace
