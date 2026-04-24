// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/IdTableHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/ConstructTripleGenerator.h"
#include "engine/Result.h"
#include "util/CancellationHandle.h"

namespace {

using namespace qlever::constructExport;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Field;
using Triples = ad_utility::sparql_types::Triples;

// Matcher for StringTriple: checks all three string values.
static auto matchTriple(const std::string& s, const std::string& p,
                        const std::string& o) {
  return AllOf(Field(&StringTriple::subject_, s),
               Field(&StringTriple::predicate_, p),
               Field(&StringTriple::object_, o));
}

// Drain all StringTriples from an InputRangeTypeErased into a vector.
static std::vector<StringTriple> collectAll(
    ad_utility::InputRangeTypeErased<StringTriple> range) {
  std::vector<StringTriple> result;
  while (auto t = range.get()) {
    result.push_back(std::move(*t));
  }
  return result;
}

// =============================================================================
// Test fixture.
// Builds a small index from:
//   <s> <p> <o> .
//   <s> <q> "hello" .
//
// Provides helpers to build IdTables, Triples templates, and TableWithRange
// values.
// =============================================================================

class ConstructTripleGeneratorTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ =
      ad_utility::testing::getQec("<s> <p> <o> . <s> <q> \"hello\" .");
  const Index& index_ = qec_->getIndex();

  Id idS_ = ad_utility::testing::makeGetId(index_)("<s>");
  Id idP_ = ad_utility::testing::makeGetId(index_)("<p>");
  Id idO_ = ad_utility::testing::makeGetId(index_)("<o>");
  Id idQ_ = ad_utility::testing::makeGetId(index_)("<q>");

  // Create a non-cancelled CancellationHandle.
  static ad_utility::SharedCancellationHandle makeHandle() {
    return std::make_shared<
        ad_utility::SharedCancellationHandle::element_type>();
  }

  // Wrap an IdTable in a shared Result (moves the table in).
  static std::shared_ptr<const Result> makeResult(IdTable table) {
    return std::make_shared<const Result>(
        std::move(table), std::vector<ColumnIndex>{}, LocalVocab{});
  }

  // Create a TableWithRange referencing the Result's IdTable, covering rows
  // [start, end).
  static TableWithRange makeRange(const Result& result, uint64_t start,
                                  uint64_t end) {
    return {TableConstRefWithVocab{result.idTable(), result.localVocab()},
            ql::views::iota(start, end)};
  }

  // Wrap a single TableWithRange in an InputRangeTypeErased.
  static ad_utility::InputRangeTypeErased<TableWithRange> singleTableRange(
      TableWithRange table) {
    return ad_utility::InputRangeTypeErased<TableWithRange>{
        ad_utility::OwningView{std::vector<TableWithRange>{std::move(table)}}};
  }

  // Run the generator over a single table and collect StringTriples.
  std::vector<StringTriple> run(
      Triples triples, VariableToColumnMap varMap, TableWithRange table,
      ad_utility::SharedCancellationHandle handle = makeHandle()) {
    auto stringTriples = ConstructTripleGenerator::generateStringTriples(
        triples, varMap, index_, handle, singleTableRange(std::move(table)), 0);

    std::vector<StringTriple> resultTriples;
    for (auto& triple : stringTriples) {
      resultTriples.push_back(triple);
    }

    return resultTriples;
  }

  // Build a single-triple CONSTRUCT template.
  static Triples oneTriple(GraphTerm s, GraphTerm p, GraphTerm o) {
    return {std::array<GraphTerm, 3>{std::move(s), std::move(p), std::move(o)}};
  }
};

// =============================================================================
// Tests
// =============================================================================

// No rows in the view -> no triples emitted, regardless of the template.
TEST_F(ConstructTripleGeneratorTest, emptyTable) {
  auto result = makeResult(makeIdTableFromVector({}));
  auto table = makeRange(*result, 0, 0);
  auto triples = oneTriple(Iri{"<s>"}, Iri{"<p>"}, Iri{"<o>"});

  EXPECT_TRUE(run(triples, {}, table).empty());
}

// All-constants template: every result row emits one identical triple,
// regardless of IdTable cell contents.
TEST_F(ConstructTripleGeneratorTest, allConstantsYieldsOneTriplePerRow) {
  auto result = makeResult(makeIdTableFromVector(
      {{Id::makeUndefined()}, {Id::makeUndefined()}, {Id::makeUndefined()}}));
  auto table = makeRange(*result, 0, 3);
  auto triples = oneTriple(Iri{"<s>"}, Iri{"<p>"}, Iri{"<o>"});

  EXPECT_THAT(run(triples, {}, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<s>", "<p>", "<o>")));
}

// Variable in subject position: correctly resolved from the IdTable column.
TEST_F(ConstructTripleGeneratorTest, variableInSubjectResolved) {
  //              col 0
  // row 0:       <s>
  // row 1:       <o>
  auto result = makeResult(makeIdTableFromVector({{idS_}, {idO_}}));
  auto table = makeRange(*result, 0, 2);
  auto triples = oneTriple(Variable{"?sub"}, Iri{"<p>"}, Iri{"<o>"});
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(triples, varMap, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<o>", "<p>", "<o>")));
}

// A row where a variable resolves to an undefined Id -> that triple is dropped.
// Rows before and after the undefined row are unaffected.
TEST_F(ConstructTripleGeneratorTest, undefDropsTriple) {
  auto result = makeResult(
      makeIdTableFromVector({{idS_}, {Id::makeUndefined()}, {idO_}}));
  auto table = makeRange(*result, 0, 3);
  auto triples = oneTriple(Variable{"?sub"}, Iri{"<p>"}, Iri{"<o>"});
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(triples, varMap, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o>"),
                          matchTriple("<o>", "<p>", "<o>")));
}

// Multiple template triples: for each result row all template triples are
// emitted in row-major order (all triples for row 0, then row 1, ...).
TEST_F(ConstructTripleGeneratorTest, multipleTemplateTriples) {
  auto result = makeResult(makeIdTableFromVector({{idS_}, {idO_}}));
  auto table = makeRange(*result, 0, 2);
  Triples triples{
      std::array<GraphTerm, 3>{Variable{"?sub"}, Iri{"<p>"}, Iri{"<o1>"}},
      std::array<GraphTerm, 3>{Variable{"?sub"}, Iri{"<q>"}, Iri{"<o2>"}},
  };
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(triples, varMap, table),
              ElementsAre(matchTriple("<s>", "<p>", "<o1>"),
                          matchTriple("<s>", "<q>", "<o2>"),
                          matchTriple("<o>", "<p>", "<o1>"),
                          matchTriple("<o>", "<q>", "<o2>")));
}

// Blank-node row IDs combine rowOffset (accumulated over prior tables),
// firstRow (view start), and the in-batch row index:
//   blankNodeRowId = rowOffset + firstRow + rowInBatch
TEST_F(ConstructTripleGeneratorTest, blankNodeUsesCorrectRowId) {
  auto result = makeResult(makeIdTableFromVector(
      {{Id::makeUndefined()}, {Id::makeUndefined()}, {Id::makeUndefined()}}));

  // View starts at absolute row 1 of the IdTable: firstRow=1, numRows=2.
  auto table = makeRange(*result, 1, 3);
  // Template: _:u<rowId>_node <p> <o>  (rowOffset=0)
  // row 0 of batch -> blankNodeRowId = 0 + 1 + 0 = 1
  // row 1 of batch -> blankNodeRowId = 0 + 1 + 1 = 2
  auto triples = oneTriple(BlankNode{false, "node"}, Iri{"<p>"}, Iri{"<o>"});

  EXPECT_THAT(run(triples, {}, table),
              ElementsAre(matchTriple("_:u1_node", "<p>", "<o>"),
                          matchTriple("_:u2_node", "<p>", "<o>")));
}

// rowOffset accumulates across multiple tables: blank node IDs from the second
// table incorporate the row count of the first table.
TEST_F(ConstructTripleGeneratorTest, rowOffsetAccumulatesAcrossTables) {
  // Table 1: 3 rows (view 0..3)
  auto result1 = makeResult(makeIdTableFromVector(
      {{Id::makeUndefined()}, {Id::makeUndefined()}, {Id::makeUndefined()}}));
  auto table1 = makeRange(*result1, 0, 3);

  // Table 2: 2 rows (view 5..7, i.e. rows 5 and 6 of its IdTable)
  auto result2 = makeResult(makeIdTableFromVector({{Id::makeUndefined()},
                                                   {Id::makeUndefined()},
                                                   {Id::makeUndefined()},
                                                   {Id::makeUndefined()},
                                                   {Id::makeUndefined()},
                                                   {Id::makeUndefined()},
                                                   {Id::makeUndefined()}}));
  auto table2 = makeRange(*result2, 5, 7);

  auto triples = oneTriple(BlankNode{false, "x"}, Iri{"<p>"}, Iri{"<o>"});

  std::vector<TableWithRange> tables{table1, table2};
  auto range = ConstructTripleGenerator::generateStringTriples(
      triples, {}, index_, makeHandle(),
      ad_utility::InputRangeTypeErased<TableWithRange>{
          ad_utility::OwningView{std::move(tables)}},
      0);

  // Table 1: rowOffset=0, firstRow=0
  //   row 0: rowId = 0+0+0 = 0
  //   row 1: rowId = 0+0+1 = 1
  //   row 2: rowId = 0+0+2 = 2
  // Table 2: rowOffset=3 (3 rows processed from table1), firstRow=5
  //   row 0: rowId = 3+5+0 = 8
  //   row 1: rowId = 3+5+1 = 9
  EXPECT_THAT(collectAll(std::move(range)),
              ElementsAre(matchTriple("_:u0_x", "<p>", "<o>"),
                          matchTriple("_:u1_x", "<p>", "<o>"),
                          matchTriple("_:u2_x", "<p>", "<o>"),
                          matchTriple("_:u8_x", "<p>", "<o>"),
                          matchTriple("_:u9_x", "<p>", "<o>")));
}

// A view starting at a non-zero index reads the correct rows from the IdTable.
TEST_F(ConstructTripleGeneratorTest, viewSubrangeReadsCorrectRows) {
  //              col 0
  // row 0:       <s>     <- not in the view
  // row 1:       <p>     <- firstRow (view starts here)
  // row 2:       <o>
  // row 3:       <q>     <- endRow (exclusive)
  auto result =
      makeResult(makeIdTableFromVector({{idS_}, {idP_}, {idO_}, {idQ_}}));
  auto table = makeRange(*result, 1, 3);
  auto triples = oneTriple(Variable{"?sub"}, Iri{"<pred>"}, Iri{"<obj>"});
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  EXPECT_THAT(run(triples, varMap, table),
              ElementsAre(matchTriple("<p>", "<pred>", "<obj>"),
                          matchTriple("<o>", "<pred>", "<obj>")));
}

// More than ConstructTripleGenerator::DEFAULT_BATCH_SIZE rows: the generator
// correctly crosses the internal batch boundary and yields triples for all
// rows.
TEST_F(ConstructTripleGeneratorTest, acrossBatchBoundary) {
  constexpr size_t N = ConstructTripleGenerator::DEFAULT_BATCH_SIZE + 1;

  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto result = makeResult(makeIdTableFromVector(rows));
  auto table = makeRange(*result, 0, N);
  auto triples = oneTriple(Variable{"?sub"}, Iri{"<p>"}, Iri{"<o>"});
  VariableToColumnMap varMap;
  varMap[Variable{"?sub"}] = makeAlwaysDefinedColumn(0);

  auto collected = run(triples, varMap, table);

  ASSERT_EQ(collected.size(), N);
  for (const auto& t : collected) {
    EXPECT_THAT(t, matchTriple("<s>", "<p>", "<o>"));
  }
}

// After consuming all ConstructTripleGenerator::DEFAULT_BATCH_SIZE triples in
// batch 0, cancelling the handle causes the next get() call (which would start
// batch 1) to throw.
TEST_F(ConstructTripleGeneratorTest, cancellationThrowsBetweenBatches) {
  constexpr size_t N = ConstructTripleGenerator::DEFAULT_BATCH_SIZE + 1;

  std::vector<std::vector<IntOrId>> rows(N, std::vector<IntOrId>{idS_});
  auto result = makeResult(makeIdTableFromVector(rows));
  auto table = makeRange(*result, 0, N);
  auto triples = oneTriple(Iri{"<s>"}, Iri{"<p>"}, Iri{"<o>"});

  auto handle = makeHandle();
  auto range = ConstructTripleGenerator::generateStringTriples(
      triples, {}, index_, handle, singleTableRange(table), 0);

  // Drain all ConstructTripleGenerator::DEFAULT_BATCH_SIZE triples from batch
  // 0.
  for (size_t i = 0; i < ConstructTripleGenerator::DEFAULT_BATCH_SIZE; ++i) {
    ASSERT_TRUE(range.get().has_value());
  }

  // Cancel before the next get() triggers batch 1.
  handle->cancel(ad_utility::CancellationState::MANUAL);

  EXPECT_ANY_THROW(range.get());
}

}  // namespace
