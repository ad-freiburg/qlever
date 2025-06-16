//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <memory>

#include "../test/PrefilterExpressionTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "engine/IndexScan.h"
#include "index/IndexImpl.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;

namespace {
using Tc = TripleComponent;
using Var = Variable;
using LazyResult = Result::LazyResult;

using IndexPair = std::pair<size_t, size_t>;

// NOTE: All the following helper functions always use the `PSO` permutation to
// set up index scans unless explicitly stated otherwise.

// Test that the `partialLazyScanResult` when being materialized to a single
// `IdTable` yields a subset of the full result of the `fullScan`. The `subset`
// is specified via the `expectedRows`, for example {{1, 3}, {7, 8}} means that
// the partialLazyScanResult shall contain the rows number `1, 2, 7` of the full
// scan (upper bounds are not included).
void testLazyScan(Permutation::IdTableGenerator partialLazyScanResult,
                  IndexScan& fullScan,
                  const std::vector<IndexPair>& expectedRows,
                  const LimitOffsetClause& limitOffset = {},
                  source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto alloc = ad_utility::makeUnlimitedAllocator<Id>();
  IdTable lazyScanRes{0, alloc};
  size_t numBlocks = 0;
  for (const auto& block : partialLazyScanResult) {
    if (lazyScanRes.empty()) {
      lazyScanRes.setNumColumns(block.numColumns());
    }
    lazyScanRes.insertAtEnd(block);
    ++numBlocks;
  }

  if (limitOffset.isUnconstrained()) {
    EXPECT_EQ(numBlocks, partialLazyScanResult.details().numBlocksRead_);
    // The number of read elements might be a bit larger than the final result
    // size, because the first and/or last block might be incomplete, meaning
    // that they have to be completely read, but only partially contribute to
    // the result.
    EXPECT_LE(lazyScanRes.size(),
              partialLazyScanResult.details().numElementsRead_);
  }

  auto resFullScan = fullScan.getResult()->idTable().clone();
  IdTable expected{resFullScan.numColumns(), alloc};

  if (limitOffset.isUnconstrained()) {
    for (auto [lower, upper] : expectedRows) {
      for (auto index : ql::views::iota(lower, upper)) {
        expected.push_back(resFullScan.at(index));
      }
    }
  } else {
    // as soon as a limit clause is applied, we currently ignore the block
    // filter, thus the result of the lazy and the materialized scan become the
    // same.
    expected = resFullScan.clone();
  }

  if (limitOffset.isUnconstrained()) {
    EXPECT_EQ(lazyScanRes, expected);
  } else {
    // If the join on blocks could already determine that there are no matching
    // blocks, then the lazy scan will be empty even with a limit present.
    EXPECT_TRUE((lazyScanRes.empty() && expectedRows.empty()) ||
                lazyScanRes == expected);
  }
}

// Test that when two scans are set up (specified by `tripleLeft` and
// `tripleRight`) on the given knowledge graph), and each scan is lazily
// executed and only contains the blocks that are needed to join both scans,
// then the resulting lazy partial scans only contain the subset of the
// respective full scans as specified by `leftRows` and `rightRows`. For the
// specification of the subset see above.
void testLazyScanForJoinOfTwoScans(
    const std::string& kgTurtle, const SparqlTripleSimple& tripleLeft,
    const SparqlTripleSimple& tripleRight,
    const std::vector<IndexPair>& leftRows,
    const std::vector<IndexPair>& rightRows,
    ad_utility::MemorySize blocksizePermutations = 16_B,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  // As soon as there is a LIMIT clause present, we cannot use the prefiltered
  // blocks.
  std::vector<LimitOffsetClause> limits{{}, {12, 3}, {2, 3}};
  for (const auto& limit : limits) {
    TestIndexConfig config{kgTurtle};
    config.blocksizePermutations = blocksizePermutations;
    auto qec = getQec(std::move(config));
    IndexScan s1{qec, Permutation::PSO, tripleLeft};
    s1.applyLimitOffset(limit);
    IndexScan s2{qec, Permutation::PSO, tripleRight};
    auto implForSwitch = [](IndexScan& l, IndexScan& r, const auto& expectedL,
                            const auto& expectedR,
                            const LimitOffsetClause& limitL,
                            const LimitOffsetClause& limitR) {
      auto [scan1, scan2] = (IndexScan::lazyScanForJoinOfTwoScans(l, r));

      testLazyScan(std::move(scan1), l, expectedL, limitL);
      testLazyScan(std::move(scan2), r, expectedR, limitR);
    };
    implForSwitch(s1, s2, leftRows, rightRows, limit, {});
    implForSwitch(s2, s1, rightRows, leftRows, {}, limit);
  }
}

// Test that setting up the lazy partial scans between `tripleLeft` and
// `tripleRight` on the given `kg` throws an exception.
void testLazyScanThrows(const std::string& kg,
                        const SparqlTripleSimple& tripleLeft,
                        const SparqlTripleSimple& tripleRight,
                        source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO, tripleLeft};
  IndexScan s2{qec, Permutation::PSO, tripleRight};
  EXPECT_ANY_THROW(IndexScan::lazyScanForJoinOfTwoScans(s1, s2));
}

// Test that a lazy partial scan for a join of the `scanTriple` with a
// materialized join column result that is specified by the `columnEntries`
// yields only the subsets specified by the `expectedRows`.
void testLazyScanForJoinWithColumn(
    const std::string& kg, const SparqlTripleSimple& scanTriple,
    std::vector<TripleComponent> columnEntries,
    const std::vector<IndexPair>& expectedRows,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan scan{qec, Permutation::PSO, scanTriple};
  std::vector<Id> column;
  for (const auto& entry : columnEntries) {
    column.push_back(entry.toValueId(qec->getIndex().getVocab()).value());
  }

  auto lazyScan = scan.lazyScanForJoinOfColumnWithScan(column);
  testLazyScan(std::move(lazyScan), scan, expectedRows);
}

// Test the same scenario as the previous function, but assumes that the
// setting up of the lazy scan fails with an exception.
void testLazyScanWithColumnThrows(
    const std::string& kg, const SparqlTripleSimple& scanTriple,
    const std::vector<TripleComponent>& columnEntries,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO, scanTriple};
  std::vector<Id> column;
  for (const auto& entry : columnEntries) {
    column.push_back(entry.toValueId(qec->getIndex().getVocab()).value());
  }

  // We need this to suppress the warning about a [[nodiscard]] return value
  // being unused.
  auto makeScan = [&column, &s1]() {
    [[maybe_unused]] auto scan = s1.lazyScanForJoinOfColumnWithScan(column);
  };
  EXPECT_ANY_THROW(makeScan());
}

//______________________________________________________________________________
// Check that the `IndexScan` computes correct prefiltered `IdTable`s w.r.t.
// the applied `PrefilterExpression` given a <PrefilterExpression, Variable>
// pair was successfully set. For convenience we assert this for the IdTable
// column on which the `PrefilterExpression` was applied.
const auto testSetAndMakeScanWithPrefilterExpr =
    [](const std::string& kg, const SparqlTripleSimple& triple,
       const Permutation::Enum permutation, IndexScan::PrefilterVariablePair pr,
       const std::vector<ValueId>& expectedIdsOnFilterColumn,
       bool prefilterCanBeSet = true,
       source_location l = source_location::current()) {
      auto t = generateLocationTrace(l);
      IndexScan scan{getQec(kg), permutation, triple};
      auto variable = pr.second;
      auto optUpdatedQet = scan.setPrefilterGetUpdatedQueryExecutionTree(
          makeFilterExpression::filterHelper::makePrefilterVec(std::move(pr)));
      if (optUpdatedQet.has_value()) {
        auto updatedQet = optUpdatedQet.value();
        ASSERT_TRUE(prefilterCanBeSet);
        // Check that the prefiltering procedure yields the correct result given
        // that the <PrefilterExpression, Variable> pair is correctly assigned
        // to the IndexScan.
        IdTable idTableFiltered = updatedQet->getRootOperation()
                                      ->computeResultOnlyForTesting()
                                      .idTable()
                                      .clone();
        auto isColumnIdSpan =
            idTableFiltered.getColumn(updatedQet->getVariableColumn(variable));
        ASSERT_EQ(
            (std::vector<Id>{isColumnIdSpan.begin(), isColumnIdSpan.end()}),
            expectedIdsOnFilterColumn);
      } else {
        // Check our prediction that the prefilter with the given
        // <PrefilterExpression, Variable> pair is not applicable (no updated
        // QueryExecutionTree is returned).
        ASSERT_FALSE(prefilterCanBeSet);
      }
    };

}  // namespace

TEST(IndexScan, lazyScanForJoinOfTwoScans) {
  SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
  SparqlTripleSimple xqz{Tc{Var{"?x"}}, iri("<q>"), Tc{Var{"?z"}}};
  {
    // In the tests we have a blocksize of two triples per block, and a new
    // block is started for a new relation. That explains the spacing of the
    // following example knowledge graphs.
    std::string kg =
        "<a> <p> <A>. <a> <p> <A2>. "
        "<a> <p> <A3> . <b> <p> <B>. "
        "<b> <p> <B2> ."
        "<b> <q> <xb>. <b> <q> <xb2> .";

    // When joining the <p> and <x> relations, we only need to read the last two
    // blocks of the <p> relation, as <a> never appears as a subject in <x>.
    // This means that the lazy partial scan can skip the first two triples.
    testLazyScanForJoinOfTwoScans(kg, xpy, xqz, {{2, 5}}, {{0, 2}});
  }
  {
    std::string kg =
        "<a> <p2> <A>. <a> <p2> <A2>. "
        "<a> <p2> <A3> . <b> <p2> <B>. "
        "<b> <q2> <xb>. <b> <q2> <xb2> .";
    // No triple for relation <p> (which doesn't even appear in the knowledge
    // graph), so both lazy scans are empty.
    testLazyScanForJoinOfTwoScans(kg, xpy, xqz, {}, {});
  }
  {
    // No triple for relation <x> (which does appear in the knowledge graph, but
    // not as a predicate), so both lazy scans are empty.
    std::string kg =
        "<a> <p> <A>. <a> <p> <A2>. "
        "<a> <p> <A3> . <b> <p> <B>. "
        "<b> <x2> <x>. <b> <x2> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, xpy, xqz, {}, {});
  }
  SparqlTripleSimple bpx{Tc{iri("<b>")}, iri("<p>"), Tc{Var{"?x"}}};
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<b> <p> <x2> . <b> <p> <x3>. "
        "<b> <p> <x4> . <b> <p> <x7>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> ."
        "<x5> <q> <xb>. <x9> <q> <xb2> ."
        "<x91> <q> <xb>. <x93> <q> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {{1, 5}}, {{0, 4}});
  }
  {
    // In this example we use 3 triples per block (24 bytes) and the `<p>`
    // permutation is standing in a single block together with the previous
    // `<o>` relation. The lazy scans are however still aware that the relevant
    // part of the block (`<b> <p> ?x`) only  goes from `<x80>` through `<x90>`,
    // so it is not necessary to scan the first block of the `<q>` relation
    // which only has subjects <= `<x5>`.
    std::string kg =
        "<a> <o> <a1>. <b> <p> <x80>. <b> <p> <x90>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> . <x5> <q> <xb>. "
        "<x9> <q> <xb2> . <x91> <q> <xb>. <x93> <q> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {{0, 2}}, {{3, 6}}, 24_B);
  }
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> ."
        "<x5> <q> <xb>. <x9> <q> <xb2> ."
        "<x91> <q> <xb>. <x93> <q> <xb2> .";
    // Scan for a fixed subject that appears in the kg but not as the subject of
    // the <p> predicate.
    SparqlTripleSimple xb2px{Tc{iri("<xb2>")}, iri("<p>"), Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {}, {});
  }
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<x2> <q> <xb>. <x5> <q> <xb2> ."
        "<x5> <q> <xb>. <x9> <q> <xb2> ."
        "<x91> <q> <xb>. <x93> <q> <xb2> .";
    // Scan for a fixed subject that is not even in the knowledge graph.
    SparqlTripleSimple xb2px{Tc{iri("<notInKg>")}, iri("<p>"), Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {}, {});
  }

  // Corner cases
  {
    std::string kg = "<a> <b> <c> .";
    SparqlTripleSimple xyz{Tc{Var{"?x"}}, Var{"?y"}, Tc{Var{"?z"}}};
    testLazyScanThrows(kg, xyz, xqz);
    testLazyScanThrows(kg, xyz, xqz);
    testLazyScanThrows(kg, xyz, xyz);
    testLazyScanThrows(kg, xqz, xyz);

    // The first variable must be matching (subject variable is ?a vs ?x)
    SparqlTripleSimple abc{Tc{Var{"?a"}}, iri("<b>"), Tc{Var{"?c"}}};
    testLazyScanThrows(kg, abc, xqz);

    // If both scans have two variables, then the second variable must not
    // match.
    testLazyScanThrows(kg, abc, abc);
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanTwoVariables) {
  SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
  // In the tests we have a blocksize of two triples per block, and a new
  // block is started for a new relation. That explains the spacing of the
  // following example knowledge graphs.
  std::string kg =
      "<a> <p> <A>. <a> <p> <A2>. "
      "<a> <p> <A3> . <b> <p> <B>. "
      "<b> <p> <B2> ."
      "<b> <q> <xb>. <b> <q> <xb2> .";
  {
    std::vector<TripleComponent> column{iri("<a>"), iri("<b>"), iri("<q>"),
                                        iri("<xb>")};
    // We need to scan all the blocks that contain the `<p>` predicate.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 5}});
  }
  {
    std::vector<TripleComponent> column{iri("<b>"), iri("<q>"), iri("<xb>")};
    // The first block only contains <a> which doesn't appear in the first
    // block.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{2, 5}});
  }
  {
    std::vector<TripleComponent> column{iri("<a>"), iri("<q>"), iri("<xb>")};
    // The first block only contains <a> which only appears in the first two
    // blocks.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 4}});
  }
  {
    std::vector<TripleComponent> column{iri("<a>"), iri("<q>"), iri("<xb>")};
    // <f> does not appear as a predicate, so the result is empty.
    SparqlTripleSimple efg{Tc{Var{"?e"}}, iri("<f>"), Tc{Var{"?g"}}};
    testLazyScanForJoinWithColumn(kg, efg, column, {});
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanOneVariable) {
  SparqlTripleSimple bpy{Tc{iri("<b>")}, iri("<p>"), Tc{Var{"?x"}}};
  std::string kg =
      "<a> <p> <s0>. <a> <p> <s7>. "
      "<a> <p> <s99> . <b> <p> <s0>. "
      "<b> <p> <s2> . <b> <p> <s3>. "
      "<b> <p> <s6> . <b> <p> <s9>. "
      "<b> <q> <s3>. <b> <q> <s5> .";
  {
    // The subject (<b>) and predicate (<b>) are fixed, so the object is the
    // join column
    std::vector<TripleComponent> column{iri("<s0>"), iri("<s7>"), iri("<s99>")};
    // We don't need to scan the middle block that only has <s2> and <s3>
    testLazyScanForJoinWithColumn(kg, bpy, column, {{0, 1}, {3, 5}});
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanCornerCases) {
  SparqlTripleSimple threeVars{Tc{Var{"?x"}}, Var{"?b"}, Tc{Var{"?y"}}};
  std::string kg =
      "<a> <p> <A>. <a> <p> <A2>. "
      "<a> <p> <A3> . <b> <p> <B>. "
      "<b> <p> <B2> ."
      "<b> <q> <xb>. <b> <q> <xb2> .";

  // Full index scan (three variables).
  std::vector<TripleComponent> column{iri("<a>"), iri("<b>"), iri("<q>"),
                                      iri("<xb>")};
  // only `<q>` matches (we join on the predicate), so we only get the last
  // block.
  testLazyScanForJoinWithColumn(kg, threeVars, column, {{5, 7}});

  // The join column must be sorted.
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    std::vector<TripleComponent> unsortedColumn{iri("<a>"), iri("<b>"),
                                                iri("<a>")};
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
    testLazyScanWithColumnThrows(kg, xpy, unsortedColumn);
  }
}

TEST(IndexScan, additionalColumn) {
  auto qec = getQec("<x> <y> <z>.");
  using V = Variable;
  SparqlTripleSimple triple{V{"?x"}, iri("<y>"), V{"?z"}};
  triple.additionalScanColumns_.emplace_back(
      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN, V{"?xpattern"});
  triple.additionalScanColumns_.emplace_back(
      ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN, V{"?ypattern"});
  auto scan = IndexScan{qec, Permutation::PSO, triple};
  ASSERT_EQ(scan.getResultWidth(), 4);
  auto col = makeAlwaysDefinedColumn;
  VariableToColumnMap expected = {{V{"?x"}, col(0)},
                                  {V{"?z"}, col(1)},
                                  {V("?xpattern"), col(2)},
                                  {V("?ypattern"), col(3)}};
  ASSERT_THAT(scan.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expected));
  ASSERT_THAT(scan.getCacheKey(),
              ::testing::ContainsRegex("Additional Columns: 4 5"));
  auto res = scan.computeResultOnlyForTesting();
  auto getId = makeGetId(qec->getIndex());
  auto I = IntId;
  // <x> is the only subject, so it has pattern 0, <z> doesn't appear as a
  // subject, so it has no pattern.
  auto exp = makeIdTableFromVector(
      {{getId("<x>"), getId("<z>"), I(0), I(Pattern::NoPattern)}});
  EXPECT_THAT(res.idTable(), ::testing::ElementsAreArray(exp));
}

// Test that the graphs by which an `IndexScan` is to be filtered is correctly
// reflected in its cache key and its `ScanSpecification`.
TEST(IndexScan, namedGraphs) {
  auto qec = getQec("<x> <y> <z>.");
  using V = Variable;
  SparqlTripleSimple triple{V{"?x"}, iri("<y>"), V{"?z"}};
  ad_utility::HashSet<TripleComponent> graphs{
      TripleComponent::Iri::fromIriref("<graph1>"),
      TripleComponent::Iri::fromIriref("<graph2>")};
  auto scan = IndexScan{qec, Permutation::PSO, triple, graphs};
  using namespace testing;
  EXPECT_THAT(scan.graphsToFilter(), Optional(graphs));
  EXPECT_THAT(scan.getCacheKey(),
              HasSubstr("Filtered by Graphs:<graph1> <graph2>"));
  EXPECT_THAT(scan.getScanSpecificationTc().graphsToFilter(), Optional(graphs));

  auto scanNoGraphs = IndexScan{qec, Permutation::PSO, triple};
  EXPECT_EQ(scanNoGraphs.graphsToFilter(), std::nullopt);
  EXPECT_THAT(scanNoGraphs.getCacheKey(),
              Not(HasSubstr("Filtered by Graphs:")));
  EXPECT_THAT(scanNoGraphs.getScanSpecificationTc().graphsToFilter(),
              Eq(std::nullopt));
}

TEST(IndexScan, getResultSizeOfScan) {
  auto qec = getQec("<x> <p> <s1>, <s2>. <x> <p2> <s1>.");
  auto getId = makeGetId(qec->getIndex());
  [[maybe_unused]] auto x = getId("<x>");
  [[maybe_unused]] auto p = getId("<p>");
  [[maybe_unused]] auto s1 = getId("<s1>");
  [[maybe_unused]] auto s2 = getId("<s2>");
  [[maybe_unused]] auto p2 = getId("<p2>");
  using V = Variable;
  using I = TripleComponent::Iri;

  {
    SparqlTripleSimple scanTriple{V{"?x"}, V("?y"), V{"?z"}};
    IndexScan scan{qec, Permutation::Enum::PSO, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 3);
  }
  {
    SparqlTripleSimple scanTriple{V{"?x"}, I::fromIriref("<p>"), V{"?y"}};
    IndexScan scan{qec, Permutation::Enum::PSO, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 2);
  }
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x>"), I::fromIriref("<p>"),
                                  V{"?y"}};
    IndexScan scan{qec, Permutation::Enum::PSO, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 2);
  }
  {
    SparqlTripleSimple scanTriple{V("?x"), I::fromIriref("<p>"),
                                  I::fromIriref("<s1>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 1);
  }
  // 0 variables
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x>"), I::fromIriref("<p>"),
                                  I::fromIriref("<s1>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 1);
    EXPECT_ANY_THROW(scan.getMultiplicity(0));
    auto res = scan.computeResultOnlyForTesting();
    ASSERT_EQ(res.idTable().numRows(), 1);
    ASSERT_EQ(res.idTable().numColumns(), 0);
  }
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x2>"), I::fromIriref("<p>"),
                                  I::fromIriref("<s1>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 1);
    EXPECT_EQ(scan.getExactSize(), 0);
  }
  {
    SparqlTripleSimple scanTriple{I::fromIriref("<x>"), I::fromIriref("<p>"),
                                  I::fromIriref("<p>")};
    IndexScan scan{qec, Permutation::Enum::POS, scanTriple};
    EXPECT_EQ(scan.getSizeEstimate(), 0);
    EXPECT_ANY_THROW(scan.getMultiplicity(0));
    auto res = scan.computeResultOnlyForTesting();
    ASSERT_EQ(res.idTable().numRows(), 0);
    ASSERT_EQ(res.idTable().numColumns(), 0);
  }
}

namespace {
// Return a `QueryExecutionContext` that is used in some of the tests below,
auto getQecWithoutPatterns = []() {
  TestIndexConfig config{"<x> <p> <s1>, <s2>. <x> <p2> <s1>."};
  config.usePatterns = false;
  return getQec(std::move(config));
};
}  // namespace
// _____________________________________________________________________________
TEST(IndexScan, computeResultCanBeConsumedLazily) {
  using V = Variable;
  auto qec = getQecWithoutPatterns();
  auto getId = makeGetId(qec->getIndex());
  auto x = getId("<x>");
  auto p = getId("<p>");
  auto s1 = getId("<s1>");
  auto s2 = getId("<s2>");
  auto p2 = getId("<p2>");
  SparqlTripleSimple scanTriple{V{"?x"}, V{"?y"}, V{"?z"}};
  IndexScan scan{qec, Permutation::Enum::POS, scanTriple};

  Result result = scan.computeResultOnlyForTesting(true);

  ASSERT_FALSE(result.isFullyMaterialized());

  IdTable resultTable{3, ad_utility::makeUnlimitedAllocator<Id>()};

  for (Result::IdTableVocabPair& pair : result.idTables()) {
    resultTable.insertAtEnd(pair.idTable_);
  }

  EXPECT_EQ(resultTable,
            makeIdTableFromVector({{p, s1, x}, {p, s2, x}, {p2, s1, x}}));
}

// _____________________________________________________________________________
TEST(IndexScan, computeResultReturnsEmptyGeneratorIfScanIsEmpty) {
  using V = Variable;
  using I = TripleComponent::Iri;
  auto qec = getQecWithoutPatterns();
  SparqlTripleSimple scanTriple{V{"?x"}, I::fromIriref("<abcdef>"), V{"?z"}};
  IndexScan scan{qec, Permutation::Enum::POS, scanTriple};

  Result result = scan.computeResultOnlyForTesting(true);

  ASSERT_FALSE(result.isFullyMaterialized());

  for ([[maybe_unused]] Result::IdTableVocabPair& pair : result.idTables()) {
    ADD_FAILURE() << "Generator should be empty" << std::endl;
  }
}

// _____________________________________________________________________________
TEST(IndexScan, unlikelyToFitInCacheCalculatesSizeCorrectly) {
  using ad_utility::MemorySize;
  using V = Variable;
  using I = TripleComponent::Iri;
  using enum Permutation::Enum;
  auto qec = getQecWithoutPatterns();
  auto x = I::fromIriref("<x>");
  auto p = I::fromIriref("<p>");
  auto p2 = I::fromIriref("<p2>");

  auto expectMaximumCacheableSize = [&](const IndexScan& scan, size_t numRows,
                                        size_t numCols,
                                        source_location l =
                                            source_location::current()) {
    auto locationTrace = generateLocationTrace(l);

    EXPECT_TRUE(scan.unlikelyToFitInCache(MemorySize::bytes(0)));
    size_t byteCount = numRows * numCols * sizeof(Id);
    EXPECT_TRUE(scan.unlikelyToFitInCache(MemorySize::bytes(byteCount - 1)));
    EXPECT_FALSE(scan.unlikelyToFitInCache(MemorySize::bytes(byteCount)));
  };

  {
    IndexScan scan{qec, POS, {V{"?x"}, V{"?y"}, V{"?z"}}};
    expectMaximumCacheableSize(scan, 3, 3);
  }

  {
    IndexScan scan{qec, SPO, {x, V{"?y"}, V{"?z"}}};
    expectMaximumCacheableSize(scan, 3, 2);
  }

  {
    IndexScan scan{qec, POS, {V{"?x"}, p, V{"?z"}}};
    expectMaximumCacheableSize(scan, 2, 2);
  }

  {
    IndexScan scan{qec, SPO, {x, p2, V{"?z"}}};
    expectMaximumCacheableSize(scan, 1, 1);
  }
}

// _____________________________________________________________________________
TEST(IndexScan, getSizeEstimateAndExactSizeWithAppliedPrefilter) {
  using namespace makeFilterExpression;
  using namespace filterHelper;
  using I = TripleComponent::Iri;

  std::string kg =
      "<a> <price_tag> 10.00 . <b> <price_tag> 12.00 . <b> <price_tag> "
      "12.001 . <b> <price_tag> 21.99 . <b> <price_tag> 24.33 . <b> "
      "<price_tag> 147.32 . <b> <price_tag> 189.99 . <b> <price_tag> 194.67 "
      ".";
  auto qec = getQec(kg);

  auto assertEstimatedAndExactSize = [](IndexScan& indexScan,
                                        IndexScan::PrefilterVariablePair pair,
                                        const size_t estimateSize,
                                        const size_t exactSize) {
    auto optUpdatedQet = indexScan.setPrefilterGetUpdatedQueryExecutionTree(
        makePrefilterVec(std::move(pair)));
    ASSERT_TRUE(optUpdatedQet.has_value());
    auto updatedQet = optUpdatedQet.value();
    ASSERT_EQ(updatedQet->getSizeEstimate(), estimateSize);
    std::shared_ptr<IndexScan> scanPtr =
        std::dynamic_pointer_cast<IndexScan>(updatedQet->getRootOperation());
    ASSERT_EQ(scanPtr->getExactSize(), exactSize);
  };

  {
    SparqlTripleSimple triple{Tc{Variable{"?b"}}, iri("<price_tag>"),
                              Tc{Variable{"?price"}}};
    IndexScan scan{qec, Permutation::POS, triple};
    assertEstimatedAndExactSize(
        scan,
        pr(andExpr(gt(IntId(0)), lt(DoubleId(234.35))), Variable{"?price"}), 8,
        8);
    assertEstimatedAndExactSize(
        scan,
        pr(andExpr(gt(DoubleId(12.00)), lt(IntId(190))), Variable{"?price"}), 6,
        6);
    assertEstimatedAndExactSize(
        scan, pr(le(DoubleId(21.99)), Variable{"?price"}), 4, 4);
  }

  {
    SparqlTripleSimple triple{I::fromIriref("<b>"), iri("<price_tag>"),
                              Variable{"?price"}};
    IndexScan scan{qec, Permutation::PSO, triple};
    assertEstimatedAndExactSize(
        scan, pr(le(DoubleId(21.99)), Variable{"?price"}), 3, 3);
    assertEstimatedAndExactSize(
        scan, pr(eq(DoubleId(24.33)), Variable{"?price"}), 3, 3);
    assertEstimatedAndExactSize(
        scan, pr(orExpr(le(IntId(12)), gt(IntId(22))), Variable{"?price"}), 5,
        5);
  }
}

// _____________________________________________________________________________
TEST(IndexScan, SetPrefilterVariablePairAndCheckCacheKey) {
  using namespace makeFilterExpression;
  using namespace filterHelper;
  using V = Variable;
  auto qec = getQec("<x> <y> <z>.");
  SparqlTripleSimple triple{V{"?x"}, iri("<y>"), V{"?z"}};
  auto scan = IndexScan{qec, Permutation::PSO, triple};
  auto prefilterPairs =
      makePrefilterVec(pr(lt(IntId(10)), V{"?a"}), pr(gt(IntId(5)), V{"?b"}),
                       pr(lt(IntId(5)), V{"?x"}));
  auto updatedQet =
      scan.setPrefilterGetUpdatedQueryExecutionTree(std::move(prefilterPairs));
  // We have a corresponding column for ?x (ColumnIndex 1), which is also the
  // first sorted variable column. Thus, we expect that PrefilterExpression (<
  // 5, ?x) will be set as a prefilter for this IndexScan.
  auto setPrefilterExpr = lt(IntId(5));
  ColumnIndex columnIdx = 1;
  std::stringstream os;
  os << "Added PrefiterExpression: \n";
  os << *setPrefilterExpr;
  os << "\nApplied on column: " << columnIdx << ".";
  EXPECT_THAT(updatedQet.value()->getRootOperation()->getCacheKey(),
              ::testing::HasSubstr(os.str()));

  // Assert that we don't set a <PrefilterExpression, ColumnIndex> pair for the
  // second Variable.
  prefilterPairs = makePrefilterVec(pr(lt(IntId(10)), V{"?a"}),
                                    pr(gt(DoubleId(22)), V{"?z"}),
                                    pr(gt(IntId(10)), V{"?b"}));
  updatedQet =
      scan.setPrefilterGetUpdatedQueryExecutionTree(std::move(prefilterPairs));
  // No PrefilterExpression should be set for this IndexScan, we don't expect a
  // updated QueryExecutionTree.
  EXPECT_TRUE(!updatedQet.has_value());
}

// _____________________________________________________________________________
TEST(IndexScan, checkEvaluationWithPrefiltering) {
  using namespace makeFilterExpression;
  using namespace filterHelper;
  auto I = ad_utility::testing::IntId;
  std::string kg =
      "<P1> <price_tag> 10 . <P2> <price_tag> 12 . <P3> <price_tag> "
      "18 . <P4> <price_tag> 22 . <P5> <price_tag> 25 . <P6> "
      "<price_tag> 147 . <P7> <price_tag> 174 . <P8> <price_tag> 174 "
      ". <P9> <price_tag> 189 . <P10> <price_tag> 194 .";
  SparqlTripleSimple triple{Tc{Variable{"?x"}}, iri("<price_tag>"),
                            Tc{Variable{"?price"}}};

  // For the following tests, the <PrefilterExpression, Variable> pair is set
  // and applied for the respective IndexScan.
  testSetAndMakeScanWithPrefilterExpr(kg, triple, Permutation::POS,
                                      pr(ge(IntId(10)), Variable{"?price"}),
                                      {I(10), I(12), I(18), I(22), I(25),
                                       I(147), I(174), I(174), I(189), I(194)});
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(lt(DoubleId(147.32)), Variable{"?price"}),
      {I(10), I(12), I(18), I(22), I(25), I(147)});
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}),
      {I(18), I(22), I(25), I(147), I(174), I(174)});

  // For the following test, the Variable value doesn't match any of the scan
  // triple Variable values. We expect that the prefilter is not applicable (=>
  // set bool flag to false).
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?y"}), {},
      false);

  // For the following tests, the first sorted column given the permutation
  // doesn't match with the corresponding column for the Variable of the
  // <PrefilterExpression, Variable> pair. We expect that the provided prefilter
  // is not applicable (and can't be set).
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::PSO,
      pr(andExpr(gt(DoubleId(12.00)), le(IntId(174))), Variable{"?price"}), {},
      false);
  testSetAndMakeScanWithPrefilterExpr(
      kg, triple, Permutation::POS,
      pr(andExpr(gt(VocabId(0)), lt(VocabId(100))), Variable{"?x"}), {}, false);

  // This knowledge graph yields an incomplete first and last block.
  std::string kgFirstAndLastIncomplete =
      "<a> <price_tag> 10 . <b> <price_tag> 12 . <b> <price_tag> "
      "18 . <b> <price_tag> 22 . <b> <price_tag> 25 . <b> "
      "<price_tag> 147 . <b> <price_tag> 189 . <c> <price_tag> 194 "
      ".";
  // The following test verifies that the prefilter procedure is successfully
  // applicable under the condition that the first and last block are
  // potentially incomplete.
  testSetAndMakeScanWithPrefilterExpr(
      kgFirstAndLastIncomplete, triple, Permutation::POS,
      pr(orExpr(gt(IntId(100)), le(IntId(10))), Variable{"?price"}),
      {I(10), I(12), I(25), I(147), I(189), I(194)});
  testSetAndMakeScanWithPrefilterExpr(
      kgFirstAndLastIncomplete, triple, Permutation::POS,
      pr(andExpr(gt(IntId(10)), lt(IntId(194))), Variable{"?price"}),
      {I(10), I(12), I(18), I(22), I(25), I(147), I(189), I(194)});
}

class IndexScanWithLazyJoin : public ::testing::TestWithParam<bool> {
 protected:
  QueryExecutionContext* qec_ = nullptr;

  void SetUp() override {
    std::string kg =
        "<a> <p> <A> . <a> <p> <A2> . "
        "<b> <p> <B> . <b> <p> <B2> . "
        "<c> <p> <C> . <c> <p> <C2> . "
        "<b> <q> <xb> . <b> <q> <xb2> .";
    qec_ = getQec(std::move(kg));
  }

  // Convert a TripleComponent to a ValueId.
  Id toValueId(const TripleComponent& tc) const {
    return tc.toValueId(qec_->getIndex().getVocab()).value();
  }

  // Create an id table with a single column from a vector of
  // `TripleComponent`s.
  IdTable makeIdTable(std::vector<TripleComponent> entries) const {
    IdTable result{1, makeAllocator()};
    result.reserve(entries.size());
    for (const TripleComponent& entry : entries) {
      result.emplace_back();
      result.back()[0] = toValueId(entry);
    }
    return result;
  }

  // Convert a vector of a tuple of triples to an id table.
  IdTable tableFromTriples(
      std::vector<std::array<TripleComponent, 2>> triples) const {
    IdTable result{2, makeAllocator()};
    result.reserve(triples.size());
    for (const auto& triple : triples) {
      result.emplace_back();
      result.back()[0] = toValueId(triple.at(0));
      result.back()[1] = toValueId(triple.at(1));
    }
    return result;
  }

  // Create a common `IndexScan` instance.
  IndexScan makeScan() const {
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
    // We need to scan all the blocks that contain the `<p>` predicate.
    return IndexScan{qec_, Permutation::PSO, xpy};
  }

  // Consume generator `first` first and store it in a vector, then do the same
  // with `second`.
  static std::pair<std::vector<Result::IdTableVocabPair>,
                   std::vector<Result::IdTableVocabPair>>
  consumeSequentially(Result::Generator first, Result::Generator second) {
    std::vector<Result::IdTableVocabPair> firstResult;
    std::vector<Result::IdTableVocabPair> secondResult;

    for (Result::IdTableVocabPair& element : first) {
      firstResult.push_back(std::move(element));
    }
    for (Result::IdTableVocabPair& element : second) {
      secondResult.push_back(std::move(element));
    }
    return {std::move(firstResult), std::move(secondResult)};
  }

  // Consume the generators and store the results in vectors using the
  // parameterized strategy.
  static std::pair<std::vector<Result::IdTableVocabPair>,
                   std::vector<Result::IdTableVocabPair>>
  consumeGenerators(
      std::pair<Result::Generator, Result::Generator> generatorPair) {
    std::vector<Result::IdTableVocabPair> joinSideResults;
    std::vector<Result::IdTableVocabPair> scanResults;

    bool rightFirst = GetParam();
    if (rightFirst) {
      std::tie(scanResults, joinSideResults) = consumeSequentially(
          std::move(generatorPair.second), std::move(generatorPair.first));
    } else {
      std::tie(joinSideResults, scanResults) = consumeSequentially(
          std::move(generatorPair.first), std::move(generatorPair.second));
    }
    return {std::move(joinSideResults), std::move(scanResults)};
  }
};

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesDoesFilterCorrectly) {
  IndexScan scan = makeScan();

  auto makeJoinSide = [](auto* self) -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{self->makeIdTable({iri("<a>"), iri("<c>")}), LocalVocab{}};
    co_yield p1;
    P p2{self->makeIdTable({iri("<c>")}), LocalVocab{}};
    co_yield p2;
    LocalVocab vocab;
    vocab.getIndexAndAddIfNotContained(LocalVocabEntry{
        ad_utility::triple_component::Literal::literalWithoutQuotes("Test")});
    P p3{self->makeIdTable({iri("<c>"), iri("<q>"), iri("<xb>")}),
         std::move(vocab)};
    co_yield p3;
  };

  auto [joinSideResults, scanResults] = consumeGenerators(
      scan.prefilterTables(LazyResult{makeJoinSide(this)}, 0));

  ASSERT_EQ(scanResults.size(), 2);
  ASSERT_EQ(joinSideResults.size(), 3);

  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(0).localVocab_.empty());

  EXPECT_TRUE(scanResults.at(1).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(1).localVocab_.empty());

  EXPECT_FALSE(joinSideResults.at(2).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<a>"), iri("<A2>")}}));
  EXPECT_EQ(joinSideResults.at(0).idTable_,
            makeIdTable({iri("<a>"), iri("<c>")}));

  EXPECT_EQ(
      scanResults.at(1).idTable_,
      tableFromTriples({{iri("<c>"), iri("<C>")}, {iri("<c>"), iri("<C2>")}}));
  EXPECT_EQ(joinSideResults.at(1).idTable_, makeIdTable({iri("<c>")}));

  EXPECT_EQ(joinSideResults.at(2).idTable_,
            makeIdTable({iri("<c>"), iri("<q>"), iri("<xb>")}));
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin,
       prefilterTablesDoesFilterCorrectlyWithOverlappingValues) {
  std::string kg = "<a> <p> <A> . <b> <p> <B>. ";
  qec_ = getQec(std::move(kg));
  IndexScan scan = makeScan();

  auto makeJoinSide = [](auto* self) -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{self->makeIdTable({iri("<a>")}), LocalVocab{}};
    co_yield p1;
    P p2{self->makeIdTable({iri("<b>")}), LocalVocab{}};
    co_yield p2;
  };

  auto [joinSideResults, scanResults] = consumeGenerators(
      scan.prefilterTables(LazyResult{makeJoinSide(this)}, 0));

  ASSERT_EQ(scanResults.size(), 1);
  ASSERT_EQ(joinSideResults.size(), 2);

  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(0).localVocab_.empty());
  EXPECT_TRUE(joinSideResults.at(1).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<b>"), iri("<B>")}}));
  EXPECT_EQ(joinSideResults.at(0).idTable_, makeIdTable({iri("<a>")}));

  EXPECT_EQ(joinSideResults.at(1).idTable_, makeIdTable({iri("<b>")}));
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin,
       prefilterTablesDoesFilterCorrectlyWithSkipTablesWithoutMatchingBlock) {
  std::string kg = "<a> <p> <A> . <b> <p> <B>. ";
  qec_ = getQec(std::move(kg));
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{makeIdTableFromVector({{Id::makeFromBool(true)}}), LocalVocab{}};
    co_yield p1;
    P p2{makeIdTableFromVector({{Id::makeFromBool(true)}}), LocalVocab{}};
    co_yield p2;
  };

  auto [joinSideResults, scanResults] =
      consumeGenerators(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 0);
  ASSERT_EQ(joinSideResults.size(), 0);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesDoesNotFilterOnUndefined) {
  IndexScan scan = makeScan();

  auto makeJoinSide = [](auto* self) -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p1;
    P p2{makeIdTableFromVector({{Id::makeUndefined()}}), LocalVocab{}};
    co_yield p2;
    P p3{makeIdTableFromVector({{Id::makeUndefined()}}), LocalVocab{}};
    co_yield p3;
    P p4{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p4;
    P p5{self->makeIdTable({iri("<a>"), iri("<c>")}), LocalVocab{}};
    co_yield p5;
    P p6{self->makeIdTable({iri("<c>"), iri("<q>"), iri("<xb>")}),
         LocalVocab{}};
    co_yield p6;
    P p7{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p7;
  };

  auto [_, scanResults] = consumeGenerators(
      scan.prefilterTables(LazyResult{makeJoinSide(this)}, 0));

  ASSERT_EQ(scanResults.size(), 3);
  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(1).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(2).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<a>"), iri("<A2>")}}));

  EXPECT_EQ(
      scanResults.at(1).idTable_,
      tableFromTriples({{iri("<b>"), iri("<B>")}, {iri("<b>"), iri("<B2>")}}));

  EXPECT_EQ(
      scanResults.at(2).idTable_,
      tableFromTriples({{iri("<c>"), iri("<C>")}, {iri("<c>"), iri("<C2>")}}));
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesDoesNotFilterWithSingleUndefined) {
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p1{makeIdTableFromVector({{Id::makeUndefined()}}), LocalVocab{}};
    co_yield p1;
  };

  auto [_, scanResults] =
      consumeGenerators(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 3);
  EXPECT_TRUE(scanResults.at(0).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(1).localVocab_.empty());
  EXPECT_TRUE(scanResults.at(2).localVocab_.empty());

  EXPECT_EQ(
      scanResults.at(0).idTable_,
      tableFromTriples({{iri("<a>"), iri("<A>")}, {iri("<a>"), iri("<A2>")}}));

  EXPECT_EQ(
      scanResults.at(1).idTable_,
      tableFromTriples({{iri("<b>"), iri("<B>")}, {iri("<b>"), iri("<B2>")}}));

  EXPECT_EQ(
      scanResults.at(2).idTable_,
      tableFromTriples({{iri("<c>"), iri("<C>")}, {iri("<c>"), iri("<C2>")}}));
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesWorksWithSingleEmptyTable) {
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator {
    using P = Result::IdTableVocabPair;
    P p{IdTable{1, makeAllocator()}, LocalVocab{}};
    co_yield p;
  };

  auto [_, scanResults] =
      consumeGenerators(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 0);
}

// _____________________________________________________________________________
TEST_P(IndexScanWithLazyJoin, prefilterTablesWorksWithEmptyGenerator) {
  IndexScan scan = makeScan();

  auto makeJoinSide = []() -> Result::Generator { co_return; };

  auto [_, scanResults] =
      consumeGenerators(scan.prefilterTables(LazyResult{makeJoinSide()}, 0));

  ASSERT_EQ(scanResults.size(), 0);
}

INSTANTIATE_TEST_SUITE_P(IndexScanWithLazyJoinSuite, IndexScanWithLazyJoin,
                         ::testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "RightSideFirst"
                                             : "LeftSideFirst";
                         });

// _____________________________________________________________________________
TEST(IndexScan, prefilterTablesWithEmptyIndexScanReturnsEmptyGenerators) {
  auto* qec = getQec("<a> <p> <A>");
  // Match with something that does not match.
  SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<not_p>"), Tc{Var{"?y"}}};
  IndexScan scan{qec, Permutation::PSO, xpy};

  auto makeJoinSide = []() -> Result::Generator {
    ADD_FAILURE()
        << "The generator should not be consumed when the IndexScan is empty."
        << std::endl;
    co_return;
  };

  auto [leftGenerator, rightGenerator] =
      scan.prefilterTables(Result::LazyResult{makeJoinSide()}, 0);

  EXPECT_EQ(leftGenerator.begin(), leftGenerator.end());
  EXPECT_EQ(rightGenerator.begin(), rightGenerator.end());
}
// _____________________________________________________________________________
TEST(IndexScan, clone) {
  auto* qec = getQec();
  {
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<not_p>"), Tc{Var{"?y"}}};
    IndexScan scan{qec, Permutation::PSO, xpy};

    auto clone = scan.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(scan), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), scan.getDescriptor());
  }
  {
    using namespace makeFilterExpression;
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<not_p>"), Tc{Var{"?y"}}};
    IndexScan scan{
        qec,
        Permutation::PSO,
        xpy,
        std::nullopt,
        {{filterHelper::pr(ge(IntId(10)), Variable{"?price"}).first, 0}}};

    auto clone = scan.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(scan), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), scan.getDescriptor());
  }
}

// _____________________________________________________________________________
TEST(IndexScan, columnOriginatesFromGraphOrUndef) {
  auto* qec = getQec();
  IndexScan scan1{qec, Permutation::PSO,
                  SparqlTripleSimple{Var{"?x"}, Var{"?y"}, Var{"?z"}}};
  EXPECT_TRUE(scan1.columnOriginatesFromGraphOrUndef(Var{"?x"}));
  EXPECT_TRUE(scan1.columnOriginatesFromGraphOrUndef(Var{"?y"}));
  EXPECT_TRUE(scan1.columnOriginatesFromGraphOrUndef(Var{"?z"}));
  EXPECT_THROW(scan1.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  IndexScan scan2{
      qec, Permutation::PSO,
      SparqlTripleSimple{
          Var{"?x"}, Var{"?y"}, Var{"?z"}, {std::pair{3, Var{"?g"}}}}};
  EXPECT_TRUE(scan2.columnOriginatesFromGraphOrUndef(Var{"?x"}));
  EXPECT_TRUE(scan2.columnOriginatesFromGraphOrUndef(Var{"?y"}));
  EXPECT_TRUE(scan2.columnOriginatesFromGraphOrUndef(Var{"?z"}));
  EXPECT_FALSE(scan2.columnOriginatesFromGraphOrUndef(Var{"?g"}));
  EXPECT_THROW(scan2.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  IndexScan scan3{qec, Permutation::OSP,
                  SparqlTripleSimple{iri("<a>"), Var{"?y"}, iri("<c>")}};
  EXPECT_THROW(scan3.columnOriginatesFromGraphOrUndef(Var{"?x"}),
               ad_utility::Exception);
  EXPECT_TRUE(scan3.columnOriginatesFromGraphOrUndef(Var{"?y"}));
  EXPECT_THROW(scan3.columnOriginatesFromGraphOrUndef(Var{"?z"}),
               ad_utility::Exception);
}
