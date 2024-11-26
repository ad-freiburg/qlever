//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "engine/IndexScan.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;

namespace {
using Tc = TripleComponent;
using Var = Variable;

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
      for (auto index : std::views::iota(lower, upper)) {
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
    const std::string& kgTurtle, const SparqlTriple& tripleLeft,
    const SparqlTriple& tripleRight, const std::vector<IndexPair>& leftRows,
    const std::vector<IndexPair>& rightRows,
    ad_utility::MemorySize blocksizePermutations = 16_B,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  // As soon as there is a LIMIT clause present, we cannot use the prefiltered
  // blocks.
  std::vector<LimitOffsetClause> limits{{}, {12, 3}, {2, 3}};
  for (const auto& limit : limits) {
    auto qec = getQec(kgTurtle, true, true, true, blocksizePermutations);
    IndexScan s1{qec, Permutation::PSO, tripleLeft};
    s1.setLimit(limit);
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
void testLazyScanThrows(const std::string& kg, const SparqlTriple& tripleLeft,
                        const SparqlTriple& tripleRight,
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
    const std::string& kg, const SparqlTriple& scanTriple,
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
    const std::string& kg, const SparqlTriple& scanTriple,
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

}  // namespace

TEST(IndexScan, lazyScanForJoinOfTwoScans) {
  SparqlTriple xpy{Tc{Var{"?x"}}, "<p>", Tc{Var{"?y"}}};
  SparqlTriple xqz{Tc{Var{"?x"}}, "<q>", Tc{Var{"?z"}}};
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
  SparqlTriple bpx{Tc{iri("<b>")}, "<p>", Tc{Var{"?x"}}};
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
    SparqlTriple xb2px{Tc{iri("<xb2>")}, "<p>", Tc{Var{"?x"}}};
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
    SparqlTriple xb2px{Tc{iri("<notInKg>")}, "<p>", Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {}, {});
  }

  // Corner cases
  {
    std::string kg = "<a> <b> <c> .";
    SparqlTriple xyz{Tc{Var{"?x"}}, "?y", Tc{Var{"?z"}}};
    testLazyScanThrows(kg, xyz, xqz);
    testLazyScanThrows(kg, xyz, xqz);
    testLazyScanThrows(kg, xyz, xyz);
    testLazyScanThrows(kg, xqz, xyz);

    // The first variable must be matching (subject variable is ?a vs ?x)
    SparqlTriple abc{Tc{Var{"?a"}}, "<b>", Tc{Var{"?c"}}};
    testLazyScanThrows(kg, abc, xqz);

    // If both scans have two variables, then the second variable must not
    // match.
    testLazyScanThrows(kg, abc, abc);
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanTwoVariables) {
  SparqlTriple xpy{Tc{Var{"?x"}}, "<p>", Tc{Var{"?y"}}};
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
    SparqlTriple efg{Tc{Var{"?e"}}, "<f>", Tc{Var{"?g"}}};
    testLazyScanForJoinWithColumn(kg, efg, column, {});
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanOneVariable) {
  SparqlTriple bpy{Tc{iri("<b>")}, "<p>", Tc{Var{"?x"}}};
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
  SparqlTriple threeVars{Tc{Var{"?x"}}, "?b", Tc{Var{"?y"}}};
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
    SparqlTriple xpy{Tc{Var{"?x"}}, "<p>", Tc{Var{"?y"}}};
    testLazyScanWithColumnThrows(kg, xpy, unsortedColumn);
  }
}

TEST(IndexScan, additionalColumn) {
  auto qec = getQec("<x> <y> <z>.");
  using V = Variable;
  SparqlTriple triple{V{"?x"}, "<y>", V{"?z"}};
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
      {{getId("<x>"), getId("<z>"), I(0), I(NO_PATTERN)}});
  EXPECT_THAT(res.idTable(), ::testing::ElementsAreArray(exp));
}

// Test that the graphs by which an `IndexScan` is to be filtered is correctly
// reflected in its cache key and its `ScanSpecification`.
TEST(IndexScan, namedGraphs) {
  auto qec = getQec("<x> <y> <z>.");
  using V = Variable;
  SparqlTriple triple{V{"?x"}, "<y>", V{"?z"}};
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
    EXPECT_EQ(scan.getSizeEstimate(), 0);
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

// _____________________________________________________________________________
TEST(IndexScan, computeResultCanBeConsumedLazily) {
  using V = Variable;
  auto qec = getQec("<x> <p> <s1>, <s2>. <x> <p2> <s1>.", true, false);
  auto getId = makeGetId(qec->getIndex());
  auto x = getId("<x>");
  auto p = getId("<p>");
  auto s1 = getId("<s1>");
  auto s2 = getId("<s2>");
  auto p2 = getId("<p2>");
  SparqlTripleSimple scanTriple{V{"?x"}, V{"?y"}, V{"?z"}};
  IndexScan scan{qec, Permutation::Enum::POS, scanTriple};

  ProtoResult result = scan.computeResultOnlyForTesting(true);

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
  auto qec = getQec("<x> <p> <s1>, <s2>. <x> <p2> <s1>.", true, false);
  SparqlTripleSimple scanTriple{V{"?x"}, I::fromIriref("<abcdef>"), V{"?z"}};
  IndexScan scan{qec, Permutation::Enum::POS, scanTriple};

  ProtoResult result = scan.computeResultOnlyForTesting(true);

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
  auto qec = getQec("<x> <p> <s1>, <s2>. <x> <p2> <s1>.", true, false);
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
