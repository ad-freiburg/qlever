//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "../util/GTestHelpers.h"
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
                  source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto alloc = ad_utility::makeUnlimitedAllocator<Id>();
  IdTable lazyScanRes{0, alloc};
  size_t numBlocks = 0;
  for (const auto& block : partialLazyScanResult) {
    if (lazyScanRes.empty()) {
      lazyScanRes.setNumColumns(block.numColumns());
    }
    lazyScanRes.insertAtEnd(block.begin(), block.end());
    ++numBlocks;
  }

  EXPECT_EQ(numBlocks, partialLazyScanResult.details().numBlocksRead_);
  EXPECT_EQ(lazyScanRes.size(),
            partialLazyScanResult.details().numElementsRead_);

  auto resFullScan = fullScan.getResult()->idTable().clone();
  IdTable expected{resFullScan.numColumns(), alloc};

  for (auto [lower, upper] : expectedRows) {
    for (auto index : std::views::iota(lower, upper)) {
      expected.push_back(resFullScan.at(index));
    }
  }

  EXPECT_EQ(lazyScanRes, expected);
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
  auto qec = getQec(kgTurtle, true, true, true, blocksizePermutations);
  IndexScan s1{qec, Permutation::PSO, tripleLeft};
  IndexScan s2{qec, Permutation::PSO, tripleRight};
  auto implForSwitch = [](IndexScan& l, IndexScan& r, const auto& expectedL,
                          const auto& expectedR) {
    auto [scan1, scan2] = (IndexScan::lazyScanForJoinOfTwoScans(l, r));

    testLazyScan(std::move(scan1), l, expectedL);
    testLazyScan(std::move(scan2), r, expectedR);
  };
  implForSwitch(s1, s2, leftRows, rightRows);
  implForSwitch(s2, s1, rightRows, leftRows);
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
    std::vector<std::string> columnEntries,
    const std::vector<IndexPair>& expectedRows,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan scan{qec, Permutation::PSO, scanTriple};
  std::vector<Id> column;
  for (const auto& entry : columnEntries) {
    column.push_back(
        TripleComponent{entry}.toValueId(qec->getIndex().getVocab()).value());
  }

  auto lazyScan = IndexScan::lazyScanForJoinOfColumnWithScan(column, scan);
  testLazyScan(std::move(lazyScan), scan, expectedRows);
}

// Test the same scenario as the previous function, but assumes that the
// setting up of the lazy scan fails with an exception.
void testLazyScanWithColumnThrows(
    const std::string& kg, const SparqlTriple& scanTriple,
    const std::vector<std::string>& columnEntries,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO, scanTriple};
  std::vector<Id> column;
  for (const auto& entry : columnEntries) {
    column.push_back(
        TripleComponent{entry}.toValueId(qec->getIndex().getVocab()).value());
  }

  // We need this to suppress the warning about a [[nodiscard]] return value
  // being unused.
  auto makeScan = [&column, &s1]() {
    [[maybe_unused]] auto scan =
        IndexScan::lazyScanForJoinOfColumnWithScan(column, s1);
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
    // not as a predicate), so both lazy scans arek.
    std::string kg =
        "<a> <p> <A>. <a> <p> <A2>. "
        "<a> <p> <A3> . <b> <p> <B>. "
        "<b> <x2> <x>. <b> <x2> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, xpy, xqz, {}, {});
  }
  SparqlTriple bpx{Tc{"<b>"}, "<p>", Tc{Var{"?x"}}};
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
    SparqlTriple xb2px{Tc{"<xb2>"}, "<p>", Tc{Var{"?x"}}};
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
    SparqlTriple xb2px{Tc{"<notInKg>"}, "<p>", Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xqz, {}, {});
  }

  // Corner cases
  {
    std::string kg = "<a> <b> <c> .";
    // Triples with three variables are not supported.
    SparqlTriple xyz{Tc{Var{"?x"}}, "?y", Tc{Var{"?z"}}};
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
    std::vector<std::string> column{"<a>", "<b>", "<q>", "<xb>"};
    // We need to scan all the blocks that contain the `<p>` predicate.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 5}});
  }
  {
    std::vector<std::string> column{"<b>", "<q>", "<xb>"};
    // The first block only contains <a> which doesn't appear in the first
    // block.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{2, 5}});
  }
  {
    std::vector<std::string> column{"<a>", "<q>", "<xb>"};
    // The first block only contains <a> which only appears in the first two
    // blocks.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 4}});
  }
  {
    std::vector<std::string> column{"<a>", "<q>", "<xb>"};
    // <f> does not appear as a predicate, so the result is empty.
    SparqlTriple efg{Tc{Var{"?e"}}, "<f>", Tc{Var{"?g"}}};
    testLazyScanForJoinWithColumn(kg, efg, column, {});
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanOneVariable) {
  SparqlTriple bpy{Tc{"<b>"}, "<p>", Tc{Var{"?x"}}};
  std::string kg =
      "<a> <p> <s0>. <a> <p> <s7>. "
      "<a> <p> <s99> . <b> <p> <s0>. "
      "<b> <p> <s2> . <b> <p> <s3>. "
      "<b> <p> <s6> . <b> <p> <s9>. "
      "<b> <q> <s3>. <b> <q> <s5> .";
  {
    // The subject (<b>) and predicate (<b>) are fixed, so the object is the
    // join column
    std::vector<std::string> column{"<s0>", "<s7>", "<s99>"};
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

  // Full index scans (three variables) are not supported.
  std::vector<std::string> column{"<a>", "<b>", "<q>", "<xb>"};
  testLazyScanWithColumnThrows(kg, threeVars, column);

  // The join column must be sorted.
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    std::vector<std::string> unsortedColumn{"<a>", "<b>", "<a>"};
    SparqlTriple xpy{Tc{Var{"?x"}}, "<p>", Tc{Var{"?y"}}};
    testLazyScanWithColumnThrows(kg, xpy, unsortedColumn);
  }
}

TEST(IndexScan, additionalColumn) {
  auto qec = getQec("<x> <y> <z>.");
  using V = Variable;
  SparqlTriple triple{V{"?x"}, "<y>", V{"?z"}};
  triple._additionalScanColumns.emplace_back(1, V{"?blib"});
  triple._additionalScanColumns.emplace_back(0, V{"?blub"});
  auto scan = IndexScan{qec, Permutation::PSO, triple};
  ASSERT_EQ(scan.getResultWidth(), 4);
  auto col = makeAlwaysDefinedColumn;
  VariableToColumnMap expected = {{V{"?x"}, col(0)},
                                  {V{"?z"}, col(1)},
                                  {V("?blib"), col(2)},
                                  {V("?blub"), col(3)}};
  ASSERT_THAT(scan.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expected));
  ASSERT_THAT(scan.getCacheKey(),
              ::testing::ContainsRegex("Additional Columns: 1 0"));
  // Executing such a query that has the same column multiple times is currently
  // not supported and fails with an exception inside the `IdTable.h` module
  // TODO<joka921> Add proper tests as soon as we can properly add additional
  // columns. Maybe we cann add additional columns generically during the index
  // build by adding a generic transformation function etc.
  AD_EXPECT_THROW_WITH_MESSAGE(scan.computeResultOnlyForTesting(),
                               ::testing::ContainsRegex("IdTable.h"));
}
