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
void testLazyScan(Permutation::IdTableGenerator lazyScan, IndexScan& scanOp,
                  const std::vector<IndexPair>& expectedRows,
                  source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto alloc = ad_utility::makeUnlimitedAllocator<Id>();
  IdTable lazyScanRes{0, alloc};
  size_t numBlocks = 0;
  for (const auto& block : lazyScan) {
    if (lazyScanRes.empty()) {
      lazyScanRes.setNumColumns(block.numColumns());
    }
    lazyScanRes.insertAtEnd(block.begin(), block.end());
    ++numBlocks;
  }

  EXPECT_EQ(numBlocks, lazyScan.details().numBlocksRead_);
  EXPECT_EQ(lazyScanRes.size(), lazyScan.details().numElementsRead_);

  auto resFullScan = scanOp.getResult()->idTable().clone();
  IdTable expected{resFullScan.numColumns(), alloc};

  for (auto [lower, upper] : expectedRows) {
    for (auto index : std::views::iota(lower, upper)) {
      expected.push_back(resFullScan.at(index));
    }
  }

  EXPECT_EQ(lazyScanRes, expected);
}

void testLazyScanForJoinOfTwoScans(
    const std::string& kg, const SparqlTriple& tripleLeft,
    const SparqlTriple& tripleRight, const std::vector<IndexPair>& leftRows,
    const std::vector<IndexPair>& rightRows,
    source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
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

void testLazyScanThrows(const std::string& kg, const SparqlTriple& tripleLeft,
                        const SparqlTriple& tripleRight,
                        source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO, tripleLeft};
  IndexScan s2{qec, Permutation::PSO, tripleRight};
  EXPECT_ANY_THROW(IndexScan::lazyScanForJoinOfTwoScans(s1, s2));
}

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

void testLazyScanWithColumnThrows(
    const std::string& kg, const SparqlTriple& scanTriple,
    std::vector<std::string> columnEntries,
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
  SparqlTriple xpz{Tc{Var{"?x"}}, "<x>", Tc{Var{"?z"}}};
  {
    // In the tests we have a blocksize of two triples per block, and a new
    // block is started for a new relation. That explains the spacing of the
    // following example knowledge graphs.
    std::string kg =
        "<a> <p> <A>. <a> <p> <A2>. "
        "<a> <p> <A3> . <b> <p> <B>. "
        "<b> <p> <B2> ."
        "<b> <x> <xb>. <b> <x> <xb2> .";

    // When joining the <p> and <x> relations, we only need to read the last two
    // blocks of the <p> relation, as <a> never appears as a subject in <x>.
    // This means that the lazy partial scan can skip the first two triples.
    testLazyScanForJoinOfTwoScans(kg, xpy, xpz, {{2, 5}}, {{0, 2}});
  }
  {
    std::string kg =
        "<a> <p2> <A>. <a> <p2> <A2>. "
        "<a> <p2> <A3> . <b> <p2> <B>. "
        "<b> <x> <xb>. <b> <x> <xb2> .";
    // No triple for relation <p> (which doesn't even appear in the knowledge
    // graph), so both lazy scans are empty.
    testLazyScanForJoinOfTwoScans(kg, xpy, xpz, {}, {});
  }
  {
    // No triple for relation <x> (which does appear in the knowledge graph, but
    // not as a predicate), so both lazy scans arek.
    std::string kg =
        "<a> <p> <A>. <a> <p> <A2>. "
        "<a> <p> <A3> . <b> <p> <B>. "
        "<b> <x2> <x>. <b> <x2> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, xpy, xpz, {}, {});
  }
  SparqlTriple bpx{Tc{"<b>"}, "<p>", Tc{Var{"?x"}}};
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<b> <p> <x2> . <b> <p> <x3>. "
        "<b> <p> <x4> . <b> <p> <x7>. "
        "<x2> <x> <xb>. <x5> <x> <xb2> ."
        "<x5> <x> <xb>. <x9> <x> <xb2> ."
        "<x91> <x> <xb>. <x93> <x> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, bpx, xpz, {{1, 5}}, {{0, 4}});
  }
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<x2> <x> <xb>. <x5> <x> <xb2> ."
        "<x5> <x> <xb>. <x9> <x> <xb2> ."
        "<x91> <x> <xb>. <x93> <x> <xb2> .";
    // Scan for a fixed subject that appears in the kg but not as the subject of
    // the <p> predicate.
    SparqlTriple xb2px{Tc{"<xb2>"}, "<p>", Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xpz, {}, {});
  }
  {
    std::string kg =
        "<a> <p> <a1>. <a> <p> <a2>. "
        "<a> <p> <a3> . <b> <p> <x1>. "
        "<x2> <x> <xb>. <x5> <x> <xb2> ."
        "<x5> <x> <xb>. <x9> <x> <xb2> ."
        "<x91> <x> <xb>. <x93> <x> <xb2> .";
    // Scan for a fixed subject that is not even in the knowledge graph.
    SparqlTriple xb2px{Tc{"<notInKg>"}, "<p>", Tc{Var{"?x"}}};
    testLazyScanForJoinOfTwoScans(kg, bpx, xpz, {}, {});
  }

  // Corner cases
  {
    std::string kg = "<a> <b> <c> .";
    // Triples with three variables are not supported.
    SparqlTriple xyz{Tc{Var{"?x"}}, "?y", Tc{Var{"?z"}}};
    testLazyScanThrows(kg, xyz, xpz);
    testLazyScanThrows(kg, xyz, xyz);
    testLazyScanThrows(kg, xpz, xyz);

    // The first variable must be matching (subject variable is ?a vs ?x)
    SparqlTriple abc{Tc{Var{"?a"}}, "<b>", Tc{Var{"?c"}}};
    testLazyScanThrows(kg, abc, xpz);

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
      "<b> <x> <xb>. <b> <x> <xb2> .";
  {
    std::vector<std::string> column{"<a>", "<b>", "<x>", "<xb>"};
    // We need to scan all the blocks that contain the `<p>` predicate.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 5}});
  }
  {
    std::vector<std::string> column{"<b>", "<x>", "<xb>"};
    // The first block only contains <a> which doesn't appear in the first
    // block.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{2, 5}});
  }
  {
    std::vector<std::string> column{"<a>", "<x>", "<xb>"};
    // The first block only contains <a> which only appears in the first two
    // blocks.
    testLazyScanForJoinWithColumn(kg, xpy, column, {{0, 4}});
  }
  {
    std::vector<std::string> column{"<a>", "<x>", "<xb>"};
    // <f> does not appear as a predicate, so the result is empty.
    SparqlTriple efg{Tc{Var{"?e"}}, "<f>", Tc{Var{"?g"}}};
    testLazyScanForJoinWithColumn(kg, efg, column, {});
  }
}

TEST(IndexScan, lazyScanForJoinOfColumnWithScanOneVariable) {
  SparqlTriple bpy{Tc{"<b>"}, "<p>", Tc{Var{"?x"}}};
  std::string kg =
      "<a> <p> <s0>. <a> <p> <s7>. "
      "<a> <p> <s99> . <b> <p> <s1>. "
      "<b> <p> <s2> . <b> <p> <s3>. "
      "<b> <p> <s6> . <b> <p> <s9>. "
      "<b> <x> <s3>. <b> <x> <s5> .";
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
      "<b> <x> <xb>. <b> <x> <xb2> .";

  // Full index scans (three variables) are not supported.
  std::vector<std::string> column{"<a>", "<b>", "<x>", "<xb>"};
  testLazyScanWithColumnThrows(kg, threeVars, column);

  // The join column must be sorted.
  if constexpr (ad_utility::areExpensiveChecksEnabled()) {
    std::vector<std::string> unsortedColumn{"<a>", "<b>", "<a>"};
    SparqlTriple xpy{Tc{Var{"?x"}}, "<p>", Tc{Var{"?y"}}};
    testLazyScanWithColumnThrows(kg, xpy, unsortedColumn);
  }
}
