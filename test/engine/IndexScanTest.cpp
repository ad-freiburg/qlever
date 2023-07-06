//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "engine/IndexScan.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;

using IndexPair = std::pair<size_t, size_t>;
void testLazyScan(auto lazyScan, IndexScan& scanOp,
                  const std::vector<IndexPair>& expectedRows) {
  auto alloc = ad_utility::makeUnlimitedAllocator<Id>();
  IdTable lazyScanRes{2, alloc};
  size_t numBlocks = 0;
  for (const auto& block : lazyScan) {
    lazyScanRes.insertAtEnd(block.begin(), block.end());
    ++numBlocks;
  }

  auto resFullScan = scanOp.getResult()->idTable().clone();
  IdTable expected{resFullScan.numColumns(), alloc};

  for (auto [lower, upper] : expectedRows) {
    for (auto index : std::views::iota(lower, upper)) {
      expected.push_back(resFullScan.at(index));
    }
  }

  EXPECT_EQ(lazyScanRes, expected);
}

void testLazyScanForJoinOfTwoScans(const std::string& kg,
                                   const std::vector<IndexPair>& leftRows,
                                   const std::vector<IndexPair>& rightRows) {
  using Tc = TripleComponent;
  using Var = Variable;
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO,
               SparqlTriple{Tc{Var{"?x"}}, "<p>", Tc{Var{"?y"}}}};
  IndexScan s2{qec, Permutation::PSO,
               SparqlTriple{Tc{Var{"?x"}}, "<x>", Tc{Var{"?z"}}}};
  auto [scan1, scan2] = (IndexScan::lazyScanForJoinOfTwoScans(s1, s2));

  testLazyScan(std::move(scan1), s1, leftRows);
  testLazyScan(std::move(scan2), s2, rightRows);
}

TEST(IndexScan, lazyScanForJoinOfTwoScans) {
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
    testLazyScanForJoinOfTwoScans(kg, {{2, 5}}, {{0, 2}});
  }
  {
    // No triple for relation <p>, so both scans can be empty.
    std::string kg =
        "<a> <p2> <A>. <a> <p2> <A2>. "
        "<a> <p2> <A3> . <b> <p2> <B>. "
        "<b> <x> <xb>. <b> <x> <xb2> .";
    testLazyScanForJoinOfTwoScans(kg, {}, {});
  }
}
