//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "engine/IndexScan.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;

std::string kg = "<a> <p> <A>. <b> <p> <B>. <a> <x> <xa>. <b> <x> <xb>.";

TEST(IndexScan, lazyScanForJoinOfTwoScans) {
  using Tc = TripleComponent;
  using Var = Variable;
  auto qec = getQec(kg);
  IndexScan s1{qec, Permutation::PSO,
               SparqlTriple{Tc{Var{"?x"}}, "<p>", Tc{Var{"?y"}}}};
  IndexScan s2{qec, Permutation::PSO,
               SparqlTriple{Tc{Var{"?x"}}, "<x>", Tc{Var{"?z"}}}};
  auto [scan1, scan2] = (IndexScan::lazyScanForJoinOfTwoScans(s1, s2));

  IdTable res1{2, ad_utility::makeUnlimitedAllocator<Id>()};
  IdTable res2{2, ad_utility::makeUnlimitedAllocator<Id>()};

  for (const auto& block : scan1) {
    res1.insertAtEnd(block.begin(), block.end());
  }
  for (const auto& block : scan2) {
    res2.insertAtEnd(block.begin(), block.end());
  }

  EXPECT_EQ(res1.size(), 2u);
  EXPECT_EQ(res2.size(), 2u);

  // TODO<joka921> We need additional tests that are not only dummys. To make
  // this work, we need to have the blocksize of the index class configurable, I
  // will split this into a separate PR.
}
