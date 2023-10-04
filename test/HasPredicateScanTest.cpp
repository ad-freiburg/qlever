// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>

#include "./IndexTestHelpers.h"
#include "./util/AllocatorTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"

namespace {
auto I = ad_utility::testing::IntId;
using Var = Variable;
}  // namespace

// TODO<joka921> More expressive examples with more than one pattern/subject.

/*
TEST(CountAvailablePredicate, fullPatternTrick) {
  std::string kg = "<s1> <p1> <o1>. <s1> <p1> <o2> . <s1> <p2> <o2>";
  auto qec = ad_utility::testing::getQec(kg);
  CountAvailablePredicates count(qec, Variable{"?pred"}, Variable{"?count"});
  auto table = count.computeResultOnlyForTesting().idTable().clone();

  auto id = ad_utility::testing::makeGetId(qec->getIndex());

  auto expected =
      makeIdTableFromVector({{id("<p1>"), I(1)}, {id("<p2>"), I(1)}});

  // TODO<joka921> This fails spuriously because the order of the patterns is not deterministic, we should order the query.
  EXPECT_EQ(table, expected);
}

TEST(CountAvailablePredicate, PatternTrickWithJoin) {
  std::string kg = "<s1> <p1> <o1>. <s1> <p1> <o2> . <s1> <p2> <o2>";
  auto qec = ad_utility::testing::getQec(kg);
  CountAvailablePredicates count(qec, Variable{"?pred"}, Variable{"?count"});
  auto scan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{Var{"?x"}, HAS_PATTERN_PREDICATE, Var{"?p"}});
  auto scan2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, SparqlTriple{Var{"?x"}, "<p1>", Var{"?y"}});
  auto join = ad_utility::makeExecutionTree<Join>(qec, scan, scan2, 0, 0);
  CountAvailablePredicates(qec, join, 0, Var{"?p"}, Var{"?count"});
  auto table = count.computeResultOnlyForTesting().idTable().clone();

  auto id = ad_utility::testing::makeGetId(qec->getIndex());

  auto expected =
      makeIdTableFromVector({{id("<p1>"), I(1)}, {id("<p2>"), I(1)}});

    // TODO<joka921> This fails spuriously because the order of the patterns is not deterministic, we should order the query.
  EXPECT_EQ(table, expected);
}
 */

TEST(CountAvailablePredicate, fullHasPredicateScan) {
  std::string kg = "<s1> <p1> <o1>. <s1> <p1> <o2> . <s1> <p2> <o2>";
  auto qec = ad_utility::testing::getQec(kg);
  IndexScan scan(qec, Permutation::Enum::PSO,
                 SparqlTriple{Var{"?x"}, HAS_PREDICATE_PREDICATE, Var{"?y"}});
  auto table = scan.computeResultOnlyForTesting().idTable().clone();

  auto id = ad_utility::testing::makeGetId(qec->getIndex());

  auto expected = makeIdTableFromVector(
      {{id("<s1>"), id("<p1>")}, {id("<s1>"), id("<p2>")}});

  EXPECT_EQ(table, expected);
}
