// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>

#include "./IndexTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/HasPredicateScan.h"
#include "engine/ValuesForTesting.h"

using ad_utility::testing::makeAllocator;
namespace {
auto Int = ad_utility::testing::IntId;
class HasPredicateScanTest : public ::testing::Test {
 public:
  using Var = Variable;
  std::string kg =
      "<x> <p> <o>. <x> <p2> <o2>. <x> <p2> <o3> . <y> <p> <o> . <y> <p3> "
      "<o4>. <z> <p3> <o2>.";
  QueryExecutionContext* qec = ad_utility::testing::getQec(kg);
  std::function<Id(const std::string&)> getId =
      ad_utility::testing::makeGetId(qec->getIndex());
  Id x = getId("<x>");
  Id y = getId("<y>");
  Id z = getId("<z>");
  Id p = getId("<p>");
  Id p2 = getId("<p2>");
  Id p3 = getId("<p3>");

  void runTest(Operation& op, const VectorTable& expectedElements) {
    auto expected = makeIdTableFromVector(expectedElements);
    EXPECT_THAT(op.getResult()->idTable(),
                ::testing::ElementsAreArray(expected));
  }

  void runTestUnordered(Operation& op, const VectorTable& expectedElements) {
    auto expected = makeIdTableFromVector(expectedElements);
    EXPECT_THAT(op.getResult()->idTable(),
                ::testing::UnorderedElementsAreArray(expected));
  }
};
}  // namespace

TEST_F(HasPredicateScanTest, freeS) {
  auto scan = HasPredicateScan{
      qec, SparqlTriple{Variable{"?x"}, HAS_PREDICATE_PREDICATE, "<p>"}};
  runTest(scan, {{x}, {y}});
}

TEST_F(HasPredicateScanTest, freeO) {
  auto scan = HasPredicateScan{
      qec, SparqlTriple{"<x>", HAS_PREDICATE_PREDICATE, Variable{"?p"}}};
  runTest(scan, {{p}, {p2}});
}

TEST_F(HasPredicateScanTest, fullScan) {
  auto scan = HasPredicateScan{
      qec,
      SparqlTriple{Variable{"?s"}, HAS_PREDICATE_PREDICATE, Variable{"?p"}}};
  runTest(scan, {{x, p}, {x, p2}, {y, p}, {y, p3}, {z, p3}});
}

TEST_F(HasPredicateScanTest, subtree) {
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::OPS, SparqlTriple{Var{"?x"}, "?y", "<o4>"});
  auto scan = HasPredicateScan{qec, indexScan, 1, "?predicate"};
  runTest(scan, {{p3, y, p}, {p3, y, p3}});
}

TEST_F(HasPredicateScanTest, patternTrickWithSubtree) {
  auto triple = SparqlTriple{Var{"?x"}, "<p3>", Var{"?y"}};
  triple._additionalScanColumns.emplace_back(2, Variable{"?predicate"});
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, triple);
  auto patternTrick = CountAvailablePredicates(
      qec, indexScan, 1, Var{"?predicate"}, Var{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(2)}, {p, Int(1)}});
}

TEST_F(HasPredicateScanTest, patternTrickAllEntities) {
  auto triple =
      SparqlTriple{Var{"?x"}, HAS_PATTERN_PREDICATE, Var{"?predicate"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, triple);
  auto patternTrick = CountAvailablePredicates(
      qec, indexScan, 0, Var{"?predicate"}, Var{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(2)}, {p2, Int(1)}, {p, Int(2)}});
}
