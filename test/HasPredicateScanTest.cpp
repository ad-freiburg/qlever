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

namespace {
using ad_utility::testing::makeAllocator;
auto Int = ad_utility::testing::IntId;

// A text fixture that is used in the following. It consists of a small index and variables for all the IDs that
// appear in the index.
class HasPredicateScanTest : public ::testing::Test {
 public:
  using Var = Variable;
  std::string kg =
      "<x> <p> <o>. <x> <p2> <o2>. <x> <p2> <o3> . <y> <p> <o> . <y> <p3> "
      "<o4>. <z> <p3> <o2>.";
  // Mapping from subjects to distinct predicates (makes reading the test results easier).
  // x -> p p2
  // y -> p p3
  // z -> p3
  QueryExecutionContext* qec = ad_utility::testing::getQec(kg);
  std::function<Id(const std::string&)> getId =
      ad_utility::testing::makeGetId(qec->getIndex());
  Id x = getId("<x>");
  Id y = getId("<y>");
  Id z = getId("<z>");
  Id p = getId("<p>");
  Id p2 = getId("<p2>");
  Id p3 = getId("<p3>");

  // Expect that the result of the `operation` matches the `expectedElements`.
  void runTest(Operation& operation, const VectorTable& expectedElements) {
    auto expected = makeIdTableFromVector(expectedElements);
    EXPECT_THAT(operation.getResult()->idTable(),
                ::testing::ElementsAreArray(expected));
  }

  // Expect that the result of the `operation` matches the `expectedElements`, but without
  // taking the order into account.
  void runTestUnordered(Operation& op, const VectorTable& expectedElements) {
    auto expected = makeIdTableFromVector(expectedElements);
    EXPECT_THAT(op.getResult()->idTable(),
                ::testing::UnorderedElementsAreArray(expected));
  }
};
}  // namespace

// TODO<joka921> In addition to the manual setups of the operations, we could
// also test the query setup in an E2E session by going through the
// queryPlanner.
// _____________________________________________________________
TEST_F(HasPredicateScanTest, freeS) {
  // ?x ql:has-predicate <p>, expected result : <x> and <y>
  auto scan = HasPredicateScan{
      qec, SparqlTriple{Variable{"?x"}, HAS_PREDICATE_PREDICATE, "<p>"}};
  runTest(scan, {{x}, {y}});
}

// _____________________________________________________________
TEST_F(HasPredicateScanTest, freeO) {
  // <x> ql:has-predicate ?p, expected result : <p> and <p2>
  auto scan = HasPredicateScan{
      qec, SparqlTriple{"<x>", HAS_PREDICATE_PREDICATE, Variable{"?p"}}};
  runTest(scan, {{p}, {p2}});
}

// _____________________________________________________________
TEST_F(HasPredicateScanTest, fullScan) {
  // ?x ql:has-predicate ?y, expect the full mapping.
  auto scan = HasPredicateScan{
      qec,
      SparqlTriple{Variable{"?s"}, HAS_PREDICATE_PREDICATE, Variable{"?p"}}};
  runTest(scan, {{x, p}, {x, p2}, {y, p}, {y, p3}, {z, p3}});

  // Full scans with the same variable in the subject and object are not
  // supported.
  auto makeIllegalScan = [this] {
    return HasPredicateScan{
        qec,
        SparqlTriple{Variable{"?s"}, HAS_PREDICATE_PREDICATE, Variable{"?s"}}};
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeIllegalScan(),
      ::testing::ContainsRegex(
          "same variable for subject and object not supported"));

  // Triples without any variables also aren't supported currently.
  auto makeIllegalScan2 = [this] {
    return HasPredicateScan{
        qec, SparqlTriple{"<x>", HAS_PREDICATE_PREDICATE, "<y>"}};
  };
  EXPECT_ANY_THROW(makeIllegalScan2());
}

// _____________________________________________________________
TEST_F(HasPredicateScanTest, subtree) {
  // ?x ?y <o4> . ?x ql:has-predicate ?predicate.
  // The first triple matches only `<y> <p3> <o4>`, so we get the pattern
  // for `y` with an additional column that always is `<p3.`
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::OPS, SparqlTriple{Var{"?x"}, "?y", "<o4>"});
  auto scan = HasPredicateScan{qec, indexScan, 1, "?predicate"};
  runTest(scan, {{p3, y, p}, {p3, y, p3}});
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickWithSubtree) {
  /* Manual setup of the operations for the following pattern trick
   * query:
   * SELECT ?predicate COUNT(DISTINCT ?x) WHERE {
   *   ?x <p3> ?y.
   *   ?x ?predicate ?o
   * } GROUP BY ?predicate
  */
  auto triple = SparqlTriple{Var{"?x"}, "<p3>", Var{"?y"}};
  triple._additionalScanColumns.emplace_back(
      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN, Variable{"?predicate"});
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, triple);
  auto patternTrick = CountAvailablePredicates(
      qec, indexScan, 1, Var{"?predicate"}, Var{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(2)}, {p, Int(1)}});
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickWithSubtreeTwoFixedElements) {
  /* Manual setup of the operations for the following pattern trick
   * query (not so different, but increases the test coverage):
   * SELECT ?predicate COUNT(DISTINCT ?x) WHERE {
   *   ?x <p3> <o4>.
   *   ?x ?predicate ?o
   * } GROUP BY ?predicate
   */
  auto triple = SparqlTriple{Var{"?x"}, "<p3>", "<o4>"};
  triple._additionalScanColumns.emplace_back(
      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN, Variable{"?predicate"});
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::POS, triple);
  auto patternTrick = CountAvailablePredicates(
      qec, indexScan, 0, Var{"?predicate"}, Var{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(1)}, {p, Int(1)}});
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickAllEntities) {
  /* Manual setup of the operations for the full pattern trick:
   * SELECT ?predicate COUNT(DISTINCT ?x) WHERE {
   *   ?x ?predicate ?o
   * } GROUP BY ?predicate
   */
  auto triple =
      SparqlTriple{Var{"?x"}, HAS_PATTERN_PREDICATE, Var{"?predicate"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, triple);
  auto patternTrick = CountAvailablePredicates(
      qec, indexScan, 0, Var{"?predicate"}, Var{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(2)}, {p2, Int(1)}, {p, Int(2)}});
}
