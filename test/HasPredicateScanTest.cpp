// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/HasPredicateScan.h"
#include "engine/IndexScan.h"
#include "engine/PermutationSelector.h"
#include "engine/ValuesForTesting.h"
#include "global/Pattern.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

namespace {
using ad_utility::testing::makeAllocator;
auto Int = ad_utility::testing::IntId;
auto iri = ad_utility::testing::iri;

// A text fixture that is used in the following. It consists of a small index
// and variables for all the IDs that appear in the index.
class HasPredicateScanTest : public ::testing::Test {
 public:
  using V = Variable;
  std::string kg =
      "<x> <p> <o>. <x> <p2> <o2>. <x> <p2> <o3> . <y> <p> <o> . <y> <p3> "
      "<o4>. <z> <p3> <o2>.";
  // Mapping from subjects to distinct predicates (makes reading the test
  // results easier). x -> p p2 y -> p p3 z -> p3
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
    auto res = operation.computeResultOnlyForTesting();
    EXPECT_THAT(res.idTableView(), ::testing::ElementsAreArray(expected));
  }

  // Expect that the result of the `operation` matches the `expectedElements`,
  // but without taking the order into account.
  void runTestUnordered(Operation& op, const VectorTable& expectedElements) {
    auto expected = makeIdTableFromVector(expectedElements);
    EXPECT_THAT(op.computeResultOnlyForTesting().idTableView(),
                ::testing::UnorderedElementsAreArray(expected));
  }
};
}  // namespace

// TODO<joka921> In addition to the manual setups of the operations, we could
// also test the query setup in an E2E session by going through the
// queryPlanner.
// _____________________________________________________________
TEST_F(HasPredicateScanTest, freeS) {
  // Free the cache to get a fresh `IndexScan`.
  qec->getQueryTreeCache().clearAll();
  // ?x ql:has-predicate <p>, expected result : <x> and <y>
  auto scan = HasPredicateScan{
      qec,
      SparqlTriple{Variable{"?x"}, iri(HAS_PREDICATE_PREDICATE), iri("<p>")}};
  runTest(scan, {{x}, {y}});
  // Run again to test handling a cached `IndexScan`.
  runTest(scan, {{x}, {y}});
}

// _____________________________________________________________
TEST_F(HasPredicateScanTest, freeO) {
  // <x> ql:has-predicate ?p, expected result : <p> and <p2>
  auto scan = HasPredicateScan{
      qec,
      SparqlTriple{iri("<x>"), iri(HAS_PREDICATE_PREDICATE), Variable{"?p"}}};
  runTest(scan, {{p}, {p2}});
}
// _____________________________________________________________
TEST_F(HasPredicateScanTest, clone) {
  {
    HasPredicateScan scan{
        qec,
        SparqlTriple{Variable{"?x"}, iri(HAS_PREDICATE_PREDICATE), iri("<p>")}};

    auto clone = scan.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(scan), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), scan.getDescriptor());

    EXPECT_EQ(scan.getChildren().empty(), cloneReference.getChildren().empty());
  }
  {
    HasPredicateScan scan{qec,
                          ad_utility::makeExecutionTree<ValuesForTesting>(
                              qec, makeIdTableFromVector({{0}}),
                              std::vector<std::optional<V>>{{V{"?p"}}}),
                          0, V{"?x"}};

    auto clone = scan.clone();
    ASSERT_TRUE(clone);
    const auto& cloneReference = *clone;
    EXPECT_EQ(typeid(scan), typeid(cloneReference));
    EXPECT_EQ(cloneReference.getDescriptor(), scan.getDescriptor());

    EXPECT_NE(scan.getChildren().at(0), cloneReference.getChildren().at(0));
  }
}

// _____________________________________________________________
TEST_F(HasPredicateScanTest, fullScan) {
  // Free the cache to get a fresh `IndexScan`.
  qec->getQueryTreeCache().clearAll();
  // ?x ql:has-predicate ?y, expect the full mapping.
  auto scan = HasPredicateScan{
      qec, SparqlTriple{Variable{"?s"}, iri(HAS_PREDICATE_PREDICATE),
                        Variable{"?p"}}};
  runTest(scan, {{x, p}, {x, p2}, {y, p}, {y, p3}, {z, p3}});
  // Run again to test handling a cached `IndexScan`.
  runTest(scan, {{x, p}, {x, p2}, {y, p}, {y, p3}, {z, p3}});

  // Full scans with the same variable in the subject and object are not
  // supported.
  auto makeIllegalScan = [this] {
    return HasPredicateScan{
        qec, SparqlTriple{Variable{"?s"}, iri(HAS_PREDICATE_PREDICATE),
                          Variable{"?s"}}};
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeIllegalScan(),
      ::testing::ContainsRegex(
          "same variable for subject and object not supported"));

  // Triples without any variables also aren't supported currently.
  auto makeIllegalScan2 = [this] {
    return HasPredicateScan{
        qec, SparqlTriple{"<x>", iri(HAS_PREDICATE_PREDICATE), "<y>"}};
  };
  EXPECT_ANY_THROW(makeIllegalScan2());
}

// _____________________________________________________________
TEST_F(HasPredicateScanTest, subtree) {
  // ?x ?y <o4> . ?x ql:has-predicate ?predicate.
  // The first triple matches only `<y> <p3> <o4>`, so we get the pattern
  // for `y` with an additional column that always is `<p3.`
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::OPS,
      SparqlTripleSimple{V{"?x"}, V{"?y"}, iri("<o4>")});
  auto scan = HasPredicateScan{qec, indexScan, 1, V{"?predicate"}};
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
  auto triple = SparqlTripleSimple{V{"?x"}, iri("<p3>"), V{"?y"}};
  triple.additionalScanColumns_.emplace_back(
      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN, V{"?predicate"});
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, triple);
  auto patternTrick =
      CountAvailablePredicates(qec, indexScan, 1, V{"?predicate"}, V{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(2)}, {p, Int(1)}});
}
// ____________________________________________________________
TEST_F(HasPredicateScanTest, cloneCountAvailablePredicates) {
  auto triple = SparqlTripleSimple{V{"?x"}, iri("<p3>"), V{"?y"}};
  triple.additionalScanColumns_.emplace_back(
      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN, V{"?predicate"});
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, triple);
  CountAvailablePredicates patternTrick{qec, indexScan, 1, V{"?predicate"},
                                        V{"?count"}};

  auto clone = patternTrick.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(patternTrick, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), patternTrick.getDescriptor());
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
  auto triple = SparqlTripleSimple{V{"?x"}, iri("<p3>"), iri("<o4>")};
  triple.additionalScanColumns_.emplace_back(
      ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN, Variable{"?predicate"});
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::POS, triple);
  auto patternTrick =
      CountAvailablePredicates(qec, indexScan, 0, V{"?predicate"}, V{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(1)}, {p, Int(1)}});
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickIllegalInput) {
  auto I = ad_utility::testing::IntId;
  auto Voc = ad_utility::testing::VocabId;
  // The subtree of the `CountAvailablePredicates` is illegal, because the
  // pattern index column contains the entry `273` which is neither `NoPattern`
  // nor a valid pattern index.
  auto illegalInput = makeIdTableFromVector(
      {{Voc(0), I(273)}, {Voc(1), I(Pattern::NoPattern)}});
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(illegalInput),
      std::vector<std::optional<Variable>>{V{"?x"}, V{"?predicate"}});

  auto patternTrick =
      CountAvailablePredicates(qec, subtree, 1, V{"?predicate"}, V{"?count"});
  EXPECT_ANY_THROW(runTestUnordered(patternTrick, {{p3, Int(2)}, {p, Int(1)}}));
}

// ____________________________________________________________
// Build a `CountAvailablePredicates` operation for the given `input`, of which
// column 0 holds the subjects and column 1 the pattern indices. The input is
// declared as sorted by the subjects, so that no additional `Sort` is added.
static std::shared_ptr<QueryExecutionTree> makePatternTrickSubtree(
    QueryExecutionContext* qec, IdTable input) {
  return ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input),
      std::vector<std::optional<Variable>>{Variable{"?x"},
                                           Variable{"?predicate"}},
      false, std::vector<ColumnIndex>{0});
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, countAvailablePredicatesEstimates) {
  IdTable input{2, makeAllocator()};
  input.push_back({Int(0), Int(0)});
  auto patternTrick = CountAvailablePredicates(
      qec, makePatternTrickSubtree(qec, std::move(input)), 0, V{"?predicate"},
      V{"?count"});
  // The multiplicity of the counts cannot be determined without computing the
  // result, so it always is 1.
  EXPECT_FLOAT_EQ(patternTrick.getMultiplicity(0), 1.0f);
  EXPECT_FLOAT_EQ(patternTrick.getMultiplicity(1), 1.0f);
  // The estimates are very rough, we only check that they are computed at all.
  EXPECT_NO_THROW(patternTrick.getSizeEstimate());
  EXPECT_NO_THROW(patternTrick.getCostEstimate());
  EXPECT_THAT(patternTrick.getCacheKey(),
              ::testing::HasSubstr("COUNT_AVAILABLE_PREDICATES (col 0)"));
  EXPECT_EQ(patternTrick.getDescriptor(), "CountAvailablePredicates");
  EXPECT_THAT(patternTrick.getChildren(), ::testing::SizeIs(1));
  EXPECT_FALSE(patternTrick.knownEmptyResult());
  EXPECT_THAT(patternTrick.resultSortedOn(), ::testing::IsEmpty());
  EXPECT_EQ(patternTrick.getResultWidth(), 2u);
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickEmptyInput) {
  // The pattern trick on an empty input yields an empty result.
  IdTable input{2, makeAllocator()};
  auto patternTrick = CountAvailablePredicates(
      qec, makePatternTrickSubtree(qec, std::move(input)), 0, V{"?predicate"},
      V{"?count"});
  EXPECT_EQ(patternTrick.computeResultOnlyForTesting().idTableView().numRows(),
            0u);
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickEntitiesWithoutPattern) {
  // Entities that have no pattern at all contribute no predicates.
  auto Voc = ad_utility::testing::VocabId;
  IdTable input{2, makeAllocator()};
  input.push_back({Voc(0), Int(Pattern::NoPattern)});
  input.push_back({Voc(1), Int(Pattern::NoPattern)});
  auto patternTrick = CountAvailablePredicates(
      qec, makePatternTrickSubtree(qec, std::move(input)), 0, V{"?predicate"},
      V{"?count"});
  EXPECT_EQ(patternTrick.computeResultOnlyForTesting().idTableView().numRows(),
            0u);
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickEntitiesWithoutPatternWiderInput) {
  // The same as above, but with an input of three columns, which instantiates a
  // different `computePatternTrick<WIDTH>`. The middle column is ignored.
  auto Voc = ad_utility::testing::VocabId;
  IdTable input{3, makeAllocator()};
  input.push_back({Voc(0), Voc(17), Int(0)});
  input.push_back({Voc(1), Voc(17), Int(Pattern::NoPattern)});
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input),
      std::vector<std::optional<Variable>>{V{"?x"}, V{"?y"}, V{"?predicate"}},
      false, std::vector<ColumnIndex>{0});
  auto patternTrick =
      CountAvailablePredicates(qec, subtree, 0, V{"?predicate"}, V{"?count"});
  // Only the first entity has a pattern, and pattern `0` holds two predicates.
  const auto& patterns = qec->getIndex().getPatterns();
  VectorTable expected;
  for (Id predicate : patterns[0]) {
    expected.push_back({predicate, Int(1)});
  }
  runTestUnordered(patternTrick, expected);
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickWithLargeInput) {
  // An input that is large enough to be split into several chunks that are
  // processed in parallel, the partial results of which then have to be merged
  // (see `CHUNK_SIZE_ROWS` in `CountAvailablePredicates.cpp`).
  auto Voc = ad_utility::testing::VocabId;
  const auto& patterns = qec->getIndex().getPatterns();
  // The patterns of the index of this fixture, see above: `x -> p p2`,
  // `y -> p p3`, and `z -> p3`.
  ASSERT_EQ(patterns.size(), 3u);

  // Each subject appears twice (the duplicates must not be counted twice), and
  // each of the patterns is used by the same number of subjects. The number of
  // rows has to exceed `CHUNK_SIZE_ROWS` for the input to be split.
  constexpr size_t numSubjects = 501'000;
  static_assert(numSubjects % 3 == 0);
  static_assert(2 * numSubjects > 500'000);
  IdTable input{2, makeAllocator()};
  input.reserve(2 * numSubjects);
  for (size_t i = 0; i < numSubjects; ++i) {
    input.push_back({Voc(i), Int(i % 3)});
    input.push_back({Voc(i), Int(i % 3)});
  }

  // Each predicate is counted once for each subject the pattern of which
  // contains that predicate.
  ad_utility::HashMap<Id, size_t> expectedCounts;
  for (size_t patternIdx = 0; patternIdx < patterns.size(); ++patternIdx) {
    for (Id predicate : patterns[patternIdx]) {
      expectedCounts[predicate] += numSubjects / 3;
    }
  }
  VectorTable expected;
  for (const auto& [predicate, count] : expectedCounts) {
    expected.push_back({predicate, Int(count)});
  }
  ASSERT_THAT(expected, ::testing::SizeIs(3));

  auto patternTrick = CountAvailablePredicates(
      qec, makePatternTrickSubtree(qec, std::move(input)), 0, V{"?predicate"},
      V{"?count"});
  runTestUnordered(patternTrick, expected);
}

// ____________________________________________________________
TEST_F(HasPredicateScanTest, patternTrickAllEntities) {
  /* Manual setup of the operations for the full pattern trick:
   * SELECT ?predicate COUNT(DISTINCT ?x) WHERE {
   *   ?x ?predicate ?o
   * } GROUP BY ?predicate
   */
  auto indexScan = HasPredicateScan::makePatternScan(
      qec, TripleComponent{V{"?x"}}, V{"?predicate"});
  auto patternTrick =
      CountAvailablePredicates(qec, indexScan, 0, V{"?predicate"}, V{"?count"});

  runTestUnordered(patternTrick, {{p3, Int(2)}, {p2, Int(1)}, {p, Int(2)}});
}
