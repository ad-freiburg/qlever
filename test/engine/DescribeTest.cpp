// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "engine/Describe.h"
#include "engine/IndexScan.h"
#include "engine/NeutralElementOperation.h"
#include "engine/QueryExecutionTree.h"

using namespace ad_utility::testing;

namespace {
// Return a matcher that matches a `span<const Id>` or something similar  that
// contains `expectedNumUnique` elements.
auto numUnique = [](size_t expectedNumUnique) {
  using namespace ::testing;
  auto getNumUnique = [](const auto& col) {
    return ad_utility::HashSet<Id>(col.begin(), col.end()).size();
  };
  return ResultOf(getNumUnique, expectedNumUnique);
};
}  // namespace

// Test DESCRIBE query with a fixed IRI and sveral blank nodes that need to be
// expanded.
TEST(Describe, recursiveBlankNodes) {
  auto qec = getQec(
      " <s> <p>   <o> ."
      " <s> <p>  _:g1 ."
      "_:g1 <p2> <o2> ."
      "_:g1 <p2> _:g1 ."
      "_:g1 <p2> _:g2 ."
      "_:g2 <p>  <o4> ."
      "<s2> <p>   <o> ."
      "_:g4 <p>  _:g5 .");
  parsedQuery::Describe parsedDescribe;
  parsedDescribe.resources_.push_back(TripleComponent::Iri::fromIriref("<s>"));
  Describe describe{qec,
                    ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
                    parsedDescribe};
  auto res = describe.computeResultOnlyForTesting();
  const auto& table = res.idTable();
  // The expected result is as follows:
  //
  //   <s> <p>   <o>
  //   <s> <p>  _:g1
  //  _:g1 <p2> <o2>
  //  _:g1 <p2> _:g1
  //  _:g1 <p2> _:g2
  //  _:g2 <p>  <o4>
  //
  // However, we cannot control the names given to the blank nodes, but we can
  // at least check the statistics.
  EXPECT_EQ(table.size(), 6);
  EXPECT_THAT(table.getColumn(0), numUnique(3));
  EXPECT_THAT(table.getColumn(1), numUnique(2));
  EXPECT_THAT(table.getColumn(2), numUnique(5));
}

// Test DESCRIBE query with a fixed IRI and a variable in the DESCRIBE clause,
// and various blank nodes that need to be expanded.
TEST(Describe, describeWithVariable) {
  auto qec = getQec(
      " <s> <p>   <o> ."
      " <s> <p>  _:g1 ."
      "_:g1 <p2> <o2> ."
      "<s2> <p>   <o> ."
      "<s2> <p2> _:g1 ."
      "<s2> <p2> _:g2 ."
      "_:g2 <p3> <o3> ."
      "<s3> <p2>  <o> ."
      "<s4> <p2> <o2> .");

  // On the above knowledge graph, evaluate `DESCRIBE <s4> ?x { ?x <p> <o> }`.
  parsedQuery::Describe parsedDescribe;
  parsedDescribe.resources_.push_back(TripleComponent::Iri::fromIriref("<s4>"));
  parsedDescribe.resources_.push_back(Variable{"?x"});
  SparqlTripleSimple triple{Variable{"?x"},
                            TripleComponent::Iri::fromIriref("<p>"),
                            TripleComponent::Iri::fromIriref("<o>")};
  Describe describe{qec,
                    ad_utility::makeExecutionTree<IndexScan>(
                        qec, Permutation::Enum::POS, triple),
                    parsedDescribe};
  auto res = describe.computeResultOnlyForTesting();
  const auto& table = res.idTable();
  // The expected result is as follows (the resources are `<s4>`, which is
  // explicitly requested, and `<s>` and `<s2>`, which match `?x` in the WHERE
  // clause):
  //
  //   <s> <p>   <o>
  //   <s> <p>  _:g1
  //  _:g1 <p2> <o2>
  //  <s2> <p>   <o>
  //  <s2> <p2> _:g1    [note that _:g1 has already been expanded above]
  //  <s2> <p2> _:g2
  //  _:g2 <p3> <o3>
  //  <s4> <p2> <o2>
  //
  // However, we cannot control the names given to the blank nodes, but we can
  // at least check the statistics.
  EXPECT_EQ(table.size(), 8);
  EXPECT_THAT(table.getColumn(0), numUnique(5));
  EXPECT_THAT(table.getColumn(1), numUnique(3));
  EXPECT_THAT(table.getColumn(2), numUnique(5));
}

// Test DESCRIBE query with a variable but not WHERE clause (which should
// return an empty result).
TEST(Describe, describeWithVariableButNoWhereClause) {
  auto qec = getQec("<s> <p> <o>");
  parsedQuery::Describe parsedDescribe;
  parsedDescribe.resources_.push_back(Variable{"?x"});
  auto noWhere = ad_utility::makeExecutionTree<NeutralElementOperation>(qec);
  Describe describe{qec, noWhere, parsedDescribe};
  auto result = describe.computeResultOnlyForTesting();
  EXPECT_EQ(result.idTable().size(), 0);
  EXPECT_EQ(result.idTable().numColumns(), 3);
}

// TODO<joka921> Add tests with inputs from a different graph, but those are
// Currently hard to do with the given `getQec` function.

// Test the various member functions of the `Describe` operation.
TEST(Describe, simpleMembers) {
  auto qec = getQec(
      " <s> <p>   <o> ."
      " <s> <p>  _:g1 ."
      "_:g1 <p2> <o2> ."
      "_:g1 <p2> _:g1 ."
      "_:g1 <p2> _:g2 ."
      "_:g2 <p>  <o4> ."
      "<s2> <p>   <o> ."
      "_:g4 <p>  _:g5 .");
  parsedQuery::Describe parsedDescribe;
  parsedDescribe.resources_.push_back(TripleComponent::Iri::fromIriref("<s>"));
  Describe describe{qec,
                    ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
                    parsedDescribe};

  EXPECT_EQ(describe.getDescriptor(), "DESCRIBE");
  EXPECT_EQ(describe.getResultWidth(), 3);
  EXPECT_EQ(describe.getCostEstimate(), 0u);
  EXPECT_EQ(describe.getSizeEstimate(), 2u);
  EXPECT_FLOAT_EQ(describe.getMultiplicity(42), 1.0f);
  EXPECT_FALSE(describe.knownEmptyResult());

  // Test the cache key.
  using namespace ::testing;
  EXPECT_THAT(
      describe.getCacheKey(),
      AllOf(HasSubstr("DESCRIBE"), HasSubstr("<s>"), Not(HasSubstr("<p>")),
            HasSubstr("Neutral Element"), Not(HasSubstr("Filtered"))));

  // Test the cache key of the same query, but with a FROM clause.
  auto parsedDescribe2 = parsedDescribe;
  parsedDescribe2.datasetClauses_ =
      parsedQuery::DatasetClauses::fromClauses(std::vector{DatasetClause{
          TripleComponent::Iri::fromIriref("<default-graph-1>"), false}});
  Describe describe2{
      qec, ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
      parsedDescribe2};
  EXPECT_THAT(describe2.getCacheKey(),
              AllOf(HasSubstr("DESCRIBE"), HasSubstr("<s>"),
                    Not(HasSubstr("<p>")), HasSubstr("Neutral Element"),
                    HasSubstr("Filtered by Graphs:<default-graph-1>")));

  auto col = makeAlwaysDefinedColumn;
  VariableToColumnMap expected{{Variable{"?subject"}, col(0)},
                               {Variable{"?predicate"}, col(1)},
                               {Variable{"?object"}, col(2)}};

  auto children = describe.getChildren();
  ASSERT_EQ(children.size(), 1);
  EXPECT_THAT(children.at(0)->getRootOperation()->getDescriptor(),
              Eq("NeutralElement"));
}

// _____________________________________________________________________________
TEST(Describe, clone) {
  auto qec = getQec();
  parsedQuery::Describe parsedDescribe;
  parsedDescribe.resources_.push_back(TripleComponent::Iri::fromIriref("<s>"));
  Describe describe{qec,
                    ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
                    parsedDescribe};

  auto clone = describe.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(describe, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), describe.getDescriptor());
}
