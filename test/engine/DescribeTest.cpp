//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
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

// _____________________________________________________________________________
TEST(Describe, recursiveBlankNodes) {
  auto qec = getQec(
      "<s> <p> <o> . <s> <p> _:g1 . _:g1 <p2> <o2> . _:g1 <p2> _:g1 . _:g1 "
      "<p2> _:g2 . _:g2 <p> <o4>. <s2> <p> <o> . _:g4 <p> _:g5");
  parsedQuery::Describe parsedDescribe;
  parsedDescribe.resources_.push_back(TripleComponent::Iri::fromIriref("<s>"));
  Describe describe{qec,
                    ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
                    parsedDescribe};
  auto res = describe.computeResultOnlyForTesting();
  const auto& table = res.idTable();
  /* The expected result is:
     <s> <p> <o>
     <s> <p> _:g1
     _:g1 <p2> <o2>
     _:g1 <p2> _:g1
     _:g1 <p2> _:g2
     _:g2 <p> <o4>
  */
  // As the blank nodes are renamed, we cannot easily assert the exact result,
  // but we can at least check the statisticts.
  EXPECT_EQ(table.size(), 6);
  EXPECT_THAT(table.getColumn(0), numUnique(3));
  EXPECT_THAT(table.getColumn(1), numUnique(2));
  EXPECT_THAT(table.getColumn(2), numUnique(5));
}

// _____________________________________________________________________________
TEST(Describe, describeWithVariable) {
  auto qec = getQec(
      "<s> <p> <o> . <s> <p> _:g1 . _:g1 <p2> <o2> . <s2> <p> <o> . <s2> <p2> "
      "_:g1 . <s2> <p2> _:g2 . _:g2 <p3> <o3> . <s3> <p2> <o> . <s4> <p2> <o2> "
      ".");
  // On the above knowledge graph, evaluate `DESCRIBE <s2> ?x { ?x <p> <o>}`.
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
  /* The expected result is: (Expand <s> and <s2> because they match the WHERE
     clause for ?x, and expand <s4> because it was explicitly requested)
     <s> <p> <o>
     <s> <p> _:g1
     _:g1 <p2> <o2>
     <s2> <p> <o>
     <s2> <p2> _:g1>  // Note: _:g1 has already been expanded above.
     <s2> <p2> _:g2>
     _:g2 <p3> <o3>
     <s4> <p2> <o2>
  */
  // As the blank nodes are renamed, we cannot easily assert the exact result,
  // but we can at least check the statisticts.
  EXPECT_EQ(table.size(), 8);
  EXPECT_THAT(table.getColumn(0), numUnique(5));
  EXPECT_THAT(table.getColumn(1), numUnique(3));
  EXPECT_THAT(table.getColumn(2), numUnique(5));
}

// TODO<joka921> We need tests with inputs in a different graph, but those are
// Currently hard to do with the given `getQec` function.

// _____________________________________________________________________________
TEST(Describe, simpleMembers) {
  auto qec = getQec(
      "<s> <p> <o> . <s> <p> _:g1 . _:g1 <p2> <o2> . _:g1 <p2> _:g1 . _:g1 "
      "<p2> _:g2 . _:g2 <p> <o4>. <s2> <p> <o> . _:g4 <p> _:g5");
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

  using namespace ::testing;
  EXPECT_THAT(
      describe.getCacheKey(),
      AllOf(HasSubstr("DESCRIBE"), HasSubstr("<s>"), Not(HasSubstr("<p>")),
            HasSubstr("Neutral Element"), Not(HasSubstr("Filtered"))));
  {
    auto parsedDescribe2 = parsedDescribe;
    parsedDescribe2.datasetClauses_.defaultGraphs_.emplace(
        {TripleComponent::Iri::fromIriref("<default-graph-1>")});
    Describe describe2{
        qec, ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
        parsedDescribe2};
    EXPECT_THAT(describe2.getCacheKey(),
                AllOf(HasSubstr("DESCRIBE"), HasSubstr("<s>"),
                      Not(HasSubstr("<p>")), HasSubstr("Neutral Element"),
                      HasSubstr("Filtered by Graphs:<default-graph-1>")));
  }

  auto col = makeAlwaysDefinedColumn;
  using V = Variable;
  VariableToColumnMap expected{{V{"?subject"}, col(0)},
                               {V{"?predicate"}, col(1)},
                               {V{"?object"}, col(2)}};

  auto children = describe.getChildren();
  ASSERT_EQ(children.size(), 1);
  EXPECT_THAT(children.at(0)->getRootOperation()->getDescriptor(),
              Eq("NeutralElement"));
}
