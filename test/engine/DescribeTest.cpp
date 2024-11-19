//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/Describe.h"
#include "engine/NeutralElementOperation.h"
#include "engine/QueryExecutionTree.h"

using namespace ad_utility::testing;

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
  auto numUnique = [](size_t size) {
    using namespace ::testing;
    auto getNumUnique = [](const auto& col) {
      return ad_utility::HashSet<Id>(col.begin(), col.end()).size();
    };
    return ResultOf(getNumUnique, size);
  };
  EXPECT_THAT(table.getColumn(0), numUnique(3));
  EXPECT_THAT(table.getColumn(1), numUnique(2));
  EXPECT_THAT(table.getColumn(2), numUnique(5));
}

// _____________________________________________________________________________
TEST(Describe, describeWithVariable) {
  auto qec = getQec();
  parsedQuery::Describe parsedDescribe;
  parsedDescribe.resources_.push_back(Variable{"?x"});
  Describe describe{qec,
                    ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
                    parsedDescribe};

  AD_EXPECT_THROW_WITH_MESSAGE(
      describe.computeResultOnlyForTesting(),
      ::testing::HasSubstr("DESCRIBE with a variable"));
}

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
  EXPECT_THAT(describe.getCacheKey(),
              AllOf(HasSubstr("DESCRIBE"), HasSubstr("<s>"),
                    Not(HasSubstr("<p>")), HasSubstr("Neutral Element")));

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
