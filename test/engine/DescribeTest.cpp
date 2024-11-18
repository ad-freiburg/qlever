//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

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
