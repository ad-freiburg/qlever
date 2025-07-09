//  Copyright 2022-2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "parser/UpdateTriples.h"

using namespace updateClause;
using V = Variable;

// _____________________________________________________________________________
TEST(UpdateTriples, DefaultConstructor) {
  UpdateTriples tr{};
  EXPECT_TRUE(tr.triples_.empty());
  EXPECT_TRUE(tr.localVocab_.empty());
}

// _____________________________________________________________________________
TEST(UpdateTriples, ConstructorsAndAssignments) {
  LocalVocab l;
  auto iri = LocalVocabEntry::iriref("<hallo>");
  l.getIndexAndAddIfNotContained(LocalVocabEntry{iri});
  std::vector<SparqlTripleSimpleWithGraph> triples;

  SparqlTripleSimpleWithGraph triple{V{"?x"}, V{"?y"}, V{"?z"},
                                     std::monostate{}};
  triples.push_back(triple);

  auto testTriples = [&](auto&& tr,
                         ad_utility::source_location loc =
                             ad_utility::source_location::current()) {
    auto trace = generateLocationTrace(loc);
    EXPECT_EQ(tr.triples_, triples);
    EXPECT_THAT(tr.localVocab_.getAllWordsForTesting(),
                ::testing::ElementsAre(iri));
    ;
  };

  UpdateTriples tr{triples, std::move(l)};
  testTriples(tr);
  testTriples(UpdateTriples{tr});
  UpdateTriples tr2{std::move(tr)};
  testTriples(tr2);
  EXPECT_TRUE(tr.triples_.empty());
  tr = tr2;
  testTriples(tr);
  UpdateTriples tr3;
  tr3 = std::move(tr);
  testTriples(tr3);
  EXPECT_TRUE(tr.triples_.empty());
}
