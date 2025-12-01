//   Copyright 2024 - 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//            Johannes Kalmbach <kalmbach@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/GTestHelpers.h"

using namespace sparqlExpression::detail;

// _____________________________________________________________________________
TEST(SparqlExpressionGenerators, makeStringResultGetter) {
  using ad_utility::triple_component::LiteralOrIri;
  auto literal = LiteralOrIri::literalWithoutQuotes("Test String");
  LocalVocab localVocab{};

  auto function = makeStringResultGetter(&localVocab);

  auto result = function(literal);

  EXPECT_EQ(result.getLocalVocabIndex()->toStringRepresentation(),
            "\"Test String\"");
}

// _____________________________________________________________________________
TEST(SparqlExpressionGenerators, idOrLiteralOrIriToId) {
  using ad_utility::triple_component::LiteralOrIri;
  auto literal = LiteralOrIri::literalWithoutQuotes("Test String");
  LocalVocab localVocab{};

  auto result = idOrLiteralOrIriToId(literal, &localVocab);

  EXPECT_EQ(result.getLocalVocabIndex()->toStringRepresentation(),
            "\"Test String\"");
  // Should be the identity function for regular ids.
  EXPECT_EQ(result.getBits(),
            idOrLiteralOrIriToId(result, &localVocab).getBits());
}

// _____________________________________________________________________________
TEST(SparqlExpressionGenerators, resultGeneratorSetOfIntervals) {
  auto t = Id::makeFromBool(true);
  auto f = Id::makeFromBool(false);
  {
    ad_utility::SetOfIntervals s{{{1, 3}, {3, 3}, {3, 4}, {5, 6}}};
    auto generator = sparqlExpression::detail::resultGenerator(s, 10);
    std::vector<Id> res;
    ql::ranges::copy(generator, std::back_inserter(res));
    EXPECT_THAT(res, ::testing::ElementsAre(f, t, t, t, f, t, f, f, f, f));
  }
  {
    ad_utility::SetOfIntervals s{{{0, 3}, {3, 3}, {3, 4}, {8, 10}}};
    auto generator = sparqlExpression::detail::resultGenerator(s, 10);
    std::vector<Id> res;
    ql::ranges::copy(generator, std::back_inserter(res));
    EXPECT_THAT(res, ::testing::ElementsAre(t, t, t, t, f, f, f, f, t, t));
  }
  {
    ad_utility::SetOfIntervals s{{{3, 11}}};
    auto consumeGen = [&]() {
      auto gen = sparqlExpression::detail::resultGenerator(s, 10);
      for (auto&& unused : gen) {
        (void)unused;
      }
    };
    AD_EXPECT_THROW_WITH_MESSAGE(
        consumeGen(), ::testing::HasSubstr(
                          "exceeds the total size of the evaluation context"));
  }
}
