//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

using namespace sparqlExpression::detail;

TEST(SparqlExpressionGenerators, makeStringResultGetter) {
  using ad_utility::triple_component::LiteralOrIri;
  auto literal = LiteralOrIri::literalWithoutQuotes("Test String");
  LocalVocab localVocab{};

  auto function = makeStringResultGetter(&localVocab);

  auto result = function(literal);

  EXPECT_EQ(result.getLocalVocabIndex()->toStringRepresentation(),
            "\"Test String\"");
}
