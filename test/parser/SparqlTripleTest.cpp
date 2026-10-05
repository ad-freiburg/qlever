// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "parser/SparqlTriple.h"

namespace {
auto iri = ad_utility::testing::iri;

// Note: these are functions and not global constants. Constructing a `Variable`
// validates the name via the ANTLR parser, which at namespace scope runs before
// ANTLR's own globals are initialized and then aborts on a null
// `SemanticContext`.
TripleComponent subject() { return TripleComponent{Variable{"?s"}}; }
TripleComponent object() { return TripleComponent{Variable{"?o"}}; }
}  // namespace

// _____________________________________________________________________________
TEST(SparqlTriple, getSimpleWithVariablePredicate) {
  Variable predicate{"?p"};
  SparqlTriple triple{subject(), predicate, object()};
  SparqlTripleSimple simple = triple.getSimple();

  EXPECT_EQ(simple.s_, subject());
  EXPECT_EQ(simple.p_, TripleComponent{predicate});
  EXPECT_EQ(simple.o_, object());
}

// _____________________________________________________________________________
TEST(SparqlTriple, getSimpleWithIriPredicate) {
  auto predicate = iri("<http://example.org/p>");
  SparqlTriple triple{subject(), predicate, object()};

  EXPECT_EQ(triple.getSimple().p_, TripleComponent{predicate});
}

// _____________________________________________________________________________
TEST(SparqlTriple, getSimplePreservesAdditionalScanColumns) {
  SparqlTriple triple{subject(),
                      PropertyPath::fromIri(iri("<p>")),
                      object(),
                      {{3, Variable{"?pattern"}}}};

  EXPECT_THAT(triple.getSimple().additionalScanColumns_,
              ::testing::ElementsAre(::testing::Pair(3, Variable{"?pattern"})));
}

// _____________________________________________________________________________
TEST(SparqlTriple, getSimpleWithLanguageTaggedPredicate) {
  // A language filter rewrites the predicate into QLever's internal
  // `@langtag@<iri>` format, which is not an IRI reference. `getSimple` must
  // pass it through unchanged rather than re-parsing it as an IRI reference.
  auto predicate = iri("@en@<http://www.w3.org/2000/01/rdf-schema#label>");
  SparqlTriple triple{subject(), predicate, object()};

  EXPECT_EQ(triple.getSimple().p_, TripleComponent{predicate});
  EXPECT_EQ(triple.getSimple().p_.getIri().toStringRepresentation(),
            "@en@<http://www.w3.org/2000/01/rdf-schema#label>");
}

// _____________________________________________________________________________
TEST(SparqlTriple, getSimpleThrowsForActualPropertyPath) {
  auto path = PropertyPath::makeInverse(PropertyPath::fromIri(iri("<p>")));
  SparqlTriple triple{subject(), path, object()};

  AD_EXPECT_THROW_WITH_MESSAGE(triple.getSimple(),
                               ::testing::HasSubstr("path.isIri()"));
}
