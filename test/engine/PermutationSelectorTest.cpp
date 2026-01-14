//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/PermutationSelector.h"
#include "index/IndexImpl.h"

auto testPermutationSelection = [](auto retrievalFunction, auto getExpected,
                                   auto getExpectedInternal,
                                   ad_utility::source_location sourceLocation =
                                       AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);

  TripleComponent internalIri{ad_utility::triple_component::Iri::fromIriref(
      makeQleverInternalIri("something"))};
  TripleComponent languageTaggedIri{
      ad_utility::triple_component::Iri::fromIriref("@en@<abc>")};
  TripleComponent regularIri{
      ad_utility::triple_component::Iri::fromIriref("<abc>")};
  TripleComponent regularLiteral{1};

  for (auto permutation : Permutation::ALL) {
    const auto* expectedPtr = getExpected(permutation);
    for (const auto& triple :
         {SparqlTripleSimple{regularIri, regularIri, regularIri},
          SparqlTripleSimple{regularLiteral, regularLiteral, regularLiteral},
          SparqlTripleSimple{regularLiteral, regularIri, regularLiteral}}) {
      EXPECT_EQ(retrievalFunction(permutation, triple).get(), expectedPtr);
    }
  }

  for (auto permutation : Permutation::INTERNAL) {
    const auto* expectedPtr = getExpectedInternal(permutation);
    for (const auto& triple :
         {SparqlTripleSimple{internalIri, regularIri, regularIri},
          SparqlTripleSimple{regularIri, internalIri, regularIri},
          SparqlTripleSimple{regularIri, regularIri, internalIri},
          SparqlTripleSimple{languageTaggedIri, regularIri, regularIri},
          SparqlTripleSimple{regularIri, languageTaggedIri, regularIri},
          SparqlTripleSimple{regularIri, regularIri, languageTaggedIri}}) {
      EXPECT_EQ(retrievalFunction(permutation, triple).get(), expectedPtr);
    }
  }

  using enum Permutation::Enum;
  // Unsupported configurations.
  for (auto permutation : {OPS, OSP, SOP, SPO}) {
    EXPECT_THROW(
        retrievalFunction(permutation,
                          SparqlTripleSimple{languageTaggedIri, internalIri,
                                             languageTaggedIri}),
        ad_utility::Exception);
  }
};

// _____________________________________________________________________________
TEST(PermutationSelectorTest, internalPrefixIsCorrectlyChosen) {
  auto* qec = ad_utility::testing::getQec();
  const auto& index = qec->getIndex();

  testPermutationSelection(
      [&index](auto p, const auto& triple) {
        return qlever::getPermutationForTriple(p, index, triple);
      },

      [&index](const auto& p) { return &index.getImpl().getPermutation(p); },
      [&index](const auto& p) {
        return &index.getImpl().getPermutation(p).internalPermutation();
      });
}

// _____________________________________________________________________________
TEST(PermutationSelectorTest, getLocatedTriplesPerBlockForTriple) {
  const auto* qec = ad_utility::testing::getQec();
  const auto& locatedTriples = qec->locatedTriplesSharedState();

  testPermutationSelection(
      [&locatedTriples](auto p, const auto& triple) {
        return qlever::getLocatedTriplesPerBlockForTriple(p, locatedTriples,
                                                          triple);
      },
      [&locatedTriples](const auto& p) {
        return &locatedTriples->getLocatedTriplesForPermutation<false>(p);
      },
      [&locatedTriples](const auto& p) {
        return &locatedTriples->getLocatedTriplesForPermutation<true>(p);
      });
}
