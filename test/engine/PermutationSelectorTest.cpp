//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/IndexTestHelpers.h"
#include "engine/PermutationSelector.h"
#include "index/IndexImpl.h"

// _____________________________________________________________________________
TEST(PermutationSelectorTest, internalPrefixIsCorrectlyChosen) {
  auto* qec = ad_utility::testing::getQec();
  const auto& index = qec->getIndex();
  TripleComponent internalIri{ad_utility::triple_component::Iri::fromIriref(
      makeQleverInternalIri("something"))};
  TripleComponent languageTaggedIri{
      ad_utility::triple_component::Iri::fromIriref("@en@<abc>")};
  TripleComponent regularIri{
      ad_utility::triple_component::Iri::fromIriref("<abc>")};
  TripleComponent regularLiteral{1};

  for (auto permutation : Permutation::ALL) {
    const auto* permutationPtr = &index.getImpl().getPermutation(permutation);
    for (const auto& triple :
         {SparqlTripleSimple{regularIri, regularIri, regularIri},
          SparqlTripleSimple{regularLiteral, regularLiteral, regularLiteral},
          SparqlTripleSimple{regularLiteral, regularIri, regularLiteral}}) {
      EXPECT_EQ(
          qlever::getPermutationForTriple(permutation, index, triple).get(),
          permutationPtr);
    }
  }

  for (auto permutation : Permutation::INTERNAL) {
    const auto* permutationPtr =
        &index.getImpl().getPermutation(permutation).internalPermutation();
    for (const auto& triple :
         {SparqlTripleSimple{internalIri, regularIri, regularIri},
          SparqlTripleSimple{regularIri, internalIri, regularIri},
          SparqlTripleSimple{regularIri, regularIri, internalIri},
          SparqlTripleSimple{languageTaggedIri, regularIri, regularIri},
          SparqlTripleSimple{regularIri, languageTaggedIri, regularIri},
          SparqlTripleSimple{regularIri, regularIri, languageTaggedIri}}) {
      EXPECT_EQ(
          qlever::getPermutationForTriple(permutation, index, triple).get(),
          permutationPtr);
    }
  }

  using enum Permutation::Enum;
  // Unsupported configurations.
  for (auto permutation : {OPS, OSP, SOP, SPO}) {
    EXPECT_THROW(qlever::getPermutationForTriple(
                     permutation, index,
                     SparqlTripleSimple{languageTaggedIri, internalIri,
                                        languageTaggedIri}),
                 ad_utility::Exception);
  }
}
