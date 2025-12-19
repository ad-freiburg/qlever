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
    EXPECT_EQ(qlever::getPermutationForTriple(
                  permutation, index,
                  SparqlTripleSimple{regularIri, regularIri, regularIri})
                  .get(),
              permutationPtr);
    EXPECT_EQ(
        qlever::getPermutationForTriple(
            permutation, index,
            SparqlTripleSimple{regularLiteral, regularLiteral, regularLiteral})
            .get(),
        permutationPtr);
    EXPECT_EQ(
        qlever::getPermutationForTriple(
            permutation, index,
            SparqlTripleSimple{regularLiteral, regularIri, regularLiteral})
            .get(),
        permutationPtr);
  }

  for (auto permutation : Permutation::INTERNAL) {
    const auto* permutationPtr =
        &index.getImpl().getPermutation(permutation).internalPermutation();
    EXPECT_EQ(qlever::getPermutationForTriple(
                  permutation, index,
                  SparqlTripleSimple{internalIri, regularIri, regularIri})
                  .get(),
              permutationPtr);
    EXPECT_EQ(qlever::getPermutationForTriple(
                  permutation, index,
                  SparqlTripleSimple{regularIri, internalIri, regularIri})
                  .get(),
              permutationPtr);
    EXPECT_EQ(qlever::getPermutationForTriple(
                  permutation, index,
                  SparqlTripleSimple{regularIri, regularIri, internalIri})
                  .get(),
              permutationPtr);

    EXPECT_EQ(qlever::getPermutationForTriple(
                  permutation, index,
                  SparqlTripleSimple{languageTaggedIri, regularIri, regularIri})
                  .get(),
              permutationPtr);
    EXPECT_EQ(qlever::getPermutationForTriple(
                  permutation, index,
                  SparqlTripleSimple{regularIri, languageTaggedIri, regularIri})
                  .get(),
              permutationPtr);
    EXPECT_EQ(qlever::getPermutationForTriple(
                  permutation, index,
                  SparqlTripleSimple{regularIri, regularIri, languageTaggedIri})
                  .get(),
              permutationPtr);
  }

  using enum Permutation::Enum;
  // Unsupported configurations.
  EXPECT_THROW(qlever::getPermutationForTriple(
                   OPS, index,
                   SparqlTripleSimple{languageTaggedIri, internalIri,
                                      languageTaggedIri}),
               ad_utility::Exception);
  EXPECT_THROW(qlever::getPermutationForTriple(
                   OSP, index,
                   SparqlTripleSimple{languageTaggedIri, internalIri,
                                      languageTaggedIri}),
               ad_utility::Exception);
  EXPECT_THROW(qlever::getPermutationForTriple(
                   SOP, index,
                   SparqlTripleSimple{languageTaggedIri, internalIri,
                                      languageTaggedIri}),
               ad_utility::Exception);
  EXPECT_THROW(qlever::getPermutationForTriple(
                   SPO, index,
                   SparqlTripleSimple{languageTaggedIri, internalIri,
                                      languageTaggedIri}),
               ad_utility::Exception);
}
