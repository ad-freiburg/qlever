//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/PermutationSelector.h"

#include "index/IndexImpl.h"

namespace {

// Return true if the string representation of the `iri` starts with '@' or
// `QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET` and is thus considered
// to be internal.
bool hasInternalPrefix(const ad_utility::triple_component::Iri& iri) {
  const auto& string = iri.toStringRepresentation();
  return ql::starts_with(string, '@') ||
         ql::starts_with(string,
                         QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET);
}

// Return true if the passed `tripleComponent` represents an IRI and it has an
// internal prefix.
bool isInternalComponent(const TripleComponent& tripleComponent) {
  return tripleComponent.isIri() && hasInternalPrefix(tripleComponent.getIri());
}

// Return true if one of the 3 triple components of `triple` contains an
// internal IRI.
bool containsInternalIri(const SparqlTripleSimple& triple) {
  return isInternalComponent(triple.s_) || isInternalComponent(triple.p_) ||
         isInternalComponent(triple.o_);
}
}  // namespace

namespace qlever {
// _____________________________________________________________________________
std::shared_ptr<const Permutation> getPermutationForTriple(
    Permutation::Enum permutation, const Index& index,
    const SparqlTripleSimple& triple) {
  auto actualPermutation = index.getImpl().getPermutationPtr(permutation);

  if (containsInternalIri(triple)) {
    // Create alias shared pointer of internal permutation.
    const auto& internalPermutation = actualPermutation->internalPermutation();
    return std::shared_ptr<const Permutation>{std::move(actualPermutation),
                                              &internalPermutation};
  }
  return actualPermutation;
}
}  // namespace qlever
