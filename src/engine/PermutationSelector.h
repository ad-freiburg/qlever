//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_PERMUTATIONSELECTOR_H
#define QLEVER_SRC_ENGINE_PERMUTATIONSELECTOR_H

#include <memory>

#include "index/Index.h"
#include "index/Permutation.h"
#include "parser/SparqlTriple.h"

namespace qlever {

// Return a shared pointer to the correct permutation in `index` based on the
// `permutation` enum and the values in `triple`. In particular return the
// associated internal permutation if the passed `triple` contains an internal
// IRI at any position. If no internal permutation is available for the passed
// `permutation` enum, throw instead.Internal IRIs include language-tagged IRIs.
// like `@en@rdfs:label` for example, or ones that start with
// `<http://qlever.cs.uni-freiburg.de/builtin-functions/`.
std::shared_ptr<const Permutation> getPermutationForTriple(
    Permutation::Enum permutation, const Index& index,
    const SparqlTripleSimple& triple);
}  // namespace qlever

#endif  // QLEVER_SRC_ENGINE_PERMUTATIONSELECTOR_H
