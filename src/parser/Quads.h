// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <vector>

#include "GraphPatternOperation.h"
#include "SparqlTriple.h"
#include "data/Types.h"

// A class for the intermediate parsing results of `quads`. Provides utilities
// for converting the quads into the required formats. The Quads/Triples can be
// used as `vector<GraphPatternOperation>` (Query Body in `DELETE WHERE`) or
// `vector<SparqlTripleSimpleWithGraph>` (Quad Template in many Update
// Operations).
struct Quads {
  using IriOrVariable =
      std::variant<ad_utility::triple_component::Iri, Variable>;
  // A single block of triples wrapped in a `GRAPH ... { ... }`. Corresponds to
  // the `quadsNotTriples` grammar rule.
  using GraphBlock =
      std::tuple<IriOrVariable, ad_utility::sparql_types::Triples>;

  ad_utility::sparql_types::Triples freeTriples_{};
  std::vector<GraphBlock> graphTriples_{};

  std::vector<SparqlTripleSimpleWithGraph> getQuads() const;
  std::vector<parsedQuery::GraphPatternOperation> getOperations() const;
};
