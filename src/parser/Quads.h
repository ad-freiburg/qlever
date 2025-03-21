// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <vector>

#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "parser/data/Types.h"

// A class for the intermediate parsing results of `quads`. Provides utilities
// for converting the quads into the required formats. The Quads/Triples can be
// used as `vector<GraphPatternOperation>` (Query Body in `DELETE WHERE`) or
// `vector<SparqlTripleSimpleWithGraph>` (Quad Template in many Update
// Operations).
struct Quads {
  // A single block of triples wrapped in a `GRAPH ... { ... }`. Corresponds to
  // the `quadsNotTriples` grammar rule.
  using GraphBlock = std::tuple<ad_utility::sparql_types::VarOrIri,
                                ad_utility::sparql_types::Triples>;

  bool operator==(const Quads&) const = default;

  // Free triples are outside a `GRAPH ...` clause.
  ad_utility::sparql_types::Triples freeTriples_{};
  // Graph triples are inside a `GRAPH ...` clause.
  std::vector<GraphBlock> graphTriples_{};

  void forAllVariables(std::function<void(const Variable&)> f);

  std::vector<SparqlTripleSimpleWithGraph> toTriplesWithGraph() const;
  std::vector<parsedQuery::GraphPatternOperation> toGraphPatternOperations()
      const;
};
