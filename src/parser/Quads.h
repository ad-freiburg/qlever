// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <vector>

#include "GraphPatternOperation.h"
#include "SparqlTriple.h"
#include "data/Types.h"

// A class for the intermediate parsing results of `quads`. The Quads/Triples
// are added and can then be produced in the required formats. The Quads/Triples
// can be used as `vector<GraphPatternOperation>` (Query Body in `DELETE WHERE`)
// or `vector<SparqlTripleSimpleWithGraph>` (Quad Template in many Update
// Operations).
class Quads {
 public:
  using IriOrVariable = std::variant<Iri, Variable>;
  // A single block of triples wrapped in a `GRAPH ... { ... }`. Corresponds to
  // the `quadsNotTriples` grammar rule.
  using GraphBlock =
      std::tuple<IriOrVariable, ad_utility::sparql_types::Triples>;

 private:
  ad_utility::sparql_types::Triples freeTriples_{};
  std::vector<GraphBlock> graphTriples_{};

 public:
  void addFreeTriples(ad_utility::sparql_types::Triples triples);
  void addGraphTriples(IriOrVariable graph,
                       ad_utility::sparql_types::Triples triples);

  std::vector<SparqlTripleSimpleWithGraph> getQuads() const;
  std::vector<parsedQuery::GraphPatternOperation> getOperations() const;
};
