// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <absl/functional/function_ref.h>

#include <vector>

#include "engine/LocalVocab.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "parser/UpdateTriples.h"
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

  // Run the function for all variables in the quads. The function may be called
  // twice for the same variable.
  void forAllVariables(absl::FunctionRef<void(const Variable&)> f);

  // This struct is used inside `toTriplesWithGraph` to consistently map blank
  // node labels to IDs.
  struct BlankNodeAdder {
    // The used blank node IDs are store in the `LocalVocab` via the
    // `LocalBlankNodeManager`.
    LocalVocab localVocab_;
    // Store the mapping from labelds to IDs.
    ad_utility::HashMap<std::string, Id> map_;
    // The (global) blank node manager used to obtain new unique blank node IDs.
    ad_utility::BlankNodeManager* bnodeManager_;

    // Get an `Id` for the `label`. If the same `label` was previously passed to
    // the same `BlankNodeAdder`, this will result in the same `Id`.
    Id getBlankNodeIndex(std::string_view label);
  };
  // Return the quads in a format for use as an update template.
  // The `defaultGraph` is used for the `freeTriples_`. It for example is set
  // when using a `WITH` clause. It can also be `std::monostate{}`, in which
  // case the global default graph will be used later on.
  updateClause::UpdateTriples toTriplesWithGraph(
      const SparqlTripleSimpleWithGraph::Graph& defaultGraph,
      BlankNodeAdder& blankNodeAdder) const;
  // Return the quads in a format for use in a GraphPattern.
  std::vector<parsedQuery::GraphPatternOperation> toGraphPatternOperations()
      const;
};
