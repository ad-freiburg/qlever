// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "engine/QueryExecutionContext.h"
#include "index/DeltaTriples.h"
#include "index/Index.h"
#include "parser/TripleComponent.h"
#include "util/CancellationHandle.h"

// OWL 2 RL / RDFS forward-chaining materialisation engine.
//
// The reasoner iterates the rule set from `ReasonerRules.h` in a fixpoint
// loop, executing each active rule as a SPARQL CONSTRUCT and inserting the
// resulting triples as delta triples. Iteration stops when no new triples are
// produced (fixpoint) or the configured round limit is reached.
//
// Semi-naive evaluation: after each round the engine tracks which output
// predicates gained new triples and only re-schedules rules whose input
// predicates overlap with those outputs.  Rules with variable-predicate
// CONSTRUCT heads (prp-spo1, prp-inv1/2, prp-symp, prp-trp) use a
// "wildcard" marker that re-activates all rules whose input predicate set
// includes the wildcard sentinel.
class Reasoner {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  // Aggregated result returned by `materialize()`.
  struct MaterializationResult {
    // Total new triples inserted across all rounds.
    size_t totalNewTriples = 0;
    // Number of fixpoint rounds executed (includes the terminating round
    // that produced 0 new triples).
    size_t numRounds = 0;
    // Number of distinct rules that fired at least once.
    size_t numRulesActivated = 0;
    // Cumulative per-rule new-triple counts (rule name → count).
    std::vector<std::pair<std::string, size_t>> triplesPerRule;
    // numInserted() value of `deltaTriples` before/after the full run.
    // int64_t matches the return type of DeltaTriples::numInserted().
    int64_t numInsertedBefore = 0;
    int64_t numInsertedAfter = 0;
  };

  // Run the full RDFS / OWL 2 RL forward-chaining materialisation.
  //
  // All inferred triples are inserted into `targetGraph` as delta triples.
  // The caller is responsible for:
  //   1. Having called `qec.setLocatedTriplesForEvaluation(...)` with the
  //      same `deltaTriples` before calling this function.
  //   2. Clearing the query-result cache after this function returns (since
  //      the delta triples have changed).
  //
  // Throws if cancellation is requested via `handle`.
  static MaterializationResult materialize(
      Index& index, DeltaTriples& deltaTriples, QueryExecutionContext& qec,
      const ad_utility::triple_component::Iri& targetGraph,
      const CancellationHandle& handle);
};
