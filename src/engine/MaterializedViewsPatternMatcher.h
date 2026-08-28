// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSPATTERNMATCHER_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSPATTERNMATCHER_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "engine/MaterializedViewsQueryAnalysis.h"
#include "engine/VariableToColumnMap.h"
#include "parser/GraphPatternOperation.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Variable.h"
#include "util/HashMap.h"

class QueryExecutionContext;

namespace materializedViewsQueryAnalysis {

// Searches for all mappings of a `ViewPattern` into a query's triples via
// a backtracking subgraph isomorphism algorithm, appending a
// `MaterializedViewJoinReplacement` for each valid complete match (up to
// `maxNumReplacementPlans` in total). The search is capped at `budget` number
// of variable assignments.
class PatternMatcher {
 public:
  using Limits = materializedViewsQueryAnalysis::PatternMatcherLimits;

  // Status returned by `findReplacementPlans`.
  enum class MatchStatus {
    // The search covered every candidate assignment.
    Complete,
    // The maximum number of assignments was reached and the search was stopped.
    TruncatedByBudget,
    // The maximum number of results was produced and the search was stopped.
    TruncatedByMaxReplacements,
    // Not every edge's predicate appears in the query. The search was skipped.
    Skipped,
  };

  // Return value of `findReplacementPlans`: the outcome status and how much
  // of `limits` was actually used.
  struct MatchReport {
    MatchStatus status_;
    Limits used_;
  };

  // Builds a `PatternMatcher` and runs it to completion.
  static MatchReport findReplacementPlans(
      const ViewPattern& pattern, const parsedQuery::BasicGraphPattern& triples,
      const TriplesByPredicate& triplesByPredicate, QueryExecutionContext* qec,
      Limits limits, std::vector<MaterializedViewJoinReplacement>& result);

 private:
  // Private constructor used by `findReplacementPlans`.
  PatternMatcher(const ViewPattern& pattern,
                 const parsedQuery::BasicGraphPattern& triples,
                 const TriplesByPredicate& triplesByPredicate,
                 QueryExecutionContext* qec, Limits limits,
                 std::vector<MaterializedViewJoinReplacement>& result);

  const ViewPattern& pattern_;
  const parsedQuery::BasicGraphPattern& triples_;
  // Per pattern edge, a bitmask of the triple indices sharing that edge's
  // predicate.
  std::vector<uint64_t> candidatesByEdge_;
  QueryExecutionContext* qec_;
  const VariableToColumnMap& viewCols_;

  // The original limits. Used to compute how much was actually used.
  Limits limits_;
  // The remaining limits for this instance.
  size_t stepsRemaining_;
  size_t numResultsRemaining_;

  // `status_` is `nullopt` until the search is finished.
  std::optional<MatchStatus> status_;

  std::vector<MaterializedViewJoinReplacement>& result_;

  // Current candidate assignment: maps view variables to the query-side node
  // they are matched to.
  ad_utility::HashMap<Variable, TripleComponent> assignment_;
  // Bitmask of the query triples used by the current assignment so far.
  uint64_t coveredTriples_ = 0;

  // Fills `candidatesByEdge_`: for each edge, a bitmask containing the indices
  // of triples sharing the edge's predicate.
  void buildCandidatesByEdge(const ViewPattern& pattern,
                             const TriplesByPredicate& triplesByPredicate);

  // Tries to match `viewSide` against `queryNode`. Returns `true` if the
  // assignment was made (or on equality for fixed components).
  bool tryAssignment(const TripleComponent& viewSide,
                     const TripleComponent& queryNode);

  // Whether some already-assigned view variable is already bound to
  // `queryNode` (injectivity check).
  bool isAlreadyBound(const TripleComponent& queryNode) const;

  // Whether some already-assigned view variable with a smaller column index
  // than `col` is bound to a variable.
  bool hasVariableBeforeFixedColumn(size_t col) const;

  // Whether `tripleIdx` is already used by the current assignment.
  bool isTripleCovered(size_t tripleIdx) const;

  // Marks/unmarks `tripleIdx` as used by the current assignment.
  void coverTriple(size_t tripleIdx);
  void uncoverTriple(size_t tripleIdx);

  // Decrement the budget and return `false` if no budget is left.
  bool decrementAndCheckBudget();

  // Whether `tryAssignment` would add a new binding (that needs to be removed
  // on backtrack). Must be called before `tryAssignment`.
  bool isNewBinding(const TripleComponent& viewSide) const;

  // Reverses `tryAssignment` if it added a new binding(`wasNew`), a no-op
  // otherwise.
  void undoAssignment(const TripleComponent& viewSide, bool wasNew);

  // Checks a complete match against `isLegalFixedValuePrefix` and, if legal,
  // builds the resulting `MaterializedViewJoinReplacement` and adds it to
  // `result_`.
  void emitIfLegal();

  // Whether `boundColumnsMask` represents a configuration allowed when scanning
  // materialized views: only subject, subject + predicate, or
  // subject + predicate + object may be fixed.
  static bool isLegalFixedValuePrefix(uint64_t boundColumnsMask);

  // Recursively extends the current assignment by matching the edges from the
  // view query against each of the user query's triples with the same
  // predicate. Every complete match is validated and turned into a replacement
  // plan (see `emitIfLegal`). The runtime is capped by both a maximum number of
  // results and a maximum number of assignments tried (whichever is reached
  // first).
  void extendMatch(size_t edgeIdx);

  // Make the `MatchReport`: the resolved status and how much of `limits_` was
  // actually used.
  MatchReport makeReport() const;
};

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSPATTERNMATCHER_H_
