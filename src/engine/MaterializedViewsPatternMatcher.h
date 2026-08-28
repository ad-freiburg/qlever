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
#include <vector>

#include "engine/MaterializedViewsQueryAnalysis.h"
#include "engine/VariableToColumnMap.h"
#include "parser/GraphPatternOperation.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Variable.h"
#include "util/HashMap.h"

class QueryExecutionContext;

namespace materializedViewsQueryAnalysis {

// Searches for all embeddings of a `ViewPattern` into a query's triples via
// plain VF2-style backtracking (subgraph isomorphism), appending a
// `MaterializedViewJoinReplacement` to the `result` passed to the constructor
// for each valid completed match (see `isLegalFixedValuePrefix` in the .cpp
// file). One instance handles one call to `QueryPatternCache::matchPattern`:
// the constructor takes everything fixed for the whole search, and `run()`
// performs the recursion, threading the current candidate assignment
// (`assignment_`, `coveredTriples_`) through member state instead of an
// explicit parameter.
class PatternMatcher {
 public:
  PatternMatcher(const ViewPattern& pattern,
                 const parsedQuery::BasicGraphPattern& triples,
                 std::vector<const std::vector<size_t>*> candidatesByEdge,
                 QueryExecutionContext* qec, size_t budget,
                 std::vector<MaterializedViewJoinReplacement>& result);

  // Runs the search, starting from the first pattern edge.
  void run() { extendMatch(0); }

  // Whether the search stopped early because it exceeded the step budget or
  // the per-query replacement cap; `result` may then be missing some
  // applicable rewrites for this view.
  bool truncated() const { return truncated_; }

 private:
  const ViewPattern& pattern_;
  const parsedQuery::BasicGraphPattern& triples_;
  std::vector<const std::vector<size_t>*> candidatesByEdge_;
  QueryExecutionContext* qec_;
  const VariableToColumnMap& viewCols_;
  size_t stepsRemaining_;
  bool truncated_ = false;
  std::vector<MaterializedViewJoinReplacement>& result_;

  // Current candidate assignment: maps view variables to the query-side node
  // they are matched to.
  ad_utility::HashMap<Variable, TripleComponent> assignment_;
  // Bitmask of the query triples used by the current assignment so far.
  uint64_t coveredTriples_ = 0;

  // Match `viewSide` against `queryNode`. A fixed `viewSide` is a plain
  // equality check (not bound; it's not a view column). Otherwise `viewSide`
  // is a view variable: enforces injectivity for query *variables* (no two
  // view variables map to the same query variable, since a single index scan
  // can't assert their equality) and prunes bindings that can never satisfy
  // `isLegalFixedValuePrefix` later (a smaller-column variable already bound
  // to a query variable rules out fixing this one). Two view variables both
  // fixed to the same query *constant* is fine (e.g. a chain view
  // `?a <p1> ?b . ?b <p2> ?c` answering `<x> <p1> <x> . <x> <p2> ?c`) -- that
  // is just two independent equality filters on the view's scan, which
  // `MaterializedView::makeScanConfig` explicitly allows. Returns `false`,
  // leaving `assignment_` unmodified, on rejection.
  bool tryAssign(const TripleComponent& viewSide,
                 const TripleComponent& queryNode);

  // Whether some already-assigned view variable is bound to `queryNode`
  // (the injectivity check `tryAssign` needs for a query-variable side).
  bool isAlreadyBound(const TripleComponent& queryNode) const;

  // Whether some already-assigned view variable with a smaller column index
  // than `col` is still bound to a query variable, i.e. not itself fixed
  // (the fixed-value prefix check `tryAssign` needs for a fixed-value side).
  bool hasVariableBeforeFixedColumn(size_t col) const;

  // Whether `tripleIdx` is already used by the current assignment.
  bool isTripleCovered(size_t tripleIdx) const;

  // Marks/unmarks `tripleIdx` as used by the current assignment.
  void coverTriple(size_t tripleIdx);
  void uncoverTriple(size_t tripleIdx);

  // Whether `tryAssign(viewSide, ...)` would add a new binding (that
  // `undoAssign` then needs to remove on backtrack). Must be called before
  // `tryAssign`, which may itself insert that binding.
  bool isNewBinding(const TripleComponent& viewSide) const;

  // Reverses `tryAssign(viewSide, ...)` if it added a new binding (`wasNew`,
  // from `isNewBinding`); otherwise a no-op.
  void undoAssign(const TripleComponent& viewSide, bool wasNew);

  // Checks the completed match in `assignment_` against
  // `isLegalFixedValuePrefix` and, if legal, builds the resulting
  // `MaterializedViewJoinReplacement` and adds it to `result_`.
  void emitIfLegal();

  // Recursively extends the current assignment (which already covers
  // `edges[0, edgeIdx)`, `edges` being `pattern_.edges_`) by matching
  // `edges[edgeIdx]` against each of its candidate query triples
  // (`candidatesByEdge_[edgeIdx]`, precomputed per predicate in
  // `matchPattern`), then recursing into the remaining edges. A completed
  // match is validated and turned into a replacement immediately
  // (`emitIfLegal`) instead of being collected into an intermediate list
  // first. Plain VF2-style backtracking: `undoAssign` only removes the
  // bindings this step added, and `isTripleCovered` is a single bit test
  // against the `coveredTriples_` bitmask.
  //
  // `stepsRemaining_` caps the total number of candidates tried across the
  // whole search; `result_.size() >= kMaxReplacements` (see the .cpp file)
  // separately caps the total number of matches collected, since a
  // cheap-to-complete match (e.g. a two-edge view with a repeated predicate)
  // can exhaust neither the steps nor find anything illegal, yet still
  // complete tens of thousands of times within the step budget. Either limit
  // sets `truncated_` and stops the search.
  void extendMatch(size_t edgeIdx);
};

}  // namespace materializedViewsQueryAnalysis

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWSPATTERNMATCHER_H_
