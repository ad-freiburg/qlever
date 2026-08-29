// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/MaterializedViewsPatternMatcher.h"

#include "backports/algorithm.h"
#include "engine/MaterializedViews.h"
#include "util/Algorithm.h"
#include "util/BitUtils.h"

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
PatternMatcher::MatchReport PatternMatcher::findReplacementPlans(
    const ViewPattern& pattern, const parsedQuery::BasicGraphPattern& triples,
    const TriplesByPredicate& triplesByPredicate, QueryExecutionContext* qec,
    Limits limits, std::vector<MaterializedViewJoinReplacement>& result) {
  PatternMatcher matcher{pattern, triples, triplesByPredicate,
                         qec,     limits,  result};
  if (matcher.status_ != MatchStatus::Skipped) {
    // Runs the search, starting from the first pattern edge.
    matcher.extendMatch(0);
  }
  return matcher.makeReport();
}

// _____________________________________________________________________________
PatternMatcher::PatternMatcher(
    const ViewPattern& pattern, const parsedQuery::BasicGraphPattern& triples,
    const TriplesByPredicate& triplesByPredicate, QueryExecutionContext* qec,
    Limits limits, std::vector<MaterializedViewJoinReplacement>& result)
    : pattern_{pattern},
      triples_{triples},
      qec_{qec},
      viewCols_{pattern.view_->variableToColumnMap()},
      limits_{limits},
      numAssignmentsRemaining_{limits.numAssignments_},
      numReplacementPlansRemaining_{limits.numReplacementPlans_},
      result_{result} {
  buildCandidatesByEdge(pattern, triplesByPredicate);
}

// _____________________________________________________________________________
void PatternMatcher::buildCandidatesByEdge(
    const ViewPattern& pattern, const TriplesByPredicate& triplesByPredicate) {
  candidatesByEdge_.reserve(pattern.edges_.size());
  for (const auto& edge : pattern.edges_) {
    auto candidates = ad_utility::findOptional(triplesByPredicate, edge.p_);
    if (!candidates.has_value()) {
      // No embedding can possibly exist; leave `candidatesByEdge_` as-is,
      // it's never read once `status_` is `Skipped`.
      status_ = MatchStatus::Skipped;
      return;
    }
    candidatesByEdge_.push_back(candidates.value());
  }
}

// _____________________________________________________________________________
bool PatternMatcher::tryAssignment(const TripleComponent& viewSide,
                                   const TripleComponent& queryNode) {
  // Fixed values must match exactly. Nothing to assign as the value is also
  // fixed in the view's query.
  if (!viewSide.isVariable()) {
    return queryNode == viewSide;
  }

  // If `viewVar` was already assigned, `queryNode` must match the already
  // assigned value.
  const Variable& viewVar = viewSide.getVariable();
  if (auto bound = ad_utility::findOptional(assignment_, viewVar)) {
    return bound.value() == queryNode;
  }

  if (queryNode.isVariable()) {
    // User query contains a variable in this position: check injectivity (no
    // view variable may already be bound to this same query variable).
    if (isAlreadyBound(queryNode)) {
      return false;
    }
  } else {
    // User query contains a fixed value in this position. Prune invalid
    // fixed-value configurations.

    // A payload column (index > 2) bound to a fixed value is always illegal,
    // regardless of what else is assigned.
    size_t col = viewCols_.at(viewVar).columnIndex_;
    if (col > 2) {
      return false;
    }

    // A view variable with a smaller column index that is already bound to a
    // query variable rules out fixing a value at a larger column index.
    if (hasVariableBeforeFixedColumn(col)) {
      return false;
    }
  }

  // Assignment is allowed.
  assignment_.emplace(viewVar, queryNode);
  return true;
}

// _____________________________________________________________________________
bool PatternMatcher::isAlreadyBound(const TripleComponent& queryNode) const {
  return ql::ranges::any_of(assignment_, [&queryNode](const auto& entry) {
    return entry.second == queryNode;
  });
}

// _____________________________________________________________________________
bool PatternMatcher::hasVariableBeforeFixedColumn(size_t col) const {
  return ql::ranges::any_of(assignment_, [this, col](const auto& entry) {
    return entry.second.isVariable() &&
           viewCols_.at(entry.first).columnIndex_ < col;
  });
}

// _____________________________________________________________________________
bool PatternMatcher::isTripleCovered(size_t tripleIdx) const {
  return (coveredTriples_ & (uint64_t{1} << tripleIdx)) != 0;
}

// _____________________________________________________________________________
void PatternMatcher::coverTriple(size_t tripleIdx) {
  coveredTriples_ |= (uint64_t{1} << tripleIdx);
}

// _____________________________________________________________________________
void PatternMatcher::uncoverTriple(size_t tripleIdx) {
  coveredTriples_ &= ~(uint64_t{1} << tripleIdx);
}

// _____________________________________________________________________________
bool PatternMatcher::hasLegalFixedValuePrefix() const {
  uint64_t boundColumnsMask = 0;
  for (const auto& [viewVar, node] : assignment_) {
    if (!node.isVariable()) {
      size_t col = viewCols_.at(viewVar).columnIndex_;
      if (col > 2) {
        return false;
      }
      boundColumnsMask |= (uint64_t{1} << col);
    }
  }
  return boundColumnsMask == 0b000u || boundColumnsMask == 0b001u ||
         boundColumnsMask == 0b011u || boundColumnsMask == 0b111u;
}

// _____________________________________________________________________________
bool PatternMatcher::isNewBinding(const TripleComponent& viewSide) const {
  return viewSide.isVariable() && !assignment_.contains(viewSide.getVariable());
}

// _____________________________________________________________________________
void PatternMatcher::undoAssignment(const TripleComponent& viewSide,
                                    bool wasNew) {
  if (wasNew) {
    assignment_.erase(viewSide.getVariable());
  }
}

// _____________________________________________________________________________
void PatternMatcher::emitIfLegal() {
  if (!hasLegalFixedValuePrefix()) {
    return;
  }

  // Configuration is allowed. Construct `MaterializedViewJoinReplacement`.
  result_.push_back(
      {pattern_.view_->makeIndexScan(
           qec_, parsedQuery::MaterializedViewQuery{pattern_.view_->name(),
                                                    assignment_}),
       coveredTriples_});
  --numReplacementPlansRemaining_;
}

// _____________________________________________________________________________
void PatternMatcher::extendMatch(size_t edgeIdx) {
  // If no replacement plans are left, no further plans should be generated.
  if (numReplacementPlansRemaining_ == 0) {
    if (!status_.has_value()) {
      status_ = MatchStatus::TruncatedByNumReplacementPlans;
    }
    return;
  }

  const auto& edges = pattern_.edges_;
  // Base case: all edges have been matched to query triples.
  if (edgeIdx == edges.size()) {
    emitIfLegal();
    return;
  }

  // For this edge, iterate all user query triples that have the edge's
  // predicate.
  const auto& edge = edges[edgeIdx];
  ad_utility::forEachSetBit(candidatesByEdge_[edgeIdx], [&](size_t tripleIdx) {
    if (isTripleCovered(tripleIdx)) {
      return true;
    }
    if (!decrementAndCheckNumAssignments()) {
      return false;
    }
    const auto& triple = triples_._triples.at(tripleIdx);
    bool subjectWasNew = isNewBinding(edge.s_);
    if (tryAssignment(edge.s_, triple.s_)) {
      bool objectWasNew = isNewBinding(edge.o_);
      if (tryAssignment(edge.o_, triple.o_)) {
        coverTriple(tripleIdx);
        extendMatch(edgeIdx + 1);
        uncoverTriple(tripleIdx);
      }
      undoAssignment(edge.o_, objectWasNew);
    }
    undoAssignment(edge.s_, subjectWasNew);
    return true;
  });
}

// _____________________________________________________________________________
bool PatternMatcher::decrementAndCheckNumAssignments() {
  if (numAssignmentsRemaining_ == 0) {
    if (!status_.has_value()) {
      status_ = MatchStatus::TruncatedByNumAssignments;
    }
    return false;
  }
  --numAssignmentsRemaining_;
  return true;
}

// _____________________________________________________________________________
PatternMatcher::MatchReport PatternMatcher::makeReport() const {
  return {status_.value_or(MatchStatus::Complete),
          {limits_.numAssignments_ - numAssignmentsRemaining_,
           limits_.numReplacementPlans_ - numReplacementPlansRemaining_}};
}

}  // namespace materializedViewsQueryAnalysis
