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

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
PatternMatcher::PatternMatcher(
    const ViewPattern& pattern, const parsedQuery::BasicGraphPattern& triples,
    std::vector<const std::vector<size_t>*> candidatesByEdge,
    QueryExecutionContext* qec, size_t budget,
    std::vector<MaterializedViewJoinReplacement>& result)
    : pattern_{pattern},
      triples_{triples},
      candidatesByEdge_{std::move(candidatesByEdge)},
      qec_{qec},
      viewCols_{pattern.view_->variableToColumnMap()},
      stepsRemaining_{budget},
      result_{result} {}

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
bool PatternMatcher::isLegalFixedValuePrefix(uint64_t boundColumnsMask) {
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
  // Fixed query values must land on a legal column prefix; a payload column
  // (index > 2) bound to a fixed value is always illegal.
  uint64_t boundColumnsMask = 0;
  for (const auto& [viewVar, node] : assignment_) {
    if (!node.isVariable()) {
      size_t col = viewCols_.at(viewVar).columnIndex_;
      if (col > 2) {
        return;
      }
      boundColumnsMask |= (uint64_t{1} << col);
    }
  }
  if (!isLegalFixedValuePrefix(boundColumnsMask)) {
    return;
  }

  // Configuration is allowed. Construct `MaterializedViewJoinReplacement`.
  result_.push_back(
      {pattern_.view_->makeIndexScan(
           qec_, parsedQuery::MaterializedViewQuery{pattern_.view_->name(),
                                                    assignment_}),
       coveredTriples_});
}

// _____________________________________________________________________________
void PatternMatcher::extendMatch(size_t edgeIdx) {
  // If already `numMaxReplacementPlans` have been found, no further plans
  // should be generated.
  if (result_.size() >= numMaxReplacementPlans) {
    truncated_ = true;
    return;
  }

  const auto& edges = pattern_.edges_;
  // Base case: all edges of the view query have been matched to query triples.
  if (edgeIdx == edges.size()) {
    emitIfLegal();
    return;
  }

  const auto& edge = edges[edgeIdx];
  for (size_t tripleIdx : *candidatesByEdge_[edgeIdx]) {
    if (isTripleCovered(tripleIdx)) {
      continue;
    }
    if (!decrementAndCheckBudget()) {
      return;
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
  }
}

// _____________________________________________________________________________
bool PatternMatcher::decrementAndCheckBudget() {
  if (stepsRemaining_ == 0) {
    truncated_ = true;
    return false;
  }
  --stepsRemaining_;
  return true;
}

}  // namespace materializedViewsQueryAnalysis
