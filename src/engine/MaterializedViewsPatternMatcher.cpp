// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/MaterializedViewsPatternMatcher.h"

#include <algorithm>

#include "engine/MaterializedViews.h"

namespace materializedViewsQueryAnalysis {

namespace {

// Whether `boundColumnsMask` (bit `i` set iff view column `i` is bound to a
// fixed value from the query) is a legal prefix for a single SPO-sorted view
// permutation: only subject, subject+predicate, or subject+predicate+object
// (`0b001`/`0b011`/`0b111`, or `0b000`) may be fixed.
bool isLegalFixedValuePrefix(unsigned boundColumnsMask) {
  return boundColumnsMask == 0b000u || boundColumnsMask == 0b001u ||
         boundColumnsMask == 0b011u || boundColumnsMask == 0b111u;
}

// Hard cap on how many replacements `makeJoinReplacementIndexScans` collects
// in total across every candidate view for one query: each one becomes a
// candidate plan the query planner must separately consider, so leaving this
// unbounded would let the total planning cost scale with the number of
// loaded views on top of the pattern-match budget already bounding each
// individual view's search. ponytail: fixed constant, comfortably above what
// any realistic view/query shape needs; promote to a runtime parameter if a
// real workload needs it tuned.
constexpr size_t kMaxReplacements = 1000;

}  // namespace

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
bool PatternMatcher::tryAssign(const TripleComponent& viewSide,
                               const TripleComponent& queryNode) {
  if (!viewSide.isVariable()) {
    return queryNode == viewSide;
  }
  const Variable& viewVar = viewSide.getVariable();
  auto it = assignment_.find(viewVar);
  if (it != assignment_.end()) {
    return it->second == queryNode;
  }
  if (queryNode.isVariable()) {
    // Injectivity: no view variable may already be bound to this same query
    // variable.
    for (const auto& [otherVar, otherNode] : assignment_) {
      if (otherNode == queryNode) {
        return false;
      }
    }
  } else {
    // A payload column (index > 2) bound to a fixed value is always illegal
    // (see `isLegalFixedValuePrefix`), regardless of what else is assigned.
    size_t col = viewCols_.at(viewVar).columnIndex_;
    if (col > 2) {
      return false;
    }
    // Fixed-value prefix pruning: a smaller-column view variable that is
    // still bound to a query variable rules out fixing this (larger) column.
    for (const auto& [otherVar, otherNode] : assignment_) {
      if (otherNode.isVariable() && viewCols_.at(otherVar).columnIndex_ < col) {
        return false;
      }
    }
  }
  assignment_.emplace(viewVar, queryNode);
  return true;
}

// _____________________________________________________________________________
bool PatternMatcher::isNewBinding(const TripleComponent& viewSide) const {
  return viewSide.isVariable() && !assignment_.contains(viewSide.getVariable());
}

// _____________________________________________________________________________
void PatternMatcher::undoAssign(const TripleComponent& viewSide, bool wasNew) {
  if (wasNew) {
    assignment_.erase(viewSide.getVariable());
  }
}

// _____________________________________________________________________________
void PatternMatcher::emitIfLegal() {
  // Fixed query values must land on a legal column prefix; a payload column
  // (index > 2) bound to a fixed value is always illegal.
  unsigned boundColumnsMask = 0;
  for (const auto& [viewVar, node] : assignment_) {
    if (!node.isVariable()) {
      size_t col = viewCols_.at(viewVar).columnIndex_;
      if (col > 2) {
        return;
      }
      boundColumnsMask |= (1u << col);
    }
  }
  if (!isLegalFixedValuePrefix(boundColumnsMask)) {
    return;
  }
  result_.push_back(
      {pattern_.view_->makeIndexScan(
           qec_, parsedQuery::MaterializedViewQuery{pattern_.view_->name(),
                                                    assignment_}),
       coveredTriples_});
}

// _____________________________________________________________________________
void PatternMatcher::extendMatch(size_t edgeIdx) {
  if (result_.size() >= kMaxReplacements) {
    truncated_ = true;
    return;
  }
  const auto& edges = pattern_.edges_;
  if (edgeIdx == edges.size()) {
    emitIfLegal();
    return;
  }
  const auto& edge = edges[edgeIdx];
  for (size_t tripleIdx : *candidatesByEdge_[edgeIdx]) {
    if (std::find(coveredTriples_.begin(), coveredTriples_.end(), tripleIdx) !=
        coveredTriples_.end()) {
      continue;
    }
    if (stepsRemaining_ == 0) {
      truncated_ = true;
      return;
    }
    --stepsRemaining_;
    const auto& triple = triples_._triples.at(tripleIdx);
    bool subjectWasNew = isNewBinding(edge.s_);
    if (tryAssign(edge.s_, triple.s_)) {
      bool objectWasNew = isNewBinding(edge.o_);
      if (tryAssign(edge.o_, triple.o_)) {
        coveredTriples_.push_back(tripleIdx);

        extendMatch(edgeIdx + 1);

        coveredTriples_.pop_back();
      }
      undoAssign(edge.o_, objectWasNew);
    }
    undoAssign(edge.s_, subjectWasNew);
  }
}

}  // namespace materializedViewsQueryAnalysis
