// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathBinSearch.h"

#include <memory>
#include <utility>

#include "engine/TransitivePathBase.h"

// _____________________________________________________________________________
TransitivePathBinSearch::TransitivePathBinSearch(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, Graphs activeGraphs)
    : TransitivePathImpl<BinSearchMap>(
          qec, std::move(child), std::move(leftSide), std::move(rightSide),
          minDist, maxDist, std::move(activeGraphs)) {
  auto [startSide, targetSide] = decideDirection();
  alternativelySortedSubtree_ = QueryExecutionTree::createSortedTree(
      subtree_, {targetSide.subCol_, startSide.subCol_});
  subtree_ = QueryExecutionTree::createSortedTree(
      subtree_, {startSide.subCol_, targetSide.subCol_});
}

// _____________________________________________________________________________
BinSearchMap TransitivePathBinSearch::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  return BinSearchMap{dynSub.getColumn(startSide.subCol_),
                      dynSub.getColumn(targetSide.subCol_)};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> TransitivePathBinSearch::cloneImpl() const {
  auto copy = std::make_unique<TransitivePathBinSearch>(*this);
  copy->subtree_ = subtree_->clone();
  copy->lhs_ = lhs_.clone();
  copy->rhs_ = rhs_.clone();
  return copy;
}
