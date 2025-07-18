// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathBinSearch.h"

#include <memory>
#include <utility>

#include "engine/TransitivePathBase.h"

// _____________________________________________________________________________
BinSearchMap::BinSearchMap(ql::span<const Id> startIds,
                           ql::span<const Id> targetIds,
                           ql::span<const Id> graphIds)
    : startIds_{startIds},
      targetIds_{targetIds},
      graphIds_{graphIds},
      size_{startIds_.size()} {
  AD_CORRECTNESS_CHECK(startIds.size() == targetIds.size());
  AD_CORRECTNESS_CHECK(startIds.size() == graphIds.size() || graphIds.empty());
}

// _____________________________________________________________________________
ql::span<const Id> BinSearchMap::successors(Id node) const {
  auto range = ql::ranges::equal_range(startIds_.subspan(offset_, size_), node);

  auto startIndex = std::distance(startIds_.begin(), range.begin());

  return targetIds_.subspan(startIndex, range.size());
}

// _____________________________________________________________________________
absl::InlinedVector<std::pair<Id, Id>, 1> BinSearchMap::getEquivalentIds(
    Id node) const {
  absl::InlinedVector<std::pair<Id, Id>, 1> result;
  if (graphIds_.empty()) {
    if (node.isUndefined()) {
      for (Id id : startIds_) {
        if (result.empty() || result.back().first != id) {
          result.emplace_back(id, Id::makeUndefined());
        }
      }
    } else {
      auto range = ql::ranges::equal_range(startIds_, node);
      if (!range.empty()) {
        result.emplace_back(range.front(), Id::makeUndefined());
      }
    }
  } else {
    for (auto [id, graphId] : ::ranges::views::zip(startIds_, graphIds_)) {
      bool isNewEntry = result.empty() || result.back().first != id ||
                        result.back().second != graphId;
      if ((id == node || node.isUndefined()) && isNewEntry) {
        result.emplace_back(id, graphId);
      }
    }
  }
  return result;
}

// _____________________________________________________________________________
void BinSearchMap::setGraphId(Id graphId) {
  if (graphId.isUndefined()) {
    // If the graph id is undefined, this means that all graphs should match,
    // which only works if we sorted by the actual values, not by graphs.
    AD_CORRECTNESS_CHECK(graphIds_.empty());
  } else {
    auto range = ql::ranges::equal_range(graphIds_, graphId);
    auto startIndex = std::distance(graphIds_.begin(), range.begin());

    offset_ = startIndex;
    size_ = range.size();
  }
}

// _____________________________________________________________________________
TransitivePathBinSearch::TransitivePathBinSearch(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, Graphs activeGraphs,
    const std::optional<Variable>& graphVariable)
    : TransitivePathImpl<BinSearchMap>(
          qec, std::move(child), std::move(leftSide), std::move(rightSide),
          minDist, maxDist, std::move(activeGraphs), graphVariable) {
  auto [startSide, targetSide] = decideDirection();
  auto makeSortColumns = [this, &graphVariable](ColumnIndex first,
                                                ColumnIndex second) {
    std::vector<ColumnIndex> sortColumns;
    if (graphVariable.has_value()) {
      sortColumns.push_back(subtree_->getVariableColumn(graphVariable.value()));
    }
    sortColumns.push_back(first);
    sortColumns.push_back(second);
    return sortColumns;
  };
  alternativelySortedSubtree_ = QueryExecutionTree::createSortedTree(
      subtree_, makeSortColumns(targetSide.subCol_, startSide.subCol_));
  subtree_ = QueryExecutionTree::createSortedTree(
      subtree_, makeSortColumns(startSide.subCol_, targetSide.subCol_));
}

// _____________________________________________________________________________
BinSearchMap TransitivePathBinSearch::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  return BinSearchMap{
      dynSub.getColumn(startSide.subCol_), dynSub.getColumn(targetSide.subCol_),
      graphVariable_.has_value() ? dynSub.getColumn(subtree_->getVariableColumn(
                                       graphVariable_.value()))
                                 : ql::span<const Id>{}};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> TransitivePathBinSearch::cloneImpl() const {
  auto copy = std::make_unique<TransitivePathBinSearch>(*this);
  copy->subtree_ = subtree_->clone();
  copy->lhs_ = lhs_.clone();
  copy->rhs_ = rhs_.clone();
  return copy;
}
