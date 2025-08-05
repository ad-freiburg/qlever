// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//   2025-     Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/TransitivePathBinSearch.h"

#include <memory>
#include <utility>

#include "engine/TransitivePathBase.h"

// _____________________________________________________________________________
BinSearchMap::BinSearchMap(ql::span<const Id> startIds,
                           ql::span<const Id> targetIds,
                           const std::optional<ql::span<const Id>>& graphIds)
    : startIds_{startIds},
      targetIds_{targetIds},
      graphIds_{graphIds.has_value() ? graphIds.value() : ql::span<const Id>{}},
      // Set size to zero if graphs are active to avoid undefined behaviour in
      // case we forget to call `setActiveGraph`.
      sizeOfActiveGraph_{graphIds.has_value() ? 0 : startIds_.size()} {
  AD_CORRECTNESS_CHECK(startIds.size() == targetIds.size());
  AD_CORRECTNESS_CHECK(startIds.size() == graphIds_.size() ||
                       !graphIds.has_value());
  if (graphIds.has_value()) {
    AD_EXPENSIVE_CHECK(
        ql::ranges::is_sorted(::ranges::views::zip(graphIds_, startIds)));
  } else {
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(startIds));
  }
}

// _____________________________________________________________________________
ql::span<const Id> BinSearchMap::successors(Id node) const {
  auto range = ql::ranges::equal_range(
      startIds_.subspan(offsetOfActiveGraph_, sizeOfActiveGraph_), node);

  auto startIndex = std::distance(startIds_.begin(), range.begin());

  return targetIds_.subspan(startIndex, range.size());
}

// _____________________________________________________________________________
IdWithGraphs BinSearchMap::getEquivalentIdAndMatchingGraphs(Id node) const {
  IdWithGraphs result;
  if (graphIds_.empty()) {
    // We fill the graph id with undefined here, because it's supposed to be
    // unused, and `setGraphId` is no-op in this case using this value.
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

    offsetOfActiveGraph_ = startIndex;
    sizeOfActiveGraph_ = range.size();
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
  auto makeSortColumns = [&graphVariable](ColumnIndex first,
                                          ColumnIndex second) {
    std::vector<ColumnIndex> sortColumns;
    AD_CORRECTNESS_CHECK(!graphVariable.has_value());
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
  AD_CORRECTNESS_CHECK(!graphVariable_.has_value());
  return BinSearchMap{dynSub.getColumn(startSide.subCol_),
                      dynSub.getColumn(targetSide.subCol_), std::nullopt};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> TransitivePathBinSearch::cloneImpl() const {
  auto copy = std::make_unique<TransitivePathBinSearch>(*this);
  copy->subtree_ = subtree_->clone();
  copy->lhs_ = lhs_.clone();
  copy->rhs_ = rhs_.clone();
  return copy;
}
