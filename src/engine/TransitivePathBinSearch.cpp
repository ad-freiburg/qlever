// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathBinSearch.h"

#include <memory>
#include <optional>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"

// _____________________________________________________________________________
TransitivePathBinSearch::TransitivePathBinSearch(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist)
    : TransitivePathImpl<BinSearchMap>(qec, std::move(child),
                                       std::move(leftSide),
                                       std::move(rightSide), minDist, maxDist) {
  auto [startSide, targetSide] = decideDirection();
  subtree_ = QueryExecutionTree::createSortedTree(
      subtree_, {startSide.subCol_, targetSide.subCol_});
}

// _____________________________________________________________________________
Map TransitivePathBinSearch::transitiveHull(const BinSearchMap& edges,
                                            const std::vector<Id>& startNodes,
                                            std::optional<Id> target) const {
  // For every node do a dfs on the graph
  Map hull{allocator()};

  std::vector<std::pair<Id, size_t>> stack;
  ad_utility::HashSetWithMemoryLimit<Id> marks{
      getExecutionContext()->getAllocator()};
  for (auto startNode : startNodes) {
    if (hull.contains(startNode)) {
      // We have already computed the hull for this node
      continue;
    }

    marks.clear();
    stack.clear();
    stack.push_back({startNode, 0});

    if (minDist_ == 0 && (!target.has_value() || startNode == target.value())) {
      insertIntoMap(hull, startNode, startNode);
    }

    while (stack.size() > 0) {
      checkCancellation();
      auto [node, steps] = stack.back();
      stack.pop_back();

      if (steps <= maxDist_ && marks.count(node) == 0) {
        if (steps >= minDist_) {
          marks.insert(node);
          if (!target.has_value() || node == target.value()) {
            insertIntoMap(hull, startNode, node);
          }
        }

        auto successors = edges.successors(node);
        for (auto successor : successors) {
          stack.push_back({successor, steps + 1});
        }
      }
    }
  }
  return hull;
}

// _____________________________________________________________________________
BinSearchMap TransitivePathBinSearch::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  return BinSearchMap(dynSub.getColumn(startSide.subCol_),
                      dynSub.getColumn(targetSide.subCol_));
}
