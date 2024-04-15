// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathHashMap.h"

#include <memory>
#include <optional>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"

// _____________________________________________________________________________
TransitivePathHashMap::TransitivePathHashMap(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist)
    : TransitivePathImpl<HashMapWrapper>(
          qec, std::move(child), std::move(leftSide), std::move(rightSide),
          minDist, maxDist) {}

// _____________________________________________________________________________
Map TransitivePathHashMap::transitiveHull(const HashMapWrapper& edges,
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
HashMapWrapper TransitivePathHashMap::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  return CALL_FIXED_SIZE((std::array{dynSub.numColumns()}),
                         &TransitivePathHashMap::setupEdgesMap, this, dynSub,
                         startSide, targetSide);
}

// _____________________________________________________________________________
template <size_t SUB_WIDTH>
HashMapWrapper TransitivePathHashMap::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  const IdTableView<SUB_WIDTH> sub = dynSub.asStaticView<SUB_WIDTH>();
  Map edges{allocator()};
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);

  for (size_t i = 0; i < sub.size(); i++) {
    checkCancellation();
    insertIntoMap(edges, startCol[i], targetCol[i]);
  }
  auto wrapper = HashMapWrapper(edges);
  return wrapper;
}
