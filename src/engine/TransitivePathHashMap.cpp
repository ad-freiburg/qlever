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
    const TransitivePathSide leftSide, const TransitivePathSide rightSide,
    size_t minDist, size_t maxDist)
    : TransitivePathImpl<Map>(qec, std::move(child), std::move(leftSide),
                              std::move(rightSide), minDist, maxDist) {}

// _____________________________________________________________________________
Map TransitivePathHashMap::transitiveHull(const Map& edges,
                                          const std::vector<Id>& startNodes,
                                          std::optional<Id> target) const {
  using MapIt = Map::const_iterator;
  // For every node do a dfs on the graph
  Map hull{allocator()};

  // Stores nodes we already have a path to. This avoids cycles.
  ad_utility::HashSetWithMemoryLimit<Id> marks{
      getExecutionContext()->getAllocator()};

  // The stack used to store the dfs' progress
  std::vector<Set::const_iterator> positions;

  // Used to store all edges leading away from a node for every level.
  // Reduces access to the hashmap, and is safe as the map will not
  // be modified after this point.
  std::vector<const Set*> edgeCache;

  for (Id currentStartNode : startNodes) {
    if (hull.contains(currentStartNode)) {
      // We have already computed the hull for this node
      continue;
    }

    // Reset for this iteration
    marks.clear();

    MapIt rootEdges = edges.find(currentStartNode);
    if (rootEdges != edges.end()) {
      positions.push_back(rootEdges->second.begin());
      edgeCache.push_back(&rootEdges->second);
    }
    if (minDist_ == 0 &&
        (!target.has_value() || currentStartNode == target.value())) {
      insertIntoMap(hull, currentStartNode, currentStartNode);
    }

    // While we have not found the entire transitive hull and have not reached
    // the max step limit
    while (!positions.empty()) {
      checkCancellation();
      size_t stackIndex = positions.size() - 1;
      // Process the next child of the node at the top of the stack
      Set::const_iterator& pos = positions[stackIndex];
      const Set* nodeEdges = edgeCache.back();

      if (pos == nodeEdges->end()) {
        // We finished processing this node
        positions.pop_back();
        edgeCache.pop_back();
        continue;
      }

      Id child = *pos;
      ++pos;
      size_t childDepth = positions.size();
      if (childDepth <= maxDist_ && marks.count(child) == 0) {
        // process the child
        if (childDepth >= minDist_) {
          marks.insert(child);
          if (!target.has_value() || child == target.value()) {
            insertIntoMap(hull, currentStartNode, child);
          }
        }
        // Add the child to the stack
        MapIt it = edges.find(child);
        if (it != edges.end()) {
          positions.push_back(it->second.begin());
          edgeCache.push_back(&it->second);
        }
      }
    }
  }
  return hull;
}

// _____________________________________________________________________________
Map TransitivePathHashMap::setupEdgesMap(
    const IdTable& dynSub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  return CALL_FIXED_SIZE((std::array{dynSub.numColumns()}),
                         &TransitivePathHashMap::setupEdgesMap, this, dynSub,
                         startSide, targetSide);
}

// _____________________________________________________________________________
template <size_t SUB_WIDTH>
Map TransitivePathHashMap::setupEdgesMap(
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
  return edges;
}
