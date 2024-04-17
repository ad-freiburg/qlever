// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathHashMap.h"

#include <memory>

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
  return HashMapWrapper{std::move(edges), allocator()};
}
