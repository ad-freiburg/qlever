// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <memory>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TransitivePathImpl.h"
#include "engine/idTable/IdTable.h"

struct HashMapWrapper {
  Map map_;

  auto successors(const Id node) const {
    auto iterator = map_.find(node);
    if (iterator == map_.end()) {
      return std::vector<Id>();
    }
    std::vector<Id> result(iterator->second.begin(), iterator->second.end());
    return result;
  }
};

/**
 * @class TransitivePathHashMap
 * @brief This class implements the transitive path operation. The
 * implementation uses a hash map to represent the graph and find successors
 * of given nodes.
 *
 *
 */
class TransitivePathHashMap : public TransitivePathImpl<HashMapWrapper> {
 public:
  TransitivePathHashMap(QueryExecutionContext* qec,
                        std::shared_ptr<QueryExecutionTree> child,
                        TransitivePathSide leftSide,
                        TransitivePathSide rightSide, size_t minDist,
                        size_t maxDist);

 private:
  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @tparam SIDE_WIDTH Number of columns of the startSideTable
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param targetSide The TransitivePathSide where the edges end
   * @param startSideTable An IdTable containing the Ids for the startSide
   * @return std::pair<Map, std::vector<Id>> A Map and Id vector (nodes) for the
   * transitive hull computation
   */
  template <size_t SUB_WIDTH, size_t SIDE_WIDTH>
  std::pair<Map, std::vector<Id>> setupMapAndNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide,
      const IdTable& startSideTable) const;

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param targetSide The TransitivePathSide where the edges end
   * @return std::pair<Map, std::vector<Id>> A Map and Id vector (nodes) for the
   * transitive hull computation
   */
  template <size_t SUB_WIDTH>
  std::pair<Map, std::vector<Id>> setupMapAndNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const;

  // initialize the map from the subresult
  HashMapWrapper setupEdgesMap(
      const IdTable& dynSub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const override;

  template <size_t SUB_WIDTH>
  HashMapWrapper setupEdgesMap(const IdTable& dynSub,
                               const TransitivePathSide& startSide,
                               const TransitivePathSide& targetSide) const;
};
