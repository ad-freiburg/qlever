// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#pragma once

#include <memory>

#include "TransitivePathBase.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"

class TransitivePathFallback : public TransitivePathBase {
 public:
  TransitivePathFallback(QueryExecutionContext* qec,
                         std::shared_ptr<QueryExecutionTree> child,
                         const TransitivePathSide& leftSide,
                         const TransitivePathSide& rightSide, size_t minDist,
                         size_t maxDist);

  /**
   * @brief Compute the transitive hull with a bound side.
   * This function is called when the startSide is bound and
   * it is a variable. The other IdTable contains the result
   * of the start side and will be used to get the start nodes.
   *
   * @tparam RES_WIDTH Number of columns of the result table
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @tparam SIDE_WIDTH Number of columns of the
   * @param res The result table which will be filled in-place
   * @param sub The IdTable for the sub result
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   * @param startSideTable The IdTable of the startSide
   */

  template <size_t RES_WIDTH, size_t SUB_WIDTH, size_t SIDE_WIDTH>
  void computeTransitivePathBound(IdTable* res, const IdTable& sub,
                                  const TransitivePathSide& startSide,
                                  const TransitivePathSide& targetSide,
                                  const IdTable& startSideTable) const;

  /**
   * @brief Compute the transitive hull.
   * This function is called when no side is bound (or an id).
   *
   * @tparam RES_WIDTH Number of columns of the result table
   * @tparam SUB_WIDTH Number of columns of the sub table
   * @param res The result table which will be filled in-place
   * @param sub The IdTable for the sub result
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   */

  template <size_t RES_WIDTH, size_t SUB_WIDTH>
  void computeTransitivePath(IdTable* res, const IdTable& sub,
                             const TransitivePathSide& startSide,
                             const TransitivePathSide& targetSide) const;

 private:
  /**
   * @brief Compute the result for this TransitivePath operation
   * This function chooses the start and target side for the transitive
   * hull computation. This choice of the start side has a large impact
   * on the time it takes to compute the hull. The set of nodes on the
   * start side should be as small as possible.
   *
   * @return ResultTable The result of the TransitivePath operation
   */
  ResultTable computeResult() override;

  /**
   * @brief Compute the transitive hull starting at the given nodes,
   * using the given Map.
   *
   * @param edges Adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param nodes A list of Ids. These Ids are used as starting points for the
   * transitive hull. Thus, this parameter guides the performance of this
   * algorithm.
   * @param target Optional target Id. If supplied, only paths which end
   * in this Id are added to the hull.
   * @return Map Maps each Id to its connected Ids in the transitive hull
   */
  Map transitiveHull(const Map& edges, const std::vector<Id>& startNodes,
                     std::optional<Id> target) const;

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
  template <size_t SUB_WIDTH>
  Map setupEdgesMap(const IdTable& dynSub, const TransitivePathSide& startSide,
                    const TransitivePathSide& targetSide) const;
};
