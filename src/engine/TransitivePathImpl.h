// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"
#include "util/Exception.h"
#include "util/Timer.h"

/**
 * @class TransitivePathImpl
 * @brief This class implements common functions for the concrete TransitivePath
 * classes TransitivePathBinSearch and TransitivePathHashMap. The template can
 * be set to a map data structure which is used for the transitive hull
 * computation.
 *
 * @tparam T A map data structure for the transitive hull computation.
 */
template <typename T>
class TransitivePathImpl : public TransitivePathBase {
 public:
  TransitivePathImpl(QueryExecutionContext* qec,
                     std::shared_ptr<QueryExecutionTree> child,
                     TransitivePathSide leftSide, TransitivePathSide rightSide,
                     size_t minDist, size_t maxDist)
      : TransitivePathBase(qec, std::move(child), std::move(leftSide),
                           std::move(rightSide), minDist, maxDist){};

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
  void computeTransitivePathBound(IdTable* dynRes, const IdTable& dynSub,
                                  const TransitivePathSide& startSide,
                                  const TransitivePathSide& targetSide,
                                  const IdTable& startSideTable) const {
    auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
    timer.start();

    auto [edges, nodes] = setupMapAndNodes<SUB_WIDTH, SIDE_WIDTH>(
        dynSub, startSide, targetSide, startSideTable);

    timer.stop();
    auto initTime = timer.msecs();
    timer.start();

    Map hull(allocator());
    if (!targetSide.isVariable()) {
      hull = transitiveHull(edges, nodes, std::get<Id>(targetSide.value_));
    } else {
      hull = transitiveHull(edges, nodes, std::nullopt);
    }

    timer.stop();
    auto hullTime = timer.msecs();
    timer.start();

    fillTableWithHull(*dynRes, hull, nodes, startSide.outputCol_,
                      targetSide.outputCol_, startSideTable,
                      startSide.treeAndCol_.value().second);

    timer.stop();
    auto fillTime = timer.msecs();

    auto& info = runtimeInfo();
    info.addDetail("Initialization time", initTime.count());
    info.addDetail("Hull time", hullTime.count());
    info.addDetail("IdTable fill time", fillTime.count());
  };

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
  void computeTransitivePath(IdTable* dynRes, const IdTable& dynSub,
                             const TransitivePathSide& startSide,
                             const TransitivePathSide& targetSide) const {
    auto timer = ad_utility::Timer(ad_utility::Timer::Stopped);
    timer.start();

    auto [edges, nodes] =
        setupMapAndNodes<SUB_WIDTH>(dynSub, startSide, targetSide);

    timer.stop();
    auto initTime = timer.msecs();
    timer.start();

    Map hull{allocator()};
    if (!targetSide.isVariable()) {
      hull = transitiveHull(edges, nodes, std::get<Id>(targetSide.value_));
    } else {
      hull = transitiveHull(edges, nodes, std::nullopt);
    }

    timer.stop();
    auto hullTime = timer.msecs();
    timer.start();

    fillTableWithHull(*dynRes, hull, startSide.outputCol_,
                      targetSide.outputCol_);
    timer.stop();
    auto fillTime = timer.msecs();

    auto& info = runtimeInfo();
    info.addDetail("Initialization time", initTime.count());
    info.addDetail("Hull time", hullTime.count());
    info.addDetail("IdTable fill time", fillTime.count());
  };

 protected:
  /**
   * @brief Compute the result for this TransitivePath operation
   * This function chooses the start and target side for the transitive
   * hull computation. This choice of the start side has a large impact
   * on the time it takes to compute the hull. The set of nodes on the
   * start side should be as small as possible.
   *
   * @return ResultTable The result of the TransitivePath operation
   */
  ResultTable computeResult() override {
    if (minDist_ == 0 && !isBoundOrId() && lhs_.isVariable() &&
        rhs_.isVariable()) {
      AD_THROW(
          "This query might have to evalute the empty path, which is currently "
          "not supported");
    }
    auto [startSide, targetSide] = decideDirection();
    shared_ptr<const ResultTable> subRes = subtree_->getResult();

    IdTable idTable{allocator()};

    idTable.setNumColumns(getResultWidth());

    size_t subWidth = subRes->idTable().numColumns();

    if (startSide.isBoundVariable()) {
      shared_ptr<const ResultTable> sideRes =
          startSide.treeAndCol_.value().first->getResult();
      size_t sideWidth = sideRes->idTable().numColumns();

      CALL_FIXED_SIZE((std::array{resultWidth_, subWidth, sideWidth}),
                      &TransitivePathImpl<T>::computeTransitivePathBound, this,
                      &idTable, subRes->idTable(), startSide, targetSide,
                      sideRes->idTable());

      return {std::move(idTable), resultSortedOn(),
              ResultTable::getMergedLocalVocab(*sideRes, *subRes)};
    }
    CALL_FIXED_SIZE((std::array{resultWidth_, subWidth}),
                    &TransitivePathImpl<T>::computeTransitivePath, this,
                    &idTable, subRes->idTable(), startSide, targetSide);

    // NOTE: The only place, where the input to a transitive path operation is
    // not an index scan (which has an empty local vocabulary by default) is the
    // `LocalVocabTest`. But it doesn't harm to propagate the local vocab here
    // either.
    return {std::move(idTable), resultSortedOn(),
            subRes->getSharedLocalVocab()};
  };

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
  Map transitiveHull(const T& edges, const std::vector<Id>& startNodes,
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

      if (minDist_ == 0 &&
          (!target.has_value() || startNode == target.value())) {
        insertIntoMap(hull, startNode, startNode);
      }

      while (!stack.empty()) {
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

          const auto& successors = edges.successors(node);
          for (auto successor : successors) {
            stack.push_back({successor, steps + 1});
          }
        }
      }
    }
    return hull;
  }

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
  std::pair<T, std::vector<Id>> setupMapAndNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const {
    std::vector<Id> nodes;
    auto edges = setupEdgesMap(sub, startSide, targetSide);

    // id -> var|id
    if (!startSide.isVariable()) {
      nodes.push_back(std::get<Id>(startSide.value_));
      // var -> var
    } else {
      std::span<const Id> startNodes = sub.getColumn(startSide.subCol_);
      // TODO<C++23> Use ranges::to.
      nodes.insert(nodes.end(), startNodes.begin(), startNodes.end());
      if (minDist_ == 0) {
        std::span<const Id> targetNodes = sub.getColumn(targetSide.subCol_);
        nodes.insert(nodes.end(), targetNodes.begin(), targetNodes.end());
      }
    }

    return {std::move(edges), std::move(nodes)};
  };

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
  std::pair<T, std::vector<Id>> setupMapAndNodes(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide,
      const IdTable& startSideTable) const {
    std::vector<Id> nodes;
    auto edges = setupEdgesMap(sub, startSide, targetSide);

    // Bound -> var|id
    std::span<const Id> startNodes =
        startSideTable.getColumn(startSide.treeAndCol_.value().second);
    // TODO<C++23> Use ranges::to.
    nodes.insert(nodes.end(), startNodes.begin(), startNodes.end());

    return {std::move(edges), std::move(nodes)};
  };

  virtual T setupEdgesMap(const IdTable& dynSub,
                          const TransitivePathSide& startSide,
                          const TransitivePathSide& targetSide) const = 0;
};
