// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#pragma once

#include <iterator>
#include <memory>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TransitivePathImpl.h"
#include "engine/idTable/IdTable.h"

/**
 * @class BinSearchMap
 * @brief This struct represents a simple Binary Search Map. Given an Id, the
 * BinSearchMap can return a list (i.e. span) of successor Ids.
 *
 * It is expected that the two input spans startIds_ and targetIds_ are sorted
 * first by start id and then by target id.
 * Example:
 *
 * startId | targetId
 * ------------------
 *       1 |        1
 *       1 |        2
 *       2 |        4
 *       3 |        2
 *       3 |        4
 *       3 |        6
 *       5 |        2
 *       5 |        6
 *
 */
struct BinSearchMap {
  std::span<const Id> startIds_;
  std::span<const Id> targetIds_;

  /**
   * @brief Return the successors for the given id.
   * The successors are all target ids, where the corresponding start id is
   * equal to the given id `node`.
   *
   * @param node The input id, which will be looked up in startIds_
   * @return A std::span<Id>, which consists of all targetIds_ where
   * startIds_ == node.
   */
  auto successors(const Id node) const {
    auto range = ql::ranges::equal_range(startIds_, node);

    auto startIndex = std::distance(startIds_.begin(), range.begin());

    return targetIds_.subspan(startIndex, range.size());
  }
};

/**
 * @class TransitivePathBinSearch
 * @brief This class implements the transitive path operation. The
 * implementation represents the graph as adjacency lists and uses binary search
 * to find successors of given nodes.
 */
class TransitivePathBinSearch : public TransitivePathImpl<BinSearchMap> {
 public:
  TransitivePathBinSearch(QueryExecutionContext* qec,
                          std::shared_ptr<QueryExecutionTree> child,
                          TransitivePathSide leftSide,
                          TransitivePathSide rightSide, size_t minDist,
                          size_t maxDist);

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  // initialize the map from the subresult
  BinSearchMap setupEdgesMap(
      const IdTable& dynSub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const override;

  // We store the subtree in two different orderings such that the appropriate
  // ordering is available when the right side of the transitive path operation
  // is bound. When the left side is bound, we already have the correct
  // ordering.
  std::shared_ptr<QueryExecutionTree> alternativelySortedSubtree_;
  std::span<const std::shared_ptr<QueryExecutionTree>> alternativeSubtrees()
      const override {
    return {&alternativelySortedSubtree_, 1};
  }
};
