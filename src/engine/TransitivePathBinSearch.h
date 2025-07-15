// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHBINSEARCH_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHBINSEARCH_H

#include <iterator>
#include <memory>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TransitivePathImpl.h"
#include "engine/idTable/IdTable.h"
#include "util/Exception.h"

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
class BinSearchMap {
  ql::span<const Id> startIds_;
  ql::span<const Id> targetIds_;
  ql::span<const Id> graphIds_;
  size_t offset_ = 0;
  size_t size_;

 public:
  BinSearchMap(ql::span<const Id> startIds, ql::span<const Id> targetIds,
               ql::span<const Id> graphIds)
      : startIds_{startIds},
        targetIds_{targetIds},
        graphIds_{graphIds},
        size_{startIds_.size()} {
    AD_CORRECTNESS_CHECK(startIds.size() == targetIds.size());
    AD_CORRECTNESS_CHECK(startIds.size() == graphIds.size() ||
                         graphIds.empty());
  }

  /**
   * @brief Return the successors for the given id.
   * The successors are all target ids, where the corresponding start id is
   * equal to the given id `node`.
   *
   * @param node The input id, which will be looked up in startIds_
   * @return A ql::span<Id>, which consists of all targetIds_ where
   * startIds_ == node.
   */
  auto successors(const Id node) const {
    auto range =
        ql::ranges::equal_range(startIds_.subspan(offset_, size_), node);

    auto startIndex = std::distance(startIds_.begin(), range.begin());

    return targetIds_.subspan(startIndex, range.size());
  }

  // Return equivalent ids from the index, along with an associated graph id in
  // case these are available.
  std::vector<std::pair<Id, Id>> getEquivalentIds(Id node) const {
    std::vector<std::pair<Id, Id>> result;
    if (graphIds_.empty()) {
      auto range = ql::ranges::equal_range(startIds_, node);
      if (!range.empty()) {
        result.emplace_back(range.front(), Id::makeUndefined());
      }
    } else {
      for (auto [id, graphId] : ::ranges::views::zip(startIds_, graphIds_)) {
        if (id == node) {
          // Duplicates are fine, the only usage of this function deduplicates
          // this.
          result.emplace_back(id, graphId);
        }
      }
    }
    return result;
  }

  void setGraphId(const Id& graphId) {
    if (!graphId.isUndefined()) {
      auto range = ql::ranges::equal_range(graphIds_, graphId);
      auto startIndex = std::distance(graphIds_.begin(), range.begin());

      offset_ = startIndex;
      size_ = range.size();
    } else {
      offset_ = 0;
      size_ = startIds_.size();
    }
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
                          size_t maxDist, Graphs activeGraphs,
                          const std::optional<Variable>& graphVariable);

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
  ql::span<const std::shared_ptr<QueryExecutionTree>> alternativeSubtrees()
      const override {
    return {&alternativelySortedSubtree_, 1};
  }
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHBINSEARCH_H
