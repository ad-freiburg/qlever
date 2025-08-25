// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//   2025-     Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHBINSEARCH_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHBINSEARCH_H

#include <absl/container/inlined_vector.h>

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
class BinSearchMap {
  // All of these have the same size, for convenience the code represents "no
  // graph" as an empty span for `graphIds_`, regardless of the amount of
  // elements of the other ranges.
  ql::span<const Id> startIds_;
  ql::span<const Id> targetIds_;
  ql::span<const Id> graphIds_;
  // Set the bounds of the currently active graph.
  size_t offsetOfActiveGraph_ = 0;
  size_t sizeOfActiveGraph_;

 public:
  BinSearchMap(
      ql::span<const Id> startIds, ql::span<const Id> targetIds,
      const std::optional<ql::span<const Id>>& graphIds = std::nullopt);

  /**
   * @brief Return the successors for the given id.
   * The successors are all target ids, where the corresponding start id is
   * equal to the given id `node`. Only values matching the active graph (set
   * via `setGraphId`) will be returned.
   *
   * @param node The input id, which will be looked up in startIds_
   * @return A ql::span<Id>, which consists of all targetIds_ where
   * startIds_ == node.
   */
  ql::span<const Id> successors(Id node) const;

  // Return a pair of matching ids + graph ids. If `node` originates from a
  // `LocalVocab` an equivalent entry from the graph is used instead,
  // eliminating the need to keep the `LocalVocab` around any longer. If no
  // entry matches an empty vector is returned. The first id of the pair is
  // always the same element. It is flattened out because all callers of this
  // function need it in this format for convenience. The most common case is
  // that there's a single matching entry, (especially when using this without
  // an active graph,) which is why `absl::InlinedVector` is used with size 1.
  //  If `node` is undefined, it
  // will return all elements in the currently active graph, or all elements if
  // no graph is set. Active graphs set via `setGraphId` are ignored. Entries
  // are deduplicated.
  IdWithGraphs getEquivalentIdAndMatchingGraphs(Id node) const;

  // Prefilter the map for values of a certain graph. If graphs are active, i.e.
  // the constructor of this class was called with a graph column, this has to
  // be set before calling `successors`.
  void setGraphId(Id graphId);
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
