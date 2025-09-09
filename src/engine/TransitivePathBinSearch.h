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

// A set of edges of the implicit graph of a transitive path operation together
// with the graph IRI of the source node of each edge. The edges are sorted by
// graph and then by source node. That way, we can efficiently find a given
// source node in a given graph using binary search.
//
// NOTE: In the comments and variable names below, "node" and "id" are used
// interchangeably.
class BinSearchMap {
  // The edges with their source and target nodes and the graph IRI of the
  // source node. The first two ranges have the same size. The third one either
  // has the same size (for a transitive path operation inside a GRAPH clause)
  // or otherwise is empty.
  ql::span<const Id> startIds_;
  ql::span<const Id> targetIds_;
  ql::span<const Id> graphIds_;

  // The index of the first edge of the currently active graph and the number
  // of edges in that graph.
  size_t offsetOfActiveGraph_ = 0;
  size_t sizeOfActiveGraph_;

 public:
  // Construct with the given edges. The `sizeOfActiveGraph_` is set to the
  // total number of edges if no graphs are given, or to zero otherwise. In the
  // latter case, the correct size has to be set via `setGraphId`.
  BinSearchMap(
      ql::span<const Id> startIds, ql::span<const Id> targetIds,
      const std::optional<ql::span<const Id>>& graphIds = std::nullopt);

  // Return all target nodes for the given source node in the currently
  // active graph.
  ql::span<const Id> successors(Id node) const;

  // Find all `Id`s in `startIds_` that are equal to `id` together with the
  // corresponding graph `Id`s, with the following special cases:
  //
  // 1. If `id` is undefined, return all distinct `Id`s in `startIds_`.
  //
  // 2. if `id` holds a `LocalVocabIndex`, return those `Id`s from `startIds_`
  // that represent the same IRI or literal (`startIds_` never holds entries
  // from the `LocalVocab`).
  //
  // 3. If `graphIds_` is empty, use `Id::makeUndefined()` as graph `Id`.
  //
  // NOTE: If no such `Id`s exist, an empty vector is returned.
  IdWithGraphs getEquivalentIdAndMatchingGraphs(Id id) const;

  // Set `offsetOfActiveGraph_` and `sizeOfActiveGraph_` according to the given
  // `graphId`.
  void setGraphId(Id graphId);
};

// Subclass of `TransitivePathImpl` for evaluation a transitive path operation
// using the `BinSearchMap` from above.
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

  // Create the `BinSearchMap` from the given `edges`, which is an `IdTable`
  // with either two columns (without graph variable) or three columns (with
  // graph variable).
  BinSearchMap setupEdgesMap(
      const IdTable& edges, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const override;

  // Alternative subtree sorted by (graph, target, source). This is used then
  // the right side of the transitive path operation is bound.
  std::shared_ptr<QueryExecutionTree> alternativelySortedSubtree_;
  ql::span<const std::shared_ptr<QueryExecutionTree>> alternativeSubtrees()
      const override {
    return {&alternativelySortedSubtree_, 1};
  }
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHBINSEARCH_H
