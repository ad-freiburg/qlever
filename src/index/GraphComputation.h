//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_INDEX_GRAPHCOMPUTATION_H
#define QLEVER_SRC_INDEX_GRAPHCOMPUTATION_H

#include <optional>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/Exception.h"

// Helper function to compute the distinct graphs contained in a block. Returns
// `nullopt` if there are more than `MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA`
// distinct graphs, otherwise returns the distinct graphs as a vector. The
// `initializer` is used to initialize the vector of distinct graphs.
CPP_template(typename T)(requires ql::ranges::range<T>&&
                             std::same_as<ql::ranges::range_value_t<T>, Id>)
    std::optional<std::vector<Id>> computeDistinctGraphs(
        T&& idRange, ql::span<const Id> initializer = {}) {
  AD_CORRECTNESS_CHECK(initializer.size() <=
                       MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA);
  size_t foundGraphs = initializer.size();
  // O(MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA * n), but good for cache
  // efficiency.
  std::array<Id, MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA> graphs;
  ql::ranges::copy(initializer, graphs.begin());
  for (Id graph : idRange) {
    auto actualEnd = graphs.begin() + foundGraphs;
    if (ql::ranges::find(graphs.begin(), actualEnd, graph.getBits(),
                         &Id::getBits) != actualEnd) {
      continue;
    }
    if (foundGraphs == MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA) {
      return std::nullopt;
    }
    graphs.at(foundGraphs) = graph;
    ++foundGraphs;
  }
  return std::vector<Id>(graphs.begin(), graphs.begin() + foundGraphs);
}

// Helper function to check whether the block contains only one graph.
inline bool hasOnlyOneGraph(const std::optional<std::vector<Id>>& graphs) {
  return graphs.has_value() && graphs->size() == 1;
}

// Find out whether the sorted `block` contains duplicates and whether it
// contains only a few distinct graphs such that we can store this information
// in the block metadata.
inline std::pair<bool, std::optional<std::vector<Id>>> getGraphInfo(
    const IdTable& block) {
  AD_CORRECTNESS_CHECK(block.numColumns() > ADDITIONAL_COLUMN_GRAPH_ID);
  // Return true iff the block contains duplicates when only considering the
  // actual triple of S, P, and O.
  auto hasDuplicates = [&block]() {
    using C = ColumnIndex;
    auto withoutGraphAndAdditionalPayload =
        block.asColumnSubsetView(std::array{C{0}, C{1}, C{2}})
            .asStaticView<3>();
    return ql::ranges::adjacent_find(withoutGraphAndAdditionalPayload) !=
           ql::ranges::end(withoutGraphAndAdditionalPayload);
  };

  auto graphs =
      computeDistinctGraphs(block.getColumn(ADDITIONAL_COLUMN_GRAPH_ID));
  // If there's only one graph, we know that there are no duplicates across
  // different graphs.
  return {!hasOnlyOneGraph(graphs) && hasDuplicates(), std::move(graphs)};
}

#endif  // QLEVER_SRC_INDEX_GRAPHCOMPUTATION_H
