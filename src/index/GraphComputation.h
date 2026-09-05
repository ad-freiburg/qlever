//  Copyright 2026 The QLever Authors, in particular:
//
//  2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
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
// `preexistingGraphs` is used to initialize the vector of distinct graphs, so
// this function can be used to extend existing metadata.
CPP_template(typename T)(requires ql::ranges::range<T>&& ql::concepts::same_as<
                         ql::ranges::range_value_t<T>, Id>)
    std::optional<std::vector<Id>> computeDistinctGraphs(
        T&& idRange, ql::span<const Id> preexistingGraphs = {}) {
  AD_CORRECTNESS_CHECK(preexistingGraphs.size() <=
                       MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA);
  size_t foundGraphs = preexistingGraphs.size();
  // O(MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA * n), but good for cache
  // efficiency.
  std::array<Id, MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA> graphs;
  ql::ranges::copy(preexistingGraphs, graphs.begin());
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

// Return true iff the sorted `block` contains two consecutive rows that agree
// on the columns `0`, `1`, and `2`, that is, on the actual triple of S, P, and
// O. The graph column and all additional payload columns are ignored. As an
// `IdTable` is column-based, scan the three contiguous columns in a single
// pass instead of iterating row-wise via the row proxies.
inline bool hasDuplicateTriples(const IdTable& block) {
  AD_CORRECTNESS_CHECK(block.numColumns() >= 3);
  ql::span<const Id> col0 = block.getColumn(0);
  ql::span<const Id> col1 = block.getColumn(1);
  ql::span<const Id> col2 = block.getColumn(2);
  // Note: The condition `i + 1 < numRows` also correctly handles the cases of
  // zero and one row without any underflow.
  size_t numRows = block.numRows();
  for (size_t i = 0; i + 1 < numRows; ++i) {
    if (col0[i] == col0[i + 1] && col1[i] == col1[i + 1] &&
        col2[i] == col2[i + 1]) {
      return true;
    }
  }
  return false;
}

// Find out whether the sorted `block` contains duplicates and whether it
// contains only a few distinct graphs such that we can store this information
// in the block metadata.
inline std::pair<bool, std::optional<std::vector<Id>>> getGraphInfo(
    const IdTable& block) {
  AD_CORRECTNESS_CHECK(block.numColumns() > ADDITIONAL_COLUMN_GRAPH_ID);
  // Return true iff the block contains duplicates when only considering the
  // actual triple of S, P, and O.
  auto hasDuplicates = [&block]() { return hasDuplicateTriples(block); };

  auto graphs =
      computeDistinctGraphs(block.getColumn(ADDITIONAL_COLUMN_GRAPH_ID));
  // If there's only one graph, we know that there are no duplicates across
  // different graphs.
  return {!hasOnlyOneGraph(graphs) && hasDuplicates(), std::move(graphs)};
}

#endif  // QLEVER_SRC_INDEX_GRAPHCOMPUTATION_H
