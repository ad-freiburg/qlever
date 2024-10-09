//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <cstdint>

// This module implements the efficient counting of the number of connected
// subgraphs in a given graph. This routine can be used to analyze the
// complexity of query graphs and to choose an appropriate query planner (see
// `QueryPlanner.cpp`). The algorithm is taken from
// Neumann and Radke, Adaptive Optimization of Very Large Join Queries, see
// https://dl.acm.org/doi/pdf/10.1145/3183713.3183733
namespace countConnectedSubgraphs {

// A representation of an undirected graph with at most 64 nodes.
// Each vertex consists of a unique ID (`0 <= ID < 64`) and the set of its
// neighbors, which is stored in a single 64-bit number, where the i-th bit is 1
// iff the node with ID `i` is a neighbor.
struct PlainVertex {
  uint64_t neighbors_{};
};
using PlainGraph = std::vector<PlainVertex>;

// Compute the number of connected subgraphs in the `graph`. If the number of
// such subraphs is `> budget` return `budget + 1`.
size_t countSubgraphs(PlainGraph graph, size_t budget);

// Recursive implementation of `countSubgraphs`. Compute the number of connected
// subgraphs in the `graph` that contain all the nodes in the `subgraph` but
// none of the nodes in `ignored`. Assume that `c` subgraphs have been
// previously found and therefore count towards the `budget`. The `subgraph` and
// `ignored` are 1-hot encoded bitsets (see above).
size_t countSubgraphsRecursively(const PlainGraph& graph, uint64_t subgraph,
                                 uint64_t ignored, size_t c, size_t budget);

// Convert `x` to a string of bits, with the leading zeros removed, e.g.
// `3` will become "11". This is very useful for debugging the above algorithms
std::string toBitsetString(uint64_t x);

}  // namespace countConnectedSubgraphs
