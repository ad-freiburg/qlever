//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_COUNTCONNECTEDSUBGRAPHS_H
#define QLEVER_SRC_ENGINE_COUNTCONNECTEDSUBGRAPHS_H

#include <cstdint>
#include <string>
#include <vector>

// This module implements the efficient counting of the number of connected
// subgraphs in a given graph. This routine can be used to analyze the
// complexity of query graphs and to choose an appropriate query planner (see
// `QueryPlanner.cpp`). The algorithm is taken from
// Neumann and Radke, Adaptive Optimization of Very Large Join Queries, see
// https://dl.acm.org/doi/pdf/10.1145/3183713.3183733
namespace countConnectedSubgraphs {

// A representation of an undirected graph with at most 64 nodes. Each node is
// represented by a 64-bit number, where the i-th bit is 1 iff the corresponding
// node is a neighbor of the node.
struct Node {
  uint64_t neighbors_{};
};
using Graph = std::vector<Node>;

// Compute the number of connected subgraphs in the `graph`. If the number of
// such subraphs is `> budget`, return `budget + 1`.
size_t countSubgraphs(const Graph& graph, size_t budget);

// Recursive implementation of `countSubgraphs`. Compute the number of connected
// subgraphs in `graph` that contains all the nodes in `nodes`, but none of the
// nodes in `ignored`. Assume that `count` subgraphs have been previously found
// and therefore count towards the `budget`. The `nodes` and `ignored` are 1-hot
// encoded bitsets (see above).
size_t countSubgraphsRecursively(const Graph& graph, uint64_t nodes,
                                 uint64_t ignored, size_t count, size_t budget);

// Convert `x` to a string of bits, with the leading zeros removed, e.g.,
// `3` will become "11". This is useful for debugging the functions above.
std::string toBitsetString(uint64_t x);

}  // namespace countConnectedSubgraphs

#endif  // QLEVER_SRC_ENGINE_COUNTCONNECTEDSUBGRAPHS_H
