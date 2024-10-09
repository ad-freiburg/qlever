//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <cstdint>

namespace countConnectedSubgraphs {

// A representation of an undirected graph with at most 64 nodes.
// Each vertex consists of a unique ID (`0 <= ID < 64`) and the set of its neighbors,
// which is stored in a single 64-bit number, where the i-th bit is 1 iff the
// node with ID `i` is a neighbor.
struct PlainVertex {
  uint64_t neighbors_{};
};
using PlainGraph = std::vector<PlainVertex>;

// Compute the number of connected subgraphs in the `graph`. If the number of
// such subraphs is `> budget` return budget.
size_t countSubgraphs(PlainGraph graph, size_t budget);

// Recursive implementation of `countSubgraphs`. Compute the number of connected
// subgraphs in the `graph` that contain all the nodes in the `subgraph` but
// none of the nodes in `ignored`. Assume that `c` subgraphs have been
// previously found and therefore count towards the `budget`. The `subgraph` and
// `ignored` are 1-hot encoded bitsets (see above).
size_t countSubgraphsRecursively(const PlainGraph& graph, uint64_t subgraph,
                                 uint64_t ignored, size_t c, size_t budget);

}

