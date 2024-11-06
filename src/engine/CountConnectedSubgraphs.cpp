//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/CountConnectedSubgraphs.h"

#include "util/BitUtils.h"

namespace countConnectedSubgraphs {

// _____________________________________________________________________________
size_t countSubgraphs(const Graph& graph, size_t budget) {
  size_t count = 0;
  // For each node `i`, recursively count all subgraphs that contain `i`, but no
  // node `k < i` (because these have already been counted previously, when we
  // ran the loop for `k`).
  for (size_t i = 0; i < graph.size(); ++i) {
    ++count;
    if (count > budget) {
      return budget + 1;
    }
    // The set of nodes that only consists of node `i` is encoded by a single
    // `1` bit. The ignored set has `1`s in all `i` bits that have a lower index
    // than `i` (e.g. if `i` is 3, then `nodes` is `[0 x 56] 0000 1000` and
    // `ignored` is `[0 x 56] 0000 0111`.
    uint64_t nodes = 1ULL << i;
    uint64_t ignored = ad_utility::bitMaskForLowerBits(i);
    count = countSubgraphsRecursively(graph, nodes, ignored, count, budget);
  }
  return count;
}

// Return the set of nodes in `graph` that are adjacent to at least one of the
// nodes in `nodes`. Nodes that are `ignored` are excluded from the result. Note
// that the result may contain nodes from the `nodes` itself. The result is
// returned using the same encoding as `nodes` and `ignored`.
static uint64_t computeNeighbors(const Graph& graph, uint64_t nodes,
                                 uint64_t ignored) {
  uint64_t neighbors{};
  for (size_t i = 0; i < 64; ++i) {
    bool set = nodes & (1ULL << i);
    if (set) {
      neighbors |= graph[i].neighbors_;
    }
  }
  neighbors &= (~ignored);
  return neighbors;
}

// For a number `i` from 0 .. 2^`neighbors.size()` - 1, return the `i`th
// subset of the elements of `neighbors`. All elements in `neighbors` have
// to be from 0..63 so that the final result can be expressed as a bitmap.
static uint64_t subsetIndexToBitmap(size_t i,
                                    const std::vector<uint8_t>& neighbors) {
  // Note: This can probably be done more efficiently using bit fiddling, but it
  // is efficient enough for now.
  uint64_t subset = 0;
  for (size_t k = 0; k < neighbors.size(); ++k) {
    if (1 << k & i) {
      subset |= (1ULL << neighbors[k]);
    }
  }
  return subset;
}

// Convert a bitset to a vector of the indices of the bits that are set. For
// example, `13` (`1101` as bits) will be converted to `[0, 2, 3]`;
static std::vector<uint8_t> bitsetToVector(uint64_t bitset) {
  std::vector<uint8_t> result;
  for (uint8_t i = 0; i < 64; ++i) {
    if (bitset & (1ULL << i)) {
      result.push_back(i);
    }
  }
  return result;
};

// _____________________________________________________________________________
std::string toBitsetString(uint64_t x) {
  auto res = std::bitset<64>{x}.to_string();
  auto pos = res.find('1');
  if (pos >= res.size()) {
    return "0";
  }
  return res.substr(pos);
}

// _____________________________________________________________________________
size_t countSubgraphsRecursively(const Graph& graph, uint64_t nodes,
                                 uint64_t ignored, size_t count,
                                 size_t budget) {
  // Compute the set of direct neighbors of the `nodes` that is not
  // ignored
  uint64_t neighbors = computeNeighbors(graph, nodes, ignored);

  std::vector<uint8_t> neighborsAsVector = bitsetToVector(neighbors);

  // This is the recursion level which handles all the subsets of the neigrbors,
  // and the above recursion levels deal with `nodes`, so we have to exclude
  // them further down.
  auto newIgnored = ignored | neighbors | nodes;

  //   Iterate over all Subsets of the neighbors
  size_t upperBound = 1ULL << neighborsAsVector.size();
  for (size_t i = 1; i < upperBound; ++i) {
    ++count;
    if (count > budget) {
      return budget + 1;
    }
    auto subset = subsetIndexToBitmap(i, neighborsAsVector);
    count = countSubgraphsRecursively(graph, nodes | subset, newIgnored, count,
                                      budget);
  }
  return count;
}
}  // namespace countConnectedSubgraphs
