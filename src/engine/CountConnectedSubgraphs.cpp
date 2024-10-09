//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/CountConnectedSubgraphs.h"

#include "util/BitUtils.h"

namespace countConnectedSubgraphs {

// _____________________________________________________________________________
size_t countSubgraphs(PlainGraph graph, size_t budget) {
  size_t c = 0;
  // For each node i, recursively count all subgraphs that contain `i`, but no
  // node `k < i` (because these have already been counted previously, when we
  // ran the loop for `k`.
  for (size_t i = 0; i < graph.size(); ++i) {
    ++c;
    if (c > budget) {
      return budget + 1;
    }
    // The subgraph that only consists of `i` is encoded by a single `1`bit.
    // The ignored set has `1`s in all `i` bits that have a lower index than `i`
    // (e.g. if `i` is 3, then the subgraph is `[0 *56] 0000 1000` and the
    // ignored set is `[0 * 56] 0000 0111`.
    uint64_t subgraph = 1ull << i;
    uint64_t ignored = ad_utility::bitMaskForLowerBits(i);
    c = countSubgraphsRecursively(graph, subgraph, ignored, c, budget);
  }
  return c;
}

// Return the set of nodes in the `graph` that are adjacent to at least one of
// the nodes in the `subgraph`. Nodes that are `ignored` are excluded from the
// result. Note that the result may contain nodes from the `subgraph` itself.
// All inputs and outputs use the same bit encoding like all the other
// functions.
static uint64_t computeNeighbors(const PlainGraph& graph, uint64_t subgraph,
                                 uint64_t ignored) {
  uint64_t NeighborsOfS{};
  for (size_t i = 0; i < 64; ++i) {
    bool set = subgraph & (1ull << i);
    if (set) {
      NeighborsOfS |= graph[i].neighbors_;
    }
  }
  NeighborsOfS &= (~ignored);
  return NeighborsOfS;
}

// For a number `i` with `0 <= i < 2^neighborVec.size()` return the `i`-th
// subset of the elements of `neighborVec`. All elements in `neighborVec` have
// to be `<64`, so that the final result can be expressed as a bitmap.
static uint64_t subsetIndexToBitmap(size_t i,
                                    const std::vector<uint8_t>& neighborVec) {
  // TODO<joka921> This can probably be done more efficiently using bit
  // fiddling, but it is efficient enough for now.
  uint64_t newSubgraph = 0;
  for (size_t k = 0; k < neighborVec.size(); ++k) {
    if (1 << k & i) {
      newSubgraph |= (1ull << neighborVec[k]);
    }
  }
  return newSubgraph;
}

// Convert a bitset to a vector of the indices of the bits that are set, e.g
// `13` (`1101` as bits) will be converted to `[0, 2, 3]`;
static std::vector<uint8_t> bitsetToVec(uint64_t bitset) {
  std::vector<uint8_t> neighborVec;
  for (uint64_t i = 0; i < 64; ++i) {
    if (bitset & (1ull << i)) {
      neighborVec.push_back(i);
    }
  }
  return neighborVec;
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
size_t countSubgraphsRecursively(const PlainGraph& graph, uint64_t subgraph,
                                 uint64_t ignored, size_t c, size_t budget) {
  // Compute the set of direct neighbors of the `subgraph` that is not
  // ignored
  uint64_t neighbors = computeNeighbors(graph, subgraph, ignored);

  std::vector<uint8_t> neighborVec = bitsetToVec(neighbors);

  // This is the recursion level which handles all the subsets of the neighbors,
  // and the above recursion levels deal with the `subgraph`, so we have to
  // exclude them further down.
  auto newIgnored = ignored | neighbors | subgraph;

  //   Iterate over all Subsets of the neighbors
  size_t upperBound = 1ull << neighborVec.size();
  for (size_t i = 1; i < upperBound; ++i) {
    ++c;
    if (c > budget) {
      return budget + 1;
    }
    auto subset = subsetIndexToBitmap(i, neighborVec);
    c = countSubgraphsRecursively(graph, subgraph | subset, newIgnored, c,
                                  budget);
  }
  return c;
}
}  // namespace countConnectedSubgraphs
