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
    if (c > budget) {
      return c;
    }
    // The subgraph that only consists of `i` is encoded by a single `1`bit.
    // The ingored set has `1`s in all `i` bits that have a lower index than `i`
    // (e.g. if `i` is 3, then the subgraph is `[0 *56] 0000 1000` and the
    // ignored set is `[0 * 56] 0000 0111`.
    uint64_t subgraph = 1ull << i;
    uint64_t ignored = ad_utility::bitMaskForLowerBits(i);
    c = countSubgraphsRecursively(graph, subgraph, ignored, c, budget);
  }
  return c;
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
static uint64_t subsetIndexToBitmap(size_t i, const auto& neighborVec) {
  // TODO<joka921> This can probably be done more efficiently using bit fiddling,
  // but it is efficient enough for now.
  uint64_t newSubgraph = 0;
  for (size_t k = 0; k < neighborVec.size(); ++k) {
    if (1 << k & i) {
      newSubgraph |= (1ull << neighborVec[k]);
    }
  }
  return newSubgraph;
}

// _____________________________________________________________________________
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
size_t countSubgraphsRecursively(const PlainGraph& graph, uint64_t subgraph,
                                 uint64_t ignored, size_t c, size_t budget) {
  // Compute the set of direct neighbors of the `subgraph` that is not
  // ignored
  uint64_t NeighborsOfS = computeNeighbors(graph, subgraph, ignored);

  std::vector<uint8_t> neighborVec = bitsetToVec(NeighborsOfS);

  auto newIgnored = ignored | NeighborsOfS;

  //   Iterate over all Subsets of the neighbors
  //   TODO<joka921> Should we include the empty subset here?
  //   TODO<joka921> Should we eliminate the neighbors that are contained
  // in the subgraph itself from the neighbor set?
  size_t upperBound = 1ull << neighborVec.size();
  for (size_t i = 1; i < upperBound; ++i) {
    ++c;
    if (c > budget) {
      return c;
    }
    auto subset = subsetIndexToBitmap(i, neighborVec);
    c = countSubgraphsRecursively(graph, subgraph | subset, newIgnored, c, budget);
  }
  return c;
}
}
