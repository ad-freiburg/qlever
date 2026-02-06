//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Claude (Anthropic AI)

#ifndef QLEVER_TEST_ENGINE_LAZYJOINTESTHELPERS_H
#define QLEVER_TEST_ENGINE_LAZYJOINTESTHELPERS_H

#include "../util/AllocatorTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "parser/TripleComponent.h"

namespace ad_utility::testing {

// Helper class providing common functionality for testing lazy joins with
// IndexScans. This is used by both IndexScanTest and OptionalJoinTest.
class LazyJoinTestHelper {
 protected:
  QueryExecutionContext* qec_ = nullptr;

  // Get the EncodedIriManager singleton.
  static const EncodedIriManager& encodedIriManager() {
    static EncodedIriManager manager;
    return manager;
  }

  // Convert a TripleComponent to a ValueId.
  Id toValueId(const TripleComponent& tc) const {
    return tc.toValueId(qec_->getIndex().getVocab(), encodedIriManager())
        .value();
  }

  // Create an id table with a single column from a vector of TripleComponents.
  IdTable makeIdTable(std::vector<TripleComponent> entries) const {
    IdTable result{1, makeAllocator()};
    result.reserve(entries.size());
    for (const TripleComponent& entry : entries) {
      result.emplace_back();
      result.back()[0] = toValueId(entry);
    }
    return result;
  }

  // Create an id table with two columns from a vector of TripleComponent
  // arrays.
  IdTable tableFromTriples(
      std::vector<std::array<TripleComponent, 2>> triples) const {
    IdTable result{2, makeAllocator()};
    result.reserve(triples.size());
    for (const auto& triple : triples) {
      result.emplace_back();
      result.back()[0] = toValueId(triple.at(0));
      result.back()[1] = toValueId(triple.at(1));
    }
    return result;
  }

  // Setup helper to create a QEC with a knowledge graph and optional block
  // size.
  void setupQecWithKnowledgeGraph(
      const std::string& kg,
      std::optional<ad_utility::MemorySize> blockSize = std::nullopt) {
    if (blockSize.has_value()) {
      TestIndexConfig config{kg};
      config.blocksizePermutations = blockSize.value();
      qec_ = getQec(std::move(config));
    } else {
      qec_ = getQec(kg);
    }
  }
};

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_ENGINE_LAZYJOINTESTHELPERS_H
