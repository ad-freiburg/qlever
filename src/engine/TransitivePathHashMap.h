// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H

#include <memory>

#include "engine/TransitivePathImpl.h"
#include "engine/idTable/IdTable.h"
#include "util/AllocatorWithLimit.h"

/**
 * @class HashMapWrapper
 * @brief A wrapper for the Map class from TransitivePathBase. This wrapper
 * implements the successors function, which is used in transitiveHull function.
 *
 */
struct HashMapWrapper {
  Map map_;
  Set emptySet_;

  HashMapWrapper(Map map, const ad_utility::AllocatorWithLimit<Id>& allocator)
      : map_(std::move(map)), emptySet_(allocator) {}

  /**
   * @brief Return the successors for the given Id. The successors are all ids,
   * which are stored under the key 'node'
   *
   * @param node The input id
   * @return A const Set&, consisting of all target ids which have an ingoing
   * edge from 'node'
   */
  const auto& successors(const Id node) const {
    auto iterator = map_.find(node);
    if (iterator == map_.end()) {
      return emptySet_;
    }
    return iterator->second;
  }

  // Retrieve pointer to equal id from `map_`, or nullptr if not present.
  // This is used to get `Id`s that do do not depend on a specific `LocalVocab`,
  // but instead are backed by the index.
  const Id* getEquivalentId(Id node) const {
    auto iterator = map_.find(node);
    if (iterator == map_.end()) {
      return nullptr;
    }
    return &iterator->first;
  }
};

/**
 * @class TransitivePathHashMap
 * @brief This class implements the transitive path operation. The
 * implementation uses a hash map to represent the graph and find successors
 * of given nodes.
 *
 *
 */
class TransitivePathHashMap : public TransitivePathImpl<HashMapWrapper> {
 public:
  using TransitivePathImpl::TransitivePathImpl;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  // initialize the map from the subresult
  HashMapWrapper setupEdgesMap(
      const IdTable& dynSub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const override;

  template <size_t SUB_WIDTH>
  HashMapWrapper setupEdgesMap(const IdTable& dynSub,
                               const TransitivePathSide& startSide,
                               const TransitivePathSide& targetSide) const;
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H
