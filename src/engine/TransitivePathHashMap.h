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
  // We deliberately use the `std::` variants of a hash map because `absl`s
  // types are not exception safe.
  using Map = std::unordered_map<
      Id, Set, absl::Hash<Id>, std::equal_to<Id>,
      ad_utility::AllocatorWithLimit<std::pair<const Id, Set>>>;
  using MapOfMaps = std::unordered_map<
      Id, Map, absl::Hash<Id>, std::equal_to<Id>,
      ad_utility::AllocatorWithLimit<std::pair<const Id, Map>>>;

  MapOfMaps graphMap_;
  Map* map_;
  Set emptySet_;
  Map emptyMap_;

  // Constructor with no graph column.
  HashMapWrapper(Map map, const ad_utility::AllocatorWithLimit<Id>& allocator)
      : graphMap_{allocator},
        map_{nullptr},
        emptySet_{allocator},
        emptyMap_{allocator} {
    graphMap_.emplace(Id::makeUndefined(), std::move(map));
    map_ = &graphMap_.at(Id::makeUndefined());
  }

  // Constructor with graph column.
  HashMapWrapper(MapOfMaps graphMap,
                 const ad_utility::AllocatorWithLimit<Id>& allocator)
      : graphMap_{std::move(graphMap)},
        map_{&emptyMap_},
        emptySet_{allocator},
        emptyMap_{allocator} {}

  /**
   * @brief Return the successors for the given Id. The successors are all ids,
   * which are stored under the key 'node'
   *
   * @param node The input id
   * @return A const Set&, consisting of all target ids which have an ingoing
   * edge from 'node'
   */
  const auto& successors(const Id node) const {
    auto iterator = map_->find(node);
    if (iterator == map_->end()) {
      return emptySet_;
    }
    return iterator->second;
  }

  // Return equivalent ids from the index, along with an associated graph id in
  // case these are available.
  std::vector<std::pair<Id, Id>> getEquivalentIds(Id node) const {
    std::vector<std::pair<Id, Id>> result;
    for (const auto& [graph, map] : graphMap_) {
      auto iterator = map.find(node);
      if (iterator != map.end()) {
        result.emplace_back(iterator->first, graph);
      }
    }
    return result;
  }

  // Prefilter the map for values of a certain graph.
  void setGraphId(const Id& graphId) {
    AD_CORRECTNESS_CHECK(graphId.isUndefined() || graphMap_.size() == 1);
    if (graphMap_.contains(graphId)) {
      map_ = &graphMap_.at(graphId);
    } else {
      map_ = &emptyMap_;
    }
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
