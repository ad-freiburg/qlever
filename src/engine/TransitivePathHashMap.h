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
class HashMapWrapper {
 public:
  // We deliberately use the `std::` variants of a hash map because `absl`s
  // types are not exception safe.
  using Map = std::unordered_map<
      Id, Set, absl::Hash<Id>, std::equal_to<Id>,
      ad_utility::AllocatorWithLimit<std::pair<const Id, Set>>>;
  using MapOfMaps = std::unordered_map<
      Id, Map, absl::Hash<Id>, std::equal_to<Id>,
      ad_utility::AllocatorWithLimit<std::pair<const Id, Map>>>;

 private:
  MapOfMaps graphMap_;
  Map* map_;
  Set emptySet_;
  Map emptyMap_;

 public:
  // Constructor with no graph column.
  HashMapWrapper(Map map, const ad_utility::AllocatorWithLimit<Id>& allocator);

  // Constructor with graph column.
  HashMapWrapper(MapOfMaps graphMap,
                 const ad_utility::AllocatorWithLimit<Id>& allocator);

  /**
   * @brief Return the successors for the given Id. The successors are all ids,
   * which are stored under the key 'node'
   *
   * @param node The input id
   * @return A const Set&, consisting of all target ids which have an ingoing
   * edge from 'node'
   */
  const Set& successors(const Id node) const;

  // Return equivalent ids from the index, along with an associated graph id in
  // case these are available.
  std::vector<std::pair<Id, Id>> getEquivalentIds(Id node) const;

  // Prefilter the map for values of a certain graph.
  void setGraphId(const Id& graphId);
};

/**
 * @class TransitivePathHashMap
 * @brief This class implements the transitive path operation. The
 * implementation uses a hash map to represent the graph and find successors
 * of given nodes.
 */
class TransitivePathHashMap : public TransitivePathImpl<HashMapWrapper> {
 public:
  using TransitivePathImpl::TransitivePathImpl;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  // Initialize the map from the subresult.
  HashMapWrapper setupEdgesMap(
      const IdTable& sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide) const override;
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H
