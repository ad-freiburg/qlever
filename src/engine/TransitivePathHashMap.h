// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//   2025-     Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H

#include <absl/container/inlined_vector.h>

#include <memory>

#include "engine/TransitivePathImpl.h"
#include "engine/idTable/IdTable.h"
#include "util/AllocatorWithLimit.h"

// A set of edges of the implicit graph of a transitive path operation together
// with the graph IRI of the source node of each edge. This is analogous to
// `BinSearchMap` (see there for more details), but uses a map of maps (with
// graph IRI as the outer key and source node as the inner key).
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
  // The map of maps mentioned above.
  MapOfMaps graphMap_;
  // The map of the currently active graph.
  Map* map_;
  // Placeholders to return a reference in case no match was found.
  Set emptySet_;
  Map emptyMap_;

 public:
  // Construct when there is no GRAPH clause.
  HashMapWrapper(Map map, const ad_utility::AllocatorWithLimit<Id>& allocator);

  // Construct when there is a GRAPH clause.
  HashMapWrapper(MapOfMaps graphMap,
                 const ad_utility::AllocatorWithLimit<Id>& allocator);

  // Return all target nodes for the given source node in the currently
  // active graph.
  const Set& successors(Id node) const;

  // This does the same as `BinSearchMap::getEquivalentIdAndMatchingGraphs`
  // (see there for details), but uses a hash map instead of binary search.
  IdWithGraphs getEquivalentIdAndMatchingGraphs(Id node) const;

  // Set `map_` to the map corresponding to the given graph id.
  void setGraphId(Id graphId);
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

#endif
