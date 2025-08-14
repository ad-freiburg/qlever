// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//   2025-     Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHHASHMAP_H

#include <absl/container/inlined_vector.h>

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
  // Maps graph id -> (id -> set(id)), where the value type `Map` represents an
  // adjacency list mapping.
  MapOfMaps graphMap_;
  // Pointer to the map selected by the currently active graph.
  Map* map_;
  // Placeholders to return a reference in case no match was found.
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
   * which are stored under the key 'node'. Only values matching the active
   * graph (set via `setGraphId`) will be returned.
   *
   * @param node The input id
   * @return A const Set&, consisting of all target ids which have an ingoing
   * edge from 'node'
   */
  const Set& successors(Id node) const;

  // Return a pair of matching ids + graph ids. If `node` originates from a
  // `LocalVocab` an equivalent entry from the graph is used instead,
  // eliminating the need to keep the `LocalVocab` around any longer. If no
  // entry matches an empty vector is returned. The first id of the pair is
  // always the same element. It is flattened out because all callers of this
  // function need it in this format for convenience. The most common case is
  // that there's a single matching entry, (especially when using this without
  // an active graph,) which is why `absl::InlinedVector` is used with size 1.
  //  If `node` is undefined, it
  // will return all elements in the currently active graph, or all elements if
  // no graph is set. Active graphs set via `setGraphId` are ignored. Entries
  // are deduplicated.
  IdWithGraphs getEquivalentIdAndMatchingGraphs(Id node) const;

  // Prefilter the map for values of a certain graph. If graphs are active, i.e.
  // `graphMap_` doesn't contain the UNDEF key, this has to be set before
  // calling `successors`.
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
