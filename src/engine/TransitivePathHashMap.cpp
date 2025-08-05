// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//   2025-     Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/TransitivePathHashMap.h"

#include <memory>

#include "engine/CallFixedSize.h"
#include "engine/TransitivePathBase.h"

// _____________________________________________________________________________
HashMapWrapper::HashMapWrapper(
    Map map, const ad_utility::AllocatorWithLimit<Id>& allocator)
    : graphMap_{allocator},
      map_{nullptr},
      emptySet_{allocator},
      emptyMap_{allocator} {
  graphMap_.try_emplace(Id::makeUndefined(), std::move(map));
  map_ = &graphMap_.at(Id::makeUndefined());
}

// _____________________________________________________________________________
HashMapWrapper::HashMapWrapper(
    MapOfMaps graphMap, const ad_utility::AllocatorWithLimit<Id>& allocator)
    : graphMap_{std::move(graphMap)},
      map_{&emptyMap_},
      emptySet_{allocator},
      emptyMap_{allocator} {}

// _____________________________________________________________________________
const Set& HashMapWrapper::successors(const Id node) const {
  auto iterator = map_->find(node);
  return iterator == map_->end() ? emptySet_ : iterator->second;
}

// _____________________________________________________________________________
IdWithGraphs HashMapWrapper::getEquivalentIdAndMatchingGraphs(Id node) const {
  IdWithGraphs result;
  for (const auto& [graph, map] : graphMap_) {
    if (node.isUndefined()) {
      for (Id newId : map | ql::views::keys) {
        result.emplace_back(newId, graph);
      }
    } else {
      auto iterator = map.find(node);
      if (iterator != map.end()) {
        result.emplace_back(iterator->first, graph);
      }
    }
  }
  return result;
}

// _____________________________________________________________________________
void HashMapWrapper::setGraphId(Id graphId) {
  AD_CORRECTNESS_CHECK(!graphId.isUndefined() || graphMap_.size() == 1);
  if (graphMap_.contains(graphId)) {
    map_ = &graphMap_.at(graphId);
  } else {
    map_ = &emptyMap_;
  }
}

// _____________________________________________________________________________
HashMapWrapper TransitivePathHashMap::setupEdgesMap(
    const IdTable& sub, const TransitivePathSide& startSide,
    const TransitivePathSide& targetSide) const {
  decltype(auto) startCol = sub.getColumn(startSide.subCol_);
  decltype(auto) targetCol = sub.getColumn(targetSide.subCol_);
  AD_CORRECTNESS_CHECK(!graphVariable_.has_value());
  HashMapWrapper::Map edges{allocator()};

  for (size_t i = 0; i < sub.size(); i++) {
    checkCancellation();
    auto [it, _] = edges.try_emplace(startCol[i], allocator());
    it->second.insert(targetCol[i]);
  }
  return HashMapWrapper{std::move(edges), allocator()};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> TransitivePathHashMap::cloneImpl() const {
  auto copy = std::make_unique<TransitivePathHashMap>(*this);
  copy->subtree_ = subtree_->clone();
  copy->lhs_ = lhs_.clone();
  copy->rhs_ = rhs_.clone();
  return copy;
}
