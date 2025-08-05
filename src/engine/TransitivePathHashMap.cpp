// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathHashMap.h"

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
  if (iterator == map_->end()) {
    return emptySet_;
  }
  return iterator->second;
}

// _____________________________________________________________________________
absl::InlinedVector<std::pair<Id, Id>, 1>
HashMapWrapper::getEquivalentIdAndMatchingGraphs(Id node) const {
  absl::InlinedVector<std::pair<Id, Id>, 1> result;
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
  if (graphVariable_.has_value()) {
    decltype(auto) graphCol =
        sub.getColumn(subtree_->getVariableColumn(graphVariable_.value()));
    HashMapWrapper::MapOfMaps edgesWithGraph{allocator()};
    for (size_t i = 0; i < sub.size(); i++) {
      checkCancellation();
      auto it1 = edgesWithGraph.try_emplace(graphCol[i], allocator()).first;
      auto it2 = it1->second.try_emplace(startCol[i], allocator()).first;
      it2->second.insert(targetCol[i]);
    }
    return HashMapWrapper{std::move(edgesWithGraph), allocator()};
  }
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
