#pragma once

#include "../global/Id.h"
#include "../util/File.h"
#include "../util/Generator.h"
#include "../util/AllocatorWithLimit.h"
#include "CompressedRelation.h"
#include <vector>
#include <utility>

/**
 * This allows iterating over one of the permutations of the index once.
 **/
 namespace detail {
 using IgnoredRelations = std::vector<std::pair<Id, Id>>;
 inline auto alwaysReturnFalse = [](auto&&...) {return false;};
 }

 template<typename IgnoreTriple = decltype(detail::alwaysReturnFalse)>
cppcoro::generator<std::array<Id, 3>> TriplesView(const auto& permutation, ad_utility::AllocatorWithLimit<Id> allocator, detail::IgnoredRelations ignoredRelations = {}, IgnoreTriple ignoreTriple = IgnoreTriple{}) {
  std::sort(ignoredRelations.begin(), ignoredRelations.end());

  const auto& metaData = permutation._meta.data();
  using It = std::decay_t<decltype(metaData.ordered_begin())>;
  std::vector<std::pair<It, It>> iterators;
  ignoredRelations.insert(ignoredRelations.begin(), {0, 0});
  ignoredRelations.insert(ignoredRelations.end(), {std::numeric_limits<Id>::max(), std::numeric_limits<Id>::max()});
  auto orderedBegin = metaData.ordered_begin();
  auto orderedEnd = metaData.ordered_end();
  for (const auto& [a, b]  :ignoredRelations) {
    LOG(INFO) << "ignored Relations:" << a << " " << b << std::endl;
  }
  for (auto it = ignoredRelations.begin(); it != ignoredRelations.end() - 1; ++it) {
    auto beginOfAllowed = std::lower_bound(orderedBegin, orderedEnd, it->second, [](const auto& meta, const auto& id) {
      return decltype(orderedBegin)::getIdFromElement(meta) < id;
    });
    LOG(INFO) << "BeginAndEndPair" << std::endl;
    LOG(INFO) << beginOfAllowed.getId()  <<std::endl;
    auto endOfAllowed = std::lower_bound(orderedBegin, orderedEnd, (it+1)->first, [](const auto& meta, const auto& id) {
      return decltype(orderedBegin)::getIdFromElement(meta) < id;
    });
    if (endOfAllowed != orderedEnd) {
      LOG(INFO) << endOfAllowed.getId() << std::endl;
    } else {
      LOG(INFO) << Id(-1) << std::endl;
    }
    iterators.emplace_back(beginOfAllowed, endOfAllowed);
  }

  using Tuple = std::array<Id, 2>;
  auto tupleAllocator = allocator.as<Tuple>();
  std::vector<Tuple, ad_utility::AllocatorWithLimit<Tuple>> col2And3{tupleAllocator};
  for (auto& [it, end]: iterators) {
    for (; it != end; ++it) {
      col2And3.clear();
      uint64_t id = it.getId();
      CompressedRelationMetaData::scan(id, &col2And3, permutation);
      for (const auto& [col2, col3] : col2And3) {
        std::array<Id, 3> triple{id, col2, col3};
        if (ignoreTriple(triple)) [[unlikely]]{
          continue;
        }
        co_yield triple;
      }
    }
  }
}
