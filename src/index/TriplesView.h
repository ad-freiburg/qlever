#pragma once

#include <utility>
#include <vector>

#include "../global/Id.h"
#include "../util/AllocatorWithLimit.h"
#include "../util/File.h"
#include "../util/Generator.h"
#include "CompressedRelation.h"

namespace detail {
using IgnoredRelations = std::vector<std::pair<Id, Id>>;
inline auto alwaysReturnFalse = [](auto&&...) { return false; };
}  // namespace detail

/**
 * @brief Yield all triples from a given permutations
 * @param permutation
 * @param allocator The current implementation reads complete relations into
 *        memory. The `allocator` is used  to allocate the needed buffers for
 *        this.
 * @param ignoredRelations For each pair (a, b) in `ignoredRelations` a triple
 *         will not be yielded if a <= triple[0] < b. Specifying contiguous
 *         ranges of ignored relations is more efficient than the
 * `isTripleIgnored` callable (see below), because the ignored relations will
 * never be read from disk. If the different pairs overlap, the behavior is
 *         undefined.
 * @param isTripleIgnored For each triple `isTripleIgnored(triple)` is called.
 *        The triple is only yielded, if the result of this call is `false` ̇.
 */
template <typename IsTripleIgnored = decltype(detail::alwaysReturnFalse)>
cppcoro::generator<std::array<Id, 3>> TriplesView(
    const auto& permutation, ad_utility::AllocatorWithLimit<Id> allocator,
    detail::IgnoredRelations ignoredRelations = {},
    IsTripleIgnored isTripleIgnored = IsTripleIgnored{}) {
  std::sort(ignoredRelations.begin(), ignoredRelations.end());

  const auto& metaData = permutation._meta.data();
  // The argument specifies the ignored relations, but we need to know which
  // relations have to be yielded (the inverse).
  using Iterator = std::decay_t<decltype(metaData.ordered_begin())>;
  std::vector<std::pair<Iterator, Iterator>> nonIgnoredRanges;

  // Add sentinels
  ignoredRelations.insert(ignoredRelations.begin(), {0, 0});
  ignoredRelations.insert(
      ignoredRelations.end(),
      {std::numeric_limits<Id>::max(), std::numeric_limits<Id>::max()});
  auto orderedBegin = metaData.ordered_begin();
  auto orderedEnd = metaData.ordered_end();

  // Convert the `ignoredRelations` to the `nonIgnoredRanges`. The algorithm
  // works because of the sentinels.
  for (auto it = ignoredRelations.begin(); it != ignoredRelations.end() - 1;
       ++it) {
    auto beginOfAllowed = std::lower_bound(
        orderedBegin, orderedEnd, it->second,
        [](const auto& meta, const auto& id) {
          return decltype(orderedBegin)::getIdFromElement(meta) < id;
        });
    auto endOfAllowed = std::lower_bound(
        orderedBegin, orderedEnd, (it + 1)->first,
        [](const auto& meta, const auto& id) {
          return decltype(orderedBegin)::getIdFromElement(meta) < id;
        });
    nonIgnoredRanges.emplace_back(beginOfAllowed, endOfAllowed);
  }

  // Currently complete relations are yielded at once. This might take a lot of
  // space for certain predicates in the Pxx permutations, so we respect the
  // specified memory limit.
  // TODO<joka921> Implement the scanning of large relations lazily and in
  // blocks, making the limit here unnecessary.
  using Tuple = std::array<Id, 2>;
  auto tupleAllocator = allocator.as<Tuple>();
  std::vector<Tuple, ad_utility::AllocatorWithLimit<Tuple>> col2And3{
      tupleAllocator};
  for (auto& [it, end] : nonIgnoredRanges) {
    for (; it != end; ++it) {
      col2And3.clear();
      uint64_t id = it.getId();
      // TODO<joka921> We could also pass a timeout pointer here.
      permutation.scan(id, &col2And3);
      for (const auto& [col2, col3] : col2And3) {
        std::array<Id, 3> triple{id, col2, col3};
        if (isTripleIgnored(triple)) [[unlikely]] {
          continue;
        }
        co_yield triple;
      }
    }
  }
}
