#pragma once

#include <utility>
#include <vector>

#include "CompressedRelation.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/File.h"
#include "util/Generator.h"

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
 * @param ignoredRanges For each range (a, b) in `ignoredRanges` a triple
 *        will not be yielded if a <= triple[0] < b. Specifying contiguous
 *        ranges of ignored relations is more efficient than the
 *        `isTripleIgnored` callable (see below), because the ignored relations
 *        will never be read from disk. If the different pairs overlap, the
 *        behavior is undefined.
 * @param isTripleIgnored For each triple `isTripleIgnored(triple)` is called.
 *        The triple is only yielded, if the result of this call is `false` ̇.
 */
template <typename IsTripleIgnored = decltype(detail::alwaysReturnFalse)>
cppcoro::generator<std::array<Id, 3>> TriplesView(
    const auto& permutation, ad_utility::AllocatorWithLimit<Id> allocator,
    detail::IgnoredRelations ignoredRanges = {},
    IsTripleIgnored isTripleIgnored = IsTripleIgnored{}) {
  std::sort(ignoredRanges.begin(), ignoredRanges.end());

  const auto& metaData = permutation.meta_.data();
  // The argument `ignoredRanges` specifies the ignored ranges, but we need to
  // compute the ranges that are allowed (the inverse).
  using Iterator = std::decay_t<decltype(metaData.ordered_begin())>;
  std::vector<std::pair<Iterator, Iterator>> allowedRanges;

  // Add sentinels.
  // TODO<joka921> implement Index::prefixRange with all the logic.
  ignoredRanges.insert(ignoredRanges.begin(), {Id::min(), Id::min()});
  ignoredRanges.insert(ignoredRanges.end(), {Id::max(), Id::max()});
  auto orderedBegin = metaData.ordered_begin();
  auto orderedEnd = metaData.ordered_end();

  // Convert the `ignoredRanges` to the `allowedRanges`. The algorithm
  // works because of the sentinels.
  for (auto it = ignoredRanges.begin(); it != ignoredRanges.end() - 1; ++it) {
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
    allowedRanges.emplace_back(beginOfAllowed, endOfAllowed);
  }

  // Currently complete relations are yielded at once. This might take a lot of
  // space for certain predicates in the Pxx permutations, so we respect the
  // specified memory limit.
  // TODO<joka921> Implement the scanning of large relations lazily and in
  // blocks, making the limit here unnecessary.
  using Tuple = std::array<Id, 2>;
  auto tupleAllocator = allocator.as<Tuple>();
  IdTable col1And2{2, allocator};
  for (auto& [begin, end] : allowedRanges) {
    for (auto it = begin; it != end; ++it) {
      col1And2.clear();
      Id id = it.getId();
      // TODO<joka921> We could also pass a timeout pointer here.
      permutation.scan(id, &col1And2);
      for (const auto& row : col1And2) {
        std::array<Id, 3> triple{id, row[0], row[1]};
        if (isTripleIgnored(triple)) [[unlikely]] {
          continue;
        }
        co_yield triple;
      }
    }
  }
}
