#pragma once

#include <utility>
#include <vector>

#include "CompressedRelation.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
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
    const auto& permutation,
    ad_utility::SharedCancellationHandle cancellationHandle,
    detail::IgnoredRelations ignoredRanges = {},
    IsTripleIgnored isTripleIgnored = IsTripleIgnored{}) {
  std::ranges::sort(ignoredRanges);

  auto blockGenerator = permutation.lazyScan(
      {std::nullopt, std::nullopt, std::nullopt}, std::nullopt,
      CompressedRelationReader::ColumnIndices{}, cancellationHandle);
  auto ignoreIt = ignoredRanges.begin();
  Id ignoreLow = Id::max();
  Id ignoreHigh = Id::max();
  if (ignoreIt != ignoredRanges.end()) {
    std::tie(ignoreLow, ignoreHigh) = *ignoreIt;
    ++ignoreIt;
  }
  for (const IdTable& cols : blockGenerator) {
    AD_CORRECTNESS_CHECK(cols.numColumns() == 3);
    for (const auto& row : cols) {
      // TODO<joka921> This can be done much more efficient without copying etc.
      // and with prefiltering.
      std::array<Id, 3> triple{row[0], row[1], row[2]};
      if (isTripleIgnored(triple)) [[unlikely]] {
        continue;
      }
      if (triple[0] > ignoreHigh) {
        if (ignoreIt != ignoredRanges.end()) {
          std::tie(ignoreLow, ignoreHigh) = *ignoreIt;
          ++ignoreIt;
        } else {
          ignoreLow = Id::max();
          ignoreHigh = Id::max();
        }
      }
      if (triple[0] >= ignoreLow && triple[0] < ignoreHigh) {
        continue;
      }
      co_yield triple;
    }
    cancellationHandle->throwIfCancelled();
  }
}
