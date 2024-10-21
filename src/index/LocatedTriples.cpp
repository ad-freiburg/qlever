// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "index/LocatedTriples.h"

#include <algorithm>

#include "absl/strings/str_join.h"
#include "index/CompressedRelation.h"
#include "util/ChunkedForLoop.h"

// ____________________________________________________________________________
std::vector<LocatedTriple> LocatedTriple::locateTriplesInPermutation(
    std::span<const IdTriple<0>> triples,
    std::span<const CompressedBlockMetadata> blockMetadata,
    const std::array<size_t, 3>& keyOrder, bool shouldExist,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  std::vector<LocatedTriple> out;
  out.reserve(triples.size());
  ad_utility::chunkedForLoop<10'000>(
      0, triples.size(),
      [&triples, &out, &blockMetadata, &keyOrder, &shouldExist](size_t i) {
        auto triple = triples[i].permute(keyOrder);
        // A triple belongs to the first block that contains at least one triple
        // that larger than or equal to the triple. See `LocatedTriples.h` for a
        // discussion of the corner cases.
        size_t blockIndex =
            std::ranges::lower_bound(blockMetadata, triple.toPermutedTriple(),
                                     std::less<>{},
                                     &CompressedBlockMetadata::lastTriple_) -
            blockMetadata.begin();
        out.emplace_back(blockIndex, triple, shouldExist);
      },
      [&cancellationHandle]() { cancellationHandle->throwIfCancelled(); });

  return out;
}

// ____________________________________________________________________________
bool LocatedTriplesPerBlock::hasUpdates(size_t blockIndex) const {
  return map_.contains(blockIndex);
}

// ____________________________________________________________________________
NumAddedAndDeleted LocatedTriplesPerBlock::numTriples(size_t blockIndex) const {
  // If no located triples for `blockIndex_` exist, there is no entry in `map_`.
  if (!hasUpdates(blockIndex)) {
    return {0, 0};
  }

  auto blockUpdateTriples = map_.at(blockIndex);
  size_t countInserts = std::ranges::count_if(
      blockUpdateTriples, &LocatedTriple::shouldTripleExist_);
  return {countInserts, blockUpdateTriples.size() - countInserts};
}

// ____________________________________________________________________________
// Collect the relevant entries of a LocatedTriple into a triple.
template <size_t numIndexColumns, bool includeGraphColumn>
requires(numIndexColumns >= 1 && numIndexColumns <= IdTriple<0>::NumCols)
auto tieIdTableRow(auto& row) {
  return [&row]<size_t... I>(std::index_sequence<I...>) {
    return std::tie(row[I]...);
  }(std::make_index_sequence<numIndexColumns +
                             static_cast<size_t>(includeGraphColumn)>{});
}

// ____________________________________________________________________________
// Collect the relevant entries of a LocatedTriple into a triple.
template <size_t numIndexColumns, bool includeGraphColumn>
requires(numIndexColumns >= 1 && numIndexColumns <= 3)
auto tieLocatedTriple(auto& lt) {
  constexpr auto indices = []() {
    std::array<size_t,
               numIndexColumns + static_cast<size_t>(includeGraphColumn)>
        a;
    for (size_t i = 0; i < numIndexColumns; ++i) {
      a[i] = 3 - numIndexColumns + i;
    }
    if (includeGraphColumn) {
      // The graph column resides at index `3` of the located triple.
      a.back() = 3;
    }
    return a;
  }();
  auto& ids = lt->triple_.ids_;
  return [&ids]<size_t... I>(ad_utility::ValueSequence<size_t, I...>) {
    return std::tie(ids[I]...);
  }(ad_utility::toIntegerSequence<indices>());
}

// ____________________________________________________________________________
template <size_t numIndexColumns, bool includeGraphColumn>
IdTable LocatedTriplesPerBlock::mergeTriplesImpl(size_t blockIndex,
                                                 const IdTable& block) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  AD_CONTRACT_CHECK(numIndexColumns + static_cast<size_t>(includeGraphColumn) <=
                    block.numColumns());

  auto numInsertsAndDeletes = numTriples(blockIndex);
  IdTable result{block.numColumns(), block.getAllocator()};
  result.resize(block.numRows() + numInsertsAndDeletes.numAdded_);

  const auto& locatedTriples = map_.at(blockIndex);

  auto lessThan = [](const auto& lt, const auto& row) {
    return tieLocatedTriple<numIndexColumns, includeGraphColumn>(lt) <
           tieIdTableRow<numIndexColumns, includeGraphColumn>(row);
  };
  auto equal = [](const auto& lt, const auto& row) {
    return tieLocatedTriple<numIndexColumns, includeGraphColumn>(lt) ==
           tieIdTableRow<numIndexColumns, includeGraphColumn>(row);
  };

  auto rowIt = block.begin();
  auto locatedTripleIt = locatedTriples.begin();
  auto resultIt = result.begin();

  // Write the given `locatedTriple` to `result` at position `resultIt` and
  // advance.
  auto writeTripleToResult = [&result, &resultIt](auto& locatedTriple) {
    // Write part from `locatedTriple` that also occurs in the input `block` to
    // the result.
    //
    // TODO: According to `mergeTriples`, `numIndexColumns` can also be 4.
    // However, this would crash with the following code, as `ids_[3 -
    // numIndexColumns + i]` would access `ids_[-1]`. Either `numIndexColumns`
    // cannot be 4 or the following code needs to be adjusted.
    static constexpr auto graphOffset = static_cast<size_t>(includeGraphColumn);
    for (size_t i = 0; i < numIndexColumns + graphOffset; i++) {
      (*resultIt)[i] = locatedTriple.triple_.ids_[3 - numIndexColumns + i];
    }
    // Write UNDEF to any additional columns.
    for (size_t i = numIndexColumns + graphOffset; i < result.numColumns();
         i++) {
      (*resultIt)[i] = ValueId::makeUndefined();
    }
    resultIt++;
  };

  while (rowIt != block.end() && locatedTripleIt != locatedTriples.end()) {
    if (lessThan(locatedTripleIt, *rowIt)) {
      if (locatedTripleIt->shouldTripleExist_) {
        // Insertion of a non-existent triple.
        writeTripleToResult(*locatedTripleIt);
      }
      locatedTripleIt++;
    } else if (equal(locatedTripleIt, *rowIt)) {
      if (!locatedTripleIt->shouldTripleExist_) {
        // Deletion of an existing triple.
        rowIt++;
      }
      locatedTripleIt++;
    } else {
      // The rowIt is not deleted - copy it
      *resultIt++ = *rowIt++;
    }
  }

  if (locatedTripleIt != locatedTriples.end()) {
    AD_CORRECTNESS_CHECK(rowIt == block.end());
    std::ranges::for_each(
        std::ranges::subrange(locatedTripleIt, locatedTriples.end()) |
            std::views::filter(&LocatedTriple::shouldTripleExist_),
        writeTripleToResult);
  }
  if (rowIt != block.end()) {
    AD_CORRECTNESS_CHECK(locatedTripleIt == locatedTriples.end());
    while (rowIt != block.end()) {
      *resultIt++ = *rowIt++;
    }
  }

  result.resize(resultIt - result.begin());
  return result;
}

// ____________________________________________________________________________
IdTable LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                             const IdTable& block,
                                             size_t numIndexColumns,
                                             bool includeGraphColumn) const {
  auto applyToGraphColumn = [&]<bool hasGraphColumn>() {
    if (numIndexColumns == 3) {
      return mergeTriplesImpl<3, hasGraphColumn>(blockIndex, block);
    } else if (numIndexColumns == 2) {
      return mergeTriplesImpl<2, hasGraphColumn>(blockIndex, block);
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return mergeTriplesImpl<1, hasGraphColumn>(blockIndex, block);
    }
  };
  if (includeGraphColumn) {
    return applyToGraphColumn.template operator()<true>();
  } else {
    return applyToGraphColumn.template operator()<false>();
  }
}

// ____________________________________________________________________________
std::vector<LocatedTriples::iterator> LocatedTriplesPerBlock::add(
    std::span<const LocatedTriple> locatedTriples) {
  std::vector<LocatedTriples::iterator> handles;
  handles.reserve(locatedTriples.size());
  for (auto triple : locatedTriples) {
    LocatedTriples& locatedTriplesInBlock = map_[triple.blockIndex_];
    auto [handle, wasInserted] = locatedTriplesInBlock.emplace(triple);
    AD_CORRECTNESS_CHECK(wasInserted == true);
    AD_CORRECTNESS_CHECK(handle != locatedTriplesInBlock.end());
    ++numTriples_;
    handles.emplace_back(handle);
  }

  updateAugmentedMetadata();

  return handles;
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::erase(size_t blockIndex,
                                   LocatedTriples::iterator iter) {
  auto blockIter = map_.find(blockIndex);
  AD_CONTRACT_CHECK(blockIter != map_.end(), "Block ", blockIndex,
                    " is not contained.");
  auto& block = blockIter->second;
  block.erase(iter);
  numTriples_--;
  if (block.empty()) {
    map_.erase(blockIndex);
  }
  updateAugmentedMetadata();
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::setOriginalMetadata(
    std::vector<CompressedBlockMetadata> metadata) {
  originalMetadata_ = std::move(metadata);
  updateAugmentedMetadata();
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::updateAugmentedMetadata() {
  // TODO<C++23> use view::enumerate
  size_t blockIndex = 0;
  // Copy to preserve originalMetadata_.
  augmentedMetadata_ = originalMetadata_;
  for (auto& blockMetadata : augmentedMetadata_.value()) {
    if (hasUpdates(blockIndex)) {
      const auto& blockUpdates = map_.at(blockIndex);
      blockMetadata.firstTriple_ =
          std::min(blockMetadata.firstTriple_,
                   blockUpdates.begin()->triple_.toPermutedTriple());
      blockMetadata.lastTriple_ =
          std::max(blockMetadata.lastTriple_,
                   blockUpdates.rbegin()->triple_.toPermutedTriple());
    }
    blockIndex++;
  }
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts) {
  os << "{ ";
  std::ranges::copy(lts, std::ostream_iterator<LocatedTriple>(os, " "));
  os << "}";
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const std::vector<IdTriple<0>>& v) {
  std::ranges::copy(v, std::ostream_iterator<IdTriple<0>>(os, ", "));
  return os;
}
