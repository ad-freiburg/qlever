// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "index/LocatedTriples.h"

#include <algorithm>

#include "absl/strings/str_join.h"
#include "index/CompressedRelation.h"
#include "index/IndexMetaData.h"
#include "index/Permutation.h"

// ____________________________________________________________________________
std::vector<LocatedTriple> LocatedTriple::locateTriplesInPermutation(
    const std::vector<IdTriple<0>>& triples,
    const std::vector<CompressedBlockMetadata>& blockMetadata,
    const std::array<size_t, 3>& keyOrder, bool shouldExist,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  // TODO<qup42> using IdTable as an input here would make it easy to have the
  // input already be permuted.
  vector<LocatedTriple> out;
  out.reserve(triples.size());
  size_t currentBlockIndex;
  for (auto triple : triples) {
    triple = triple.permute(keyOrder);
    currentBlockIndex =
        std::ranges::lower_bound(
            blockMetadata, triple.toPermutedTriple(),
            [&](const CompressedBlockMetadata::PermutedTriple& block,
                const CompressedBlockMetadata::PermutedTriple& triple) {
              return block < triple;
            },
            [](const CompressedBlockMetadata& block) {
              return block.lastTriple_;
            }) -
        blockMetadata.begin();
    out.emplace_back(currentBlockIndex, triple, shouldExist);
    cancellationHandle->throwIfCancelled();
  }

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
template <size_t numIndexColumns>
IdTable LocatedTriplesPerBlock::mergeTriplesImpl(size_t blockIndex,
                                                 const IdTable& block) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  AD_CONTRACT_CHECK(numIndexColumns <= block.numColumns());
  AD_CONTRACT_CHECK(block.numColumns() >= 1);

  auto numInsertsAndDeletes = numTriples(blockIndex);
  IdTable result{block.numColumns(), block.getAllocator()};
  result.resize(block.numRows() + numInsertsAndDeletes.numAdded_);

  const auto& locatedTriples = map_.at(blockIndex);

  auto tieRow = [](auto& row) {
    if constexpr (numIndexColumns == 3) {
      return std::tie(row[0], row[1], row[2]);
    } else if constexpr (numIndexColumns == 2) {
      return std::tie(row[0], row[1]);
    } else {
      return std::tie(row[0]);
    }
  };
  auto tieLT = [](auto lt) {
    if constexpr (numIndexColumns == 3) {
      return std::tie(lt->triple_.ids_[0], lt->triple_.ids_[1],
                      lt->triple_.ids_[2]);
    } else if constexpr (numIndexColumns == 2) {
      return std::tie(lt->triple_.ids_[1], lt->triple_.ids_[2]);
    } else {
      return std::tie(lt->triple_.ids_[2]);
    }
  };
  auto cmpLt = [&tieRow, &tieLT](auto lt, auto row) {
    return tieRow(row) > tieLT(lt);
  };
  auto cmpEq = [&tieRow, &tieLT](auto lt, auto row) {
    return tieRow(row) == tieLT(lt);
  };

  auto row = block.begin();
  auto locatedTriple = locatedTriples.begin();
  auto resultEntry = result.begin();

  auto writeTripleToResult = [&result, &resultEntry](auto& locatedTriple) {
    for (size_t i = 0; i < numIndexColumns; i++) {
      (*resultEntry)[i] = locatedTriple.triple_.ids_[3 - numIndexColumns + i];
    }
    // Write UNDEF to any additional columns.
    for (size_t i = numIndexColumns; i < result.numColumns(); i++) {
      (*resultEntry)[i] = ValueId::makeUndefined();
    }
    resultEntry++;
  };

  while (row != block.end() && locatedTriple != locatedTriples.end()) {
    if (cmpLt(locatedTriple, *row)) {
      // locateTriple < row
      if (locatedTriple->shouldTripleExist_) {
        // Insertion of a non-existent triple.
        writeTripleToResult(*locatedTriple);
      }
      locatedTriple++;
    } else if (cmpEq(locatedTriple, *row)) {
      // locateTriple == row
      if (!locatedTriple->shouldTripleExist_) {
        // Deletion of an existing triple.
        row++;
      }
      locatedTriple++;
    } else {
      // locateTriple > row
      // The row is not deleted - copy it
      *resultEntry++ = *row++;
    }
  }

  if (locatedTriple != locatedTriples.end()) {
    AD_CORRECTNESS_CHECK(row == block.end());
    std::ranges::for_each(
        std::ranges::subrange(locatedTriple, locatedTriples.end()) |
            std::views::filter(&LocatedTriple::shouldTripleExist_),
        writeTripleToResult);
  }
  if (row != block.end()) {
    AD_CORRECTNESS_CHECK(locatedTriple == locatedTriples.end());
    while (row != block.end()) {
      *resultEntry++ = *row++;
    }
  }

  result.resize(resultEntry - result.begin());
  return result;
}

IdTable LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                             const IdTable& block,
                                             size_t numIndexColumns) const {
  if (numIndexColumns == 3) {
    return mergeTriplesImpl<3>(blockIndex, block);
  } else if (numIndexColumns == 2) {
    return mergeTriplesImpl<2>(blockIndex, block);
  } else {
    AD_CORRECTNESS_CHECK(numIndexColumns == 1);
    return mergeTriplesImpl<1>(blockIndex, block);
  }
}

// ____________________________________________________________________________
std::vector<LocatedTriples::iterator> LocatedTriplesPerBlock::add(
    const std::vector<LocatedTriple>& locatedTriples) {
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
  return handles;
};

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
}

void LocatedTriplesPerBlock::updateAugmentedMetadata(
    const std::vector<CompressedBlockMetadata>& metadata) {
  // Copy block metadata to augment it with updated triples.
  IndexMetaDataMmap::BlocksType allBlocks{metadata};
  for (auto it = allBlocks.begin(); it != allBlocks.end(); ++it) {
    size_t blockIndex = it - allBlocks.begin();
    if (hasUpdates(blockIndex)) {
      const auto& block = map_.at(blockIndex);
      it->firstTriple_ =
          std::min(it->firstTriple_, block.begin()->triple_.toPermutedTriple());
      it->lastTriple_ =
          std::max(it->lastTriple_, block.rbegin()->triple_.toPermutedTriple());
    }
  }
  augmentedMetadata_ = allBlocks;
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
