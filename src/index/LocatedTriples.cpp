// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/LocatedTriples.h"

#include <algorithm>

#include "index/CompressedRelation.h"
#include "index/IndexMetaData.h"
#include "index/Permutations.h"

// ____________________________________________________________________________
template <LocatedTriplesPerBlock::MatchMode matchMode>
std::pair<size_t, size_t> LocatedTriplesPerBlock::numTriplesImpl(
    size_t blockIndex, Id id1, Id id2) const {
  // If no located triples for `blockIndex` exist, there are no delta triples
  // for that block.
  if (!map_.contains(blockIndex)) {
    return {0, 0};
  }

  // Otherwise iterate over all entries and count.
  size_t countInserted = 0;
  size_t countDeleted = 0;
  for (const LocatedTriple& locatedTriple : map_.at(blockIndex)) {
    // Helper lambda for increasing the right counter.
    auto increaseCountIf = [&](bool increase) {
      if (increase) {
        if (locatedTriple.existsInIndex) {
          ++countDeleted;
        } else {
          ++countInserted;
        }
      }
    };
    // Increase depending on the mode.
    if constexpr (matchMode == MatchMode::MatchAll) {
      increaseCountIf(true);
    } else if constexpr (matchMode == MatchMode::MatchId1) {
      increaseCountIf(locatedTriple.id1 == id1);
    } else if constexpr (matchMode == MatchMode::MatchId1AndId2) {
      increaseCountIf(locatedTriple.id1 == id1 && locatedTriple.id2 == id2);
    }
  }
  return {countInserted, countDeleted};
}

// ____________________________________________________________________________
std::pair<size_t, size_t> LocatedTriplesPerBlock::numTriples(
    size_t blockIndex) const {
  return numTriplesImpl<MatchMode::MatchAll>(blockIndex);
}

// ____________________________________________________________________________
std::pair<size_t, size_t> LocatedTriplesPerBlock::numTriples(size_t blockIndex,
                                                             Id id1) const {
  return numTriplesImpl<MatchMode::MatchId1>(blockIndex, id1);
}

// ____________________________________________________________________________
std::pair<size_t, size_t> LocatedTriplesPerBlock::numTriples(size_t blockIndex,
                                                             Id id1,
                                                             Id id2) const {
  return numTriplesImpl<MatchMode::MatchId1AndId2>(blockIndex, id1, id2);
}

// ____________________________________________________________________________
template <LocatedTriplesPerBlock::MatchMode matchMode>
void LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                          const IdTable& block, IdTable& result,
                                          size_t offsetInResult,
                                          size_t offsetOfBlock, Id id1,
                                          Id id2) const {
  // This method should only be called, if located triples in that block exist
  // and for blocks with one or two columns.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));
  AD_CONTRACT_CHECK(block.numColumns() == 1 || block.numColumns() == 2);

  AD_CONTRACT_CHECK(block.numColumns() == result.numColumns());
  auto resultEntry = result.begin() + offsetInResult;
  const auto& locatedTriples = map_.at(blockIndex);
  auto locatedTriple = locatedTriples.begin();

  // Helper lambda that checks whether the given located triple should be
  // considered, given the `matchMode`.
  auto locatedTripleMatches = [&]() {
    if constexpr (matchMode == MatchMode::MatchAll) {
      return true;
    } else if constexpr (matchMode == MatchMode::MatchId1) {
      return locatedTriple->id1 == id1;
    } else if constexpr (matchMode == MatchMode::MatchId1AndId2) {
      return locatedTriple->id1 == id1 && locatedTriple->id2 == id2;
    }
  };

  // Skip located triples that come before `offsetOfBlock` because this may be a
  // partial block.
  while (locatedTriple != locatedTriples.end() &&
         locatedTriple->rowIndexInBlock < offsetOfBlock) {
    ++locatedTriple;
  }

  // Iterate over the input block. Keep track of the row index, which is
  // `offsetInBlock` for the first element of the block.
  size_t rowIndex = offsetOfBlock;
  for (const auto& blockEntry : block) {
    // Append triples that are marked for insertion at this position to the
    // result.
    while (locatedTriple != locatedTriples.end() &&
           locatedTriple->rowIndexInBlock == rowIndex &&
           locatedTriple->existsInIndex == false) {
      if (locatedTripleMatches()) {
        if (result.numColumns() == 2) {
          (*resultEntry)[0] = locatedTriple->id2;
          (*resultEntry)[1] = locatedTriple->id3;
        } else {
          (*resultEntry)[0] = locatedTriple->id3;
        }
        ++resultEntry;
      }
      ++locatedTriple;
    }

    // Append the triple at this position to the result if and only if it is
    // marked for deletion and matches (also skip it if it doesn't match).
    bool deleteBlockEntry = false;
    if (locatedTriple != locatedTriples.end() &&
        locatedTriple->rowIndexInBlock == rowIndex &&
        locatedTriple->existsInIndex == true) {
      deleteBlockEntry = locatedTripleMatches();
      ++locatedTriple;
    }
    if (!deleteBlockEntry) {
      *resultEntry++ = blockEntry;
    }

    // Update `rowIndex` for the next `blockEntry`.
    ++rowIndex;
  };
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                          const IdTable& block, IdTable& result,
                                          size_t offsetInResult) const {
  mergeTriples<MatchMode::MatchAll>(blockIndex, block, result, offsetInResult);
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                          const IdTable& block, IdTable& result,
                                          size_t offsetInResult,
                                          size_t rowIndexOffset, Id id1) const {
  mergeTriples<MatchMode::MatchId1>(blockIndex, block, result, offsetInResult,
                                    rowIndexOffset, id1);
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                          const IdTable& block, IdTable& result,
                                          size_t offsetInResult,
                                          size_t rowIndexOffset, Id id1,
                                          Id id2) const {
  mergeTriples<MatchMode::MatchId1AndId2>(
      blockIndex, block, result, offsetInResult, rowIndexOffset, id1, id2);
}

// ____________________________________________________________________________
template <typename Permutation>
LocatedTriple LocatedTriple::locateTripleInPermutation(
    Id id1, Id id2, Id id3, const Permutation& permutation) {
  // Get the internal data structures from the permutation.
  auto& file = permutation._file;
  const auto& meta = permutation._meta;
  const auto& reader = permutation._reader;

  // Find the index of the first block where the last triple is not smaller.
  //
  // NOTE: With `_col2LastId` added to `CompressedBlockMetadata`, this can
  // now be computed without having to decompress any blocks at this point.
  // See the first revision of this branch for code, where blocks with equal
  // `id1` and `id2` were decompressed to also check for `id3`.
  const vector<CompressedBlockMetadata>& blocks = meta.blockData();
  auto matchingBlock = std::lower_bound(
      blocks.begin(), blocks.end(), std::array<Id, 3>{id1, id2, id3},
      [&](const CompressedBlockMetadata& block, const auto& triple) -> bool {
        if (block._col0LastId < triple[0]) {
          return true;
        } else if (block._col0LastId == triple[0]) {
          if (block._col1LastId < triple[1]) {
            return true;
          } else if (block._col1LastId == triple[1]) {
            return block._col2LastId < triple[2];
          }
        }
        return false;
      });
  size_t blockIndex = matchingBlock - blocks.begin();

  // Preliminary `FindTripleResult` object with the correct `blockIndex` and
  // IDs, but still an invalid `rowIndexInBlock` and `existsInIndex` set to
  // `false`.
  LocatedTriple locatedTriple{
      blockIndex, std::numeric_limits<size_t>::max(), id1, id2, id3, false};

  // If all IDs from all blocks are smaller, we return the index of the last
  // block plus one (typical "end" semantics) and any position in the block
  // (in the code that uses the result, that position will not be used in
  // this case).
  if (matchingBlock == blocks.end()) {
    AD_CORRECTNESS_CHECK(blockIndex == blocks.size());
    return locatedTriple;
  }

  // Read and decompress the block. Note that we are potentially doing this
  // a second time here (the block has probably already been looked at in
  // the call to `std::lower_bound` above).
  DecompressedBlock blockTuples =
      reader.readAndDecompressBlock(*matchingBlock, file, std::nullopt);

  // Find the smallest "relation" ID that is not smaller than `id1` and get
  // its metadata and the position of the first and last triple with that ID
  // in the block.
  //
  // IMPORTANT FIX: If relation `id1` exists in the index, but our triple is
  // larger than all triples of that relation in the index and the last
  // triple of that relation ends a block, then our block search above
  // (correctly) landed us at the next block. We can detect this by checking
  // whether the first relation ID of the block is larger than `id1` and
  // then we should get the metadata for the ID and not for `id1` (which
  // would pertain to a previous block).
  //
  // TODO: There is still a bug in `MetaDataWrapperHashMap::lower_bound`,
  // which is relevant in the rare case where a triple is inserted with an
  // `Id` for predicate that is not a new `Id`, but has not been used for a
  // predicate in the original index.
  //
  // NOTE: Since we have already handled the case, where all IDs in the
  // permutation are smaller, above, such a relation should exist.
  Id searchId =
      matchingBlock->_col0FirstId > id1 ? matchingBlock->_col0FirstId : id1;
  const auto& it = meta._data.lower_bound(searchId);
  AD_CORRECTNESS_CHECK(it != meta._data.end());
  Id id = it.getId();
  const auto& relationMetadata = meta.getMetaData(id);
  size_t offsetBegin = relationMetadata._offsetInBlock;
  size_t offsetEnd = offsetBegin + relationMetadata._numRows;
  // Note: If the relation spans multiple blocks, we know that the block we
  // found above contains only triples from that relation.
  if (offsetBegin == std::numeric_limits<uint64_t>::max()) {
    offsetBegin = 0;
    offsetEnd = blockTuples.size();
  }
  AD_CORRECTNESS_CHECK(offsetBegin <= blockTuples.size());
  AD_CORRECTNESS_CHECK(offsetEnd <= blockTuples.size());

  // If we have found `id1`, we can do a binary search in the portion of the
  // block that pertains to it (note the special case mentioned above, where
  // we are already at the beginning of the next block).
  //
  // Otherwise, `id` is the next larger ID and the position of the first
  // triple of that relation is exactly the position we are looking for.
  if (id == id1) {
    locatedTriple.rowIndexInBlock =
        std::lower_bound(blockTuples.begin() + offsetBegin,
                         blockTuples.begin() + offsetEnd,
                         std::array<Id, 2>{id2, id3},
                         [](const auto& a, const auto& b) {
                           return a[0] < b[0] || (a[0] == b[0] && a[1] < b[1]);
                         }) -
        blockTuples.begin();
    // Check if the triple at the found position is equal to `id1 id2 id3`.
    // Note that our default for `existsInIndex` was set to `false` above.
    const size_t& i = locatedTriple.rowIndexInBlock;
    AD_CORRECTNESS_CHECK(i < blockTuples.size());
    if (i < offsetEnd && blockTuples(i, 0) == id2 && blockTuples(i, 1) == id3) {
      locatedTriple.existsInIndex = true;
    }
  } else {
    AD_CORRECTNESS_CHECK(id1 < id);
    locatedTriple.rowIndexInBlock = offsetBegin;
  }

  // Return the result.
  return locatedTriple;
}

// Explicit instantiation for the six permutation.
#define INSTANTIATE_LTIP(Permutation)                               \
  template LocatedTriple                                            \
  LocatedTriple::locateTripleInPermutation<Permutation>(Id, Id, Id, \
                                                        const Permutation&);
INSTANTIATE_LTIP(Permutation::PSO_T)
INSTANTIATE_LTIP(Permutation::POS_T)
INSTANTIATE_LTIP(Permutation::SPO_T)
INSTANTIATE_LTIP(Permutation::SOP_T)
INSTANTIATE_LTIP(Permutation::OPS_T)
INSTANTIATE_LTIP(Permutation::OSP_T)
