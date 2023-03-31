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
size_t LocatedTriplesPerBlock::numTriplesInBlockImpl(size_t blockIndex, Id id1,
                                                     Id id2) const {
  size_t count = 0;
  for (const LocatedTriple& locatedTriple : map_.at(blockIndex)) {
    if constexpr (matchMode == MatchMode::MatchAll) {
      ++count;
    } else if constexpr (matchMode == MatchMode::MatchId1) {
      count += (locatedTriple.id1 == id1);
    } else if constexpr (matchMode == MatchMode::MatchId1AndId2) {
      count += (locatedTriple.id1 == id1 && locatedTriple.id2 == id2);
    }
  }
  return count;
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::numTriplesInBlock(size_t blockIndex) const {
  return numTriplesInBlockImpl<MatchMode::MatchAll>(blockIndex);
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::numTriplesInBlock(size_t blockIndex,
                                                 Id id1) const {
  return numTriplesInBlockImpl<MatchMode::MatchId1>(blockIndex, id1);
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::numTriplesInBlock(size_t blockIndex, Id id1,
                                                 Id id2) const {
  return numTriplesInBlockImpl<MatchMode::MatchId1AndId2>(blockIndex, id1, id2);
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::mergeTriplesIntoBlock(size_t blockIndex,
                                                   IdTable& idTable, size_t pos,
                                                   size_t len, int netGrowth) {
  // If there are no triples in the given block, there is nothing to do.
  if (!map_.contains(blockIndex)) {
    AD_CONTRACT_CHECK(netGrowth == 0);
    return;
  }
  const size_t numColumns = idTable.numColumns();

  // Iterate backwards over the triples from the given block and process in
  // groups of triples with the same `rowIndexInBlock`.
  //
  // NOTE: We have to keep track of the positions separately, since pointer
  // arithmetic does not work on `LocatedTriples`, which is a `std::set`. That's
  // also why we can't use `upper_bound` for searching.
  const LocatedTriples& locatedTriples = map_.at(blockIndex);
  auto locatedTriple = locatedTriples.rbegin();
  int shift = netGrowth;
  size_t previousRowIndex = len;
  while (locatedTriple != locatedTriples.rend()) {
    // Search backwards for the next triple with a new `rowIndexInBlock`. Check
    // that only the last triple in a group may already exist in the index (all
    // triples with the same row index are <= the triple at that position in
    // `idTable').
    size_t rowIndex = locatedTriple->rowIndexInBlock;
    int groupNetGrowth = locatedTriple->existsInIndex ? -1 : 0;
    auto nextLocatedTriple = locatedTriple;
    ++nextLocatedTriple;
    while (nextLocatedTriple != locatedTriples.rend() &&
           nextLocatedTriple->rowIndexInBlock == rowIndex) {
      AD_CORRECTNESS_CHECK(nextLocatedTriple->existsInIndex == false);
      ++groupNetGrowth;
      ++nextLocatedTriple;
    }
    std::transform(locatedTriple, nextLocatedTriple,
                   std::ostream_iterator<std::string>(std::cout, " "),
                   [](const LocatedTriple& lt) {
                     return absl::StrCat(lt.rowIndexInBlock, "[",
                                         lt.existsInIndex, "]");
                   });
    std::cout << "... shift = " << shift << ", net growth = " << groupNetGrowth
              << std::endl;

    // If the last triple in the group exists in the index, don't copy that
    // (recall that we are iterating in reverse order). By the way how `shift`
    // is updated (see the `shift -= groupNetGrowth` below), the matching
    // entry in `idTable` will be be overwritten.
    if (locatedTriple->existsInIndex) {
      ++locatedTriple;
    }

    // Make space in `idTable` at the right place for the new group of triples
    // and insert the new `Id`s (and overwrite those of the deleted triples).
    //
    // NOTE: If the `idTable` has two columns, we write `id2` and `id3`. If it
    // has only one column, we only write `id3`.
    AD_CONTRACT_CHECK(numColumns == 1 || numColumns == 2);
    for (size_t colIndex = 0; colIndex < numColumns; ++colIndex) {
      std::span<Id> column = idTable.getColumn(colIndex);
      // Shifting left or right requires two different functions.
      if (shift >= 0) {
        std::shift_right(column.begin() + pos + rowIndex,
                         column.begin() + pos + previousRowIndex + shift,
                         shift);
      } else {
        std::shift_left(column.begin() + pos + rowIndex,
                        column.begin() + pos + previousRowIndex - shift,
                        -shift);
      }
      // Show the changed column after the shift.
      if (1) {
        std::cout << "Col #" << colIndex << ": ";
        std::copy(column.begin(), column.end(),
                  std::ostream_iterator<Id>(std::cout, " "));
        std::cout << " [after shift]" << std::endl;
      }
      // Add the new triples.
      std::transform(
          locatedTriple, nextLocatedTriple,
          column.rbegin() + column.size() - pos - previousRowIndex - shift,
          [&colIndex, &numColumns](const LocatedTriple& lt) {
            return colIndex + 1 < numColumns ? lt.id2 : lt.id3;
          });
      // For debugging only: null the gaps.
      if (0) {
        if (shift > 0 && shift > groupNetGrowth) {
          for (int i = 0; i < shift - groupNetGrowth; ++i) {
            column[pos + rowIndex + i] = Id::makeUndefined();
          }
        }
      }
      // Show the changed column.
      if (0) {
        std::cout << "Col #" << colIndex << ": ";
        std::copy(column.begin(), column.end(),
                  std::ostream_iterator<Id>(std::cout, " "));
        std::cout << " [after add]" << std::endl;
      }
    }

    // Update for the next iteration.
    previousRowIndex = rowIndex;
    shift -= groupNetGrowth;
    locatedTriple = nextLocatedTriple;
  }
  // AD_CORRECTNESS_CHECK(shift == 0);
  // AD_CORRECTNESS_CHECK(std::is_sorted(
  //     idTable.begin() + pos, idTable.begin() + pos + len + numNewTriples));

  // Do something to `idTable` to make the compiler happy.
  // std::shift_right(idTable.begin() + pos,
  //                  idTable.begin() + pos + len + numNewTriples,
  //                  numNewTriples);
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
