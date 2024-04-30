// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/LocatedTriples.h"

#include <algorithm>

#include "absl/strings/str_join.h"
#include "index/CompressedRelation.h"
#include "index/IndexMetaData.h"
#include "index/Permutation.h"

// ____________________________________________________________________________
LocatedTriple LocatedTriple::locateTripleInPermutation(
    Id id1, Id id2, Id id3, const Permutation& permutation) {
  // Get the internal data structures from the permutation.
  const Permutation::MetaData& meta = permutation.metaData();
  const CompressedRelationReader& reader = permutation.reader();

  // Find the index of the first block where the last triple is not smaller.
  //
  // TODO: This is the standard comparator for `std::array<Id, 3>`, no need to
  // spell it out here.
  const vector<CompressedBlockMetadata>& blocks = meta.blockData();
  auto matchingBlock = std::lower_bound(
      blocks.begin(), blocks.end(), std::array<Id, 3>{id1, id2, id3},
      [&](const CompressedBlockMetadata& block, const auto& triple) -> bool {
        const auto& lastTriple = block.lastTriple_;
        if (lastTriple.col0Id_ < triple[0]) {
          return true;
        } else if (lastTriple.col0Id_ == triple[0]) {
          if (lastTriple.col1Id_ < triple[1]) {
            return true;
          } else if (lastTriple.col1Id_ == triple[1]) {
            return lastTriple.col2Id_ < triple[2];
          }
        }
        return false;
      });
  size_t blockIndex = matchingBlock - blocks.begin();

  // Preliminary `FindTripleResult` object with the correct `blockIndex` and
  // `Id`s, and a special `rowIndexInBlock` (see below) and `existsInIndex` set
  // to `false`.
  LocatedTriple locatedTriple{blockIndex, NO_ROW_INDEX, id1, id2, id3, false};

  // If all `Id`s from all blocks are smaller, we return the index of the last
  // block plus one (typical "end" semantics) and the special row index
  // `NO_ROW_INDEX` (see how this is considered in `mergeTriples`).
  if (matchingBlock == blocks.end()) {
    AD_CORRECTNESS_CHECK(blockIndex == blocks.size());
    return locatedTriple;
  }

  // Find the smallest relation `Id` that is not smaller than `id1` and get its
  // metadata and the position of the first and last triple with that `Id` in
  // the block.
  //
  // IMPORTANT: If relation `id1` exists in the index, but our triple is larger
  // than all triples of that relation in the index and the last triple of that
  // relation ends a block, then our block search above (correctly) landed us at
  // the next block. We can detect this by checking whether the first relation
  // `Id` of the block is larger than `id1` and then we should get the metadata
  // for the `Id` and not for `id1` (which would pertain to a previous block).
  //
  // TODO: There is still a bug in `MetaDataWrapperHashMap::lower_bound`,
  // which is relevant in the rare case where a triple is inserted with an
  // `Id` for predicate that is not a new `Id`, but has not been used for a
  // predicate in the original index.
  //
  // NOTE: Since we have already handled the case, where all `Id`s in the
  // permutation are smaller, above, such a relation should exist.

  // If the relation of the first triple is smaller than `id1` this means that
  // the triple is between this and the previous block.
  if (matchingBlock->firstTriple_.col0Id_ > id1) {
    locatedTriple.rowIndexInBlock = 0;
    return locatedTriple;
  }

  const auto& it = meta.data().lower_bound(id1);
  // Metadata is only written for "large" relations.
  // `it == meta.data().end()` if
  // -  the triple is after all the relations for which metadata was written
  // `it.getId() != id1` if
  // - there is no metadata for the relation of the triple but for a larger relation
  // - the triple is in a relation that does not exist in the index
  // This complicates some optimizations as we cannot assume
  // - that a relation does not exist because we found no metadata for that relation
  // - that the metadata we found is for the next relation (and not e.g. 2 relations on)
  // The search space could still be limited by searching for some relation that
  // is before/after the triple.
  if (it == meta.data().end() || it.getId() != id1) {
    // Read and decompress the block. We lack the metadata to limit the search
    // to only the correct relation. We therefore need all the columns.
    std::array<ColumnIndex, 3> columnIndices{0u, 1u, 2u};
    DecompressedBlock blockTuples =
        reader.readAndDecompressBlock(*matchingBlock, columnIndices);

    size_t offsetBegin = 0;
    size_t offsetEnd = blockTuples.size();

    if(matchingBlock->firstTriple_.col0Id_ <= id1) {
      // The triple is actually inside this block.
      locatedTriple.rowIndexInBlock =
          std::lower_bound(
              blockTuples.begin() + offsetBegin,
              blockTuples.begin() + offsetEnd, std::array<Id, 3>{id1, id2, id3},
              [](const auto& a, const auto& b) {
                return a[0] < b[0] || (a[0] == b[0] && (a[1] < b[1] || (a[1] == b[1] && a[2] < b[2])));
              }) -
          blockTuples.begin();
      // Check if the triple at the found position is equal to `id1 id2 id3` to
      // determine whether it exists in the index. If it already exists we
      // have to set `existsInIndex` to `true` (the default is `false`).
      const size_t& i = locatedTriple.rowIndexInBlock;
      AD_CORRECTNESS_CHECK(i < blockTuples.size());
      if (i < offsetEnd && blockTuples(i, 0) == id1 &&
          blockTuples(i, 1) == id2 && blockTuples(i, 2) == id3) {
        locatedTriple.existsInIndex = true;
      }
    }
  } else {
    // Metadata exists for the relation of the triple. This allows us to limit
    // the search to only this relation.

    // Read and decompress the block. We only need `col1Id` and `col2Id` columns,
    // because we limit the search to only the relation using the metadata.
    std::array<ColumnIndex, 2> columnIndices{1u, 2u};
    DecompressedBlock blockTuples =
        reader.readAndDecompressBlock(*matchingBlock, columnIndices);

    const auto& relationMetadata = meta.getMetaData(it.getId());
    size_t offsetBegin = relationMetadata.offsetInBlock_;
    size_t offsetEnd = offsetBegin + relationMetadata.numRows_;
    // This is the case if the relation spans multiple blocks. The block then
    // only contains triples from that relation. We are in the right block,
    // but have to search it completely.
    if (offsetBegin == std::numeric_limits<uint64_t>::max()) {
      offsetBegin = 0;
      offsetEnd = blockTuples.size();
    }
    AD_CORRECTNESS_CHECK(offsetBegin <= blockTuples.size());
    AD_CORRECTNESS_CHECK(offsetEnd <= blockTuples.size());

    locatedTriple.rowIndexInBlock =
        std::lower_bound(
            blockTuples.begin() + offsetBegin,
            blockTuples.begin() + offsetEnd, std::array<Id, 2>{id2, id3},
            [](const auto& a, const auto& b) {
              return a[0] < b[0] || (a[0] == b[0] && a[1] < b[1]);
            }) -
        blockTuples.begin();
    // Check if the triple at the found position is equal to `id2 id3` to
    // determine whether it exists in the index. If it already exists we
    // have to set `existsInIndex` to `true` (the default is `false`).
    const size_t& i = locatedTriple.rowIndexInBlock;
    AD_CORRECTNESS_CHECK(i < blockTuples.size());
    if (i < offsetEnd && blockTuples(i, 0) == id2 &&
        blockTuples(i, 1) == id3) {
      locatedTriple.existsInIndex = true;
    }
  }

  // Return the result.
  return locatedTriple;
}

// ____________________________________________________________________________
template <LocatedTriplesPerBlock::MatchMode matchMode>
std::pair<size_t, size_t> LocatedTriplesPerBlock::numTriplesImpl(
    size_t blockIndex, Id id1, Id id2) const {
  // If no located triples for `blockIndex` exist, there is no entry in `map_`.
  if (!map_.contains(blockIndex)) {
    return {0, 0};
  }

  // Otherwise iterate over all located triples and count how many of them exist
  // in the index ("to be deleted") and how many are new ("to be inserted").
  size_t countExists = 0;
  size_t countNew = 0;
  for (const LocatedTriple& locatedTriple : map_.at(blockIndex)) {
    // Helper lambda for increasing the right counter.
    auto increaseCountIf = [&](bool increase) {
      if (increase) {
        if (locatedTriple.existsInIndex) {
          ++countExists;
        } else {
          ++countNew;
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
  return {countNew, countExists};
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
size_t LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                            std::optional<IdTable> block,
                                            IdTable& result,
                                            size_t offsetInResult, Id id1,
                                            Id id2, size_t rowIndexInBlockBegin,
                                            size_t rowIndexInBlockEnd) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  // The special case `block == std::nullopt` (write only located triples to
  // `result`) is only allowed, when the `matchMode` is `MatchId1` or
  // `MatchId1AndId2`, but not `MatchAll`.
  AD_CONTRACT_CHECK(block.has_value() || matchMode != MatchMode::MatchAll);

  // If `rowIndexInBlockEnd` has the default value (see `LocatedTriples.h`), the
  // intended semantics is that we read the whole block (note that we can't have
  // a default value that depends on the values of previous arguments).
  if (rowIndexInBlockEnd == LocatedTriple::NO_ROW_INDEX && block.has_value()) {
    rowIndexInBlockEnd = block.value().size();
  }

  // Check that `rowIndexInBlockBegin` and `rowIndexInBlockEnd` define a valid
  // and non-emtpy range and that it is a subrange of `block` (unless the latter
  // is `std::nullopt`).
  if (block.has_value()) {
    AD_CONTRACT_CHECK(rowIndexInBlockBegin < block.value().size());
    AD_CONTRACT_CHECK(rowIndexInBlockEnd <= block.value().size());
  }
  AD_CONTRACT_CHECK(rowIndexInBlockBegin < rowIndexInBlockEnd);

  // If we restrict `id1` and `id2`, the index block and the result must have
  // one column (for the `id3`). Otherwise, they must have two columns (for the
  // `id2` and the `id3`).
  if constexpr (matchMode == MatchMode::MatchId1AndId2) {
    AD_CONTRACT_CHECK(!block.has_value() || block.value().numColumns() == 1);
    AD_CONTRACT_CHECK(result.numColumns() == 1);
  } else {
    AD_CONTRACT_CHECK(!block.has_value() || block.value().numColumns() == 2);
    AD_CONTRACT_CHECK(result.numColumns() == 2);
  }

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

  // Advance to the first located triple in the specified range.
  while (locatedTriple != locatedTriples.end() &&
         locatedTriple->rowIndexInBlock < rowIndexInBlockBegin) {
    ++locatedTriple;
  }

  // Iterate over all located triples in the specified range. In the special
  // case `block == std::nullopt` (only write located triples to `result`), all
  // relevant located triples have `rowIndexInBlock == NO_ROW_INDEX` (here we
  // need that `NO_ROW_INDEX` is the maximal `size_t` value minus one).
  if (!block.has_value()) {
    rowIndexInBlockBegin = LocatedTriple::NO_ROW_INDEX;
    rowIndexInBlockEnd = rowIndexInBlockBegin + 1;
    AD_CORRECTNESS_CHECK(rowIndexInBlockBegin < rowIndexInBlockEnd);
  }
  for (size_t rowIndex = rowIndexInBlockBegin; rowIndex < rowIndexInBlockEnd;
       ++rowIndex) {
    // Append triples that are marked for insertion at this `rowIndex` to the
    // result.
    while (locatedTriple != locatedTriples.end() &&
           locatedTriple->rowIndexInBlock == rowIndex &&
           locatedTriple->existsInIndex == false) {
      if (locatedTripleMatches()) {
        if constexpr (matchMode == MatchMode::MatchId1AndId2) {
          (*resultEntry)[0] = locatedTriple->id3;
        } else {
          (*resultEntry)[0] = locatedTriple->id2;
          (*resultEntry)[1] = locatedTriple->id3;
        }
        ++resultEntry;
      }
      ++locatedTriple;
    }

    // Append the triple at this position to the result if and only if it is not
    // marked for deletion and matches (also skip it if it does not match).
    bool deleteThisEntry = false;
    if (locatedTriple != locatedTriples.end() &&
        locatedTriple->rowIndexInBlock == rowIndex &&
        locatedTriple->existsInIndex == true) {
      deleteThisEntry = locatedTripleMatches();
      ++locatedTriple;
    }
    if (block.has_value() && !deleteThisEntry) {
      *resultEntry++ = block.value()[rowIndex];
    }
  };

  // Return the number of rows written to `result`.
  return resultEntry - (result.begin() + offsetInResult);
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                            std::optional<IdTable> block,
                                            IdTable& result,
                                            size_t offsetInResult) const {
  return mergeTriples<MatchMode::MatchAll>(blockIndex, std::move(block), result,
                                           offsetInResult);
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                            std::optional<IdTable> block,
                                            IdTable& result,
                                            size_t offsetInResult, Id id1,
                                            size_t rowIndexInBlockBegin) const {
  return mergeTriples<MatchMode::MatchId1>(
      blockIndex, std::move(block), result, offsetInResult, id1,
      Id::makeUndefined(), rowIndexInBlockBegin);
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                            std::optional<IdTable> block,
                                            IdTable& result,
                                            size_t offsetInResult, Id id1,
                                            Id id2, size_t rowIndexInBlockBegin,
                                            size_t rowIndexInBlockEnd) const {
  return mergeTriples<MatchMode::MatchId1AndId2>(
      blockIndex, std::move(block), result, offsetInResult, id1, id2,
      rowIndexInBlockBegin, rowIndexInBlockEnd);
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriple& lt) {
  os << "LT(" << lt.blockIndex << " "
     << (lt.rowIndexInBlock == LocatedTriple::NO_ROW_INDEX
             ? "x"
             : std::to_string(lt.rowIndexInBlock))
     << " " << lt.id1 << " " << lt.id2 << " " << lt.id3 << " "
     << lt.existsInIndex << ")";
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts) {
  os << "{ ";
  std::copy(lts.begin(), lts.end(),
            std::ostream_iterator<LocatedTriple>(os, " "));
  os << "}";
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriplesPerBlock& ltpb) {
  // Get the block indices in sorted order.
  std::vector<size_t> blockIndices;
  std::transform(ltpb.map_.begin(), ltpb.map_.end(),
                 std::back_inserter(blockIndices),
                 [](const auto& entry) { return entry.first; });
  std::ranges::sort(blockIndices);
  for (auto blockIndex : blockIndices) {
    os << "Block #" << blockIndex << ": " << ltpb.map_.at(blockIndex)
       << std::endl;
  }
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os,
                         const columnBasedIdTable::Row<Id>& idTableRow) {
  os << "(";
  for (size_t i = 0; i < idTableRow.numColumns(); ++i) {
    os << idTableRow[i] << (i < idTableRow.numColumns() - 1 ? " " : ")");
  }
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const IdTable& idTable) {
  os << "{ ";
  std::copy(idTable.begin(), idTable.end(),
            std::ostream_iterator<columnBasedIdTable::Row<Id>>(os, " "));
  os << "}";
  return os;
}
