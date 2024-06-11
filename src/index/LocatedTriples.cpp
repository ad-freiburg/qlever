// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/LocatedTriples.h"

#include <algorithm>

#include "absl/strings/str_join.h"
#include "index/CompressedRelation.h"
#include "index/IndexMetaData.h"
#include "index/Permutation.h"

IdTriple permute(const IdTriple& triple, const Permutation& permutation) {
  auto keyOrder = permutation.keyOrder();
  return {triple[keyOrder[0]], triple[keyOrder[1]], triple[keyOrder[2]]};
}

std::vector<LocatedTriple> LocatedTriple::locateTriplesInPermutation(
    const std::vector<IdTriple>& triples, const Permutation& permutation,
    bool shouldExist) {
  const Permutation::MetaData& meta = permutation.metaData();
  const vector<CompressedBlockMetadata>& blocks = meta.blockData();

  vector<LocatedTriple> out;
  out.reserve(triples.size());
  size_t currentBlockIndex;
  for (auto triple : triples) {
    triple = permute(triple, permutation);
    currentBlockIndex =
        std::lower_bound(blocks.begin(), blocks.end(), triple,
                         [&](const CompressedBlockMetadata& block,
                             const auto& triple) -> bool {
                           // std::array<Id, 3> kann auch < aus den komponenten
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
                         }) -
        blocks.begin();
    out.push_back(
        {currentBlockIndex, triple[0], triple[1], triple[2], shouldExist});
  }

  return out;
}

// ____________________________________________________________________________
std::pair<size_t, size_t> LocatedTriplesPerBlock::numTriples(
    size_t blockIndex, ScanSpecification scanSpec) const {
  // TODO:
  AD_CONTRACT_CHECK(!scanSpec.col2Id().has_value());

  // If no located triples for `blockIndex` exist, there is no entry in `map_`.
  if (!map_.contains(blockIndex)) {
    return {0, 0};
  }

  // Otherwise iterate over all located triples and count how many of them exist
  // in the index ("to be deleted") and how many are new ("to be inserted").
  size_t countExists = 0;
  size_t countNew = 0;
  for (const LocatedTriple& locatedTriple : map_.at(blockIndex)) {
    if (!scanSpec.col0Id() ||
        (scanSpec.col0Id().value() == locatedTriple.id1 &&
         (!scanSpec.col1Id() ||
          (scanSpec.col1Id().value() == locatedTriple.id2 &&
           (!scanSpec.col2Id() ||
            scanSpec.col2Id().value() == locatedTriple.id3))))) {
      if (locatedTriple.shouldTripleExist) {
        ++countExists;
      } else {
        ++countNew;
      }
    }
  }
  return {countNew, countExists};
}

size_t LocatedTriplesPerBlock::mergeTriples(size_t blockIndex, IdTable block,
                                            IdTable& result,
                                            size_t offsetInResult,
                                            size_t rowIndexInBlockBegin,
                                            size_t rowIndexInBlockEnd) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  // If `rowIndexInBlockEnd` has the default value (see `LocatedTriples.h`), the
  // intended semantics is that we read the whole block (note that we can't have
  // a default value that depends on the values of previous arguments).
  if (rowIndexInBlockEnd == LocatedTriple::NO_ROW_INDEX) {
    rowIndexInBlockEnd = block.size();
  }

  // Check that `rowIndexInBlockBegin` and `rowIndexInBlockEnd` define a valid
  // and non-emtpy range and that it is a subrange of `block` (unless the latter
  // is `std::nullopt`).
  AD_CONTRACT_CHECK(rowIndexInBlockBegin < block.size());
  AD_CONTRACT_CHECK(rowIndexInBlockEnd <= block.size());
  AD_CONTRACT_CHECK(rowIndexInBlockBegin < rowIndexInBlockEnd);

  // If we restrict `id1` and `id2`, the index block and the result must have
  // one column (for the `id3`). Otherwise, they must have two columns (for the
  // `id2` and the `id3`).
  auto numberOfIndexColumns = [](const IdTable& idTable) {
    auto firstRow = idTable[0];
    return std::count_if(
        firstRow.begin(), firstRow.end(), [](const ValueId& i) {
          return i.getDatatype() == Datatype::VocabIndex ||
                 i.getDatatype() == Datatype::LocalVocabIndex ||
                 i.getDatatype() == Datatype::TextRecordIndex ||
                 i.getDatatype() == Datatype::WordVocabIndex;
        });
  };
  LOG(INFO) << "Number of columns (result) " << result.numColumns()
            << std::endl;
  LOG(INFO) << "Number of columns (input) " << block.numColumns() << std::endl;
  LOG(INFO) << "Number of index columns  " << numberOfIndexColumns(block)
            << std::endl;
  LOG(INFO) << "First row " << block[0] << std::endl;

  auto numIndexColumns = numberOfIndexColumns(block);
  AD_CONTRACT_CHECK(result.numColumns() == block.numColumns());
  AD_CONTRACT_CHECK(result.numColumns() >= 1);

  auto resultEntry = result.begin() + offsetInResult;
  const auto& locatedTriples = map_.at(blockIndex);
  auto locatedTriple = locatedTriples.begin();

  // Advance to the first located triple in the specified range.
  auto cmpLt = [&numIndexColumns](auto lt, auto row) {
    LOG(INFO) << "Comparing " << *lt << " < " << row << std::endl;
    if (numIndexColumns == 3) {
      return (row[0] > lt->id1 ||
              (row[0] == lt->id1 &&
               (row[1] > lt->id2 || (row[1] == lt->id2 && row[2] > lt->id3))));
    } else if (numIndexColumns == 2) {
      return (row[0] > lt->id2 || (row[0] == lt->id2 && (row[1] > lt->id3)));
    } else if (numIndexColumns == 1) {
      return (row[0] > lt->id3);
    }
  };
  auto cmpEq = [&numIndexColumns](auto lt, auto row) {
    LOG(INFO) << "Comparing " << *lt << " == " << row << std::endl;
    if (numIndexColumns == 3) {
      return (row[0] == lt->id1 && row[1] == lt->id2 && row[2] == lt->id3);
    } else if (numIndexColumns == 2) {
      return (row[0] == lt->id2 && row[1] == lt->id3);
    } else if (numIndexColumns == 1) {
      return (row[0] == lt->id3);
    }
  };

  while (numIndexColumns == 3 && locatedTriple != locatedTriples.end() &&
         cmpLt(locatedTriple, block[rowIndexInBlockBegin])) {
    LOG(INFO) << "Skipping LocatedTriple " << *locatedTriple << std::endl;
    ++locatedTriple;
  }

  for (size_t rowIndex = rowIndexInBlockBegin; rowIndex < rowIndexInBlockEnd;
       ++rowIndex) {
    // Append triples that are marked for insertion at this `rowIndex` to the
    // result.
    LOG(INFO) << "New Block Row " << block[rowIndex] << std::endl;
    while (locatedTriple != locatedTriples.end() &&
           cmpLt(locatedTriple, block[rowIndex]) &&
           locatedTriple->shouldTripleExist == true) {
      if (numIndexColumns == 1) {
        (*resultEntry)[0] = locatedTriple->id3;
      } else if (numIndexColumns == 2) {
        (*resultEntry)[0] = locatedTriple->id2;
        (*resultEntry)[1] = locatedTriple->id3;
      } else {
        (*resultEntry)[0] = locatedTriple->id1;
        (*resultEntry)[1] = locatedTriple->id2;
        (*resultEntry)[2] = locatedTriple->id3;
      }
      ++resultEntry;
      ++locatedTriple;
      LOG(INFO) << "New LocatedTriple " << *locatedTriple << std::endl;
    }

    // Append the triple at this position to the result if and only if it is not
    // marked for deletion and matches (also skip it if it does not match).
    bool deleteThisEntry = false;
    if (locatedTriple != locatedTriples.end() &&
        cmpEq(locatedTriple, block[rowIndex]) &&
        locatedTriple->shouldTripleExist == false) {
      deleteThisEntry = true;

      ++locatedTriple;
      LOG(INFO) << "New LocatedTriple " << *locatedTriple << std::endl;
    }
    if (!deleteThisEntry) {
      *resultEntry++ = block[rowIndex];
    }
  };

  // Return the number of rows written to `result`.
  return resultEntry - (result.begin() + offsetInResult);
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriple& lt) {
  os << "LT(" << lt.blockIndex << " " << lt.id1 << " " << lt.id2 << " "
     << lt.id3 << " " << lt.shouldTripleExist << ")";
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
