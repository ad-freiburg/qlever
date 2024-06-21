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
    const std::vector<IdTriple>& triples, const Permutation& permutation,
    bool shouldExist) {
  const Permutation::MetaData& meta = permutation.metaData();
  const vector<CompressedBlockMetadata>& blocks = meta.blockData();

  vector<LocatedTriple> out;
  out.reserve(triples.size());
  size_t currentBlockIndex;
  for (auto triple : triples) {
    triple = triple.permute(permutation.keyOrder());
    currentBlockIndex =
        std::ranges::lower_bound(
            blocks, triple,
            [&](const IdTriple& block, const IdTriple& triple) {
              return block < triple;
            },
            [](const CompressedBlockMetadata& block) {
              const auto& perm = block.lastTriple_;
              return IdTriple{perm.col0Id_, perm.col1Id_, perm.col2Id_};
            }) -
        blocks.begin();
    out.emplace_back(currentBlockIndex, triple, shouldExist);
  }

  return out;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriple& lt) {
  os << "LT(" << lt.blockIndex_ << " " << lt.triple_ << " "
     << lt.shouldTripleExist_ << ")";
  return os;
}

// ____________________________________________________________________________
NumAddedAndDeleted LocatedTriplesPerBlock::numTriples(size_t blockIndex) const {
  // If no located triples for `blockIndex_` exist, there is no entry in `map_`.
  if (!map_.contains(blockIndex)) {
    return {0, 0};
  }

  auto blockUpdateTriples = map_.at(blockIndex);
  size_t countInserts = std::ranges::count_if(
      blockUpdateTriples, &LocatedTriple::shouldTripleExist_);
  return {countInserts, blockUpdateTriples.size() - countInserts};
}

// ____________________________________________________________________________
IdTable LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                             const IdTable& block,
                                             size_t numIndexColumns) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  // TODO<qup42>: We're assuming that the index columns are always {0, 1, 2},
  // {1, 2} or {2}. Is this true?
  AD_CONTRACT_CHECK(numIndexColumns <= block.numColumns());
  AD_CONTRACT_CHECK(block.numColumns() >= 1);

  auto numInsertsAndDeletes = numTriples(blockIndex);
  IdTable result{block.numColumns(), block.getAllocator()};
  result.resize(block.numRows() + numInsertsAndDeletes.numAdded);

  const auto& locatedTriples = map_.at(blockIndex);

  auto cmpLt = [&numIndexColumns](auto lt, auto row) {
    if (numIndexColumns == 3) {
      return std::tie(row[0], row[1], row[2]) > std::tie(lt->triple_.ids_[0],
                                                         lt->triple_.ids_[1],
                                                         lt->triple_.ids_[2]);
    } else if (numIndexColumns == 2) {
      return std::tie(row[0], row[1]) >
             std::tie(lt->triple_.ids_[1], lt->triple_.ids_[2]);
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return std::tie(row[0]) > std::tie(lt->triple_.ids_[2]);
    }
  };
  auto cmpEq = [&numIndexColumns](auto lt, auto row) {
    if (numIndexColumns == 3) {
      return std::tie(row[0], row[1], row[2]) == std::tie(lt->triple_.ids_[0],
                                                          lt->triple_.ids_[1],
                                                          lt->triple_.ids_[2]);
    } else if (numIndexColumns == 2) {
      return std::tie(row[0], row[1]) ==
             std::tie(lt->triple_.ids_[1], lt->triple_.ids_[2]);
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return std::tie(row[0]) == std::tie(lt->triple_.ids_[2]);
    }
  };
  auto writeTripleToResult = [&numIndexColumns, &result](auto resultEntry,
                                                         auto& locatedTriple) {
    for (size_t i = 0; i < numIndexColumns; i++) {
      (*resultEntry)[i] = locatedTriple->triple_.ids_[3 - numIndexColumns + i];
    }
    // Write UNDEF to any additional columns.
    for (size_t i = numIndexColumns; i < result.numColumns(); i++) {
      (*resultEntry)[i] = ValueId::makeUndefined();
    }
  };

  auto row = block.begin();
  auto locatedTriple = locatedTriples.begin();
  auto resultEntry = result.begin();

  while (row != block.end() && locatedTriple != locatedTriples.end()) {
    if (cmpLt(locatedTriple, *row)) {
      // locateTriple < row
      if (locatedTriple->shouldTripleExist_) {
        // Insertion of a non-existent triple.
        writeTripleToResult(resultEntry, locatedTriple);
        resultEntry++;
        locatedTriple++;
      } else {
        // Deletion of a non-existent triple.
        locatedTriple++;
      }
    } else if (cmpEq(locatedTriple, *row)) {
      // locateTriple == row
      if (locatedTriple->shouldTripleExist_) {
        // Insertion of an already existing triple.
        locatedTriple++;
      } else {
        // Deletion of an existing triple.
        locatedTriple++;
        row++;
      }
    } else {
      // locateTriple > row
      // The row is not deleted - copy it
      *resultEntry++ = *row++;
    }
  }

  if (locatedTriple != locatedTriples.end()) {
    AD_CORRECTNESS_CHECK(row == block.end());
    while (locatedTriple != locatedTriples.end()) {
      if (locatedTriple->shouldTripleExist_ == true) {
        writeTripleToResult(resultEntry, locatedTriple);
        ++resultEntry;
      }
      ++locatedTriple;
    }
  }
  if (row != block.end()) {
    AD_CORRECTNESS_CHECK(locatedTriple == locatedTriples.end());
    std::ranges::copy(row, block.end(), resultEntry);
  }

  result.resize(resultEntry - result.begin());
  return result;
}

// ____________________________________________________________________________
std::vector<LocatedTriples::iterator> LocatedTriplesPerBlock::add(
    const std::vector<LocatedTriple>& locatedTriples) {
  std::vector<LocatedTriples::iterator> handles;
  handles.reserve(locatedTriples.size());
  for (auto triple : locatedTriples) {
    LocatedTriples& locatedTriples = map_[triple.blockIndex_];
    auto [handle, wasInserted] = locatedTriples.emplace(triple);
    AD_CORRECTNESS_CHECK(wasInserted == true);
    AD_CORRECTNESS_CHECK(handle != locatedTriples.end());
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
  auto block = blockIter->second;
  block.erase(iter);
  numTriples_--;
  if (block.empty()) {
    map_.erase(blockIndex);
  }
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriplesPerBlock& ltpb) {
  // Get the block indices in sorted order.
  std::vector<size_t> blockIndices;
  std::ranges::copy(ltpb.map_ | std::views::keys,
                    std::back_inserter(blockIndices));
  std::ranges::sort(blockIndices);
  for (auto blockIndex : blockIndices) {
    os << "LTs in Block #" << blockIndex << ": " << ltpb.map_.at(blockIndex)
       << std::endl;
  }
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts) {
  os << "{ ";
  std::ranges::copy(lts, std::ostream_iterator<LocatedTriple>(os, " "));
  os << "}";
  return os;
}

// ____________________________________________________________________________
template <typename T, std::size_t N>
std::ostream& operator<<(std::ostream& os, const std::array<T, N>& v) {
  os << "(";
  std::ranges::copy(v, std::ostream_iterator<T>(os, ", "));
  os << ")";
  return os;
}

template std::ostream& operator<< <ValueId, 3ul>(
    std::ostream& os, const std::array<ValueId, 3ul>& v);

// ____________________________________________________________________________
template <typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& v) {
  std::ranges::copy(v, std::ostream_iterator<T>(os, ", "));
  return os;
}

template std::ostream& operator<< <std::array<ValueId, 3>>(
    std::ostream& os, const std::vector<std::array<ValueId, 3ul>>& v);
