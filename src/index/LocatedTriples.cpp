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

IdTriple permute(const IdTriple& triple,
                 const std::array<size_t, 3>& keyOrder) {
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
    triple = permute(triple, permutation.keyOrder());
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
std::pair<size_t, size_t> LocatedTriplesPerBlock::numTriples(
    size_t blockIndex) const {
  // If no located triples for `blockIndex_` exist, there is no entry in `map_`.
  if (!map_.contains(blockIndex)) {
    return {0, 0};
  }

  auto blockUpdateTriples = map_.at(blockIndex);
  size_t countDeletes = std::ranges::count_if(
      blockUpdateTriples,
      [](const LocatedTriple& elem) { return !elem.shouldTripleExist_; });
  size_t countInserts = blockUpdateTriples.size() - countDeletes;

  return {countInserts, countDeletes};
}

size_t LocatedTriplesPerBlock::mergeTriples(size_t blockIndex, IdTable block,
                                            IdTable& result,
                                            size_t offsetInResult,
                                            size_t numIndexColumns) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  // TODO<qup42>: We're assuming that the index columns are always {0, 1, 2},
  // {1, 2} or {2}. Is this true?
  AD_CONTRACT_CHECK(numIndexColumns <= block.numColumns());
  AD_CONTRACT_CHECK(result.numColumns() == block.numColumns());
  AD_CONTRACT_CHECK(result.numColumns() >= 1);

  auto resultEntry = result.begin() + offsetInResult;
  const auto& locatedTriples = map_.at(blockIndex);
  auto locatedTriple = locatedTriples.begin();

  // Advance to the first located triple in the specified range.
  auto cmpLt = [&numIndexColumns](auto lt, auto& row) {
    if (numIndexColumns == 3) {
      return (row[0] > lt->triple_[0] ||
              (row[0] == lt->triple_[0] &&
               (row[1] > lt->triple_[1] ||
                (row[1] == lt->triple_[1] && row[2] > lt->triple_[2]))));
    } else if (numIndexColumns == 2) {
      return (row[0] > lt->triple_[1] ||
              (row[0] == lt->triple_[1] && (row[1] > lt->triple_[2])));
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return (row[0] > lt->triple_[2]);
    }
  };
  auto cmpEq = [&numIndexColumns](auto lt, auto& row) {
    if (numIndexColumns == 3) {
      return (row[0] == lt->triple_[0] && row[1] == lt->triple_[1] &&
              row[2] == lt->triple_[2]);
    } else if (numIndexColumns == 2) {
      return (row[0] == lt->triple_[1] && row[1] == lt->triple_[2]);
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return (row[0] == lt->triple_[2]);
    }
  };
  auto writeTripleToResult = [&numIndexColumns, &result](auto resultEntry,
                                                         auto& locatedTriple) {
    for (size_t i = 0; i < numIndexColumns; i++) {
      (*resultEntry)[i] = locatedTriple->triple_[3 - numIndexColumns + i];
    }
    // Write UNDEF to any additional columns.
    for (size_t i = numIndexColumns; i < result.numColumns(); i++) {
      (*resultEntry)[i] = ValueId::makeUndefined();
    }
  };

  for (auto row : block) {
    // Append triples that are marked for insertion at this `rowIndex` to the
    // result.
    while (locatedTriple != locatedTriples.end() && cmpLt(locatedTriple, row)) {
      if (locatedTriple->shouldTripleExist_ == true) {
        // Adding an inserted Triple between two triples.
        writeTripleToResult(resultEntry, locatedTriple);
        ++resultEntry;
        ++locatedTriple;
      } else {
        // Deletion of a triple that does not exist in the index has no effect.
        ++locatedTriple;
      }
    }

    // Append the triple at this position to the result if and only if it is not
    // marked for deletion and matches (also skip it if it does not match).
    bool deleteThisEntry = false;
    if (locatedTriple != locatedTriples.end() && cmpEq(locatedTriple, row)) {
      if (locatedTriple->shouldTripleExist_ == false) {
        // A deletion of a triple that exists in the index.
        deleteThisEntry = true;

        ++locatedTriple;
      } else {
        // An insertion of a triple that already exists in the index has no
        // effect.
        ++locatedTriple;
      }
    }
    if (!deleteThisEntry) {
      *resultEntry++ = row;
    }
  }
  while (locatedTriple != locatedTriples.end() &&
         locatedTriple->shouldTripleExist_ == true) {
    writeTripleToResult(resultEntry, locatedTriple);
    ++resultEntry;
    ++locatedTriple;
  }

  // Return the number of rows written to `result`.
  return resultEntry - (result.begin() + offsetInResult);
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
  AD_CONTRACT_CHECK(map_.contains(blockIndex));
  map_[blockIndex].erase(iter);
  numTriples_--;
  if (map_[blockIndex].empty()) {
    map_.erase(blockIndex);
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
  std::ranges::copy(
      idTable, std::ostream_iterator<columnBasedIdTable::Row<Id>>(os, " "));
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
