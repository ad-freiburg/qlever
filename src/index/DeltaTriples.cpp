// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/DeltaTriples.h"

#include "absl/strings/str_cat.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "parser/TurtleParser.h"
#include "util/Timer.h"

// ____________________________________________________________________________
void DeltaTriples::clear() {
  triplesInserted_.clear();
  triplesDeleted_.clear();
}

// ____________________________________________________________________________
void DeltaTriples::insertTriple(TurtleTriple turtleTriple) {
  IdTriple idTriple = getIdTriple(std::move(turtleTriple));
  triplesInserted_.insert(idTriple);
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriple(TurtleTriple turtleTriple) {
  IdTriple idTriple = getIdTriple(std::move(turtleTriple));
  triplesDeleted_.insert(idTriple);
}

// ____________________________________________________________________________
DeltaTriples::IdTriple DeltaTriples::getIdTriple(TurtleTriple turtleTriple) {
  TripleComponent subject = std::move(turtleTriple._subject);
  TripleComponent predicate = std::move(turtleTriple._predicate);
  TripleComponent object = std::move(turtleTriple._object);
  Id subjectId = std::move(subject).toValueId(index_.getVocab(), localVocab_);
  Id predId = std::move(predicate).toValueId(index_.getVocab(), localVocab_);
  Id objectId = std::move(object).toValueId(index_.getVocab(), localVocab_);
  return IdTriple{subjectId, predId, objectId};
}

// ____________________________________________________________________________
void DeltaTriples::findTripleInAllPermutations(const IdTriple& idTriple,
                                               bool visualize) {
  auto [s, p, o] = idTriple;
  psoFindTripleResults_.emplace_back(
      findTripleInPermutation(p, s, o, index_.getImpl().PSO(), visualize));
  posFindTripleResults_.emplace_back(
      findTripleInPermutation(p, o, s, index_.getImpl().POS(), visualize));
  spoFindTripleResults_.emplace_back(
      findTripleInPermutation(s, p, o, index_.getImpl().SPO(), visualize));
  sopFindTripleResults_.emplace_back(
      findTripleInPermutation(s, o, p, index_.getImpl().SOP(), visualize));
  ospFindTripleResults_.emplace_back(
      findTripleInPermutation(o, s, p, index_.getImpl().OSP(), visualize));
  opsFindTripleResults_.emplace_back(
      findTripleInPermutation(o, p, s, index_.getImpl().OPS(), visualize));
}

// ____________________________________________________________________________
template <typename Permutation>
DeltaTriples::FindTripleResult DeltaTriples::findTripleInPermutation(
    Id id1, Id id2, Id id3, Permutation& permutation, bool visualize) const {
  // Get the internal data structures from the permutation.
  auto& file = permutation._file;
  const auto& meta = permutation._meta;
  const auto& reader = permutation._reader;
  ad_utility::SharedConcurrentTimeoutTimer timer;
  ad_utility::AllocatorWithLimit<Id> unlimitedAllocator{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};

  // Get the name of the permutation and names for the IDs from the triple
  // (for visualization only, can eventually be deleted).
  auto& pname = permutation._readableName;
  std::string name1 = getNameForId(id1);
  std::string name2 = getNameForId(id2);
  std::string name3 = getNameForId(id3);
  std::string tname = absl::StrCat(std::string{pname[0]}, "=", name1, " ",
                                   std::string{pname[1]}, "=", name2, " ",
                                   std::string{pname[2]}, "=", name3);

  // Find the index of the first block where the last triple is not smaller.
  //
  // NOTE: With `_col2LastId` added to `CompressedBlockMetadata`, this can now
  // be computed without having to decompress any blocks at this point. See the
  // first revision of this branch for code, where blocks with equal `id1` and
  // `id2` were decompressed to also check for `id3`.
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

  // If all IDs from all blocks are smaller, we return the index of the last
  // block plus one (typical "end" semantics) and any position in the block (in
  // the code that uses the result, that position will not be used in this
  // case).
  if (matchingBlock == blocks.end()) {
    if (visualize) {
      std::cout << "All triples in " << pname << " are smaller than " << tname
                << std::endl;
    }
    return FindTripleResult{blocks.size(), std::numeric_limits<size_t>::max(),
                            id1, id2, id3};
  }

  // Read and decompress the block. Note that we are potentially doing this a
  // second time here (the block has probably already been looked at in the call
  // to `std::lower_bound` above).
  DecompressedBlock blockTuples =
      reader.readAndDecompressBlock(*matchingBlock, file, std::nullopt);

  // Get the most significant IDs for this block (might only be one or several,
  // stored implicitly in the metadata).
  //
  // TODO: This is inefficient and not necessary. However, the current interface
  // of `IndexMetaData` doesn't make it easy to get the most significant IDs for
  // a block.
  size_t blockSize = blockTuples.numRows();
  std::vector<Id> mostSignificantIdsInBlock(blockSize);
  std::vector<Id> mostSignificantIdsDistinct;
  for (auto it = meta._data.begin(); it != meta._data.end(); ++it) {
    const auto& relationMetadata = meta.getMetaData(it.getId());
    Id id = relationMetadata._col0Id;
    uint64_t offset = relationMetadata._offsetInBlock;
    size_t numRows = relationMetadata._numRows;
    if (offset == std::numeric_limits<uint64_t>::max()) {
      offset = 0;
    }
    if (id >= matchingBlock->_col0FirstId && id <= matchingBlock->_col0LastId) {
      mostSignificantIdsDistinct.push_back(id);
      for (size_t i = 0; i < numRows && offset + i < blockSize; ++i) {
        mostSignificantIdsInBlock[offset + i] = id;
      }
    }
  }
  std::sort(mostSignificantIdsDistinct.begin(),
            mostSignificantIdsDistinct.end());

  // Find the first triple that is not smaller. If the triple is contained in
  // the block that will be the position of the triple. Otherwise it will be the
  // position of the first triple that is larger. Since the last triple of this
  // block is not smaller, this will not be larger than the last valid index in
  // the block.

  // First check whether `id1` occurs at all in this block. If not, the index we
  // are searching is just the position of the first ID that is larger.
  // Otherwise, we can do binary search in the portion of the block with that
  // ID as most significant ID.
  size_t rowIndexInBlock = std::numeric_limits<uint64_t>::max();
  auto mostSignificantIdsMatch =
      std::lower_bound(mostSignificantIdsDistinct.begin(),
                       mostSignificantIdsDistinct.end(), id1);
  if (*mostSignificantIdsMatch > id1) {
    rowIndexInBlock = meta.getMetaData(*mostSignificantIdsMatch)._offsetInBlock;
    if (rowIndexInBlock == std::numeric_limits<uint64_t>::max()) {
      rowIndexInBlock = 0;
    }
  } else {
    AD_CORRECTNESS_CHECK(*mostSignificantIdsMatch == id1);
    size_t offsetBegin = meta.getMetaData(id1)._offsetInBlock;
    size_t offsetEnd = offsetBegin + meta.getMetaData(id1)._numRows;
    if (offsetBegin == std::numeric_limits<uint64_t>::max()) {
      offsetBegin = 0;
      offsetEnd = blockTuples.size();
    }
    rowIndexInBlock =
        std::lower_bound(blockTuples.begin() + offsetBegin,
                         blockTuples.begin() + offsetEnd,
                         std::array<Id, 2>{id2, id3},
                         [](const auto& a, const auto& b) {
                           return a[0] < b[0] || (a[0] == b[0] && a[1] < b[1]);
                         }) -
        blockTuples.begin();
  }
  AD_CORRECTNESS_CHECK(rowIndexInBlock != std::numeric_limits<uint64_t>::max());

  // Show the respective block.
  if (visualize) {
    std::cout << std::endl;
    std::cout << "Block #" << blockIndex << " from " << pname << " (" << tname
              << "):" << std::endl;
    // Now we are ready to write the triples in the block, including the most
    // significant ID.
    for (size_t i = 0; i < blockSize; ++i) {
      std::cout << "Row #" << i << ": "
                << getNameForId(mostSignificantIdsInBlock[i]);
      for (size_t j = 0; j < blockTuples.numColumns(); ++j) {
        std::cout << " " << getNameForId(blockTuples(i, j));
      }
      if (i == rowIndexInBlock) {
        std::cout << " <--";
      }
      std::cout << std::endl;
    }
  }

  return FindTripleResult{blockIndex, rowIndexInBlock, id1, id2, id3};
}

// ____________________________________________________________________________
std::string DeltaTriples::getNameForId(Id id) const {
  auto lookupResult =
      ExportQueryExecutionTrees::idToStringAndType(index_, id, localVocab_);
  AD_CONTRACT_CHECK(lookupResult.has_value());
  const auto& [value, type] = lookupResult.value();
  return type ? absl::StrCat("\"", value, "\"^^<", type, ">") : value;
};
