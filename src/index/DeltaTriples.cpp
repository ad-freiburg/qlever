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
  triplesWithPositionsPerBlockInPSO_.clear();
  triplesWithPositionsPerBlockInPOS_.clear();
  triplesWithPositionsPerBlockInSPO_.clear();
  triplesWithPositionsPerBlockInSOP_.clear();
  triplesWithPositionsPerBlockInOSP_.clear();
  triplesWithPositionsPerBlockInOPS_.clear();
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
const DeltaTriples::TriplesWithPositionsPerBlock&
DeltaTriples::getTriplesWithPositionsPerBlock(
    Index::Permutation permutation) const {
  switch (permutation) {
    case Index::Permutation::PSO:
      return triplesWithPositionsPerBlockInPSO_;
    case Index::Permutation::POS:
      return triplesWithPositionsPerBlockInPOS_;
    case Index::Permutation::SPO:
      return triplesWithPositionsPerBlockInSPO_;
    case Index::Permutation::SOP:
      return triplesWithPositionsPerBlockInSOP_;
    case Index::Permutation::OSP:
      return triplesWithPositionsPerBlockInOSP_;
    case Index::Permutation::OPS:
      return triplesWithPositionsPerBlockInOPS_;
    default:
      AD_FAIL();
  }
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
void DeltaTriples::locateTripleInAllPermutations(const IdTriple& idTriple,
                                                 bool visualize) {
  // Helper lambda for adding `tripleWithPosition` to given
  // `TriplesWithPositionsPerBlock` list.
  auto addTripleWithPosition =
      [&](const TripleWithPosition& tripleWithPosition,
          TriplesWithPositionsPerBlock& triplesWithPositionsPerBlock) {
        triplesWithPositionsPerBlock.positionMap_[tripleWithPosition.blockIndex]
            .emplace_back(tripleWithPosition);
      };

  // Now locate the triple in each permutation and add it to the correct
  // `TriplesWithPositionsPerBlock` list.
  auto [s, p, o] = idTriple;
  addTripleWithPosition(
      locateTripleInPermutation(p, s, o, index_.getImpl().PSO(), visualize),
      triplesWithPositionsPerBlockInPSO_);
  addTripleWithPosition(
      locateTripleInPermutation(p, o, s, index_.getImpl().POS(), visualize),
      triplesWithPositionsPerBlockInPOS_);
  addTripleWithPosition(
      locateTripleInPermutation(s, p, o, index_.getImpl().SPO(), visualize),
      triplesWithPositionsPerBlockInSPO_);
  addTripleWithPosition(
      locateTripleInPermutation(s, o, p, index_.getImpl().SOP(), visualize),
      triplesWithPositionsPerBlockInSOP_);
  addTripleWithPosition(
      locateTripleInPermutation(o, s, p, index_.getImpl().OSP(), visualize),
      triplesWithPositionsPerBlockInOSP_);
  addTripleWithPosition(
      locateTripleInPermutation(o, p, s, index_.getImpl().OPS(), visualize),
      triplesWithPositionsPerBlockInOPS_);
}

// ____________________________________________________________________________
template <typename Permutation>
DeltaTriples::TripleWithPosition DeltaTriples::locateTripleInPermutation(
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

  // Preliminary `FindTripleResult` object with the correct `blockIndex` and
  // IDs, but still an invalid `rowIndexInBlock` and `existsInIndex` set to
  // `false`.
  TripleWithPosition tripleWithPosition{
      blockIndex, std::numeric_limits<size_t>::max(), id1, id2, id3, false};

  // If all IDs from all blocks are smaller, we return the index of the last
  // block plus one (typical "end" semantics) and any position in the block (in
  // the code that uses the result, that position will not be used in this
  // case).
  if (matchingBlock == blocks.end()) {
    AD_CORRECTNESS_CHECK(blockIndex == blocks.size());
    if (visualize) {
      std::cout << endl;
      std::cout << "All triples in " << pname << " are smaller than " << tname
                << std::endl;
    }
    return tripleWithPosition;
  }
  auto showTriple = [](const std::string& prefix,
                       const std::vector<Id>& triple) {
    std::cout << prefix << triple[0] << " " << triple[1] << " " << triple[2]
              << std::endl;
  };

  // Read and decompress the block. Note that we are potentially doing this a
  // second time here (the block has probably already been looked at in the call
  // to `std::lower_bound` above).
  DecompressedBlock blockTuples =
      reader.readAndDecompressBlock(*matchingBlock, file, std::nullopt);

  if (0) {
    std::vector<Id> ourTriple = {id1, id2, id3};
    std::vector<Id> lastTripleInBlock = {matchingBlock->_col0LastId,
                                         matchingBlock->_col1LastId,
                                         matchingBlock->_col2LastId};
    std::vector<Id> trueLastTripleInBlock = {
        Id::makeUndefined(), blockTuples.back()[0], blockTuples.back()[1]};
    std::cout << std::endl;
    showTriple("Ours: ", ourTriple);
    showTriple("Last: ", lastTripleInBlock);
    showTriple("True: ", trueLastTripleInBlock);
    AD_CORRECTNESS_CHECK(ourTriple <= lastTripleInBlock);
  }

  // Find the smallest "relation" ID that is not smaller than `id1` and get its
  // metadata and the position of the first and last triple with that ID in the
  // block.
  //
  // IMPORTANT FIX: If relation `id1` exists in the index, but our triple is
  // larger than all triples of that relation in the index and the last triple
  // of that relation ends a block, then our block search above (correctly)
  // landed us at the next block. We can detect this by checking whether the
  // first relation ID of the block is larger than `id1` and then we should
  // get the metadata for the ID and not for `id1` (which would pertain to a
  // previous block).
  //
  // TODO: There is still a bug in `MetaDataWrapperHashMap::lower_bound`, which
  // is relevant in the rare case where a triple is inserted with an `Id` for
  // predicate that is not a new `Id`, but has not been used for a predicate in
  // the original index.
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
  // block that pertains to it (note the special case mentioned above, where we
  // are already at the beginning of the next block).
  //
  // Otherwise, `id` is the next larger ID and the position of the first triple
  // of that relation is exactly the position we are looking for.
  if (id == id1) {
    tripleWithPosition.rowIndexInBlock =
        std::lower_bound(blockTuples.begin() + offsetBegin,
                         blockTuples.begin() + offsetEnd,
                         std::array<Id, 2>{id2, id3},
                         [](const auto& a, const auto& b) {
                           return a[0] < b[0] || (a[0] == b[0] && a[1] < b[1]);
                         }) -
        blockTuples.begin();
    // Check if the triple at the found position is equal to `id1 id2 id3`. Note
    // that our default for `existsInIndex` was set to `false` above.
    const size_t& i = tripleWithPosition.rowIndexInBlock;
    AD_CORRECTNESS_CHECK(i < blockTuples.size());
    if (i < offsetEnd && blockTuples(i, 0) == id2 && blockTuples(i, 1) == id3) {
      tripleWithPosition.existsInIndex = true;
    }
  } else {
    AD_CORRECTNESS_CHECK(id1 < id);
    tripleWithPosition.rowIndexInBlock = offsetBegin;
  }

  // Show the respective block. Note that we can show the relation ID only for
  // a part of the block (maybe the whole block, but not always).
  if (visualize) {
    std::cout << std::endl;
    std::cout << "Block #" << blockIndex << " from " << pname << " (" << tname
              << "):" << std::endl;
    // Now we are ready to write the triples in the block, including the most
    // significant ID.
    for (size_t i = 0; i < blockTuples.numRows(); ++i) {
      std::cout << "Row #" << i << ": "
                << (i >= offsetBegin && i < offsetEnd ? getNameForId(id)
                                                      : std::string{"*"});
      for (size_t j = 0; j < blockTuples.numColumns(); ++j) {
        std::cout << " " << getNameForId(blockTuples(i, j));
      }
      if (i == tripleWithPosition.rowIndexInBlock) {
        std::cout << " <-- "
                  << (tripleWithPosition.existsInIndex ? "existing triple"
                                                       : "new triple");
      }
      std::cout << std::endl;
    }
  }

  // Return the result.
  return tripleWithPosition;
}

// ____________________________________________________________________________
std::string DeltaTriples::getNameForId(Id id) const {
  auto lookupResult =
      ExportQueryExecutionTrees::idToStringAndType(index_, id, localVocab_);
  AD_CONTRACT_CHECK(lookupResult.has_value());
  const auto& [value, type] = lookupResult.value();
  // std::ostringstream os;
  // os << "[" << id << "]";
  return type ? absl::StrCat("\"", value, "\"^^<", type, ">") : value;
  // : absl::StrCat(value, " ", os.str());
};
