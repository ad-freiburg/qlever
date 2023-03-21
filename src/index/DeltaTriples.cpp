// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/DeltaTriples.h"

#include "absl/strings/str_cat.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "parser/TurtleParser.h"
#include "util/Timer.h"

// ____________________________________________________________________________
void DeltaTriples::clear() {
  triplesInserted_.clear();
  triplesDeleted_.clear();
  locatedTriplesPerBlockInPSO_.clear();
  locatedTriplesPerBlockInPOS_.clear();
  locatedTriplesPerBlockInSPO_.clear();
  locatedTriplesPerBlockInSOP_.clear();
  locatedTriplesPerBlockInOSP_.clear();
  locatedTriplesPerBlockInOPS_.clear();
}

// ____________________________________________________________________________
void DeltaTriples::eraseTripleInAllPermutations(
    DeltaTriples::LocatedTripleHandles& handles) {
  // Helper lambda for erasing for one particular permutation.
  auto erase = [](LocatedTriples::iterator locatedTriple,
                  LocatedTriplesPerBlock& locatedTriplesPerBlock) {
    size_t blockIndex = locatedTriple->blockIndex;
    locatedTriplesPerBlock.map_[blockIndex].erase(locatedTriple);
  };
  // Now erase for all permutations.
  erase(handles.forPSO, locatedTriplesPerBlockInPSO_);
  erase(handles.forPOS, locatedTriplesPerBlockInPOS_);
  erase(handles.forSPO, locatedTriplesPerBlockInSPO_);
  erase(handles.forSOP, locatedTriplesPerBlockInSOP_);
  erase(handles.forOSP, locatedTriplesPerBlockInOSP_);
  erase(handles.forOPS, locatedTriplesPerBlockInOPS_);
};

// ____________________________________________________________________________
void DeltaTriples::insertTriple(TurtleTriple turtleTriple) {
  IdTriple idTriple = getIdTriple(turtleTriple);
  // Inserting a triple (that does not exist in the original index) a second
  // time has no effect.
  //
  // TODO: Test this behavior.
  if (triplesInserted_.contains(idTriple)) {
    throw std::runtime_error(absl::StrCat(
        "Triple \"", turtleTriple.toString(), "\" was already inserted before",
        ", this insertion therefore has no effect"));
  }
  // When re-inserting a previously deleted triple, we need to remove the triple
  // from `triplesDeleted_` AND remove it from all
  // `locatedTriplesPerBlock` (one per permutation) as well.
  if (triplesDeleted_.contains(idTriple)) {
    eraseTripleInAllPermutations(triplesDeleted_.at(idTriple));
    triplesDeleted_.erase(idTriple);
    return;
  }
  // Locate the triple in one of the permutations (it does not matter which one)
  // to check if it already exists in the index. If it already exists, the
  // insertion is invalid, otherwise insert it.
  LocatedTriple locatedTriple = locateTripleInPermutation(
      idTriple[1], idTriple[0], idTriple[2], index_.getImpl().PSO());
  if (locatedTriple.existsInIndex) {
    throw std::runtime_error(
        absl::StrCat("Triple \"", turtleTriple.toString(),
                     "\" already exists in the original index",
                     ", this insertion therefore has no effect"));
  }
  auto iterators = locateTripleInAllPermutations(idTriple);
  triplesInserted_.insert({idTriple, iterators});
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriple(TurtleTriple turtleTriple) {
  IdTriple idTriple = getIdTriple(turtleTriple);
  // Deleting a triple (that does exist in the original index) a second time has
  // no effect.
  //
  // TODO: Test this behavior.
  if (triplesDeleted_.contains(idTriple)) {
    throw std::runtime_error(absl::StrCat(
        "Triple \"", turtleTriple.toString(), "\" was already deleted before",
        ", this deletion therefore has no effect"));
  }
  // When deleting a previously inserted triple (that did not exist in the index
  // before), we need to remove the triple from `triplesInserted_` AND remove it
  // from all `locatedTriplesPerBlock` (one per permutation) as well.
  if (triplesInserted_.contains(idTriple)) {
    eraseTripleInAllPermutations(triplesInserted_.at(idTriple));
    triplesInserted_.erase(idTriple);
    return;
  }
  // Locate the triple in one of the permutations (it does not matter which one)
  // to check if it actually exists in the index. If it does not exist, the
  // deletion is invalid, otherwise add as deleted triple.
  LocatedTriple locatedTriple = locateTripleInPermutation(
      idTriple[1], idTriple[0], idTriple[2], index_.getImpl().PSO());
  if (!locatedTriple.existsInIndex) {
    throw std::runtime_error(
        absl::StrCat("Triple \"", turtleTriple.toString(),
                     "\" does not exist in the original index",
                     ", the deletion has no effect"));
  }
  auto iterators = locateTripleInAllPermutations(idTriple);
  triplesDeleted_.insert({idTriple, iterators});
}

// ____________________________________________________________________________
const LocatedTriplesPerBlock& DeltaTriples::getTriplesWithPositionsPerBlock(
    Index::Permutation permutation) const {
  switch (permutation) {
    case Index::Permutation::PSO:
      return locatedTriplesPerBlockInPSO_;
    case Index::Permutation::POS:
      return locatedTriplesPerBlockInPOS_;
    case Index::Permutation::SPO:
      return locatedTriplesPerBlockInSPO_;
    case Index::Permutation::SOP:
      return locatedTriplesPerBlockInSOP_;
    case Index::Permutation::OSP:
      return locatedTriplesPerBlockInOSP_;
    case Index::Permutation::OPS:
      return locatedTriplesPerBlockInOPS_;
    default:
      AD_FAIL();
  }
}

// ____________________________________________________________________________
IdTriple DeltaTriples::getIdTriple(const TurtleTriple& turtleTriple) {
  TripleComponent subject = std::move(turtleTriple._subject);
  TripleComponent predicate = std::move(turtleTriple._predicate);
  TripleComponent object = std::move(turtleTriple._object);
  Id subjectId = std::move(subject).toValueId(index_.getVocab(), localVocab_);
  Id predId = std::move(predicate).toValueId(index_.getVocab(), localVocab_);
  Id objectId = std::move(object).toValueId(index_.getVocab(), localVocab_);
  return IdTriple{subjectId, predId, objectId};
}

// ____________________________________________________________________________
DeltaTriples::LocatedTripleHandles DeltaTriples::locateTripleInAllPermutations(
    const IdTriple& idTriple) {
  auto [s, p, o] = idTriple;
  LocatedTripleHandles handles;
  handles.forPSO = locatedTriplesPerBlockInPSO_.add(
      locateTripleInPermutation(p, s, o, index_.getImpl().PSO()));
  handles.forPOS = locatedTriplesPerBlockInPOS_.add(
      locateTripleInPermutation(p, o, s, index_.getImpl().POS()));
  handles.forSPO = locatedTriplesPerBlockInSPO_.add(
      locateTripleInPermutation(s, p, o, index_.getImpl().SPO()));
  handles.forSOP = locatedTriplesPerBlockInSOP_.add(
      locateTripleInPermutation(s, o, p, index_.getImpl().SOP()));
  handles.forOSP = locatedTriplesPerBlockInOSP_.add(
      locateTripleInPermutation(o, s, p, index_.getImpl().OSP()));
  handles.forOPS = locatedTriplesPerBlockInOPS_.add(
      locateTripleInPermutation(o, p, s, index_.getImpl().OPS()));
  return handles;
}

// ____________________________________________________________________________
template <typename Permutation>
LocatedTriple DeltaTriples::locateTripleInPermutation(
    Id id1, Id id2, Id id3, Permutation& permutation) const {
  // Get the internal data structures from the permutation.
  auto& file = permutation._file;
  const auto& meta = permutation._meta;
  const auto& reader = permutation._reader;

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
  LocatedTriple locatedTriple{
      blockIndex, std::numeric_limits<size_t>::max(), id1, id2, id3, false};

  // If all IDs from all blocks are smaller, we return the index of the last
  // block plus one (typical "end" semantics) and any position in the block (in
  // the code that uses the result, that position will not be used in this
  // case).
  if (matchingBlock == blocks.end()) {
    AD_CORRECTNESS_CHECK(blockIndex == blocks.size());
    return locatedTriple;
  }

  // Read and decompress the block. Note that we are potentially doing this a
  // second time here (the block has probably already been looked at in the call
  // to `std::lower_bound` above).
  DecompressedBlock blockTuples =
      reader.readAndDecompressBlock(*matchingBlock, file, std::nullopt);

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
    locatedTriple.rowIndexInBlock =
        std::lower_bound(blockTuples.begin() + offsetBegin,
                         blockTuples.begin() + offsetEnd,
                         std::array<Id, 2>{id2, id3},
                         [](const auto& a, const auto& b) {
                           return a[0] < b[0] || (a[0] == b[0] && a[1] < b[1]);
                         }) -
        blockTuples.begin();
    // Check if the triple at the found position is equal to `id1 id2 id3`. Note
    // that our default for `existsInIndex` was set to `false` above.
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
