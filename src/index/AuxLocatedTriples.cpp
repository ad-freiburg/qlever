// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/AuxLocatedTriples.h"

#include "backports/algorithm.h"
#include "global/Constants.h"
#include "index/GraphComputation.h"
#include "util/Exception.h"

namespace {
// Compare two permuted triples ignoring their graph, which is how a triple is
// attributed to a block, see `LocatedTriple::locateTriplesInPermutation`.
bool lessWithoutGraph(const CompressedBlockMetadata::PermutedTriple& a,
                      const CompressedBlockMetadata::PermutedTriple& b) {
  return a.tieWithoutGraph() < b.tieWithoutGraph();
}
}  // namespace

// ____________________________________________________________________________
AuxLocatedTriples::AuxLocatedTriples(
    std::shared_ptr<const AuxIndex> auxIndex, Permutation::Enum permutation,
    bool isInternal, std::shared_ptr<const BlockMetadata> mainBlockMetadata)
    : auxIndex_{std::move(auxIndex)},
      permutation_{permutation},
      isInternal_{isInternal},
      mainBlockMetadata_{std::move(mainBlockMetadata)} {
  AD_CONTRACT_CHECK(auxIndex_ != nullptr);
  AD_CONTRACT_CHECK(mainBlockMetadata_ != nullptr);
  auxBlockMetadata_ = auxIndex_->getPermutation(permutation_, isInternal_)
                          .metaData()
                          .blockData();
  computeBlockInfos();
}

// ____________________________________________________________________________
std::pair<size_t, size_t> AuxLocatedTriples::auxBlockRange(
    size_t mainBlockIndex) const {
  const auto& mainBlocks = *mainBlockMetadata_;
  AD_CONTRACT_CHECK(mainBlockIndex <= mainBlocks.size());
  // A triple `t` belongs to the block of the main index with the index
  // `mainBlockIndex` if and only if `lastTriple_[mainBlockIndex - 1] < t <=
  // lastTriple_[mainBlockIndex]` (compared without the graph), where the lower
  // bound is missing for the first block and the upper bound is missing for the
  // synthetic block after the last one.
  //
  // A block of the auxiliary index can contain such a triple if and only if its
  // last triple is greater than the lower bound and its first triple is not
  // greater than the upper bound.
  size_t first = 0;
  if (mainBlockIndex > 0) {
    const auto& lowerBound = mainBlocks.at(mainBlockIndex - 1).lastTriple_;
    first = ql::ranges::upper_bound(auxBlockMetadata_, lowerBound,
                                    &lessWithoutGraph,
                                    &CompressedBlockMetadata::lastTriple_) -
            auxBlockMetadata_.begin();
  }
  size_t last = auxBlockMetadata_.size();
  if (mainBlockIndex < mainBlocks.size()) {
    const auto& upperBound = mainBlocks.at(mainBlockIndex).lastTriple_;
    last = ql::ranges::upper_bound(auxBlockMetadata_, upperBound,
                                   &lessWithoutGraph,
                                   &CompressedBlockMetadata::firstTriple_) -
           auxBlockMetadata_.begin();
  }
  // The two bounds can cross when no block of the auxiliary index falls into
  // the range at all.
  return {first, std::max(first, last)};
}

// ____________________________________________________________________________
void AuxLocatedTriples::computeBlockInfos() {
  if (auxBlockMetadata_.empty()) {
    return;
  }
  // A single sequential pass over the whole permutation of the auxiliary index.
  // The triples arrive in ascending order, so the blocks of the main index that
  // they are attributed to are ascending as well, which means that the first
  // and the last triple of each of them are simply the first and the last
  // triple that is seen for it.
  const auto& mainBlocks = *mainBlockMetadata_;
  auto keyOrder = Permutation::toKeyOrder(permutation_);
  auto inverse = keyOrder.inverse();
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  auto [reader, blocks] =
      auxIndex_->scanFull(permutation_, isInternal_, handle);
  for (const IdTable& block : blocks) {
    for (size_t row = 0; row < block.numRows(); ++row) {
      IdTriple<0> permutedTriple{std::array<Id, 4>{
          block(row, 0), block(row, 1), block(row, 2), block(row, 3)}};
      bool isInsert =
          block(row, AuxIndex::insertOrDeleteColumn) == AuxIndex::insertedId();
      size_t mainBlockIndex = LocatedTriple::locateTripleInPermutation(
          permutedTriple.permute(inverse), mainBlocks, keyOrder);
      auto triple = permutedTriple.toPermutedTriple();
      auto [it, inserted] = blockInfos_.try_emplace(mainBlockIndex);
      auto& info = it->second;
      if (inserted) {
        info.firstTriple_ = triple;
      }
      info.lastTriple_ = triple;
      ++info.numTriples_;
      if (isInsert && info.graphsOfInsertedTriples_.has_value()) {
        info.graphsOfInsertedTriples_ =
            computeDistinctGraphs(ql::span<const Id>{&triple.graphId_, 1},
                                  info.graphsOfInsertedTriples_.value());
      }
    }
  }
}

// ____________________________________________________________________________
const AuxLocatedTriples::BlockInfo* AuxLocatedTriples::blockInfo(
    size_t mainBlockIndex) const {
  auto it = blockInfos_.find(mainBlockIndex);
  return it == blockInfos_.end() ? nullptr : &it->second;
}

// ____________________________________________________________________________
size_t AuxLocatedTriples::numTriples(size_t mainBlockIndex) const {
  const auto* info = blockInfo(mainBlockIndex);
  return info == nullptr ? 0 : info->numTriples_;
}

// ____________________________________________________________________________
std::shared_ptr<const AuxLocatedTriples::AuxBlock>
AuxLocatedTriples::getAuxBlock(size_t auxBlockIndex) const {
  {
    auto cache = blockCache_.wlock();
    if (auto cached = cache->tryGet(auxBlockIndex)) {
      return cached.value();
    }
  }
  // Read the single block. Note that this deliberately happens without holding
  // the lock on the cache: two threads may then read the same block
  // concurrently, which wastes a little work but is much better than
  // serializing all reads.
  const auto& permutation =
      auxIndex_->getPermutation(permutation_, isInternal_);
  ql::span<const CompressedBlockMetadata> singleBlock{
      auxBlockMetadata_.data() + auxBlockIndex, 1};
  Permutation::ScanSpecAndBlocks scanSpecAndBlocks{
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
      BlockMetadataRanges{{singleBlock.begin(), singleBlock.end()}}};
  std::vector<ColumnIndex> additionalColumns{ADDITIONAL_COLUMN_GRAPH_ID,
                                             AuxIndex::insertOrDeleteColumn};
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  IdTable table = permutation.scan(scanSpecAndBlocks, additionalColumns, handle,
                                   auxIndex_->emptyLocatedTriplesState());

  auto block = std::make_shared<AuxBlock>();
  block->reserve(table.numRows());
  for (size_t row = 0; row < table.numRows(); ++row) {
    block->push_back(
        {IdTriple<0>{std::array<Id, 4>{table(row, 0), table(row, 1),
                                       table(row, 2), table(row, 3)}},
         table(row, AuxIndex::insertOrDeleteColumn) == AuxIndex::insertedId()});
  }
  blockCache_.wlock()->insert(auxBlockIndex, block);
  return block;
}

// ____________________________________________________________________________
std::vector<LocatedTriple> AuxLocatedTriples::getTriplesForBlock(
    size_t mainBlockIndex) const {
  std::vector<LocatedTriple> result;
  const auto* info = blockInfo(mainBlockIndex);
  if (info == nullptr) {
    return result;
  }
  result.reserve(info->numTriples_);
  auto [first, last] = auxBlockRange(mainBlockIndex);
  const auto& mainBlocks = *mainBlockMetadata_;
  // The blocks of the auxiliary index that were selected via the block metadata
  // may also contain triples that belong to a *different* block of the main
  // index, so each triple has to be attributed exactly. Note that the triples
  // of the auxiliary index are already in the order of the permutation, whereas
  // `locateTripleInPermutation` expects the order `(S, P, O, G)`, so the
  // permutation has to be undone first.
  auto inverseKeyOrder = Permutation::toKeyOrder(permutation_).inverse();
  for (size_t i = first; i < last; ++i) {
    auto auxBlock = getAuxBlock(i);
    for (const auto& auxTriple : *auxBlock) {
      auto blockIndex = LocatedTriple::locateTripleInPermutation(
          auxTriple.triple_.permute(inverseKeyOrder), mainBlocks,
          Permutation::toKeyOrder(permutation_));
      if (blockIndex == mainBlockIndex) {
        result.push_back(
            {mainBlockIndex, auxTriple.triple_, auxTriple.insertOrDelete_});
      }
    }
  }
  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(result));
  AD_CORRECTNESS_CHECK(result.size() == info->numTriples_);
  return result;
}
