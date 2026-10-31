// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/chunkwisePermutationWriter/ChunkWriter.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/strings/str_cat.h>

#include <algorithm>

#include "index/chunkwisePermutationWriter/ChunkwisePermutationWriter.h"
#include "util/views/UniqueBlockView.h"

namespace chunkwisePermutationWriter {

using namespace compressedRelationHelpers;
using BlockToWrite = CompressedRelationWriter::BlockToWrite;

// _____________________________________________________________________________
ChunkWriter::ChunkWriter(ChunkwisePermutationWriter& shared, size_t chunkIndex)
    : Base{shared.blocksize_, shared.writer1_->takeBlockBuffer()},
      shared_{shared},
      chunkIndex_{chunkIndex},
      isLastChunk_{chunkIndex + 1 == shared.numChunks_},
      writer1_{*shared.writer1_},
      writer2_{*shared.writer2_},
      numColumns_{shared.numColumns_},
      inFirstRelation_{chunkIndex > 0},
      smallRelations_{writer1_.takeBlockBuffer(), blocksize_} {}

// _____________________________________________________________________________
ChunkWriter::~ChunkWriter() {
  // Note: The buffer of the `smallRelations_` can't be taken out of the
  // builder, so it is simply freed.
  writer1_.blockBufferPool()->giveBack(std::move(relation_));
}

// _____________________________________________________________________________
void ChunkWriter::removeDuplicates(Block& block) {
  shared_.numInputRows_.fetch_add(block.numRows());
  ad_utility::removeDuplicatesFromBlock(block, lastRowOfPreviousBlock_);
  if (!block.empty()) {
    lastRowOfPreviousBlock_ = block.back();
  }
  shared_.numUniqueRows_.fetch_add(block.numRows());
}

// _____________________________________________________________________________
SharedBlock ChunkWriter::processBlock(Block block) {
  AD_CORRECTNESS_CHECK(block.numColumns() == numColumns_);
  if (shared_.options_.removeDuplicates_) {
    removeDuplicates(block);
  } else {
    shared_.numInputRows_.fetch_add(block.numRows());
  }
  if (block.empty()) {
    return nullptr;
  }
  if (!col0IdCurrentRelation_.has_value()) {
    // This can only happen for the very first block of the chunk; afterwards
    // the current relation is reset only by
    // `RelationBlockCutter::writeCompleteSmallRelations`, which also means
    // that the first relation of the chunk is over.
    AD_CORRECTNESS_CHECK(!inFirstRelation_ || result_.numRows_ == 0);
  }
  const size_t numRows = block.numRows();
  this->addInputBlock(std::make_shared<const Block>(std::move(block)),
                      shared_.permutation_.keys().at(0),
                      shared_.permutedColIndices_);
  result_.numRows_ += numRows;
  shared_.numRowsProcessed_.fetch_add(numRows);
  return std::exchange(inputBlock_, nullptr);
}

// _____________________________________________________________________________
template <typename PermutedCols>
void ChunkWriter::addCompleteSmallRelations(Id firstCol0Id, Id lastCol0Id,
                                            const PermutedCols& permutedCols,
                                            size_t begin, size_t end,
                                            size_t numRelations) {
  result_.numMiddleRelations_ += numRelations;
  addSmallRelations(firstCol0Id, lastCol0Id, permutedCols, begin, end);
}

// _____________________________________________________________________________
template <typename Table>
void ChunkWriter::addSmallRelations(Id firstCol0Id, Id lastCol0Id,
                                    const Table& relations, size_t beginIdx,
                                    size_t endIdx) {
  AD_CORRECTNESS_CHECK(beginIdx < endIdx);
  if (!smallRelations_.fits(endIdx - beginIdx)) {
    writeBufferedSmallRelationsToSingleBlock();
  }
  smallRelations_.add(firstCol0Id, lastCol0Id, relations, beginIdx, endIdx);
}

// _____________________________________________________________________________
void ChunkWriter::writeBufferedSmallRelationsToSingleBlock() {
  if (smallRelations_.empty()) {
    return;
  }
  auto block = smallRelations_.takeBlock(writer1_.takeBlockBuffer());
  // The last argument invokes the `smallBlocksCallback_` of `writer1_`, which
  // writes the block to the twin permutation (in this thread).
  writer1_.compressAndWriteBlockInCallingThread(
      block.firstCol0_, block.lastCol0_, BlockToWrite{std::move(block.rows_)},
      true);
}

// _____________________________________________________________________________
void ChunkWriter::countRowsOfCurrentRelation(IdTableView<0> rows) {
  AD_CORRECTNESS_CHECK(!rows.empty());
  numRowsCurrentRel_ += rows.numRows();
  auto col1 = rows.getColumn(c1Idx);
  distinctCol1Counter_.addBlock(col1);
  if (!firstCol1CurrentRel_.has_value()) {
    firstCol1CurrentRel_ = col1.front();
  }
  lastCol1CurrentRel_ = col1.back();
}

// _____________________________________________________________________________
TwinSorter& ChunkWriter::twinSorter() {
  if (twinSorter_ == nullptr) {
    twinSorter_ = shared_.makeTwinSorter(
        absl::StrCat(shared_.basename_, ".twin-sorter-chunk-", chunkIndex_, "-",
                     numTwinSortersCreated_++));
  }
  return *twinSorter_;
}

// _____________________________________________________________________________
void ChunkWriter::pushTwinRows(IdTableView<0> rows) {
  IdTableView<0> twinRows = rows;
  twinRows.swapColumns(c1Idx, c2Idx);
  twinSorter().pushBlock(twinRows);
}

// _____________________________________________________________________________
void ChunkWriter::writeBlockOfLargeRelation(IdTableView<0> block,
                                            BlockToWrite::Owner owner) {
  AD_CORRECTNESS_CHECK(!block.empty());
  Id col0Id = col0IdCurrentRelation_.value();
  // A block of small relations must not contain relations on both sides of a
  // large relation, so the buffered small relations are written first.
  writeBufferedSmallRelationsToSingleBlock();
  ++numBlocksCurrentRel_;
  countRowsOfCurrentRelation(block);
  pushTwinRows(block);
  // This is a block of a large relation, so the `smallBlocksCallback_` is not
  // invoked, hence the last argument is `false`.
  writer1_.compressAndWriteBlockInCallingThread(
      col0Id, col0Id, BlockToWrite{block, std::move(owner)}, false);
}

// _____________________________________________________________________________
void ChunkWriter::addBlockForLargeRelation() {
  if (relation_.empty()) {
    return;
  }
  if (inFirstRelation_ && !head_.has_value()) {
    // The first block of the first relation of the chunk is kept in memory,
    // because it has to be consolidated with the end of the previous chunk.
    // From now on the relation is known to be large, so its twin rows go to
    // the twin sorter.
    head_ = std::move(relation_);
    relation_ = writer1_.takeBlockBuffer();
    // See `writeBlockOfLargeRelation`. Note that this cannot actually have an
    // effect, because the first relation of the chunk precedes all its small
    // relations, but it keeps the invariant obvious.
    writeBufferedSmallRelationsToSingleBlock();
    ++numBlocksCurrentRel_;
    auto headView = head_->asStaticView<0>();
    countRowsOfCurrentRelation(headView);
    pushTwinRows(headView);
    return;
  }
  auto owner = CompressedRelationWriter::BlockBufferPool::makeRecyclingOwner(
      writer1_.blockBufferPool(), std::move(relation_));
  auto block = owner->template asStaticView<0>();
  relation_ = writer1_.takeBlockBuffer();
  writeBlockOfLargeRelation(block, std::move(owner));
}

// _____________________________________________________________________________
void ChunkWriter::addBlockOfLargeRelationWithoutCopying(IdTableView<0> block) {
  AD_CORRECTNESS_CHECK(relation_.empty() && !block.empty());
  // The rows of the `block` are owned by the `inputBlock_`, which stays alive
  // until the block has been written (which happens synchronously).
  writeBlockOfLargeRelation(block, inputBlock_);
}

// _____________________________________________________________________________
DistinctIdCountOfBlock ChunkWriter::distinctCol1OfCurrentRelation() {
  AD_CORRECTNESS_CHECK(firstCol1CurrentRel_.has_value());
  return {distinctCol1Counter_.getAndReset(), firstCol1CurrentRel_.value(),
          lastCol1CurrentRel_};
}

// _____________________________________________________________________________
void ChunkWriter::resetCurrentRelation() {
  col0IdCurrentRelation_.reset();
  numBlocksCurrentRel_ = 0;
  numRowsCurrentRel_ = 0;
  distinctCol1Counter_.reset();
  firstCol1CurrentRel_.reset();
  lastCol1CurrentRel_ = Id::makeUndefined();
  relation_.clear();
  head_.reset();
}

// _____________________________________________________________________________
void ChunkWriter::finishRelation() {
  // The relation was already written completely by
  // `RelationBlockCutter::writeCompleteSmallRelations`, so there is nothing
  // left to do.
  if (!col0IdCurrentRelation_.has_value()) {
    AD_CORRECTNESS_CHECK(relation_.empty() && numBlocksCurrentRel_ == 0);
    return;
  }
  if (inFirstRelation_) {
    finishFirstRelation(false);
    return;
  }
  Id col0Id = col0IdCurrentRelation_.value();
  if (this->currentRelationIsLarge()) {
    // The relation is large.
    addBlockForLargeRelation();
    auto md1 = CompressedRelationWriter::makeLargeRelationMetadata(
        col0Id, numRowsCurrentRel_, distinctCol1Counter_.getAndReset());
    auto md2 = writeTwinOfCompleteLargeRelation(col0Id);
    result_.middleMetadata_.emplace_back(md1, md2);
  } else {
    // Small relations are batched into blocks, and no metadata is stored for
    // them. The twin permutation is handled by the `smallBlocksCallback_` of
    // `writer1_`.
    addSmallRelations(col0Id, col0Id, relation_, 0, relation_.numRows());
  }
  ++result_.numMiddleRelations_;
  resetCurrentRelation();
}

// _____________________________________________________________________________
CompressedRelationMetadata ChunkWriter::writeTwinOfCompleteLargeRelation(
    Id col0Id) {
  auto& sorter = twinSorter();
  // NOTE: The sorter merges serially in this thread (see `makeTwinSorter`),
  // so this never waits for other tasks of the thread pool. No output block
  // size is requested: a relation whose twin rows fit into a single block of
  // the sorter (the common case) is then moved out as a whole instead of
  // being copied into blocks, and `writeRowsOfLargeRelation` slices it into
  // the blocks of the permutation without copying. For a relation with
  // several runs, the merge derives the size of its output blocks from the
  // memory of the sorter.
  auto md2 = ChunkwisePermutationWriter::writeSortedBlocksOfLargeRelation(
      writer2_, col0Id, sorter.getSortedBlocks(std::nullopt), false);
  sorter.clear();
  return md2;
}

// _____________________________________________________________________________
void ChunkWriter::finishFirstRelation(bool reachesChunkEnd) {
  AD_CORRECTNESS_CHECK(inFirstRelation_ && col0IdCurrentRelation_.has_value());
  const Id col0Id = col0IdCurrentRelation_.value();
  std::optional<BoundaryPart> part;
  if (!head_.has_value()) {
    // The part is small, all its rows are still in the buffer.
    AD_CORRECTNESS_CHECK(numBlocksCurrentRel_ == 0 && !relation_.empty());
    countRowsOfCurrentRelation(relation_.asStaticView<0>());
    part.emplace(col0Id, std::move(relation_));
    relation_ = writer1_.takeBlockBuffer();
  } else {
    // NOTE: The remaining rows are handled before the head is moved out, as
    // `addBlockForLargeRelation` would otherwise capture them as a new head.
    std::optional<IdTable> tail;
    if (reachesChunkEnd) {
      if (!relation_.empty()) {
        auto tailView = relation_.asStaticView<0>();
        countRowsOfCurrentRelation(tailView);
        pushTwinRows(tailView);
        tail = std::move(relation_);
        relation_ = writer1_.takeBlockBuffer();
      }
    } else {
      // The relation ends inside this chunk, so its remaining rows are its
      // last block, which can be written directly.
      addBlockForLargeRelation();
    }
    part.emplace(col0Id, std::move(head_).value());
    head_.reset();
    part->tail_ = std::move(tail);
    part->twinSorter_ = std::move(twinSorter_);
    AD_CORRECTNESS_CHECK(part->twinSorter_ != nullptr);
  }
  part->numRows_ = numRowsCurrentRel_;
  part->distinctCol1_ = distinctCol1OfCurrentRelation();
  AD_CORRECTNESS_CHECK(!result_.firstPart_.has_value());
  result_.firstPart_ = std::move(part);
  result_.spansWholeChunk_ = reachesChunkEnd;
  inFirstRelation_ = false;
  resetCurrentRelation();
}

// _____________________________________________________________________________
void ChunkWriter::finishLastRelation() {
  AD_CORRECTNESS_CHECK(!inFirstRelation_ && col0IdCurrentRelation_.has_value());
  const Id col0Id = col0IdCurrentRelation_.value();
  std::optional<BoundaryPart> part;
  if (numBlocksCurrentRel_ == 0) {
    // No block has been written yet, so the part is small (even if the
    // relation as a whole might turn out to be large).
    AD_CORRECTNESS_CHECK(!relation_.empty());
    countRowsOfCurrentRelation(relation_.asStaticView<0>());
    part.emplace(col0Id, std::move(relation_));
    relation_ = writer1_.takeBlockBuffer();
  } else {
    // The first rows of the relation lie inside this chunk and have already
    // been written directly, so there are no rows before the first block.
    part.emplace(col0Id, IdTable{numColumns_, relation_.getAllocator()});
    if (!relation_.empty()) {
      auto tailView = relation_.asStaticView<0>();
      countRowsOfCurrentRelation(tailView);
      pushTwinRows(tailView);
      part->tail_ = std::move(relation_);
      relation_ = writer1_.takeBlockBuffer();
    }
    part->twinSorter_ = std::move(twinSorter_);
    AD_CORRECTNESS_CHECK(part->twinSorter_ != nullptr);
  }
  part->numRows_ = numRowsCurrentRel_;
  part->distinctCol1_ = distinctCol1OfCurrentRelation();
  AD_CORRECTNESS_CHECK(!result_.lastPart_.has_value());
  result_.lastPart_ = std::move(part);
  resetCurrentRelation();
}

// _____________________________________________________________________________
ChunkResult ChunkWriter::finishChunk() {
  if (col0IdCurrentRelation_.has_value()) {
    // The relation that is still open at the end of the chunk is complete if
    // this is the last chunk, and otherwise continues in the next chunk.
    if (inFirstRelation_) {
      finishFirstRelation(!isLastChunk_);
    } else if (isLastChunk_) {
      finishRelation();
    } else {
      finishLastRelation();
    }
  }
  writeBufferedSmallRelationsToSingleBlock();
  // Finish the input phase of the twin sorters of the boundary parts (which
  // sorts and writes their last block), so that the parts only hold the runs
  // on disk while they wait for the consolidation, see
  // `BoundaryPart::twinRuns_`.
  for (auto* part : {&result_.firstPart_, &result_.lastPart_}) {
    if (part->has_value() && part->value().isLarge()) {
      part->value().twinRuns_ =
          part->value().twinSorter_->finishAndGetRunsInput<0>();
    }
  }
  return std::move(result_);
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
