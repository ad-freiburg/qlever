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

#include "index/CompressedRelationPermutationWriterImpl.h"
#include "index/chunkwisePermutationWriter/ChunkwisePermutationWriter.h"
#include "util/views/UniqueBlockView.h"

namespace chunkwisePermutationWriter {

using namespace compressedRelationHelpers;
using BlockToWrite = CompressedRelationWriter::BlockToWrite;
// The static helpers of the classical writer that are reused here.
using ClassicalWriter = CompressedRelationWriter::PermutationWriter<true>;

// _____________________________________________________________________________
ChunkWriter::ChunkWriter(ChunkwisePermutationWriter& shared, size_t chunkIndex)
    : shared_{shared},
      chunkIndex_{chunkIndex},
      isLastChunk_{chunkIndex + 1 == shared.numChunks_},
      writer1_{*shared.writer1_},
      writer2_{*shared.writer2_},
      blocksize_{shared.blocksize_},
      numColumns_{shared.numColumns_},
      relation_{writer1_.takeBlockBuffer()},
      inFirstRelation_{chunkIndex > 0},
      smallRelationsBuffer_{writer1_.takeBlockBuffer()} {}

// _____________________________________________________________________________
ChunkWriter::~ChunkWriter() {
  writer1_.blockBufferPool()->giveBack(std::move(relation_));
  writer1_.blockBufferPool()->giveBack(std::move(smallRelationsBuffer_));
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
  inputBlock_ = std::make_shared<const Block>(std::move(block));
  const auto& inputBlock = *inputBlock_;
  const size_t numRows = inputBlock.numRows();
  auto firstCol = inputBlock.getColumn(shared_.permutation_.keys().at(0));
  auto permutedCols =
      inputBlock.asColumnSubsetView(shared_.permutedColIndices_);
  if (!col0IdCurrentRelation_.has_value()) {
    // This can only happen for the very first block of the chunk; afterwards
    // the current relation is reset only by `writeCompleteSmallRelations`,
    // which also means that the first relation of the chunk is over.
    AD_CORRECTNESS_CHECK(!inFirstRelation_ || result_.numRows_ == 0);
    col0IdCurrentRelation_ = firstCol[0];
  }

  // See `CompressedRelationWriter::PermutationWriter::writePermutation` for
  // the explanation of this loop.
  size_t runBegin = 0;
  while (runBegin < numRows) {
    Id col0Id = firstCol[runBegin];
    if (col0Id != col0IdCurrentRelation_) {
      finishRelation();
      col0IdCurrentRelation_ = col0Id;
    }
    size_t runEnd = ClassicalWriter::findEndOfRun(firstCol, runBegin);
    if (isCompleteSmallRelation(runBegin, runEnd, numRows)) {
      runBegin = writeCompleteSmallRelations(permutedCols, firstCol, runBegin,
                                             runEnd, numRows);
    } else {
      addRowsOfCurrentRelation(permutedCols, runBegin, runEnd);
      runBegin = runEnd;
    }
  }
  result_.numRows_ += numRows;
  shared_.numRowsProcessed_.fetch_add(numRows);
  return std::exchange(inputBlock_, nullptr);
}

// _____________________________________________________________________________
template <typename PermutedCols>
void ChunkWriter::addRowsOfCurrentRelation(const PermutedCols& permutedCols,
                                           size_t begin, size_t end) {
  while (begin < end) {
    size_t chunkEnd;
    if (relation_.numRows() < blocksize_) {
      // The first block of the first relation of the chunk is kept in memory
      // (see `head_`), so it must never be written directly from the input
      // block. Note that this flag changes inside the loop, when the head is
      // captured by `addBlockForLargeRelation`.
      const bool mayWriteDirectly = !inFirstRelation_ || head_.has_value();
      std::optional<size_t> directBlockEnd = [&]() -> std::optional<size_t> {
        if (!mayWriteDirectly || !relation_.empty() ||
            end - begin < blocksize_) {
          return std::nullopt;
        }
        size_t blockEnd = ClassicalWriter::findFirstTripleChange(
            permutedCols, begin + blocksize_, end,
            pickFirstThreeColumnsOfIdsWithoutLocalVocab(
                permutedCols[begin + blocksize_ - 1]));
        if (blockEnd == end) {
          return std::nullopt;
        }
        return blockEnd;
      }();
      if (directBlockEnd.has_value()) {
        size_t blockEnd = directBlockEnd.value();
        addBlockOfLargeRelationWithoutCopying(
            permutedCols.subView(begin, blockEnd - begin));
        begin = blockEnd;
        continue;
      }
      chunkEnd = std::min(end, begin + (blocksize_ - relation_.numRows()));
    } else {
      chunkEnd = ClassicalWriter::findFirstTripleChange(
          permutedCols, begin, end,
          pickFirstThreeColumnsOfIdsWithoutLocalVocab(relation_.back()));
      if (chunkEnd == begin) {
        addBlockForLargeRelation();
        continue;
      }
    }
    relation_.insertAtEnd(permutedCols, begin, chunkEnd);
    begin = chunkEnd;
  }
}

// _____________________________________________________________________________
bool ChunkWriter::isCompleteSmallRelation(size_t begin, size_t end,
                                          size_t numRowsOfBlock) const {
  // The first relation of the chunk is never written as a small relation by
  // the chunk, because it may continue the last relation of the previous
  // chunk.
  return !inFirstRelation_ && relation_.empty() && numBlocksCurrentRel_ == 0 &&
         end < numRowsOfBlock &&
         static_cast<double>(end - begin) <=
             0.8 * static_cast<double>(blocksize_);
}

// _____________________________________________________________________________
template <typename PermutedCols, typename Col0>
size_t ChunkWriter::writeCompleteSmallRelations(
    const PermutedCols& permutedCols, const Col0& col0, size_t begin,
    size_t firstRunEnd, size_t numRowsOfBlock) {
  AD_CORRECTNESS_CHECK(begin < firstRunEnd);
  // See `CompressedRelationWriter::PermutationWriter` for the explanation.
  size_t capacity = numRowsUntilSmallRelationBlockIsFull();
  if (firstRunEnd - begin > capacity) {
    capacity = smallRelationBlockCapacity();
  }
  size_t end = firstRunEnd;
  size_t numRelations = 1;
  Id lastCol0Id = col0[begin];
  for (;;) {
    AD_CORRECTNESS_CHECK(end < numRowsOfBlock);
    size_t nextEnd = ClassicalWriter::findEndOfRun(col0, end);
    if (!isCompleteSmallRelation(end, nextEnd, numRowsOfBlock) ||
        nextEnd - begin > capacity) {
      break;
    }
    lastCol0Id = col0[end];
    end = nextEnd;
    ++numRelations;
  }
  result_.numMiddleRelations_ += numRelations;
  addSmallRelations(col0IdCurrentRelation_.value(), lastCol0Id, permutedCols,
                    begin, end);
  col0IdCurrentRelation_.reset();
  return end;
}

// _____________________________________________________________________________
size_t ChunkWriter::numRowsUntilSmallRelationBlockIsFull() const {
  size_t capacity = smallRelationBlockCapacity();
  size_t numBuffered = smallRelationsBuffer_.numRows();
  return numBuffered >= capacity ? 0 : capacity - numBuffered;
}

// _____________________________________________________________________________
template <typename Table>
void ChunkWriter::addSmallRelations(Id firstCol0Id, Id lastCol0Id,
                                    const Table& relations, size_t beginIdx,
                                    size_t endIdx) {
  AD_CORRECTNESS_CHECK(beginIdx < endIdx && endIdx <= relations.numRows());
  AD_CORRECTNESS_CHECK(firstCol0Id == relations(beginIdx, 0) &&
                       lastCol0Id == relations(endIdx - 1, 0));
  size_t numRows = endIdx - beginIdx;
  if (numRows + smallRelationsBuffer_.numRows() >
      smallRelationBlockCapacity()) {
    writeBufferedSmallRelationsToSingleBlock();
  }
  if (smallRelationsBuffer_.numRows() == 0) {
    currentSmallBlockFirstCol0_ = firstCol0Id;
  }
  currentSmallBlockLastCol0_ = lastCol0Id;
  smallRelationsBuffer_.insertAtEnd(relations, beginIdx, endIdx);
}

// _____________________________________________________________________________
void ChunkWriter::writeBufferedSmallRelationsToSingleBlock() {
  if (smallRelationsBuffer_.empty()) {
    return;
  }
  // The last argument invokes the `smallBlocksCallback_` of `writer1_`, which
  // writes the block to the twin permutation (in this thread).
  writer1_.compressAndWriteBlockInCallingThread(
      currentSmallBlockFirstCol0_, currentSmallBlockLastCol0_,
      BlockToWrite{std::move(smallRelationsBuffer_)}, true);
  smallRelationsBuffer_ = writer1_.takeBlockBuffer();
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
  // `writeCompleteSmallRelations`, so there is nothing left to do.
  if (!col0IdCurrentRelation_.has_value()) {
    AD_CORRECTNESS_CHECK(relation_.empty() && numBlocksCurrentRel_ == 0);
    return;
  }
  if (inFirstRelation_) {
    finishFirstRelation(false);
    return;
  }
  Id col0Id = col0IdCurrentRelation_.value();
  if (numBlocksCurrentRel_ > 0 || static_cast<double>(relation_.numRows()) >
                                      0.8 * static_cast<double>(blocksize_)) {
    // The relation is large.
    addBlockForLargeRelation();
    auto md1 = ChunkwisePermutationWriter::makeMetadata(
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
