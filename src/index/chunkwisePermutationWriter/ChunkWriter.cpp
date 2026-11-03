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

#include "index/compressedRelationWriter/CompressedRelationPermutationWriterImpl.h"
#include "util/views/UniqueBlockView.h"

namespace chunkwisePermutationWriter {

using namespace compressedRelationHelpers;
using BlockToWrite = CompressedRelationWriter::BlockToWrite;

// _____________________________________________________________________________
void ChunkWriterContext::connectTwinWriters(CompressedRelationWriter& writer1,
                                            CompressedRelationWriter& writer2) {
  AD_CONTRACT_CHECK(writer1.blocksize() == writer2.blocksize());
  AD_CONTRACT_CHECK(writer1.numColumns() == writer2.numColumns());
  writer1.smallBlocksCallback_ =
      CompressedRelationWriter::AddBlockOfSmallRelationsToSwitched{writer2};
  writer2.shareBlockBufferPoolWith(writer1);
}

// _____________________________________________________________________________
ChunkWriterContext::ChunkWriterContext(
    CompressedRelationWriter& writer1, CompressedRelationWriter& writer2,
    ColumnIndex col0Column, std::vector<ColumnIndex> permutedColIndices,
    bool removeDuplicates, std::string twinSorterBasename,
    ad_utility::MemorySize twinSorterMemoryPerChunk)
    : writer1_{writer1},
      writer2_{writer2},
      col0Column_{col0Column},
      permutedColIndices_{std::move(permutedColIndices)},
      removeDuplicates_{removeDuplicates},
      twinSorterBasename_{std::move(twinSorterBasename)},
      twinSorterMemoryPerChunk_{twinSorterMemoryPerChunk} {
  AD_CONTRACT_CHECK(permutedColIndices_.size() == writer1_.numColumns());
  connectTwinWriters(writer1_, writer2_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> ChunkWriterContext::makePermutedColIndices(
    const qlever::KeyOrder& permutation, size_t numColumns) {
  auto [c0, c1, c2, c3] = permutation.keys();
  AD_CORRECTNESS_CHECK(c3 == 3,
                       "The fourth column of a permutation must always be the "
                       "graph column, which is never permuted.");
  std::vector<ColumnIndex> permutedColIndices{c0, c1, c2};
  for (size_t colIdx = 3; colIdx < numColumns; ++colIdx) {
    permutedColIndices.push_back(colIdx);
  }
  return permutedColIndices;
}

// _____________________________________________________________________________
std::unique_ptr<TwinSorter> ChunkWriterContext::makeTwinSorter(
    const std::string& filename) const {
  auto sorter = std::make_unique<TwinSorter>(
      filename, numColumns(), twinSorterMemoryPerChunk_,
      ad_utility::makeUnlimitedAllocator<Id>());
  // A chunk runs on a thread of the pool and must never wait for other tasks
  // of that pool, so the sorter sorts, writes, and merges in the thread of the
  // chunk. (The runs of the sorters of the boundary parts are merged by a
  // parallel merge of the consolidation instead.)
  sorter->setDoAllWorkInCallingThread();
  return sorter;
}

// _____________________________________________________________________________
ChunkWriter::ChunkWriter(ChunkWriterContext& context, size_t chunkIndex,
                         size_t numChunks)
    : Base{context.writer1_.blocksize(), context.writer1_.takeBlockBuffer()},
      context_{context},
      chunkIndex_{chunkIndex},
      isLastChunk_{chunkIndex + 1 == numChunks},
      inFirstRelation_{chunkIndex > 0},
      smallRelations_{context.writer1_.takeBlockBuffer(), blocksize_} {}

// _____________________________________________________________________________
ChunkWriter::~ChunkWriter() {
  const auto& pool = context_.writer1_.blockBufferPool();
  pool->giveBack(std::move(relation_));
  // The `smallRelations_` are empty if the chunk was finished (see
  // `finishChunk`), but not if it was destroyed early (e.g. because of an
  // exception), in which case its buffer is simply freed.
  if (smallRelations_.empty()) {
    pool->giveBack(std::move(smallRelations_).releaseBuffer());
  }
}

// _____________________________________________________________________________
void ChunkWriter::removeDuplicatesIfNecessary(Block& block) {
  context_.numInputRows_.fetch_add(block.numRows());
  if (!context_.removeDuplicates_) {
    return;
  }
  ad_utility::removeDuplicatesFromBlock(block, lastRowOfPreviousBlock_);
  if (!block.empty()) {
    lastRowOfPreviousBlock_ = block.back();
  }
  context_.numUniqueRows_.fetch_add(block.numRows());
}

// _____________________________________________________________________________
SharedBlock ChunkWriter::processBlock(Block block) {
  AD_CORRECTNESS_CHECK(block.numColumns() == context_.numColumns());
  removeDuplicatesIfNecessary(block);
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
                      context_.col0Column_, context_.permutedColIndices_);
  result_.numRows_ += numRows;
  context_.numRowsProcessed_.fetch_add(numRows);
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
  auto block = smallRelations_.takeBlock(context_.writer1_.takeBlockBuffer());
  // The last argument invokes the `smallBlocksCallback_` of
  // `context_.writer1_`, which writes the block to the twin permutation (in
  // this thread).
  context_.writer1_.compressAndWriteBlockInCallingThread(
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
    twinSorter_ = context_.makeTwinSorter(
        absl::StrCat(context_.twinSorterBasename_, ".twin-sorter-chunk-",
                     chunkIndex_, "-", numTwinSortersCreated_++));
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
  context_.writer1_.compressAndWriteBlockInCallingThread(
      col0Id, col0Id, BlockToWrite{block, std::move(owner)}, false);
}

// _____________________________________________________________________________
void ChunkWriter::addBlockForLargeRelation() {
  if (relation_.empty()) {
    return;
  }
  if (sizeOfFirstRelationIsUndecided()) {
    // The first block of the first relation of the chunk is kept in memory,
    // because it has to be consolidated with the end of the previous chunk.
    // From now on the relation is known to be large, so its twin rows go to
    // the twin sorter.
    head_ = std::move(relation_);
    relation_ = context_.writer1_.takeBlockBuffer();
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
      context_.writer1_.blockBufferPool(), std::move(relation_));
  auto block = owner->template asStaticView<0>();
  relation_ = context_.writer1_.takeBlockBuffer();
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
    // `context_.writer1_`.
    addSmallRelations(col0Id, col0Id, relation_, 0, relation_.numRows());
  }
  ++result_.numMiddleRelations_;
  resetCurrentRelation();
}

// _____________________________________________________________________________
CompressedRelationMetadata ChunkWriter::writeTwinOfCompleteLargeRelation(
    Id col0Id) {
  auto& sorter = twinSorter();
  // NOTE: The sorter sorts and merges in this thread (see `makeTwinSorter`),
  // so this never waits for other tasks of the thread pool. No output block
  // size is requested: a relation whose twin rows fit into a single block of
  // the sorter (the common case) is then moved out as a whole instead of
  // being copied into blocks, and `writeRowsOfLargeRelation` slices it into
  // the blocks of the permutation without copying. For a relation with
  // several runs, the merge derives the size of its output blocks from the
  // memory of the sorter.
  auto md2 = writeSortedBlocksOfLargeRelation(
      context_.writer2_, col0Id, sorter.getSortedBlocks(std::nullopt));
  sorter.clear();
  return md2;
}

// _____________________________________________________________________________
void ChunkWriter::finishFirstRelation(bool reachesChunkEnd) {
  AD_CORRECTNESS_CHECK(inFirstRelation_ && col0IdCurrentRelation_.has_value());
  const Id col0Id = col0IdCurrentRelation_.value();
  std::optional<BoundaryPart> part;
  if (sizeOfFirstRelationIsUndecided()) {
    // The part is small, all its rows are still in the buffer.
    AD_CORRECTNESS_CHECK(numBlocksCurrentRel_ == 0 && !relation_.empty());
    countRowsOfCurrentRelation(relation_.asStaticView<0>());
    part.emplace(col0Id, std::move(relation_));
    relation_ = context_.writer1_.takeBlockBuffer();
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
        relation_ = context_.writer1_.takeBlockBuffer();
      }
    } else {
      // The relation ends inside this chunk, so its remaining rows are its
      // last block, which can be written directly.
      addBlockForLargeRelation();
    }
    part.emplace(col0Id, std::move(head_).value());
    head_.reset();
    part->rowsAfterWrittenBlocks_ = std::move(tail);
    part->twinSorter_ = std::move(twinSorter_);
    AD_CORRECTNESS_CHECK(part->twinSorter_ != nullptr);
  }
  part->numRows_ = numRowsCurrentRel_;
  part->distinctCol1_ = distinctCol1OfCurrentRelation();
  finishTwinSorterOfPart(part.value());
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
    relation_ = context_.writer1_.takeBlockBuffer();
  } else {
    // The first rows of the relation lie inside this chunk and have already
    // been written directly, so there are no rows before the first block.
    part.emplace(col0Id,
                 IdTable{context_.numColumns(), relation_.getAllocator()});
    if (!relation_.empty()) {
      auto tailView = relation_.asStaticView<0>();
      countRowsOfCurrentRelation(tailView);
      pushTwinRows(tailView);
      part->rowsAfterWrittenBlocks_ = std::move(relation_);
      relation_ = context_.writer1_.takeBlockBuffer();
    }
    part->twinSorter_ = std::move(twinSorter_);
    AD_CORRECTNESS_CHECK(part->twinSorter_ != nullptr);
  }
  part->numRows_ = numRowsCurrentRel_;
  part->distinctCol1_ = distinctCol1OfCurrentRelation();
  finishTwinSorterOfPart(part.value());
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
  return std::move(result_);
}

// _____________________________________________________________________________
void ChunkWriter::finishTwinSorterOfPart(BoundaryPart& part) {
  if (part.isLarge()) {
    part.twinRuns_ = part.twinSorter_->finishAndGetRunsInput<0>();
  }
}

// _____________________________________________________________________________
void ChunkWriter::writeRowsOfLargeRelation(CompressedRelationWriter& writer,
                                           Id col0Id, IdTable rows,
                                           bool useQueue) {
  const size_t numRows = rows.numRows();
  if (numRows == 0) {
    return;
  }
  auto write = [&writer, col0Id, useQueue](BlockToWrite block) {
    // These are blocks of a large relation, so the `smallBlocksCallback_` is
    // not invoked, hence the last argument is `false`.
    if (useQueue) {
      writer.compressAndWriteBlock(col0Id, col0Id, std::move(block), false);
    } else {
      writer.compressAndWriteBlockInCallingThread(col0Id, col0Id,
                                                  std::move(block), false);
    }
  };
  const size_t blocksize = writer.blocksize();
  if (numRows <= blocksize) {
    write(BlockToWrite{std::move(rows)});
    return;
  }
  // The slices are views into the `rows`, which are shared among them.
  auto owner = std::make_shared<const IdTable>(std::move(rows));
  auto view = owner->asStaticView<0>();
  forEachSliceOfLargeBlock(
      view, blocksize, [&write, &view, &owner](size_t begin, size_t end) {
        write(BlockToWrite{view.subView(begin, end - begin), owner});
      });
}

// _____________________________________________________________________________
CompressedRelationMetadata ChunkWriter::writeSortedBlocksOfLargeRelation(
    CompressedRelationWriter& writer, Id col0Id,
    ad_utility::InputRangeTypeErased<Block> blocks) {
  DistinctIdCounter distinctCol1Counter;
  size_t numRows = 0;
  auto writeBlock = [&](Block block) {
    numRows += block.numRows();
    distinctCol1Counter.addBlock(std::as_const(block).getColumn(c1Idx));
    writeRowsOfLargeRelation(writer, col0Id, std::move(block), false);
  };
  // Equal triples (when disregarding the graph) have to stay in the same
  // block, see `TripleAlignedBlockBuffer`.
  TripleAlignedBlockBuffer<Block> alignedBlocks;
  for (Block& block : blocks) {
    if (auto complete = alignedBlocks.push(std::move(block))) {
      writeBlock(std::move(complete).value());
    }
  }
  if (auto last = alignedBlocks.finish()) {
    writeBlock(std::move(last).value());
  }
  return CompressedRelationWriter::makeLargeRelationMetadata(
      col0Id, numRows, distinctCol1Counter.getAndReset());
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
