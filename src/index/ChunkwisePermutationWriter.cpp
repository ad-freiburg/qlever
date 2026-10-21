// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/ChunkwisePermutationWriter.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <limits>

#include "engine/idTable/BitwiseKeySort.h"
#include "engine/idTable/ExternalIdTableSorterMergeConfig.h"
#include "index/CompressedRelationPermutationWriterImpl.h"
#include "util/Algorithm.h"
#include "util/Log.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

namespace chunkwisePermutationWriter {

using namespace compressedRelationHelpers;
using BlockToWrite = CompressedRelationWriter::BlockToWrite;
// The static helpers of the classical writer that are reused here.
using ClassicalWriter = CompressedRelationWriter::PermutationWriter<true>;

namespace detail {

// The runs of several sorters (see
// `CompressedExternalIdTableSorter::finishAndGetRunsInput`), combined into a
// single input for the parallel block merge (see the `InputConcept` in
// `util/parallelBlockMerge/RunsInputPolicy.h`). The runs are numbered
// consecutively, in the order of the sorters.
template <size_t N>
class CombinedRunsInput : public ad_utility::NoCopy {
 public:
  using Input = ad_utility::CompressedIdTableRunsInput<N>;
  using Block = typename Input::Block;
  using Element = typename Input::Element;
  using value_type = typename Input::value_type;

 private:
  std::vector<Input> inputs_;
  // `runOffsets_[i]` is the number of runs of all the inputs before the `i`-th
  // one, so the last entry is the total number of runs.
  std::vector<size_t> runOffsets_;

 public:
  explicit CombinedRunsInput(std::vector<Input> inputs)
      : inputs_{std::move(inputs)} {
    AD_CONTRACT_CHECK(!inputs_.empty());
    runOffsets_.push_back(0);
    for (const auto& input : inputs_) {
      runOffsets_.push_back(runOffsets_.back() + input.numRuns());
    }
  }

  // ___________________________________________________________________________
  size_t numRuns() const { return runOffsets_.back(); }

  // ___________________________________________________________________________
  size_t numBlocks(size_t run) const {
    auto [input, localRun] = locate(run);
    return input.numBlocks(localRun);
  }

  // ___________________________________________________________________________
  size_t numElementsInBlock(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.numElementsInBlock(localRun, block);
  }

  // ___________________________________________________________________________
  const Element& firstElement(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.firstElement(localRun, block);
  }

  // ___________________________________________________________________________
  const Element& lastElement(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.lastElement(localRun, block);
  }

  // ___________________________________________________________________________
  Block getBlock(size_t run, size_t block) const {
    auto [input, localRun] = locate(run);
    return input.getBlock(localRun, block);
  }

  // ___________________________________________________________________________
  Block makeEmptyBlock() const { return inputs_.front().makeEmptyBlock(); }
  void reserveBlock(Block& block, size_t numElements) const {
    inputs_.front().reserveBlock(block, numElements);
  }

  // ___________________________________________________________________________
  template <typename R>
  void appendToBlock(Block& block, const R& row) const {
    inputs_.front().appendToBlock(block, row);
  }

  // ___________________________________________________________________________
  template <typename R>
  ad_utility::MemorySize memorySizeOfElement(const R& row) const {
    return inputs_.front().memorySizeOfElement(row);
  }

 private:
  // Return the input that the `run` belongs to, and the index of the run
  // within that input.
  std::pair<const Input&, size_t> locate(size_t run) const {
    AD_CORRECTNESS_CHECK(run < numRuns());
    auto it = std::upper_bound(runOffsets_.begin(), runOffsets_.end(), run);
    size_t inputIdx = static_cast<size_t>(it - runOffsets_.begin()) - 1;
    return {inputs_.at(inputIdx), run - runOffsets_.at(inputIdx)};
  }
};
static_assert(
    ad_utility::parallelBlockMerge::InputConcept<CombinedRunsInput<0>>);

}  // namespace detail

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
  auto begin = block.begin();
  if (lastRowOfPreviousBlock_.has_value()) {
    begin = ql::ranges::find_if(
        block, [&previous = lastRowOfPreviousBlock_.value()](const auto& row) {
          return row != previous;
        });
  }
  block.erase(std::unique(begin, block.end()), block.end());
  block.erase(block.begin(), begin);
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

// _____________________________________________________________________________
ChunkwisePermutationWriter::ChunkwisePermutationWriter(
    PrivateTag, std::string basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2, qlever::KeyOrder permutation,
    std::vector<ConcurrentBlockCallback> callbacks, Options options)
    : basename_{std::move(basename)},
      writer1_{std::move(writerAndCallback1.writer_)},
      writer2_{std::move(writerAndCallback2.writer_)},
      writeMetadata_{std::move(writerAndCallback1.callback_),
                     std::move(writerAndCallback2.callback_),
                     writer1_->blocksize()},
      permutation_{std::move(permutation)},
      blocksize_{writer1_->blocksize()},
      numColumns_{writer1_->numColumns()},
      options_{options},
      callbacks_{std::move(callbacks)},
      executor_{ad_utility::globalExecutor()},
      callbackPermits_{
          executor_, std::max<size_t>(1, options.numCallbackBlocksInFlight_)} {
  // This logic only works for permutations that have the graph as the fourth
  // column.
  AD_CORRECTNESS_CHECK(permutation_.keys().at(3) == 3);
  AD_CORRECTNESS_CHECK(blocksize_ == writer2_->blocksize());
  AD_CORRECTNESS_CHECK(numColumns_ == writer2_->numColumns());
  AD_CORRECTNESS_CHECK(blocksize_ > 0);
  auto [c0, c1, c2, c3] = permutation_.keys();
  permutedColIndices_ = {c0, c1, c2};
  for (size_t colIdx = 3; colIdx < numColumns_; ++colIdx) {
    permutedColIndices_.push_back(colIdx);
  }
  // The blocks of small relations of `writer1_` are passed on to `writer2_`
  // (see `AddBlockOfSmallRelationsToSwitched`), which therefore also has to
  // use the same pool of block buffers.
  writer1_->smallBlocksCallback_ =
      CompressedRelationWriter::AddBlockOfSmallRelationsToSwitched{*writer2_};
  writer2_->shareBlockBufferPoolWith(*writer1_);
}

// _____________________________________________________________________________
std::shared_ptr<ChunkwisePermutationWriter> ChunkwisePermutationWriter::create(
    std::string basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2, qlever::KeyOrder permutation,
    std::vector<ConcurrentBlockCallback> callbacks, Options options) {
  return std::make_shared<ChunkwisePermutationWriter>(
      PrivateTag{}, std::move(basename), std::move(writerAndCallback1),
      std::move(writerAndCallback2), std::move(permutation),
      std::move(callbacks), options);
}

// _____________________________________________________________________________
std::shared_ptr<ChunkwisePermutationWriter>
ChunkwisePermutationWriter::makeSink(size_t numChunks) {
  std::lock_guard lock{mutex_};
  AD_CONTRACT_CHECK(numChunks_ == 0 && numChunks > 0,
                    "The sink of a chunkwise permutation writer can only be "
                    "created once");
  numChunks_ = numChunks;
  chunkWriters_.resize(numChunks);
  chunkResults_.resize(numChunks);
  chunkIsDone_.assign(numChunks, false);
  return shared_from_this();
}

// _____________________________________________________________________________
std::unique_ptr<TwinSorter> ChunkwisePermutationWriter::makeTwinSorter(
    const std::string& filename) const {
  auto sorter = std::make_unique<TwinSorter>(
      filename, numColumns_, options_.twinSorterMemoryPerChunk_,
      ad_utility::makeUnlimitedAllocator<Id>());
  // A parallelism of one makes the sorter merge serially in the consuming
  // thread, which is what a chunk needs: it runs on a thread of the pool and
  // must never wait for other tasks of that pool. (The runs of the sorters of
  // the boundary parts are merged by a parallel merge of the consolidation
  // instead, see `writeTwinOfBoundaryRelation`.)
  sorter->setMergeExecutor(executor_, 1);
  // The runs of the twin sorter only live for the duration of a chunk and are
  // read back right away, so they are compressed with the fastest level.
  using ad_utility::compressedExternalIdTable::MERGE_PHASE_SPILL_COMPRESSION;
  static_assert(MERGE_PHASE_SPILL_COMPRESSION.has_value());
  sorter->setRunCompressionLevel(MERGE_PHASE_SPILL_COMPRESSION.value());
  return sorter;
}

// _____________________________________________________________________________
ChunkWriter& ChunkwisePermutationWriter::chunkWriter(size_t chunkIndex) {
  // NOTE: The vector is never resized after `makeSink`, and the slot of a
  // chunk is only ever touched by the (single) operation of that chunk which
  // is currently in flight, so no lock is needed here.
  auto& writer = chunkWriters_.at(chunkIndex);
  if (writer == nullptr) {
    writer = std::make_unique<ChunkWriter>(*this, chunkIndex);
  }
  return *writer;
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::storeException(std::exception_ptr exception) {
  if (exception == nullptr) {
    return;
  }
  {
    std::lock_guard lock{mutex_};
    if (exception_ == nullptr) {
      exception_ = std::move(exception);
    }
  }
  requestStop();
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::requestStop() {
  stopRequested_.store(true);
  // Wake up the chunks that wait for a permit (they see the stop afterwards)
  // and the driving thread.
  callbackPermits_.cancel();
  conditionVariable_.notify_all();
}

// _____________________________________________________________________________
net::awaitable<bool> ChunkwisePermutationWriter::pushImpl(size_t chunkIndex,
                                                          Block block) {
  if (stopRequested()) {
    co_return false;
  }
  SharedBlock sharedBlock;
  try {
    auto timer = chunkTimer_.startMeasurement();
    sharedBlock = chunkWriter(chunkIndex).processBlock(std::move(block));
  } catch (...) {
    storeException(std::current_exception());
    co_return false;
  }
  if (sharedBlock != nullptr && !callbacks_.empty()) {
    auto [errorCode, permit] = co_await callbackPermits_.asyncAcquire(
        net::as_tuple(net::use_awaitable));
    if (errorCode || stopRequested()) {
      co_return false;
    }
    invokeCallbacks(std::move(sharedBlock), std::move(permit));
  }
  co_return !stopRequested();
}

// _____________________________________________________________________________
net::awaitable<bool> ChunkwisePermutationWriter::finishChunkImpl(
    size_t chunkIndex) {
  std::optional<ChunkResult> result;
  if (!stopRequested()) {
    try {
      auto timer = chunkTimer_.startMeasurement();
      result = chunkWriter(chunkIndex).finishChunk();
      // The chunk is done, so its writer (and the temporary file of its twin
      // sorter) can go.
      chunkWriters_.at(chunkIndex).reset();
    } catch (...) {
      storeException(std::current_exception());
    }
  }
  {
    std::lock_guard lock{mutex_};
    chunkResults_.at(chunkIndex) = std::move(result);
    chunkIsDone_.at(chunkIndex) = true;
  }
  conditionVariable_.notify_all();
  co_return !stopRequested();
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::invokeCallbacks(SharedBlock block,
                                                 Permit permit) {
  // The state that the `done` callbacks of all the callbacks of this block
  // share. The permit is given back (and the block is no longer in flight)
  // once the last of them is done. It keeps this writer alive, because a
  // `done` callback may run after the driving thread has finished.
  struct Join {
    std::shared_ptr<ChunkwisePermutationWriter> self_;
    Permit permit_;
    std::atomic<size_t> remaining_;
    Join(std::shared_ptr<ChunkwisePermutationWriter> self, Permit permit,
         size_t remaining)
        : self_{std::move(self)},
          permit_{std::move(permit)},
          remaining_{remaining} {
      std::lock_guard lock{self_->mutex_};
      ++self_->numCallbackBlocksInFlight_;
    }
  };
  auto join = std::make_shared<Join>(shared_from_this(), std::move(permit),
                                     callbacks_.size());
  auto done = [join](std::exception_ptr exception) {
    // NOTE: The copy of the `shared_ptr` keeps the writer alive until this
    // function has returned, in particular across the `notify_all` below,
    // after which the driving thread may destroy the writer at any time.
    auto self = join->self_;
    self->storeException(std::move(exception));
    if (join->remaining_.fetch_sub(1) == 1) {
      join->permit_ = Permit{};
      {
        std::lock_guard lock{self->mutex_};
        --self->numCallbackBlocksInFlight_;
      }
      self->conditionVariable_.notify_all();
    }
  };
  for (const auto& callback : callbacks_) {
    // NOTE: A callback that throws (instead of reporting its exception via
    // `done`) is treated as if it had reported the exception. It must not
    // throw after it has invoked `done`.
    try {
      callback(block, done);
    } catch (...) {
      done(std::current_exception());
    }
  }
}

// _____________________________________________________________________________
ChunkResult ChunkwisePermutationWriter::takeChunkResult(size_t chunkIndex) {
  consolidationWaitTimer_.cont();
  std::unique_lock lock{mutex_};
  auto chunkIsDoneOrMergeHasStopped = [this, chunkIndex]() {
    return chunkIsDone_.at(chunkIndex) || exception_ != nullptr ||
           stopRequested_.load();
  };
  while (!chunkIsDoneOrMergeHasStopped()) {
    // Wake up regularly to report the progress of the chunks.
    conditionVariable_.wait_for(lock, std::chrono::seconds(1));
    reportProgress();
  }
  consolidationWaitTimer_.stop();
  if (exception_ != nullptr) {
    std::rethrow_exception(exception_);
  }
  AD_CORRECTNESS_CHECK(chunkIsDone_.at(chunkIndex) && !stopRequested_.load(),
                       "The merge was stopped before chunk ", chunkIndex,
                       " was finished");
  auto result = std::move(chunkResults_.at(chunkIndex));
  chunkResults_.at(chunkIndex).reset();
  AD_CORRECTNESS_CHECK(result.has_value());
  return std::move(result).value();
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::reportProgress() {
  numRowsForProgressBar_ = numRowsProcessed_.load();
  // NOTE: The counter may have advanced by several batches since the last
  // report, hence the loop.
  while (progressBar_.update()) {
    AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
  }
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::waitForCallbacks() {
  std::unique_lock lock{mutex_};
  conditionVariable_.wait(lock,
                          [this]() { return numCallbackBlocksInFlight_ == 0; });
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::consolidate() {
  // The parts of the boundary relation that is currently open, in the order
  // of their chunks. All of them have the same `col0Id_`.
  std::vector<BoundaryPart> group;
  auto closeGroup = [this, &group]() {
    if (!group.empty()) {
      writeBoundaryRelation(std::move(group));
      group.clear();
    }
  };
  for (size_t chunkIndex = 0; chunkIndex < numChunks_; ++chunkIndex) {
    ChunkResult chunk = takeChunkResult(chunkIndex);
    consolidationWorkTimer_.cont();
    numRows_ += chunk.numRows_;
    numDistinctCol0_ += chunk.numMiddleRelations_;
    auto addToGroup = [&group, &closeGroup](BoundaryPart part) {
      if (!group.empty() && group.front().col0Id_ != part.col0Id_) {
        closeGroup();
      }
      group.push_back(std::move(part));
    };
    if (chunk.spansWholeChunk_) {
      // The single relation of the chunk continues in the next chunk, so the
      // group stays open.
      AD_CORRECTNESS_CHECK(chunk.firstPart_.has_value() &&
                           !chunk.lastPart_.has_value() &&
                           chunk.middleMetadata_.empty());
      addToGroup(std::move(chunk.firstPart_).value());
      consolidationWorkTimer_.stop();
      continue;
    }
    if (chunk.firstPart_.has_value()) {
      addToGroup(std::move(chunk.firstPart_).value());
    }
    // The first relation of the chunk (if any) is complete, because the
    // relations in the middle of the chunk follow. Those are not adjacent to
    // the boundary relations, so the block of small relations that the
    // consolidation currently buffers has to be flushed.
    closeGroup();
    writer1_->writeBufferedRelationsToSingleBlock();
    for (auto& [md1, md2] : chunk.middleMetadata_) {
      writeMetadata_(md1, md2);
    }
    if (chunk.lastPart_.has_value()) {
      addToGroup(std::move(chunk.lastPart_).value());
    }
    consolidationWorkTimer_.stop();
  }
  consolidationWorkTimer_.cont();
  closeGroup();
  consolidationWorkTimer_.stop();
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::writeBoundaryRelation(
    std::vector<BoundaryPart> parts) {
  AD_CORRECTNESS_CHECK(!parts.empty());
  const Id col0Id = parts.front().col0Id_;
  ++numDistinctCol0_;
  size_t numRows = 0;
  bool anyPartIsLarge = false;
  for (const auto& part : parts) {
    AD_CORRECTNESS_CHECK(part.col0Id_ == col0Id);
    numRows += part.numRows_;
    anyPartIsLarge = anyPartIsLarge || part.isLarge();
  }
  if (!anyPartIsLarge &&
      static_cast<double>(numRows) <= 0.8 * static_cast<double>(blocksize_)) {
    // The relation is small, so it is batched into a block of small
    // relations together with the other small boundary relations that are
    // adjacent to it (see `consolidate`).
    if (parts.size() == 1) {
      writer1_->addSmallRelation(col0Id, parts.front().rows_);
      return;
    }
    IdTable rows{numColumns_, parts.front().rows_.getAllocator()};
    rows.reserve(numRows);
    for (const auto& part : parts) {
      rows.insertAtEnd(part.rows_, 0, part.rows_.numRows());
    }
    writer1_->addSmallRelation(col0Id, rows);
    return;
  }

  // The relation is large. The blocks of small relations that are currently
  // buffered must not contain relations that lie on both sides of it.
  writer1_->writeBufferedRelationsToSingleBlock();
  // Write the rows that the parts keep in memory as blocks. The rows after
  // the last block of a part and the rows before the first block of the next
  // part are written together, because they may contain equal triples (when
  // disregarding the graph), which have to end up in the same block.
  IdTable pending = writer1_->takeBlockBuffer();
  DistinctIdCounter distinctCol1Counter;
  for (auto& part : parts) {
    distinctCol1Counter.addCountOfBlock(part.distinctCol1_);
    pending.insertAtEnd(part.rows_, 0, part.rows_.numRows());
    if (part.isLarge()) {
      if (!pending.empty()) {
        writeRowsOfLargeRelation(*writer1_, col0Id, std::move(pending), true);
        pending = writer1_->takeBlockBuffer();
      }
      if (part.tail_.has_value()) {
        pending.insertAtEnd(part.tail_.value(), 0, part.tail_->numRows());
      }
    }
  }
  if (!pending.empty()) {
    writeRowsOfLargeRelation(*writer1_, col0Id, std::move(pending), true);
  }
  auto md1 = makeMetadata(col0Id, numRows, distinctCol1Counter.getAndReset());
  auto md2 = writeTwinOfBoundaryRelation(col0Id, parts);
  AD_CORRECTNESS_CHECK(md2.numRows_ == numRows);
  writeMetadata_(md1, md2);
}

// _____________________________________________________________________________
CompressedRelationMetadata
ChunkwisePermutationWriter::writeTwinOfBoundaryRelation(
    Id col0Id, std::vector<BoundaryPart>& parts) {
  const size_t outputBlocksize = TWIN_SORTER_BLOCKSIZE_FACTOR * blocksize_;
  std::vector<TwinSorter*> sorters;
  for (auto& part : parts) {
    if (part.isLarge()) {
      sorters.push_back(part.twinSorter_.get());
    }
  }
  if (sorters.empty()) {
    // All the parts are small (but the relation as a whole is large), so all
    // the twin rows fit into memory and can be sorted there.
    IdTable twinRows{numColumns_, parts.front().rows_.getAllocator()};
    for (const auto& part : parts) {
      twinRows.insertAtEnd(part.rows_, 0, part.rows_.numRows());
    }
    twinRows.swapColumns(c1Idx, c2Idx);
    ad_utility::sortByBitwiseKeys(twinRows,
                                  ComparatorForConstCol0::bitwiseKeyColumns);
    std::vector<Block> blocks;
    blocks.push_back(std::move(twinRows));
    return writeSortedBlocksOfLargeRelation(
        *writer2_, col0Id, ad_utility::InputRangeTypeErased{std::move(blocks)},
        true);
  }
  // The runs of the large parts have been finished by their chunks (see
  // `BoundaryPart::twinRuns_`). The twin rows of the small parts are not in
  // any sorter yet, so they are sorted by a sorter of their own, whose single
  // run joins the merge.
  std::vector<ad_utility::CompressedIdTableRunsInput<0>> inputs;
  for (auto& part : parts) {
    if (part.isLarge()) {
      AD_CORRECTNESS_CHECK(part.twinRuns_.has_value());
      inputs.push_back(std::move(part.twinRuns_).value());
      part.twinRuns_.reset();
    }
  }
  std::unique_ptr<TwinSorter> sorterForSmallParts;
  for (const auto& part : parts) {
    if (!part.isLarge()) {
      if (sorterForSmallParts == nullptr) {
        sorterForSmallParts = makeTwinSorter(absl::StrCat(
            basename_, ".twin-boundary-small-parts.", col0Id.getBits()));
      }
      auto twinRows = part.rows_.asStaticView<0>();
      twinRows.swapColumns(c1Idx, c2Idx);
      sorterForSmallParts->pushBlock(twinRows);
    }
  }
  if (sorterForSmallParts != nullptr) {
    inputs.push_back(sorterForSmallParts->finishAndGetRunsInput<0>());
  }
  // The runs of all the parts are merged together, with a parallel merge that
  // is configured for the memory of the consolidation (the sorters themselves
  // are configured for the small memory of a single chunk). This runs on the
  // driving thread, which is not a thread of the pool, so the merge may be
  // parallel (unlike inside the chunks).
  detail::CombinedRunsInput<0> input{std::move(inputs)};
  using namespace ad_utility::compressedExternalIdTable;
  MergePhaseConfig config;
  config.numRuns_ = input.numRuns();
  config.numColumns_ = numColumns_;
  config.memoryLimit_ = options_.boundaryTwinMergeMemory_;
  config.inputBlockSizePerColumn_ = sorters.front()->inputBlockSizePerColumn();
  config.numBufferedOutputBlocks_ = 12;
  config.maxOutputBlockSize_ = ad_utility::MemorySize::gigabytes(1);
  config.parallelism_ = ad_utility::globalExecutorNumThreads();
  config.outputBlockSizeOverride_ = outputBlocksize;
  const auto parameters = computeMergePhaseParameters(config);
  auto mergeOptions = makeMergeOptions(config, parameters);
  auto storageFactory = makeMergePhaseBlockStorageFactory<0>(
      executor_,
      absl::StrCat(basename_, ".twin-boundary-spill.", col0Id.getBits()),
      ad_utility::makeUnlimitedAllocator<Id>(),
      parameters.numBufferedBlocksPerChunk_);
  auto merged = ad_utility::parallelBlockMerge::parallelBlockMergeToRange<true>(
      executor_, std::move(input), ComparatorForConstCol0{},
      std::move(storageFactory), std::move(mergeOptions),
      std::make_shared<ad_utility::CancellationHandle<>>());
  return writeSortedBlocksOfLargeRelation(*writer2_, col0Id, std::move(merged),
                                          true);
}

// _____________________________________________________________________________
void ChunkwisePermutationWriter::writeRowsOfLargeRelation(
    CompressedRelationWriter& writer, Id col0Id, IdTable rows, bool useQueue) {
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
  size_t begin = 0;
  while (begin < numRows) {
    size_t end = std::min(begin + blocksize, numRows);
    // Never split rows whose first three columns are equal across two blocks.
    while (end < numRows &&
           pickFirstThreeColumnsOfIdsWithoutLocalVocab(view[end]) ==
               pickFirstThreeColumnsOfIdsWithoutLocalVocab(view[end - 1])) {
      ++end;
    }
    write(BlockToWrite{view.subView(begin, end - begin), owner});
    begin = end;
  }
}

// _____________________________________________________________________________
CompressedRelationMetadata
ChunkwisePermutationWriter::writeSortedBlocksOfLargeRelation(
    CompressedRelationWriter& writer, Id col0Id,
    ad_utility::InputRangeTypeErased<Block> blocks, bool useQueue) {
  DistinctIdCounter distinctCol1Counter;
  size_t numRows = 0;
  auto writeBlock = [&](Block block) {
    numRows += block.numRows();
    distinctCol1Counter.addBlock(std::as_const(block).getColumn(c1Idx));
    writeRowsOfLargeRelation(writer, col0Id, std::move(block), useQueue);
  };
  // See `CompressedRelationWriter::addCompleteLargeRelation` for the
  // explanation of this loop, which makes sure that equal triples (when
  // disregarding the graph) stay in the same block.
  std::optional<Block> bufferedBlock;
  for (Block& block : blocks) {
    if (block.empty()) {
      continue;
    }
    if (!bufferedBlock.has_value()) {
      bufferedBlock = std::move(block);
      continue;
    }
    const auto& lastRowFromPrevious = bufferedBlock.value().back();
    const size_t upperBoundEqualTriples =
        ql::ranges::find_if(
            block,
            [&lastRowFromPrevious](const auto& row) {
              return pickFirstThreeColumnsOfIdsWithoutLocalVocab(
                         lastRowFromPrevious) !=
                     pickFirstThreeColumnsOfIdsWithoutLocalVocab(row);
            }) -
        block.begin();
    if (upperBoundEqualTriples > 0) {
      bufferedBlock->insertAtEnd(block, 0, upperBoundEqualTriples);
      block.erase(block.begin(), block.begin() + upperBoundEqualTriples);
    }
    if (block.empty()) {
      continue;
    }
    writeBlock(std::move(bufferedBlock).value());
    bufferedBlock = std::move(block);
  }
  if (bufferedBlock.has_value()) {
    writeBlock(std::move(bufferedBlock).value());
  }
  return makeMetadata(col0Id, numRows, distinctCol1Counter.getAndReset());
}

// _____________________________________________________________________________
CompressedRelationMetadata ChunkwisePermutationWriter::makeMetadata(
    Id col0Id, size_t numRows, size_t numDistinctCol1) {
  AD_CORRECTNESS_CHECK(numRows > 0);
  auto multiplicity =
      CompressedRelationWriter::computeMultiplicity(numRows, numDistinctCol1);
  return CompressedRelationMetadata{col0Id, numRows, multiplicity, multiplicity,
                                    std::numeric_limits<uint64_t>::max()};
}

// _____________________________________________________________________________
PermutationPairResult ChunkwisePermutationWriter::finish(
    std::future<void> mergeCompletion) {
  AD_CONTRACT_CHECK(numChunks_ > 0,
                    "The sink has to be created before `finish` is called");
  try {
    consolidate();
  } catch (...) {
    // The chunks that are still running are stopped, and we have to wait for
    // them (and for the callbacks) before the exception is propagated, because
    // they refer to the writers and to the callbacks.
    requestStop();
    mergeCompletion.wait();
    waitForCallbacks();
    throw;
  }
  mergeCompletion.get();
  waitForCallbacks();
  {
    std::lock_guard lock{mutex_};
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
  }
  writer1_->finish();
  writer2_->finish();

  if (options_.removeDuplicates_) {
    AD_LOG_INFO << "Number of inputs to `uniqueView`: " << numInputRows_.load()
                << '\n';
    AD_LOG_INFO << "Number of unique elements: " << numUniqueRows_.load()
                << std::endl;
  }
  numRowsForProgressBar_ = numRowsProcessed_.load();
  AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
  AD_CORRECTNESS_CHECK(numRows_ == numRowsProcessed_.load());
  AD_LOG_TIMING << "Total time of the chunks of the permutation writer "
                << ad_utility::Timer::toSeconds(chunkTimer_.msecs()) << "s"
                << std::endl;
  AD_LOG_TIMING << "Time the consolidation spent waiting for the chunks "
                << ad_utility::Timer::toSeconds(consolidationWaitTimer_.msecs())
                << "s" << std::endl;
  AD_LOG_TIMING << "Time the consolidation spent working "
                << ad_utility::Timer::toSeconds(consolidationWorkTimer_.msecs())
                << "s" << std::endl;
  return PermutationPairResult{numDistinctCol0_,
                               std::move(*writer1_).getFinishedBlocks(),
                               std::move(*writer2_).getFinishedBlocks()};
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
