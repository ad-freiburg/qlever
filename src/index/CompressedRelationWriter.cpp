// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2024 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025        Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026        Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/CompressedRelationWriter.h"

#include <algorithm>
#include <cmath>
#include <thread>

#include "global/RuntimeParameters.h"
#include "index/CompressedRelationHelpersImpl.h"
#include "index/CompressedRelationPermutationWriterImpl.h"
#include "index/GraphComputation.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/GlobalExecutor.h"

// ____________________________________________________________________________
float CompressedRelationWriter::computeMultiplicity(
    size_t numElements, size_t numDistinctElements) {
  bool functional = numElements == numDistinctElements;
  float multiplicity =
      functional ? 1.0f
                 : static_cast<float>(numElements) / float(numDistinctElements);
  // Ensure that the multiplicity is only exactly 1.0 if the relation is
  // indeed functional to prevent numerical instabilities;
  if (!functional && multiplicity == 1.0f) [[unlikely]] {
    multiplicity = std::nextafter(1.0f, 2.0f);
  }
  return multiplicity;
}

// ___________________________________________________________________________
void CompressedRelationWriter::writeBufferedRelationsToSingleBlock() {
  if (smallRelationsBuffer_.empty()) {
    return;
  }

  AD_CORRECTNESS_CHECK(smallRelationsBuffer_.numColumns() == numColumns());
  // We write small relations to a single block, so we specify the last
  // argument to `true` to invoke the `smallBlocksCallback_`.
  compressAndWriteBlock(currentBlockFirstCol0_, currentBlockLastCol0_,
                        std::move(smallRelationsBuffer_), true);
  smallRelationsBuffer_.clear();
  smallRelationsBuffer_.reserve(2 * blocksize());
}
// ____________________________________________________________________________
CompressedBlockMetadata::OffsetAndCompressedSize
CompressedRelationWriter::compressAndWriteColumn(ql::span<const Id> column) {
  std::vector<char> compressedBlock = ZstdWrapper::compress(
      (void*)(column.data()), column.size() * sizeof(column[0]));
  auto compressedSize = compressedBlock.size();
  // Reserve a range of the file and write to it with the positioned
  // `File::write`, which needs a shared lock only. The compression above and
  // the write itself therefore run concurrently for any number of blocks.
  auto offsetInFile = nextOffset_.fetch_add(static_cast<off_t>(compressedSize));
  auto numBytesWritten = outfile_.rlock()->write(compressedBlock.data(),
                                                 compressedSize, offsetInFile);
  AD_CORRECTNESS_CHECK(numBytesWritten == static_cast<ssize_t>(compressedSize),
                       "Writing a block of a permutation failed");
  return {offsetInFile, compressedSize};
}

// _____________________________________________________________________________
void CompressedRelationWriter::compressAndWriteBlock(Id firstCol0Id,
                                                     Id lastCol0Id,
                                                     BlockToWrite block,
                                                     bool invokeCallback) {
  auto timer = blockWriteQueueTimer_.startMeasurement();
  blockWriteQueue_.push([this, block = std::move(block), firstCol0Id,
                         lastCol0Id, invokeCallback]() mutable {
    compressAndWriteBlockInCallingThread(firstCol0Id, lastCol0Id,
                                         std::move(block), invokeCallback);
  });
  timer.stop();
}

// _____________________________________________________________________________
void CompressedRelationWriter::compressAndWriteBlockInCallingThread(
    Id firstCol0Id, Id lastCol0Id, BlockToWrite block, bool invokeCallback) {
  // Note: The `view` is only used before the `block` is moved from below, and
  // moving a `BlockToWrite` doesn't move the memory that the view points to
  // anyway.
  auto view = block.view();
  std::vector<CompressedBlockMetadata::OffsetAndCompressedSize> offsets;
  for (const auto& column : view.getColumns()) {
    offsets.push_back(compressAndWriteColumn(column));
  }
  AD_CORRECTNESS_CHECK(!offsets.empty());
  auto numRows = view.numRows();
  const auto& first = view[0];
  const auto& last = view[numRows - 1];
  AD_CORRECTNESS_CHECK(firstCol0Id == first[0]);
  AD_CORRECTNESS_CHECK(lastCol0Id == last[0]);

  auto [hasDuplicates, graphInfo] = getGraphInfo(view);
  blockBuffer_.wlock()->emplace_back(CompressedBlockMetadataNoBlockIndex{
      std::move(offsets),
      numRows,
      {first[0], first[1], first[2], first[3]},
      {last[0], last[1], last[2], last[3]},
      std::move(graphInfo),
      hasDuplicates});
  if (invokeCallback && smallBlocksCallback_) {
    // Only blocks of small relations invoke the callback, and those always own
    // their rows, because they are assembled in the `smallRelationsBuffer_`.
    AD_CORRECTNESS_CHECK(block.ownsRows());
    std::invoke(smallBlocksCallback_, std::move(block).extractTable());
  } else if (block.ownsRows()) {
    recycleBlock(std::move(block).extractTable());
  }
}

// _____________________________________________________________________________
IdTable CompressedRelationWriter::takeRecycledBlock(
    size_t numColumns, const ad_utility::AllocatorWithLimit<Id>& allocator) {
  auto recycledBlocks = recycledBlocks_.wlock();
  // Blocks with a different number of columns cannot be reused, but this
  // should never happen for the current users.
  if (!recycledBlocks->empty() &&
      recycledBlocks->back().numColumns() == numColumns) {
    IdTable result = std::move(recycledBlocks->back());
    recycledBlocks->pop_back();
    return result;
  }
  return IdTable{numColumns, allocator};
}

// _____________________________________________________________________________
void CompressedRelationWriter::recycleBlock(IdTable block) {
  if (!recycleBlocks_) {
    return;
  }
  block.clear();
  auto recycledBlocks = recycledBlocks_.wlock();
  if (recycledBlocks->size() < maxNumRecycledBlocks_) {
    recycledBlocks->push_back(std::move(block));
  }
}
// _____________________________________________________________________________
CompressedRelationMetadata CompressedRelationWriter::finishLargeRelation(
    size_t numDistinctC1) {
  AD_CORRECTNESS_CHECK(currentRelationPreviousSize_ != 0);
  CompressedRelationMetadata md;
  auto offset = std::numeric_limits<size_t>::max();
  auto multiplicityCol1 =
      computeMultiplicity(currentRelationPreviousSize_, numDistinctC1);
  md = CompressedRelationMetadata{currentCol0Id_, currentRelationPreviousSize_,
                                  multiplicityCol1, multiplicityCol1, offset};
  currentRelationPreviousSize_ = 0;
  // The following is used in `addBlockForLargeRelation` to assert that
  // `finishLargeRelation` was called before a new relation was started.
  currentCol0Id_ = Id::makeUndefined();
  return md;
}

// _____________________________________________________________________________
ad_utility::TaskQueueOnExecutor CompressedRelationWriter::makeBlockWriteQueue(
    std::optional<size_t> numTasksInFlightOverride) {
  // The blocks are compressed and written on the global thread pool, so the
  // number of threads that is available for them is the size of that pool,
  // which the `--num-threads / -j` option of the index builder configures (see
  // `ad_utility::setGlobalExecutorNumThreads`).
  size_t numThreads = ad_utility::globalExecutorNumThreads();
  size_t requestedTasks = numTasksInFlightOverride.value_or(numThreads);
  // A value of 0 means "as many as the pool has threads", larger values are
  // capped at that number.
  size_t numConcurrentBlocks =
      requestedTasks == 0 ? numThreads : std::min(requestedTasks, numThreads);
  // Allow at least 4 blocks to be in flight.
  size_t maxNumTasksInFlight = std::max<size_t>(4, numConcurrentBlocks * 2);
  return ad_utility::TaskQueueOnExecutor{ad_utility::globalExecutor(),
                                         maxNumTasksInFlight,
                                         "Compressing and writing blocks"};
}

// _____________________________________________________________________________
void CompressedRelationWriter::addBlockForLargeRelation(Id col0Id,
                                                        BlockToWrite relation) {
  size_t numRows = relation.view().numRows();
  AD_CORRECTNESS_CHECK(numRows != 0);
  AD_CORRECTNESS_CHECK(currentCol0Id_ == col0Id ||
                       currentCol0Id_.isUndefined());
  currentCol0Id_ = col0Id;
  currentRelationPreviousSize_ += numRows;
  writeBufferedRelationsToSingleBlock();
  // This is a block of a large relation, so we don't invoke the
  // `smallBlocksCallback_`. Hence the last argument is `false`.
  compressAndWriteBlock(currentCol0Id_, currentCol0Id_, std::move(relation),
                        false);
}

// The number of blocks of a large relation for which the number of distinct
// `col1` IDs is computed concurrently in `addCompleteLargeRelation` below. Each
// of these blocks is held in memory, so this must not be too large.
static constexpr size_t numBlocksInFlightForDistinctCol1Count = 3;

// __________________________________________________________________________
template <typename T>
CompressedRelationMetadata CompressedRelationWriter::addCompleteLargeRelation(
    Id col0Id, T&& sortedBlocks) {
  using namespace compressedRelationHelpers;
  size_t numDistinctCol1 = 0;

  // Counting the distinct IDs of column 1 is expensive, so it is performed on
  // the global thread pool. The blocks themselves are yielded in their original
  // order, because the merging of the blocks below has to happen in order.
  AsyncDistinctIdCounter<std::remove_reference_t<T>> blocks{
      sortedBlocks, c1Idx, numBlocksInFlightForDistinctCol1Count};

  // Buffer used to ensure the invariant that equal triples (when disregarding
  // the graph) stay in the same block.
  std::optional<IdTable> bufferedBlock;

  while (auto nextBlockAndCount = blocks.next()) {
    auto& [block, numDistinctCol1InBlock] = nextBlockAndCount.value();
    numDistinctCol1 += numDistinctCol1InBlock;

    if (!bufferedBlock.has_value()) {
      // First non-empty block - initialize buffer.
      bufferedBlock = std::move(block);
      continue;
    }

    const auto& lastRowFromPrevious = bufferedBlock.value().back();

    // Find how many rows from current block have the same first three columns
    // as the last row in the buffered block
    const size_t upperBoundEqualTriples =
        ql::ranges::find_if(
            block,
            [&lastRowFromPrevious](const auto& row) {
              return pickFirstThreeColumnsOfIdsWithoutLocalVocab(
                         lastRowFromPrevious) !=
                     pickFirstThreeColumnsOfIdsWithoutLocalVocab(row);
            }) -
        block.begin();

    // If we found rows to merge, add them to the buffered block
    if (upperBoundEqualTriples > 0) {
      bufferedBlock->insertAtEnd(block, 0, upperBoundEqualTriples);

      // Remove the merged rows from the current block
      block.erase(block.begin(), block.begin() + upperBoundEqualTriples);
    }

    // If the `block` is empty after moving the duplicate triples into the
    // buffer, continue without writing a block, because the next block might
    // again contain the `lastRowFromPrevious`
    if (block.empty()) {
      continue;
    }

    // At this point we know that the `block` contains at least a single triple
    // larger than `lastRowFromPrevious`, so we can safely write the
    // `bufferedBlock`.
    writeLargeRelationBlockInSlices(col0Id, std::move(*bufferedBlock));
    bufferedBlock = std::move(block);
  }

  // Write the remaining triples from the buffer.
  if (bufferedBlock.has_value()) {
    AD_CORRECTNESS_CHECK(!bufferedBlock.value().empty());
    writeLargeRelationBlockInSlices(col0Id, std::move(bufferedBlock.value()));
  }

  return finishLargeRelation(numDistinctCol1);
}

// _____________________________________________________________________________
void CompressedRelationWriter::writeLargeRelationBlockInSlices(Id col0Id,
                                                               IdTable block) {
  using namespace compressedRelationHelpers;
  const size_t numRows = block.numRows();
  AD_CORRECTNESS_CHECK(numRows > 0);
  if (numRows <= blocksize()) {
    addBlockForLargeRelation(col0Id, std::move(block));
    return;
  }
  // The slices are views into the `block`, which is shared among them and
  // lives until the last of them has been written.
  auto owner = std::make_shared<const IdTable>(std::move(block));
  auto view = owner->asStaticView<0>();
  size_t begin = 0;
  while (begin < numRows) {
    size_t end = std::min(begin + blocksize(), numRows);
    // Never split rows whose first three columns are equal across two
    // blocks, exactly like the boundaries between the input blocks above.
    while (end < numRows &&
           pickFirstThreeColumnsOfIdsWithoutLocalVocab(view[end]) ==
               pickFirstThreeColumnsOfIdsWithoutLocalVocab(view[end - 1])) {
      ++end;
    }
    addBlockForLargeRelation(
        col0Id, BlockToWrite{view.subView(begin, end - begin), owner});
    begin = end;
  }
}

// _____________________________________________________________________________
auto CompressedRelationWriter::createPermutationPair(
    const std::string& basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2,
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
    qlever::KeyOrder permutation,
    const PerBlockCallbacks& perBlockCallbacks) -> PermutationPairResult {
  PermutationWriter<true> permutationWriter{
      basename, std::move(writerAndCallback1), std::move(writerAndCallback2),
      std::move(permutation), perBlockCallbacks};
  return permutationWriter.writePermutation(std::move(sortedTriples));
}

// _____________________________________________________________________________
auto CompressedRelationWriter::createPermutation(
    WriterAndCallback writerAndCallback,
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
    qlever::KeyOrder permutation, const PerBlockCallbacks& perBlockCallbacks,
    bool showProgressBar) -> PermutationSingleResult {
  PermutationWriter<false> permutationWriter{
      std::move(writerAndCallback), std::move(permutation), perBlockCallbacks,
      showProgressBar};
  return permutationWriter.writePermutation(std::move(sortedTriples));
}
