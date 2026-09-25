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
  smallRelationsBuffer_ = takeBlockBuffer();
}
// ____________________________________________________________________________
CompressedBlockMetadata::OffsetAndCompressedSize
CompressedRelationWriter::compressAndWriteColumn(ql::span<const Id> column) {
  std::vector<char> compressedBlock = ZstdWrapper::compress(
      (void*)(column.data()), column.size() * sizeof(column[0]));
  auto compressedSize = compressedBlock.size();
  auto file = outfile_.wlock();
  auto offsetInFile = file->tell();
  file->write(compressedBlock.data(), compressedBlock.size());
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
      // Only blocks of small relations invoke the callback, and those always
      // own their rows, because they are assembled in the
      // `smallRelationsBuffer_`.
      AD_CORRECTNESS_CHECK(block.ownsRows());
      std::invoke(smallBlocksCallback_, std::move(block).extractTable());
    } else if (block.ownsRows()) {
      blockBufferPool_->giveBack(std::move(block).extractTable());
    }
  });
  timer.stop();
}

// _____________________________________________________________________________
IdTable CompressedRelationWriter::takeBlockBuffer() {
  IdTable buffer = blockBufferPool_->take(
      [this]() { return IdTable{numColumns(), allocator_}; });
  // All users of the same pool write blocks with the same number of columns.
  AD_CORRECTNESS_CHECK(buffer.numColumns() == numColumns());
  buffer.clear();
  // Note: A block may exceed the `blocksize()` (see
  // `smallRelationBlockCapacity`), but the factor of 2 suffices in almost all
  // cases. For a buffer that is reused, this `reserve` is typically a no-op.
  buffer.reserve(2 * blocksize());
  return buffer;
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
ad_utility::TaskQueue<false> CompressedRelationWriter::makeBlockWriteQueue(
    std::optional<size_t> numThreadsOverride) {
  size_t requestedThreads = numThreadsOverride.value_or(
      getRuntimeParameter<&RuntimeParameters::permutationWriterNumThreads_>());
  // `hardware_concurrency` may return 0 when it cannot determine the number
  // of hardware threads; fall back to 1, so that the queue always has a
  // worker (with 0 workers, the tasks would never run).
  uint32_t hardwareThreads = std::max(1u, std::thread::hardware_concurrency());
  // Clamp in `size_t` BEFORE casting, so that a huge requested value cannot
  // truncate to a small (or zero) thread count.
  uint32_t threadCount = requestedThreads == 0
                             ? hardwareThreads
                             : static_cast<uint32_t>(std::min<size_t>(
                                   requestedThreads, hardwareThreads));
  // Allow at least up to 4 tasks in the queue.
  uint32_t queueSize = std::max<uint32_t>(4, threadCount * 2);
  return ad_utility::TaskQueue<false>{queueSize, threadCount};
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

// __________________________________________________________________________
template <typename T>
CompressedRelationMetadata CompressedRelationWriter::addCompleteLargeRelation(
    Id col0Id, T&& sortedBlocks) {
  using namespace compressedRelationHelpers;
  DistinctIdCounter distinctCol1Counter;

  // Buffer used to ensure the invariant that equal triples (when disregarding
  // the graph) stay in the same block.
  std::optional<IdTable> bufferedBlock;

  for (auto& block :
       sortedBlocks | ql::views::filter(std::not_fn(&IdTable::empty))) {
    distinctCol1Counter.addBlock(block.getColumn(c1Idx));

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
    addBlockForLargeRelation(col0Id, std::move(*bufferedBlock));
    bufferedBlock = std::move(block);
  }

  // Write the remaining triples from the buffer.
  if (bufferedBlock.has_value()) {
    AD_CORRECTNESS_CHECK(!bufferedBlock.value().empty());
    addBlockForLargeRelation(col0Id, std::move(bufferedBlock.value()));
  }

  return finishLargeRelation(distinctCol1Counter.getAndReset());
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
