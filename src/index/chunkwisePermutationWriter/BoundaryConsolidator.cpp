// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/chunkwisePermutationWriter/BoundaryConsolidator.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/strings/str_cat.h>

#include "engine/idTable/ExternalIdTableSorterMergeConfig.h"
#include "index/compressedRelationWriter/CompressedRelationPermutationWriterImpl.h"
#include "util/GlobalExecutor.h"
#include "util/parallelBlockMerge/CombinedRunsInput.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

namespace chunkwisePermutationWriter {

using namespace compressedRelationHelpers;

static_assert(ad_utility::parallelBlockMerge::InputConcept<
              ad_utility::parallelBlockMerge::CombinedRunsInput<
                  ad_utility::CompressedIdTableRunsInput<0>>>);

// _____________________________________________________________________________
BoundaryConsolidator::BoundaryConsolidator(
    ChunkWriterContext& context, MetadataCallback metadataCallback1,
    MetadataCallback metadataCallback2, net::any_io_executor executor,
    ad_utility::MemorySize boundaryTwinMergeMemory)
    : context_{context},
      writeMetadata_{std::in_place, std::move(metadataCallback1),
                     std::move(metadataCallback2),
                     context.writer1_.blocksize()},
      executor_{std::move(executor)},
      boundaryTwinMergeMemory_{boundaryTwinMergeMemory} {}

// _____________________________________________________________________________
void BoundaryConsolidator::addChunkResult(ChunkResult chunk) {
  AD_CONTRACT_CHECK(writeMetadata_.has_value(),
                    "`addChunkResult` must not be called after `finish`");
  if (chunk.numRows_ == 0) {
    // A chunk without rows (which the merge may yield, see
    // `parallelBlockMerge::detail::pickChunkSplitPoints`) has nothing to add,
    // and the relation that is currently open may continue in the next chunk.
    AD_CORRECTNESS_CHECK(
        !chunk.firstPart_.has_value() && !chunk.lastPart_.has_value() &&
        chunk.middleMetadata_.empty() && chunk.numMiddleRelations_ == 0);
    return;
  }
  numRows_ += chunk.numRows_;
  numDistinctCol0_ += chunk.numMiddleRelations_;
  if (chunk.spansWholeChunk_) {
    // The single relation of the chunk continues in the next chunk, so the
    // relation stays open.
    AD_CORRECTNESS_CHECK(chunk.firstPart_.has_value() &&
                         !chunk.lastPart_.has_value() &&
                         chunk.middleMetadata_.empty());
    addPart(std::move(chunk.firstPart_).value());
    return;
  }
  if (chunk.firstPart_.has_value()) {
    addPart(std::move(chunk.firstPart_).value());
  }
  // The first relation of the chunk (if any) is complete, because the
  // relations in the middle of the chunk follow. Those are not adjacent to
  // the boundary relations, so the block of small relations that the
  // consolidation currently buffers has to be flushed.
  closeOpenParts();
  writer1().writeBufferedRelationsToSingleBlock();
  for (auto& [md1, md2] : chunk.middleMetadata_) {
    (*writeMetadata_)(md1, md2);
  }
  if (chunk.lastPart_.has_value()) {
    addPart(std::move(chunk.lastPart_).value());
  }
}

// _____________________________________________________________________________
PermutationPairResult BoundaryConsolidator::finish() {
  AD_CONTRACT_CHECK(writeMetadata_.has_value(),
                    "`finish` must only be called once");
  closeOpenParts();
  // Pass the last batch of metadata on to the callbacks (in the destructor of
  // the batcher). This has to happen here, and not in the destructor of this
  // class, which may run on another thread at an arbitrary later time.
  writeMetadata_.reset();
  writer1().finish();
  writer2().finish();
  return PermutationPairResult{numDistinctCol0_,
                               std::move(writer1()).getFinishedBlocks(),
                               std::move(writer2()).getFinishedBlocks()};
}

// _____________________________________________________________________________
void BoundaryConsolidator::addPart(BoundaryPart part) {
  if (!openParts_.empty() && openParts_.front().col0Id_ != part.col0Id_) {
    closeOpenParts();
  }
  openParts_.push_back(std::move(part));
}

// _____________________________________________________________________________
void BoundaryConsolidator::closeOpenParts() {
  if (!openParts_.empty()) {
    writeBoundaryRelation(std::move(openParts_));
    openParts_.clear();
  }
}

// _____________________________________________________________________________
void BoundaryConsolidator::writeBoundaryRelation(
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
  if (!anyPartIsLarge && isSmallRelation(numRows, writer1().blocksize())) {
    // The relation is small, so it is batched into a block of small
    // relations together with the other small boundary relations that are
    // adjacent to it (see `addChunkResult`).
    if (parts.size() == 1) {
      writer1().addSmallRelation(col0Id,
                                 parts.front().rowsBeforeWrittenBlocks_);
      return;
    }
    IdTable rows{context_.numColumns(),
                 parts.front().rowsBeforeWrittenBlocks_.getAllocator()};
    rows.reserve(numRows);
    for (const auto& part : parts) {
      rows.insertAtEnd(part.rowsBeforeWrittenBlocks_, 0,
                       part.rowsBeforeWrittenBlocks_.numRows());
    }
    writer1().addSmallRelation(col0Id, rows);
    return;
  }

  // The relation is large. The blocks of small relations that are currently
  // buffered must not contain relations that lie on both sides of it.
  writer1().writeBufferedRelationsToSingleBlock();
  // Write the rows that the parts keep in memory as blocks. The rows after
  // the last block of a part and the rows before the first block of the next
  // part are written together, because they may contain equal triples (when
  // disregarding the graph), which have to end up in the same block.
  IdTable pending = writer1().takeBlockBuffer();
  DistinctIdCounter distinctCol1Counter;
  for (auto& part : parts) {
    distinctCol1Counter.addCountOfBlock(part.distinctCol1_);
    pending.insertAtEnd(part.rowsBeforeWrittenBlocks_, 0,
                        part.rowsBeforeWrittenBlocks_.numRows());
    if (part.isLarge()) {
      if (!pending.empty()) {
        ChunkWriter::writeRowsOfLargeRelation(writer1(), col0Id,
                                              std::move(pending), true);
        pending = writer1().takeBlockBuffer();
      }
      if (part.rowsAfterWrittenBlocks_.has_value()) {
        pending.insertAtEnd(part.rowsAfterWrittenBlocks_.value(), 0,
                            part.rowsAfterWrittenBlocks_->numRows());
      }
    }
  }
  if (!pending.empty()) {
    ChunkWriter::writeRowsOfLargeRelation(writer1(), col0Id, std::move(pending),
                                          true);
  }
  auto md1 = CompressedRelationWriter::makeLargeRelationMetadata(
      col0Id, numRows, distinctCol1Counter.getAndReset());
  auto md2 = writeTwinOfBoundaryRelation(col0Id, parts);
  AD_CORRECTNESS_CHECK(md2.numRows_ == numRows);
  (*writeMetadata_)(md1, md2);
}

// _____________________________________________________________________________
CompressedRelationMetadata BoundaryConsolidator::writeTwinOfBoundaryRelation(
    Id col0Id, std::vector<BoundaryPart>& parts) {
  const size_t numColumns = context_.numColumns();
  const size_t outputBlocksize =
      TWIN_SORTER_BLOCKSIZE_FACTOR * writer1().blocksize();
  std::vector<TwinSorter*> sorters;
  for (auto& part : parts) {
    if (part.isLarge()) {
      sorters.push_back(part.twinSorter_.get());
    }
  }
  if (sorters.empty()) {
    // All the parts are small (but the relation as a whole is large), so all
    // the twin rows fit into memory and can be sorted there.
    IdTable twinRows{numColumns,
                     parts.front().rowsBeforeWrittenBlocks_.getAllocator()};
    for (const auto& part : parts) {
      twinRows.insertAtEnd(part.rowsBeforeWrittenBlocks_, 0,
                           part.rowsBeforeWrittenBlocks_.numRows());
    }
    twinRows.swapColumns(c1Idx, c2Idx);
    ql::ranges::sort(twinRows, ComparatorForConstCol0{});
    std::vector<Block> blocks;
    blocks.push_back(std::move(twinRows));
    return writer2().addCompleteLargeRelation(
        col0Id, ad_utility::InputRangeTypeErased<Block>{std::move(blocks)});
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
        sorterForSmallParts = context_.makeTwinSorter(
            absl::StrCat(context_.twinSorterBasename_,
                         ".twin-boundary-small-parts.", col0Id.getBits()));
      }
      auto twinRows = part.rowsBeforeWrittenBlocks_.asStaticView<0>();
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
  ad_utility::parallelBlockMerge::CombinedRunsInput input{std::move(inputs)};
  using namespace ad_utility::compressedExternalIdTable;
  MergePhaseConfig config;
  config.numRuns_ = input.numRuns();
  config.numColumns_ = numColumns;
  config.memoryLimit_ = boundaryTwinMergeMemory_;
  config.inputBlockSizePerColumn_ = sorters.front()->inputBlockSizePerColumn();
  config.numBufferedOutputBlocks_ = 12;
  config.maxOutputBlockSize_ = ad_utility::MemorySize::gigabytes(1);
  config.parallelism_ = ad_utility::globalExecutorNumThreads();
  config.outputBlockSizeOverride_ = outputBlocksize;
  const auto parameters = computeMergePhaseParameters(config);
  auto mergeOptions = makeMergeOptions(config, parameters);
  auto storageFactory = makeMergePhaseBlockStorageFactory<0>(
      executor_,
      absl::StrCat(context_.twinSorterBasename_, ".twin-boundary-spill.",
                   col0Id.getBits()),
      ad_utility::makeUnlimitedAllocator<Id>(),
      parameters.numBufferedBlocksPerChunk_);
  auto merged = ad_utility::parallelBlockMerge::parallelBlockMergeToRange<true>(
      executor_, std::move(input), ComparatorForConstCol0{},
      std::move(storageFactory), std::move(mergeOptions),
      std::make_shared<ad_utility::CancellationHandle<>>());
  // NOTE: The consolidation runs on a thread that is not a thread of the pool,
  // so it may use the (blocking) bookkeeping and block write queue of
  // `writer2`, which the chunks never touch (they only use the thread-safe
  // `compressAndWriteBlockInCallingThread`).
  return writer2().addCompleteLargeRelation(
      col0Id, ad_utility::InputRangeTypeErased<Block>{std::move(merged)});
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
