// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "CompressedRelation.h"

#include <ranges>

#include "engine/Engine.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/LocatedTriples.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Generator.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/OverloadCallOperator.h"
#include "util/ProgressBar.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/TransparentFunctors.h"
#include "util/TypeTraits.h"

using namespace std::chrono_literals;

// A small helper function to obtain the begin and end iterator of a range
static auto getBeginAndEnd(auto& range) {
  return std::pair{ql::ranges::begin(range), ql::ranges::end(range)};
}

// Return true iff the `triple` is contained in the `scanSpec`. For example, the
// triple ` 42 0 3 ` is contained in the specs `U U U`, `42 U U` and `42 0 U` ,
// but not in `42 2 U` where `U` means "scan for all possible values".
static auto isTripleInSpecification =
    [](const ScanSpecification& scanSpec,
       const CompressedBlockMetadata::PermutedTriple& triple) {
      enum struct M { GuaranteedMatch, Mismatch, MustCheckNextElement };
      auto checkElement = [](const auto& optId, Id id) {
        if (!optId.has_value()) {
          return M::GuaranteedMatch;
        } else if (optId.value() != id) {
          return M::Mismatch;
        } else {
          return M::MustCheckNextElement;
        }
      };
      auto result = checkElement(scanSpec.col0Id(), triple.col0Id_);
      if (result == M::MustCheckNextElement) {
        result = checkElement(scanSpec.col1Id(), triple.col1Id_);
      }
      if (result == M::MustCheckNextElement) {
        result = checkElement(scanSpec.col2Id(), triple.col2Id_);
      }
      // The case `result == M::MustCheckNextElement` can happen in the unlikely
      // case that there only is a single triple in the block, which is scanned
      // for explicitly.
      return result != M::Mismatch;
    };

// modify the `block` according to the `limitOffset`. Also modify the
// `limitOffset` to reflect the parts of the LIMIT and OFFSET that have been
// performed by pruning this `block`.
static void pruneBlock(auto& block, LimitOffsetClause& limitOffset) {
  auto& offset = limitOffset._offset;
  auto offsetInBlock = std::min(static_cast<size_t>(offset), block.size());
  if (offsetInBlock == block.size()) {
    block.clear();
  } else {
    block.erase(block.begin(), block.begin() + offsetInBlock);
  }
  offset -= offsetInBlock;
  auto& limit = limitOffset._limit;
  auto limitInBlock =
      std::min(block.size(), static_cast<size_t>(limit.value_or(block.size())));
  block.resize(limitInBlock);
  if (limit.has_value()) {
    limit.value() -= limitInBlock;
  }
}

// ____________________________________________________________________________
CompressedRelationReader::IdTableGenerator
CompressedRelationReader::asyncParallelBlockGenerator(
    auto beginBlock, auto endBlock, const ScanImplConfig& scanConfig,
    CancellationHandle cancellationHandle,
    LimitOffsetClause& limitOffset) const {
  // Empty range.
  if (beginBlock == endBlock) {
    co_return;
  }

  // Preparation.
  const auto& columnIndices = scanConfig.scanColumns_;
  const auto& blockGraphFilter = scanConfig.graphFilter_;
  LazyScanMetadata& details = co_await cppcoro::getDetails;
  const size_t queueSize =
      RuntimeParameters().get<"lazy-index-scan-queue-size">();
  auto blockMetadataIterator = beginBlock;
  std::mutex blockIteratorMutex;

  // Helper lambda that reads and decompessed the next block and returns it
  // together with its index relative to `beginBlock`. Return `std::nullopt`
  // when `endBlock` is reached. Returns `std::nullopt` for the block if it
  // is skipped due to the graph filter.
  auto readAndDecompressBlock = [&]()
      -> std::optional<
          std::pair<size_t, std::optional<DecompressedBlockAndMetadata>>> {
    cancellationHandle->throwIfCancelled();
    std::unique_lock lock{blockIteratorMutex};
    if (blockMetadataIterator == endBlock) {
      return std::nullopt;
    }
    // Note: taking a copy here is probably not necessary (the lifetime of
    // all the blocks is long enough, so a `const&` would suffice), but the
    // copy is cheap and makes the code more robust.
    auto blockMetadata = *blockMetadataIterator;
    // Note: The order of the following two lines is important: The index
    // of the current blockMetadata depends on the current value of
    // `blockMetadataIterator`, so we have to compute it before incrementing the
    // iterator.
    auto myIndex = static_cast<size_t>(blockMetadataIterator - beginBlock);
    ++blockMetadataIterator;
    if (blockGraphFilter.canBlockBeSkipped(blockMetadata)) {
      return std::pair{myIndex, std::nullopt};
    }
    // Note: the reading of the blockMetadata could also happen without holding
    // the lock. We still perform it inside the lock to avoid contention of the
    // file. On a fast SSD we could possibly change this, but this has to be
    // investigated.
    CompressedBlock compressedBlock =
        readCompressedBlockFromFile(blockMetadata, columnIndices);
    lock.unlock();
    auto decompressedBlockAndMetadata = decompressAndPostprocessBlock(
        compressedBlock, blockMetadata.numRows_, scanConfig, blockMetadata);
    return std::pair{myIndex,
                     std::optional{std::move(decompressedBlockAndMetadata)}};
  };

  // Prepare queue for reading and decompressing blocks concurrently using
  // `numThreads` threads.
  const size_t numThreads =
      RuntimeParameters().get<"lazy-index-scan-num-threads">();
  ad_utility::Timer popTimer{ad_utility::timer::Timer::InitialStatus::Started};
  // In case the coroutine is destroyed early we still want to have this
  // information.
  auto setTimer = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
      [&details, &popTimer]() { details.blockingTime_ = popTimer.msecs(); });
  auto queue = ad_utility::data_structures::queueManager<
      ad_utility::data_structures::OrderedThreadSafeQueue<
          std::optional<DecompressedBlockAndMetadata>>>(queueSize, numThreads,
                                                        readAndDecompressBlock);

  // Yield the blocks (in the right order) as soon as they become available.
  // Stop when all the blocks have been yielded or the LIMIT of the query is
  // reached. Keep track of various statistics.
  for (std::optional<DecompressedBlockAndMetadata>& optBlock : queue) {
    popTimer.stop();
    cancellationHandle->throwIfCancelled();
    details.update(optBlock);
    if (optBlock.has_value()) {
      auto& block = optBlock.value().block_;
      pruneBlock(block, limitOffset);
      details.numElementsYielded_ += block.numRows();
      if (!block.empty()) {
        co_yield block;
      }
      if (limitOffset._limit.value_or(1) == 0) {
        co_return;
      }
    }
    popTimer.cont();
  }
  // The `OnDestruction...` above might be called too late, so we manually stop
  // the timer here in case it wasn't already.
  popTimer.stop();
}

// _____________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::
    blockNeedsFilteringByGraph(const CompressedBlockMetadata& metadata) const {
  if (!desiredGraphs_.has_value()) {
    return false;
  }
  if (!metadata.graphInfo_.has_value()) {
    return true;
  }
  const auto& graphInfo = metadata.graphInfo_.value();
  return !ql::ranges::all_of(
      graphInfo, [&wantedGraphs = desiredGraphs_.value()](Id containedGraph) {
        return wantedGraphs.contains(containedGraph);
      });
}

// _____________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::
    filterByGraphIfNecessary(
        IdTable& block, const CompressedBlockMetadata& blockMetadata) const {
  bool needsFilteringByGraph = blockNeedsFilteringByGraph(blockMetadata);
  [[maybe_unused]] auto graphIdFromRow = [graphColumn =
                                              graphColumn_](const auto& row) {
    return row[graphColumn];
  };
  [[maybe_unused]] auto isDesiredGraphId = [this]() {
    return [&wantedGraphs = desiredGraphs_.value()](Id id) {
      return wantedGraphs.contains(id);
    };
  };
  if (needsFilteringByGraph) {
    auto removedRange = ql::ranges::remove_if(
        block, std::not_fn(isDesiredGraphId()), graphIdFromRow);
#ifdef QLEVER_CPP_17
    block.erase(removedRange, block.end());
#else
    block.erase(removedRange.begin(), block.end());
#endif
  } else {
    AD_EXPENSIVE_CHECK(
        !desiredGraphs_.has_value() ||
        ql::ranges::all_of(block, isDesiredGraphId(), graphIdFromRow));
  }
  return needsFilteringByGraph;
}

// _____________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::
    filterDuplicatesIfNecessary(IdTable& block,
                                const CompressedBlockMetadata& blockMetadata) {
  if (!blockMetadata.containsDuplicatesWithDifferentGraphs_) {
    AD_EXPENSIVE_CHECK(std::unique(block.begin(), block.end()) == block.end());
    return false;
  }
  auto endUnique = std::unique(block.begin(), block.end());
  block.erase(endUnique, block.end());
  return true;
}

// _____________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::postprocessBlock(
    IdTable& block, const CompressedBlockMetadata& blockMetadata) const {
  bool filteredByGraph = filterByGraphIfNecessary(block, blockMetadata);
  if (deleteGraphColumn_) {
    block.deleteColumn(graphColumn_);
  }

  bool filteredByDuplicates = filterDuplicatesIfNecessary(block, blockMetadata);
  return filteredByGraph || filteredByDuplicates;
}

// ______________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::canBlockBeSkipped(
    const CompressedBlockMetadata& block) const {
  if (!desiredGraphs_.has_value()) {
    return false;
  }
  if (!block.graphInfo_.has_value()) {
    return false;
  }
  const auto& containedGraphs = block.graphInfo_.value();
  return ql::ranges::none_of(
      desiredGraphs_.value(), [&containedGraphs](const auto& desiredGraph) {
        return ad_utility::contains(containedGraphs, desiredGraph);
      });
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGenerator CompressedRelationReader::lazyScan(
    ScanSpecification scanSpec,
    std::vector<CompressedBlockMetadata> blockMetadata,
    ColumnIndices additionalColumns, CancellationHandle cancellationHandle,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    LimitOffsetClause limitOffset) const {
  AD_CONTRACT_CHECK(cancellationHandle);

  // We will modify `limitOffset` as we go. We make a copy of the original
  // value for some sanity checks at the end of the function.
  const auto originalLimit = limitOffset;

  // Compute the sequence of relevant blocks. If the sequence is empty, there
  // is nothing to yield.
  auto relevantBlocks = getRelevantBlocks(scanSpec, blockMetadata);
  if (ql::ranges::empty(relevantBlocks)) {
    co_return;
  }

  // Some preparation.
  auto [beginBlockMetadata, endBlockMetadata] = getBeginAndEnd(relevantBlocks);
  LazyScanMetadata& details = co_await cppcoro::getDetails;
  size_t numBlocksTotal = endBlockMetadata - beginBlockMetadata;
  auto config =
      getScanConfig(scanSpec, additionalColumns, locatedTriplesPerBlock);

  // Helper lambda for reading the first and last block, of which only a part
  // is needed.
  auto getIncompleteBlock = [&](auto it) {
    auto result = readPossiblyIncompleteBlock(
        scanSpec, config, *it, std::ref(details), locatedTriplesPerBlock);
    cancellationHandle->throwIfCancelled();
    return result;
  };

  // Get and yield the first block.
  if (beginBlockMetadata < endBlockMetadata) {
    auto block = getIncompleteBlock(beginBlockMetadata);
    pruneBlock(block, limitOffset);
    details.numElementsYielded_ += block.numRows();
    if (!block.empty()) {
      co_yield block;
    }
  }

  // Get and yield the remaining blocks.
  if (beginBlockMetadata + 1 < endBlockMetadata) {
    // We copy the cancellationHandle because it is still captured by
    // reference inside the `getIncompleteBlock` lambda.
    auto blockGenerator = asyncParallelBlockGenerator(
        beginBlockMetadata + 1, endBlockMetadata - 1, config,
        cancellationHandle, limitOffset);
    blockGenerator.setDetailsPointer(&details);
    for (auto& block : blockGenerator) {
      co_yield block;
    }
    auto lastBlock = getIncompleteBlock(endBlockMetadata - 1);
    pruneBlock(lastBlock, limitOffset);
    if (!lastBlock.empty()) {
      details.numElementsYielded_ += lastBlock.numRows();
      co_yield lastBlock;
    }
  }

  // Some sanity checks.
  const auto& limit = originalLimit._limit;
  AD_CORRECTNESS_CHECK(!limit.has_value() ||
                       details.numElementsYielded_ <= limit.value());
  AD_CORRECTNESS_CHECK(
      numBlocksTotal == (details.numBlocksRead_ +
                         details.numBlocksSkippedBecauseOfGraph_) ||
          !limitOffset.isUnconstrained(),
      [&]() {
        return absl::StrCat(numBlocksTotal, " ", details.numBlocksRead_, " ",
                            details.numBlocksSkippedBecauseOfGraph_);
      });
}

// Helper function that enables a comparison of a triple with an `Id` in
// the function `getBlocksForJoin` below.
//
// If the given triple matches `col0Id` of the given `ScanSpecification`, then
// `col1Id` is returned.
// respective other `Id` of the triple is returned.
//
// If the given triple matches neither, a sentinel value is returned (`Id::min`
// if the triple is lower than all triples matching the `ScanSpecification`, or
// `Id::max` if it is higher).
namespace {
auto getRelevantIdFromTriple(
    CompressedBlockMetadata::PermutedTriple triple,
    const CompressedRelationReader::ScanSpecAndBlocksAndBounds&
        metadataAndBlocks) {
  // The `ScanSpecifcation`, which must ask for at least one column.
  const auto& scanSpec = metadataAndBlocks.scanSpec_;
  AD_CORRECTNESS_CHECK(!scanSpec.col2Id());

  // For a full scan, return the triples's `col0Id`.
  if (!scanSpec.col0Id().has_value()) {
    return triple.col0Id_;
  }

  // Compute the following range: If the `scanSpec` specifies both `col0Id` and
  // `col1Id`, the first and last `col2Id` of the blocks. If the `scanSpec`
  // specifies only `col0Id`, the first and last `col1Id` of the blocks.
  auto [minId, maxId] = [&]() {
    const auto& [first, last] = metadataAndBlocks.firstAndLastTriple_;
    if (scanSpec.col1Id().has_value()) {
      return std::array{first.col2Id_, last.col2Id_};
    } else {
      AD_CORRECTNESS_CHECK(scanSpec.col0Id().has_value());
      return std::array{first.col1Id_, last.col1Id_};
    }
  }();

  // Helper lambda that returns `std::nullopt` if `idFromTriple` equals `id`,
  // `minId` if is smaller, and `maxId` if it is larger.
  auto idForNonMatchingBlock = [](Id idFromTriple, Id id, Id minId,
                                  Id maxId) -> std::optional<Id> {
    if (idFromTriple < id) {
      return minId;
    }
    if (idFromTriple > id) {
      return maxId;
    }
    return std::nullopt;
  };

  // If the `col0Id` of the triple does not match that of the `scanSpec`,
  // return `minId` (if it is smaller) or `maxId` (if it is larger).
  if (auto optId = idForNonMatchingBlock(
          triple.col0Id_, scanSpec.col0Id().value(), minId, maxId)) {
    return optId.value();
  }

  // If the `col0Id` of the triple matches that of the `scanSpec`, and the
  // `scanSpec` does not specify `col1Id`, return the triples's `col1Id`.
  if (!scanSpec.col1Id().has_value()) {
    return triple.col1Id_;
  }

  // If the `col1Id` of the triple matches that of the `scanSpec`, return the
  // triples's `col2Id`. Otherwise, return `minId` (if it is smaller) or `maxId`
  // (if it is larger).
  return idForNonMatchingBlock(triple.col1Id_, scanSpec.col1Id().value(), minId,
                               maxId)
      .value_or(triple.col2Id_);
}
}  // namespace

// _____________________________________________________________________________
std::vector<CompressedBlockMetadata> CompressedRelationReader::getBlocksForJoin(
    std::span<const Id> joinColumn,
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks) {
  // Get all the blocks where `col0FirstId_ <= col0Id <= col0LastId_`.
  auto relevantBlocks = getBlocksFromMetadata(metadataAndBlocks);

  // We need symmetric comparisons between Ids and blocks.
  auto idLessThanBlock = [&metadataAndBlocks](
                             Id id, const CompressedBlockMetadata& block) {
    return id < getRelevantIdFromTriple(block.firstTriple_, metadataAndBlocks);
  };

  auto blockLessThanId = [&metadataAndBlocks](
                             const CompressedBlockMetadata& block, Id id) {
    return getRelevantIdFromTriple(block.lastTriple_, metadataAndBlocks) < id;
  };

  // `blockLessThanBlock` (a dummy) and `std::less<Id>` are only needed to
  // fulfill a concept for the `ql::ranges` algorithms.
  auto blockLessThanBlock =
      []<typename T = void>(const CompressedBlockMetadata&,
                            const CompressedBlockMetadata&)
          ->bool {
    static_assert(ad_utility::alwaysFalse<T>);
    AD_FAIL();
  };
  auto lessThan = ad_utility::OverloadCallOperator{
      idLessThanBlock, blockLessThanId, blockLessThanBlock, std::less<Id>{}};

  // Find the matching blocks by performing binary search on the `joinColumn`.
  // Note that it is tempting to reuse the `zipperJoinWithUndef` routine, but
  // this doesn't work because the implicit equality defined by
  // `!lessThan(a,b) && !lessThan(b, a)` is not transitive.
  std::vector<CompressedBlockMetadata> result;
  auto blockIsNeeded = [&joinColumn, &lessThan](const auto& block) {
    return !ql::ranges::equal_range(joinColumn, block, lessThan).empty();
  };
  ql::ranges::copy(relevantBlocks | ql::views::filter(blockIsNeeded),
                   std::back_inserter(result));
  // The following check is cheap as there are only few blocks.
  AD_CORRECTNESS_CHECK(std::unique(result.begin(), result.end()) ==
                       result.end());
  return result;
}

// _____________________________________________________________________________
std::array<std::vector<CompressedBlockMetadata>, 2>
CompressedRelationReader::getBlocksForJoin(
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks1,
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks2) {
  // Associate a block together with the relevant ID (col1 or col2) for this
  // join from the first and last triple.
  struct BlockWithFirstAndLastId {
    const CompressedBlockMetadata& block_;
    Id first_;
    Id last_;
  };

  auto blockLessThanBlock = [&](const BlockWithFirstAndLastId& block1,
                                const BlockWithFirstAndLastId& block2) {
    return block1.last_ < block2.first_;
  };

  // Transform all the relevant blocks from a `ScanSpecAndBlocksAndBounds` a
  // `BlockWithFirstAndLastId` struct (see above).
  auto getBlocksWithFirstAndLastId =
      [&blockLessThanBlock](
          const ScanSpecAndBlocksAndBounds& metadataAndBlocks) {
        auto getSingleBlock =
            [&metadataAndBlocks](const CompressedBlockMetadata& block)
            -> BlockWithFirstAndLastId {
          return {
              block,
              getRelevantIdFromTriple(block.firstTriple_, metadataAndBlocks),
              getRelevantIdFromTriple(block.lastTriple_, metadataAndBlocks)};
        };
        auto result = ql::views::transform(
            getBlocksFromMetadata(metadataAndBlocks), getSingleBlock);
        AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(result, blockLessThanBlock));
        return result;
      };

  auto blocksWithFirstAndLastId1 =
      getBlocksWithFirstAndLastId(metadataAndBlocks1);
  auto blocksWithFirstAndLastId2 =
      getBlocksWithFirstAndLastId(metadataAndBlocks2);

  // Find the matching blocks on each side by performing binary search on the
  // other side. Note that it is tempting to reuse the `zipperJoinWithUndef`
  // routine, but this doesn't work because the implicit equality defined by
  // `!lessThan(a,b) && !lessThan(b, a)` is not transitive.
  auto findMatchingBlocks = [&blockLessThanBlock](const auto& blocks,
                                                  const auto& otherBlocks) {
    std::vector<CompressedBlockMetadata> result;
    for (const auto& block : blocks) {
      if (!ql::ranges::equal_range(otherBlocks, block, blockLessThanBlock)
               .empty()) {
        result.push_back(block.block_);
      }
    }
    // The following check isn't expensive as there are only few blocks.
    AD_CORRECTNESS_CHECK(std::unique(result.begin(), result.end()) ==
                         result.end());
    return result;
  };

  return {
      findMatchingBlocks(blocksWithFirstAndLastId1, blocksWithFirstAndLastId2),
      findMatchingBlocks(blocksWithFirstAndLastId2, blocksWithFirstAndLastId1)};
}

// _____________________________________________________________________________
IdTable CompressedRelationReader::scan(
    const ScanSpecification& scanSpec,
    std::span<const CompressedBlockMetadata> blocks,
    ColumnIndicesRef additionalColumns,
    const CancellationHandle& cancellationHandle,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    const LimitOffsetClause& limitOffset) const {
  auto columnIndices = prepareColumnIndices(scanSpec, additionalColumns);
  IdTable result(columnIndices.size(), allocator_);

  // Compute an upper bound for the size and reserve enough space in the
  // result.
  auto relevantBlocks = getRelevantBlocks(scanSpec, blocks);
  auto sizes =
      relevantBlocks | ql::views::transform(&CompressedBlockMetadata::numRows_);
  auto upperBoundSize = std::accumulate(sizes.begin(), sizes.end(), size_t{0});
  if (limitOffset._limit.has_value()) {
    upperBoundSize = std::min(upperBoundSize,
                              static_cast<size_t>(limitOffset._limit.value()));
  }
  result.reserve(upperBoundSize);

  for (const auto& block :
       lazyScan(scanSpec, {relevantBlocks.begin(), relevantBlocks.end()},
                {additionalColumns.begin(), additionalColumns.end()},
                cancellationHandle, locatedTriplesPerBlock, limitOffset)) {
    result.insertAtEnd(block);
  }
  cancellationHandle->throwIfCancelled();
  return result;
}

// ____________________________________________________________________________
DecompressedBlock CompressedRelationReader::readPossiblyIncompleteBlock(
    const ScanSpecification& scanSpec, const ScanImplConfig& scanConfig,
    const CompressedBlockMetadata& blockMetadata,
    std::optional<std::reference_wrapper<LazyScanMetadata>> scanMetadata,
    const LocatedTriplesPerBlock& locatedTriples) const {
  AD_CORRECTNESS_CHECK(ADDITIONAL_COLUMN_GRAPH_ID <
                       blockMetadata.offsetsAndCompressedSize_.size());

  // We first scan the complete block including ALL columns.
  std::vector<ColumnIndex> allAdditionalColumns;
  ql::ranges::copy(
      ql::views::iota(ADDITIONAL_COLUMN_GRAPH_ID,
                      blockMetadata.offsetsAndCompressedSize_.size()),
      std::back_inserter(allAdditionalColumns));
  ScanSpecification specForAllColumns{std::nullopt,
                                      std::nullopt,
                                      std::nullopt,
                                      {},
                                      scanConfig.graphFilter_.desiredGraphs_};
  auto config = getScanConfig(specForAllColumns,
                              std::move(allAdditionalColumns), locatedTriples);
  bool manuallyDeleteGraphColumn = scanConfig.graphFilter_.deleteGraphColumn_;

  // Helper lambda that returns the decompressed block or an empty block if
  // `readAndDecompressBlock` returns `std::nullopt`.
  const DecompressedBlock& block = [&]() {
    auto result = readAndDecompressBlock(blockMetadata, config);
    if (scanMetadata.has_value()) {
      scanMetadata.value().get().update(result);
    }
    if (result.has_value()) {
      return std::move(result.value().block_);
    } else {
      return DecompressedBlock{config.scanColumns_.size(), allocator_};
    }
  }();

  // We now compute the range of the block according to the `scanSpec`. We
  // start with the full range of the block.
  size_t beginIdx = 0;
  size_t endIdx = block.numRows();

  // Set `beginIdx` and `endIdx` s.t. that they only represent the range in
  // `block` where the column with the `columnIdx` matches the `relevantId`.

  // Helper lambda that narrows down the range of the block so that all values
  // in column `columnIdx` are equal to `relevantId`. If `relevantId` is
  // `std::nullopt`, the range is not narrowed down.
  auto filterColumn = [&block, &beginIdx, &endIdx](std::optional<Id> relevantId,
                                                   size_t columnIdx) {
    if (!relevantId.has_value()) {
      return;
    }
    const auto& column = block.getColumn(columnIdx);
    auto matchingRange = ql::ranges::equal_range(
        column.begin() + beginIdx, column.begin() + endIdx, relevantId.value());
    beginIdx = matchingRange.begin() - column.begin();
    endIdx = matchingRange.end() - column.begin();
  };

  // Now narrow down the range of the block by first `scanSpec.col0Id()`,
  // then `scanSpec.col1Id()`, and then `scanSpec.col2Id()`. This order is
  // important because the rows are sorted in that order.
  filterColumn(scanSpec.col0Id(), 0);
  filterColumn(scanSpec.col1Id(), 1);
  filterColumn(scanSpec.col2Id(), 2);

  // Now copy the range `[beginIdx, endIdx)` from `block` to `result`.
  DecompressedBlock result{scanConfig.scanColumns_.size() -
                               static_cast<size_t>(manuallyDeleteGraphColumn),
                           allocator_};
  result.resize(endIdx - beginIdx);
  size_t i = 0;
  const auto& columnIndices = scanConfig.scanColumns_;
  for (const auto& inputColIdx :
       columnIndices | ql::views::filter([&](const auto& idx) {
         return !manuallyDeleteGraphColumn || idx != ADDITIONAL_COLUMN_GRAPH_ID;
       })) {
    const auto& inputCol = block.getColumn(inputColIdx);
    ql::ranges::copy(inputCol.begin() + beginIdx, inputCol.begin() + endIdx,
                     result.getColumn(i).begin());
    ++i;
  }

  // Return the result.
  return result;
};

// ____________________________________________________________________________
template <bool exactSize>
std::pair<size_t, size_t> CompressedRelationReader::getResultSizeImpl(
    const ScanSpecification& scanSpec,
    const vector<CompressedBlockMetadata>& blocks,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock)
    const {
  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  auto relevantBlocks = getRelevantBlocks(scanSpec, blocks);
  auto [beginBlock, endBlock] = getBeginAndEnd(relevantBlocks);

  auto config = getScanConfig(scanSpec, {}, locatedTriplesPerBlock);

  // The first and the last block might be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the size of the result.
  size_t numResults = 0;
  // Determine the total size of the result.
  // First accumulate the complete blocks in the "middle"
  std::size_t inserted = 0;
  std::size_t deleted = 0;

  auto readSizeOfPossiblyIncompleteBlock = [&](const auto& block) {
    if (exactSize) {
      numResults +=
          readPossiblyIncompleteBlock(scanSpec, config, block, std::nullopt,
                                      locatedTriplesPerBlock)
              .numRows();
    } else {
      // If the first and last triple of the block match, then we know that the
      // whole block belongs to the result.
      bool isComplete = isTripleInSpecification(scanSpec, block.firstTriple_) &&
                        isTripleInSpecification(scanSpec, block.lastTriple_);
      size_t divisor =
          isComplete ? 1
                     : RuntimeParameters()
                           .get<"small-index-scan-size-estimate-divisor">();
      const auto [ins, del] =
          locatedTriplesPerBlock.numTriples(block.blockIndex_);
      auto trunc = [divisor](size_t num) {
        return std::max(std::min(num, 1ul), num / divisor);
      };
      inserted += trunc(ins);
      deleted += trunc(del);
      numResults += trunc(block.numRows_);
    }
  };

  // The first and the last block might be incomplete, compute
  // and store the partial results from them.
  if (beginBlock < endBlock) {
    readSizeOfPossiblyIncompleteBlock(*beginBlock);
    ++beginBlock;
  }
  if (beginBlock < endBlock) {
    readSizeOfPossiblyIncompleteBlock(*(endBlock - 1));
    --endBlock;
  }

  if (beginBlock == endBlock) {
    return {numResults, numResults};
  }

  ql::ranges::for_each(
      ql::ranges::subrange{beginBlock, endBlock}, [&](const auto& block) {
        const auto [ins, del] =
            locatedTriplesPerBlock.numTriples(block.blockIndex_);
        if (!exactSize || (ins == 0 && del == 0)) {
          inserted += ins;
          deleted += del;
          numResults += block.numRows_;
        } else {
          // TODO<joka921> We could cache the exact size as soon as we
          // have merged the block once since the last update.
          auto b = readAndDecompressBlock(block, config);
          numResults += b.has_value() ? b.value().block_.numRows() : 0u;
        }
      });
  return {numResults - std::min(deleted, numResults), numResults + inserted};
}

// ____________________________________________________________________________
std::pair<size_t, size_t> CompressedRelationReader::getSizeEstimateForScan(
    const ScanSpecification& scanSpec,
    const vector<CompressedBlockMetadata>& blocks,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  return getResultSizeImpl<false>(scanSpec, blocks, locatedTriplesPerBlock);
}

// ____________________________________________________________________________
size_t CompressedRelationReader::getResultSizeOfScan(
    const ScanSpecification& scanSpec,
    const vector<CompressedBlockMetadata>& blocks,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock)
    const {
  auto [lower, upper] =
      getResultSizeImpl<true>(scanSpec, blocks, locatedTriplesPerBlock);
  AD_CORRECTNESS_CHECK(lower == upper);
  return lower;
}

// ____________________________________________________________________________
CPP_template_def(typename IdGetter)(
    requires ad_utility::InvocableWithConvertibleReturnType<
        IdGetter, Id, const CompressedBlockMetadata::PermutedTriple&>) IdTable
    CompressedRelationReader::getDistinctColIdsAndCountsImpl(
        IdGetter idGetter, const ScanSpecification& scanSpec,
        const std::vector<CompressedBlockMetadata>& allBlocksMetadata,
        const CancellationHandle& cancellationHandle,
        const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  // The result has two columns: one for the distinct `Id`s and one for their
  // counts.
  IdTableStatic<2> table(allocator_);

  // The current `colId` and its current count.
  std::optional<Id> currentColId;
  size_t currentCount = 0;

  // Helper lambda that processes the next `colId` and a count. If it's new, a
  // row with the previous `currentColId` and its count are added to the
  // result, and `currentColId` and its count are updated to the new `colId`.
  auto processColId = [&table, &currentColId, &currentCount](
                          std::optional<Id> colId, size_t colIdCount) {
    if (colId != currentColId) {
      if (currentColId.has_value()) {
        table.push_back({currentColId.value(), Id::makeFromInt(currentCount)});
      }
      currentColId = colId;
      currentCount = 0;
    }
    currentCount += colIdCount;
  };

  // Get the blocks needed for the scan.
  std::span<const CompressedBlockMetadata> relevantBlocksMetadata =
      getRelevantBlocks(scanSpec, allBlocksMetadata);

  // TODO<joka921> We have to read the other columns for the merging of the
  // located triples. We could skip this for blocks with no updates, but that
  // would require more arguments to the `decompressBlock` function.
  auto scanConfig = getScanConfig(scanSpec, {}, locatedTriplesPerBlock);
  // Iterate over the blocks and only read (and decompress) those which
  // contain more than one different `colId`. For the others, we can determine
  // the count from the metadata.
  for (size_t i = 0; i < relevantBlocksMetadata.size(); ++i) {
    const auto& blockMetadata = relevantBlocksMetadata[i];
    Id firstColId = std::invoke(idGetter, blockMetadata.firstTriple_);
    Id lastColId = std::invoke(idGetter, blockMetadata.lastTriple_);
    if (firstColId == lastColId) {
      // The whole block has the same `colId` -> we get all the information
      // from the metadata.
      processColId(firstColId, blockMetadata.numRows_);
    } else {
      // Multiple `colId`s -> we have to read the block.
      const auto& optionalBlock = [&]() -> std::optional<DecompressedBlock> {
        if (i == 0) {
          return readPossiblyIncompleteBlock(scanSpec, scanConfig,
                                             blockMetadata, std::nullopt,
                                             locatedTriplesPerBlock);
        } else {
          auto optionalBlock =
              readAndDecompressBlock(blockMetadata, scanConfig);
          if (!optionalBlock.has_value()) {
            return std::nullopt;
          }
          return std::move(optionalBlock.value().block_);
        }
      }();
      cancellationHandle->throwIfCancelled();
      if (!optionalBlock.has_value()) {
        // The block was skipped because of the graph filter
        continue;
      }
      const auto& block = optionalBlock.value();
      // TODO<C++23>: use `ql::views::chunkd_by`.
      for (size_t j = 0; j < block.numRows(); ++j) {
        Id colId = block(j, 0);
        processColId(colId, 1);
      }
    }
  }
  // Don't forget to add the last `col1Id` and its count.
  processColId(std::nullopt, 0);
  return std::move(table).toDynamic();
}

// ____________________________________________________________________________
IdTable CompressedRelationReader::getDistinctCol0IdsAndCounts(
    const std::vector<CompressedBlockMetadata>& allBlocksMetadata,
    const CancellationHandle& cancellationHandle,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock)
    const {
  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  return getDistinctColIdsAndCountsImpl(
      &CompressedBlockMetadata::PermutedTriple::col0Id_, scanSpec,
      allBlocksMetadata, cancellationHandle, locatedTriplesPerBlock);
}

// ____________________________________________________________________________
IdTable CompressedRelationReader::getDistinctCol1IdsAndCounts(
    Id col0Id, const std::vector<CompressedBlockMetadata>& allBlocksMetadata,
    const CancellationHandle& cancellationHandle,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock)
    const {
  ScanSpecification scanSpec{col0Id, std::nullopt, std::nullopt};

  return getDistinctColIdsAndCountsImpl(
      &CompressedBlockMetadata::PermutedTriple::col1Id_, scanSpec,
      allBlocksMetadata, cancellationHandle, locatedTriplesPerBlock);
}

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
  compressAndWriteBlock(
      currentBlockFirstCol0_, currentBlockLastCol0_,
      std::make_shared<IdTable>(std::move(smallRelationsBuffer_).toDynamic()),
      true);
  smallRelationsBuffer_.clear();
  smallRelationsBuffer_.reserve(2 * blocksize());
}

// _____________________________________________________________________________
CompressedBlock CompressedRelationReader::readCompressedBlockFromFile(
    const CompressedBlockMetadata& blockMetaData,
    ColumnIndicesRef columnIndices) const {
  CompressedBlock compressedBuffer;
  compressedBuffer.resize(columnIndices.size());
  // TODO<C++23> Use `ql::views::zip`
  for (size_t i = 0; i < compressedBuffer.size(); ++i) {
    const auto& offset =
        blockMetaData.offsetsAndCompressedSize_.at(columnIndices[i]);
    auto& currentCol = compressedBuffer[i];
    currentCol.resize(offset.compressedSize_);
    file_.read(currentCol.data(), offset.compressedSize_, offset.offsetInFile_);
  }
  return compressedBuffer;
}

// ____________________________________________________________________________
DecompressedBlock CompressedRelationReader::decompressBlock(
    const CompressedBlock& compressedBlock, size_t numRowsToRead) const {
  DecompressedBlock decompressedBlock{compressedBlock.size(), allocator_};
  decompressedBlock.resize(numRowsToRead);
  for (size_t i = 0; i < compressedBlock.size(); ++i) {
    auto col = decompressedBlock.getColumn(i);
    decompressColumn(compressedBlock[i], numRowsToRead, col.data());
  }
  return decompressedBlock;
}

// ____________________________________________________________________________
DecompressedBlockAndMetadata
CompressedRelationReader::decompressAndPostprocessBlock(
    const CompressedBlock& compressedBlock, size_t numRowsToRead,
    const CompressedRelationReader::ScanImplConfig& scanConfig,
    const CompressedBlockMetadata& metadata) const {
  auto decompressedBlock = decompressBlock(compressedBlock, numRowsToRead);
  auto [numIndexColumns, includeGraphColumn] =
      prepareLocatedTriples(scanConfig.scanColumns_);
  bool hasUpdates = false;
  if (scanConfig.locatedTriples_.containsTriples(metadata.blockIndex_)) {
    decompressedBlock = scanConfig.locatedTriples_.mergeTriples(
        metadata.blockIndex_, decompressedBlock, numIndexColumns,
        includeGraphColumn);
    hasUpdates = true;
  }
  bool wasPostprocessed =
      scanConfig.graphFilter_.postprocessBlock(decompressedBlock, metadata);
  return {std::move(decompressedBlock), wasPostprocessed, hasUpdates};
}

// ____________________________________________________________________________
template <typename Iterator>
void CompressedRelationReader::decompressColumn(
    const std::vector<char>& compressedBlock, size_t numRowsToRead,
    Iterator iterator) {
  auto numBytesActuallyRead = ZstdWrapper::decompressToBuffer(
      compressedBlock.data(), compressedBlock.size(), iterator,
      numRowsToRead * sizeof(*iterator));
  static_assert(sizeof(Id) == sizeof(*iterator));
  AD_CORRECTNESS_CHECK(numRowsToRead * sizeof(Id) == numBytesActuallyRead);
}

// ____________________________________________________________________________
std::optional<DecompressedBlockAndMetadata>
CompressedRelationReader::readAndDecompressBlock(
    const CompressedBlockMetadata& blockMetaData,
    const ScanImplConfig& scanConfig) const {
  if (scanConfig.graphFilter_.canBlockBeSkipped(blockMetaData)) {
    return std::nullopt;
  }
  CompressedBlock compressedColumns =
      readCompressedBlockFromFile(blockMetaData, scanConfig.scanColumns_);
  const auto numRowsToRead = blockMetaData.numRows_;
  return decompressAndPostprocessBlock(compressedColumns, numRowsToRead,
                                       scanConfig, blockMetaData);
}

// ____________________________________________________________________________
CompressedBlockMetadata::OffsetAndCompressedSize
CompressedRelationWriter::compressAndWriteColumn(std::span<const Id> column) {
  std::vector<char> compressedBlock = ZstdWrapper::compress(
      (void*)(column.data()), column.size() * sizeof(column[0]));
  auto compressedSize = compressedBlock.size();
  auto file = outfile_.wlock();
  auto offsetInFile = file->tell();
  file->write(compressedBlock.data(), compressedBlock.size());
  return {offsetInFile, compressedSize};
};

// Find out whether the sorted `block` contains duplicates and whether it
// contains only a few distinct graphs such that we can store this information
// in the block metadata.
static std::pair<bool, std::optional<std::vector<Id>>> getGraphInfo(
    const std::shared_ptr<IdTable>& block) {
  AD_CORRECTNESS_CHECK(block->numColumns() > ADDITIONAL_COLUMN_GRAPH_ID);
  // Return true iff the block contains duplicates when only considering the
  // actual triple of S, P, and O.
  auto hasDuplicates = [&block]() {
    using C = ColumnIndex;
    auto withoutGraphAndAdditionalPayload =
        block->asColumnSubsetView(std::array{C{0}, C{1}, C{2}});
    size_t numDistinct = Engine::countDistinct(withoutGraphAndAdditionalPayload,
                                               ad_utility::noop);
    return numDistinct != block->numRows();
  };

  // Return the contained graphs, or  `nullopt` if there are too many of them.
  auto graphInfo = [&block]() -> std::optional<std::vector<Id>> {
    std::vector<Id> graphColumn;
    ql::ranges::copy(block->getColumn(ADDITIONAL_COLUMN_GRAPH_ID),
                     std::back_inserter(graphColumn));
    ql::ranges::sort(graphColumn);
    auto endOfUnique = std::unique(graphColumn.begin(), graphColumn.end());
    size_t numGraphs = endOfUnique - graphColumn.begin();
    if (numGraphs > MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA) {
      return std::nullopt;
    }
    // Note: we cannot simply resize `graphColumn`, as this doesn't free
    // the memory that is not needed anymore. We can do either `resize +
    // shrink_to_fit`, which is not guaranteed by the standard, but works in
    // practice. We choose the alternative of simply returning a new vector
    // with the correct capacity.
    return std::vector<Id>(graphColumn.begin(),
                           graphColumn.begin() + numGraphs);
  };
  return {hasDuplicates(), graphInfo()};
}

// _____________________________________________________________________________
void CompressedRelationWriter::compressAndWriteBlock(
    Id firstCol0Id, Id lastCol0Id, std::shared_ptr<IdTable> block,
    bool invokeCallback) {
  auto timer = blockWriteQueueTimer_.startMeasurement();
  blockWriteQueue_.push([this, block = std::move(block), firstCol0Id,
                         lastCol0Id, invokeCallback]() mutable {
    std::vector<CompressedBlockMetadata::OffsetAndCompressedSize> offsets;
    for (const auto& column : block->getColumns()) {
      offsets.push_back(compressAndWriteColumn(column));
    }
    AD_CORRECTNESS_CHECK(!offsets.empty());
    auto numRows = block->numRows();
    const auto& first = (*block)[0];
    const auto& last = (*block)[numRows - 1];
    AD_CORRECTNESS_CHECK(firstCol0Id == first[0]);
    AD_CORRECTNESS_CHECK(lastCol0Id == last[0]);

    auto [hasDuplicates, graphInfo] = getGraphInfo(block);
    blockBuffer_.wlock()->emplace_back(CompressedBlockMetadataNoBlockIndex{
        std::move(offsets),
        numRows,
        {first[0], first[1], first[2], first[3]},
        {last[0], last[1], last[2], last[3]},
        std::move(graphInfo),
        hasDuplicates});
    if (invokeCallback && smallBlocksCallback_) {
      std::invoke(smallBlocksCallback_, std::move(block));
    }
  });
  timer.stop();
}

// _____________________________________________________________________________
std::span<const CompressedBlockMetadata>
CompressedRelationReader::getRelevantBlocks(
    const ScanSpecification& scanSpec,
    std::span<const CompressedBlockMetadata> blockMetadata) {
  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  CompressedBlockMetadata key;

  auto setOrDefault = [&scanSpec](auto getterA, auto getterB, auto& triple,
                                  auto defaultValue) {
    std::invoke(getterA, triple) =
        std::invoke(getterB, scanSpec).value_or(defaultValue);
  };
  auto setKey = [&setOrDefault, &key](auto getterA, auto getterB) {
    setOrDefault(getterA, getterB, key.firstTriple_, Id::min());
    setOrDefault(getterA, getterB, key.lastTriple_, Id::max());
  };
  using PermutedTriple = CompressedBlockMetadata::PermutedTriple;
  setKey(&PermutedTriple::col0Id_, &ScanSpecification::col0Id);
  setKey(&PermutedTriple::col1Id_, &ScanSpecification::col1Id);
  setKey(&PermutedTriple::col2Id_, &ScanSpecification::col2Id);

  // We currently don't filter by the graph ID here.
  key.firstTriple_.graphId_ = Id::min();
  key.lastTriple_.graphId_ = Id::max();

  // This comparator only returns true if a block stands completely before
  // another block without any overlap. In other words, the last triple of `a`
  // must be smaller than the first triple of `b` to return true.
  auto comp = [](const auto& blockA, const auto& blockB) {
    return blockA.lastTriple_ < blockB.firstTriple_;
  };

  return ql::ranges::equal_range(blockMetadata, key, comp);
}

// _____________________________________________________________________________
std::span<const CompressedBlockMetadata>
CompressedRelationReader::getBlocksFromMetadata(
    const ScanSpecAndBlocks& metadata) {
  return getRelevantBlocks(metadata.scanSpec_, metadata.blockMetadata_);
}

// _____________________________________________________________________________
auto CompressedRelationReader::getFirstAndLastTriple(
    const CompressedRelationReader::ScanSpecAndBlocks& metadataAndBlocks,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock) const
    -> std::optional<ScanSpecAndBlocksAndBounds::FirstAndLastTriple> {
  auto relevantBlocks = getBlocksFromMetadata(metadataAndBlocks);
  if (relevantBlocks.empty()) {
    return std::nullopt;
  }
  const auto& scanSpec = metadataAndBlocks.scanSpec_;

  ScanSpecification scanSpecForAllColumns{
      std::nullopt, std::nullopt, std::nullopt, {}, std::nullopt};
  auto config =
      getScanConfig(scanSpecForAllColumns,
                    std::array{ColumnIndex{ADDITIONAL_COLUMN_GRAPH_ID}},
                    locatedTriplesPerBlock);
  auto scanBlock = [&](const CompressedBlockMetadata& block) {
    // Note: the following call only returns the part of the block that
    // actually matches the col0 and col1.
    return readPossiblyIncompleteBlock(scanSpec, config, block, std::nullopt,
                                       locatedTriplesPerBlock);
  };

  auto rowToTriple =
      [&](const auto& row) -> CompressedBlockMetadata::PermutedTriple {
    AD_CORRECTNESS_CHECK(!scanSpec.col0Id().has_value() ||
                         row[0] == scanSpec.col0Id().value());
    return {row[0], row[1], row[2], row[ADDITIONAL_COLUMN_GRAPH_ID]};
  };

  auto firstBlock = scanBlock(relevantBlocks.front());
  auto lastBlock = scanBlock(relevantBlocks.back());
  if (firstBlock.empty()) {
    return std::nullopt;
  }
  AD_CORRECTNESS_CHECK(!lastBlock.empty());
  return ScanSpecAndBlocksAndBounds::FirstAndLastTriple{
      rowToTriple(firstBlock.front()), rowToTriple(lastBlock.back())};
}

// ____________________________________________________________________________
std::vector<ColumnIndex> CompressedRelationReader::prepareColumnIndices(
    std::initializer_list<ColumnIndex> baseColumns,
    ColumnIndicesRef additionalColumns) {
  std::vector<ColumnIndex> result;
  result.reserve(baseColumns.size() + additionalColumns.size());
  ql::ranges::copy(baseColumns, std::back_inserter(result));
  ql::ranges::copy(additionalColumns, std::back_inserter(result));
  return result;
}

// ____________________________________________________________________________
std::vector<ColumnIndex> CompressedRelationReader::prepareColumnIndices(
    const ScanSpecification& scanSpec, ColumnIndicesRef additionalColumns) {
  if (scanSpec.col2Id().has_value()) {
    return prepareColumnIndices({}, additionalColumns);
  } else if (scanSpec.col1Id().has_value()) {
    return prepareColumnIndices({2}, additionalColumns);
  } else if (scanSpec.col0Id().has_value()) {
    return prepareColumnIndices({1, 2}, additionalColumns);
  } else {
    return prepareColumnIndices({0, 1, 2}, additionalColumns);
  }
}

// ___________________________________________________________________________
std::pair<size_t, bool> CompressedRelationReader::prepareLocatedTriples(
    ColumnIndicesRef columns) {
  AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(columns));
  // Compute number of columns that should be read (except the graph column
  // and any payload columns).
  size_t numScanColumns = [&]() -> size_t {
    if (columns.empty() || columns[0] > 3) {
      return 0;
    } else {
      return 3 - columns[0];
    }
  }();
  // Check if one of the columns is the graph column.
  auto it = ql::ranges::find(columns, ADDITIONAL_COLUMN_GRAPH_ID);
  bool containsGraphId = it != columns.end();
  if (containsGraphId) {
    AD_CORRECTNESS_CHECK(it - columns.begin() ==
                         static_cast<int>(numScanColumns));
  }
  return {numScanColumns, containsGraphId};
}

// _____________________________________________________________________________
CompressedRelationMetadata CompressedRelationWriter::addSmallRelation(
    Id col0Id, size_t numDistinctC1, IdTableView<0> relation) {
  AD_CORRECTNESS_CHECK(!relation.empty());
  size_t numRows = relation.numRows();
  // Make sure that the blocks don't become too large: If the previously
  // buffered small relations together with the new relations would exceed
  // `1.5 * blocksize` then we start a new block for the current relation.
  //
  // NOTE: there are some unit tests that rely on this factor being `1.5`.
  if (static_cast<double>(numRows + smallRelationsBuffer_.numRows()) >
      static_cast<double>(blocksize()) * 1.5) {
    writeBufferedRelationsToSingleBlock();
  }
  auto offsetInBlock = smallRelationsBuffer_.size();

  // We have to keep track of the first and last `col0` of each block.
  if (smallRelationsBuffer_.numRows() == 0) {
    currentBlockFirstCol0_ = col0Id;
  }
  currentBlockLastCol0_ = col0Id;

  smallRelationsBuffer_.resize(offsetInBlock + numRows);
  for (size_t i = 0; i < relation.numColumns(); ++i) {
    ql::ranges::copy(
        relation.getColumn(i),
        smallRelationsBuffer_.getColumn(i).begin() + offsetInBlock);
  }
  // Note: the multiplicity of the `col2` (where we set the dummy here) will
  // be set later in `createPermutationPair`.
  return {col0Id, numRows, computeMultiplicity(numRows, numDistinctC1),
          multiplicityDummy, offsetInBlock};
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
void CompressedRelationWriter::addBlockForLargeRelation(
    Id col0Id, std::shared_ptr<IdTable> relation) {
  AD_CORRECTNESS_CHECK(!relation->empty());
  AD_CORRECTNESS_CHECK(currentCol0Id_ == col0Id ||
                       currentCol0Id_.isUndefined());
  currentCol0Id_ = col0Id;
  currentRelationPreviousSize_ += relation->numRows();
  writeBufferedRelationsToSingleBlock();
  // This is a block of a large relation, so we don't invoke the
  // `smallBlocksCallback_`. Hence the last argument is `false`.
  compressAndWriteBlock(currentCol0Id_, currentCol0Id_, std::move(relation),
                        false);
}

namespace {
// Collect elements of type `T` in batches of size 100'000 and apply the
// `Function` to each batch. For the last batch (which might be smaller)  the
// function is applied in the destructor.
template <
    typename T,
    ad_utility::InvocableWithExactReturnType<void, std::vector<T>&&> Function>
struct Batcher {
  Function function_;
  size_t blocksize_;
  std::vector<T> vec_;

  Batcher(Function function, size_t blocksize)
      : function_{std::move(function)}, blocksize_{blocksize} {}
  void operator()(T t) {
    vec_.push_back(std::move(t));
    if (vec_.size() > blocksize_) {
      function_(std::move(vec_));
      vec_.clear();
      vec_.reserve(blocksize_);
    }
  }
  ~Batcher() {
    if (!vec_.empty()) {
      function_(std::move(vec_));
    }
  }
  // No copy or move operations (neither needed nor easy to get right).
  Batcher(const Batcher&) = delete;
  Batcher& operator=(const Batcher&) = delete;
};

using MetadataCallback = CompressedRelationWriter::MetadataCallback;

// A class that is called for all pairs of `CompressedRelationMetadata` for
// the same `col0Id` and the "twin permutations" (e.g. PSO and POS). The
// multiplicity of the last column is exchanged and then the metadata are
// passed on to the respective `MetadataCallback`.
class MetadataWriter {
 private:
  using B = Batcher<CompressedRelationMetadata, MetadataCallback>;
  B batcher1_;
  B batcher2_;

 public:
  MetadataWriter(MetadataCallback callback1, MetadataCallback callback2,
                 size_t blocksize)
      : batcher1_{std::move(callback1), blocksize},
        batcher2_{std::move(callback2), blocksize} {}
  void operator()(CompressedRelationMetadata md1,
                  CompressedRelationMetadata md2) {
    md1.multiplicityCol2_ = md2.multiplicityCol1_;
    md2.multiplicityCol2_ = md1.multiplicityCol1_;
    batcher1_(md1);
    batcher2_(md2);
  }
};

// A simple class to count distinct IDs in a sorted sequence.
class DistinctIdCounter {
  Id lastSeen_ = std::numeric_limits<Id>::max();
  size_t count_ = 0;

 public:
  void operator()(Id id) {
    count_ += static_cast<size_t>(id != lastSeen_);
    lastSeen_ = id;
  }
  size_t getAndReset() {
    size_t count = count_;
    lastSeen_ = std::numeric_limits<Id>::max();
    count_ = 0;
    return count;
  }
};
}  // namespace

// __________________________________________________________________________
CompressedRelationMetadata CompressedRelationWriter::addCompleteLargeRelation(
    Id col0Id, auto&& sortedBlocks) {
  DistinctIdCounter distinctCol1Counter;
  for (auto& block : sortedBlocks) {
    ql::ranges::for_each(block.getColumn(1), std::ref(distinctCol1Counter));
    addBlockForLargeRelation(
        col0Id, std::make_shared<IdTable>(std::move(block).toDynamic()));
  }
  return finishLargeRelation(distinctCol1Counter.getAndReset());
}

// _____________________________________________________________________________
auto CompressedRelationWriter::createPermutationPair(
    const std::string& basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2,
    cppcoro::generator<IdTableStatic<0>> sortedTriples,
    std::array<size_t, 3> permutation,
    const std::vector<std::function<void(const IdTableStatic<0>&)>>&
        perBlockCallbacks) -> PermutationPairResult {
  auto [c0, c1, c2] = permutation;
  size_t numDistinctCol0 = 0;
  auto& writer1 = writerAndCallback1.writer_;
  auto& writer2 = writerAndCallback2.writer_;
  const size_t blocksize = writer1.blocksize();
  AD_CORRECTNESS_CHECK(writer2.blocksize() == writer1.blocksize());
  const size_t numColumns = writer1.numColumns();
  AD_CORRECTNESS_CHECK(writer1.numColumns() == writer2.numColumns());
  MetadataWriter writeMetadata{std::move(writerAndCallback1.callback_),
                               std::move(writerAndCallback2.callback_),
                               writer1.blocksize()};

  static constexpr size_t c1Idx = 1;
  static constexpr size_t c2Idx = 2;

  // A queue for the callbacks that have to be applied for each triple.
  // The second argument is the number of threads. It is crucial that this
  // queue is single threaded.
  ad_utility::TaskQueue blockCallbackQueue{
      3, 1, "Additional callbacks during permutation building"};

  ad_utility::Timer inputWaitTimer{ad_utility::Timer::Stopped};
  ad_utility::Timer largeTwinRelationTimer{ad_utility::Timer::Stopped};
  ad_utility::Timer blockCallbackTimer{ad_utility::Timer::Stopped};

  // Iterate over the vector and identify relation boundaries, where a
  // relation is the sequence of sortedTriples with equal first component. For
  // PSO and POS, this is a predicate (of which "relation" is a synonym).
  std::optional<Id> col0IdCurrentRelation;
  auto alloc = ad_utility::makeUnlimitedAllocator<Id>();
  // TODO<joka921> Use call_fixed_size if there is benefit to it.
  IdTableStatic<0> relation{numColumns, alloc};
  size_t numBlocksCurrentRel = 0;
  auto compare = [](const auto& a, const auto& b) {
    return std::tie(a[c1Idx], a[c2Idx], a[ADDITIONAL_COLUMN_GRAPH_ID]) <
           std::tie(b[c1Idx], b[c2Idx], b[ADDITIONAL_COLUMN_GRAPH_ID]);
  };
  // TODO<joka921> Use `CALL_FIXED_SIZE`.
  ad_utility::CompressedExternalIdTableSorter<decltype(compare), 0>
      twinRelationSorter(basename + ".twin-twinRelationSorter", numColumns,
                         4_GB, alloc);

  DistinctIdCounter distinctCol1Counter;
  auto addBlockForLargeRelation = [&numBlocksCurrentRel, &writer1,
                                   &col0IdCurrentRelation, &relation,
                                   &twinRelationSorter, &blocksize] {
    if (relation.empty()) {
      return;
    }
    auto twinRelation = relation.asStaticView<0>();
    twinRelation.swapColumns(c1Idx, c2Idx);
    for (const auto& row : twinRelation) {
      twinRelationSorter.push(row);
    }
    writer1.addBlockForLargeRelation(
        col0IdCurrentRelation.value(),
        std::make_shared<IdTable>(std::move(relation).toDynamic()));
    relation.clear();
    relation.reserve(blocksize);
    ++numBlocksCurrentRel;
  };

  // Set up the handling of small relations for the twin permutation.
  // A complete block of them is handed from `writer1` to the following lambda
  // (via the `smallBlocksCallback_` mechanism. The lambda the resorts the
  // block and feeds it to `writer2`.)
  auto addBlockOfSmallRelationsToSwitched =
      [&writer2](std::shared_ptr<IdTable> relationPtr) {
        auto& relation = *relationPtr;
        // We don't use the parallel twinRelationSorter to create the twin
        // relation as its overhead is far too high for small relations.
        relation.swapColumns(c1Idx, c2Idx);

        // We only need to sort by the columns of the triple + the graph
        // column, not the additional payload. Note: We could also use
        // `compareWithoutLocalVocab` to compare the IDs cheaper, but this
        // sort is far from being a performance bottleneck.
        auto compare = [](const auto& a, const auto& b) {
          return std::tie(a[0], a[1], a[2], a[3]) <
                 std::tie(b[0], b[1], b[2], b[3]);
        };
        ql::ranges::sort(relation, compare);
        AD_CORRECTNESS_CHECK(!relation.empty());
        writer2.compressAndWriteBlock(relation.at(0, 0),
                                      relation.at(relation.size() - 1, 0),
                                      std::move(relationPtr), false);
      };

  writer1.smallBlocksCallback_ = addBlockOfSmallRelationsToSwitched;

  auto finishRelation = [&numDistinctCol0, &twinRelationSorter, &writer2,
                         &writer1, &numBlocksCurrentRel, &col0IdCurrentRelation,
                         &relation, &distinctCol1Counter,
                         &addBlockForLargeRelation, &blocksize, &writeMetadata,
                         &largeTwinRelationTimer]() {
    ++numDistinctCol0;
    if (numBlocksCurrentRel > 0 || static_cast<double>(relation.numRows()) >
                                       0.8 * static_cast<double>(blocksize)) {
      // The relation is large;
      addBlockForLargeRelation();
      auto md1 = writer1.finishLargeRelation(distinctCol1Counter.getAndReset());
      largeTwinRelationTimer.cont();
      auto md2 = writer2.addCompleteLargeRelation(
          col0IdCurrentRelation.value(),
          twinRelationSorter.getSortedBlocks(blocksize));
      largeTwinRelationTimer.stop();
      twinRelationSorter.clear();
      writeMetadata(md1, md2);
    } else {
      // Small relations are written in one go.
      [[maybe_unused]] auto md1 = writer1.addSmallRelation(
          col0IdCurrentRelation.value(), distinctCol1Counter.getAndReset(),
          relation.asStaticView<0>());
      // We don't need to do anything for the twin permutation and writer2,
      // because we have set up `writer1.smallBlocksCallback_` to do that work
      // for us (see above).
    }
    relation.clear();
    numBlocksCurrentRel = 0;
  };
  // All columns n the order in which they have to be added to
  // the relation.
  std::vector<ColumnIndex> permutedColIndices{c0, c1, c2};
  for (size_t colIdx = 3; colIdx < numColumns; ++colIdx) {
    permutedColIndices.push_back(colIdx);
  }
  inputWaitTimer.cont();
  size_t numTriplesProcessed = 0;
  ad_utility::ProgressBar progressBar{numTriplesProcessed, "Triples sorted: "};
  for (auto& block : AD_FWD(sortedTriples)) {
    AD_CORRECTNESS_CHECK(block.numColumns() == numColumns);
    inputWaitTimer.stop();
    // This only happens when the index is completely empty.
    if (block.empty()) {
      continue;
    }
    auto firstCol = block.getColumn(c0);
    auto permutedCols = block.asColumnSubsetView(permutedColIndices);
    if (!col0IdCurrentRelation.has_value()) {
      col0IdCurrentRelation = firstCol[0];
    }
    // TODO<C++23> Use `views::zip`
    for (size_t idx : ad_utility::integerRange(block.numRows())) {
      Id col0Id = firstCol[idx];
      decltype(auto) curRemainingCols = permutedCols[idx];
      if (col0Id != col0IdCurrentRelation) {
        finishRelation();
        col0IdCurrentRelation = col0Id;
      }
      distinctCol1Counter(curRemainingCols[c1Idx]);
      relation.push_back(curRemainingCols);
      if (relation.size() >= blocksize) {
        addBlockForLargeRelation();
      }
      ++numTriplesProcessed;
      if (progressBar.update()) {
        LOG(INFO) << progressBar.getProgressString() << std::flush;
      }
    }
    // Call each of the `perBlockCallbacks` for the current block.
    blockCallbackTimer.cont();
    blockCallbackQueue.push(
        [block =
             std::make_shared<std::decay_t<decltype(block)>>(std::move(block)),
         &perBlockCallbacks]() {
          for (auto& callback : perBlockCallbacks) {
            callback(*block);
          }
        });
    blockCallbackTimer.stop();
    inputWaitTimer.cont();
  }
  LOG(INFO) << progressBar.getFinalProgressString() << std::flush;
  inputWaitTimer.stop();
  if (!relation.empty() || numBlocksCurrentRel > 0) {
    finishRelation();
  }

  writer1.finish();
  writer2.finish();
  blockCallbackTimer.cont();
  blockCallbackQueue.finish();
  blockCallbackTimer.stop();
  LOG(TIMING) << "Time spent waiting for the input "
              << ad_utility::Timer::toSeconds(inputWaitTimer.msecs()) << "s"
              << std::endl;
  LOG(TIMING) << "Time spent waiting for writer1's queue "
              << ad_utility::Timer::toSeconds(
                     writer1.blockWriteQueueTimer_.msecs())
              << "s" << std::endl;
  LOG(TIMING) << "Time spent waiting for writer2's queue "
              << ad_utility::Timer::toSeconds(
                     writer2.blockWriteQueueTimer_.msecs())
              << "s" << std::endl;
  LOG(TIMING) << "Time spent waiting for large twin relations "
              << ad_utility::Timer::toSeconds(largeTwinRelationTimer.msecs())
              << "s" << std::endl;
  LOG(TIMING)
      << "Time spent waiting for triple callbacks (e.g. the next sorter) "
      << ad_utility::Timer::toSeconds(blockCallbackTimer.msecs()) << "s"
      << std::endl;
  return {numDistinctCol0, std::move(writer1).getFinishedBlocks(),
          std::move(writer2).getFinishedBlocks()};
}

// _____________________________________________________________________________
std::optional<CompressedRelationMetadata>
CompressedRelationReader::getMetadataForSmallRelation(
    const std::vector<CompressedBlockMetadata>& allBlocksMetadata, Id col0Id,
    [[maybe_unused]] const LocatedTriplesPerBlock& locatedTriplesPerBlock)
    const {
  CompressedRelationMetadata metadata;
  metadata.col0Id_ = col0Id;
  metadata.offsetInBlock_ = 0;
  ScanSpecification scanSpec{col0Id, std::nullopt, std::nullopt};
  auto blocks = getRelevantBlocks(scanSpec, allBlocksMetadata);
  auto config = getScanConfig(scanSpec, {}, locatedTriplesPerBlock);
  AD_CONTRACT_CHECK(blocks.size() <= 1,
                    "Should only be called for small relations");
  if (blocks.empty()) {
    return std::nullopt;
  }
  auto block = readPossiblyIncompleteBlock(
      scanSpec, config, blocks.front(), std::nullopt, locatedTriplesPerBlock);
  if (block.empty()) {
    return std::nullopt;
  }

  // The `col1` is sorted, so we compute the multiplicity using
  // `std::unique`.
  const auto& blockCol = block.getColumn(0);
  auto endOfUnique = std::unique(blockCol.begin(), blockCol.end());
  size_t numDistinct = endOfUnique - blockCol.begin();
  metadata.numRows_ = block.size();
  metadata.multiplicityCol1_ =
      CompressedRelationWriter::computeMultiplicity(block.size(), numDistinct);

  // The `col2` is not sorted, so we compute the multiplicity using a hash
  // set.
  ad_utility::HashSet<Id> distinctCol2{block.getColumn(1).begin(),
                                       block.getColumn(1).end()};
  metadata.multiplicityCol2_ = CompressedRelationWriter::computeMultiplicity(
      block.size(), distinctCol2.size());
  return metadata;
}

// _____________________________________________________________________________
auto CompressedRelationReader::getScanConfig(
    const ScanSpecification& scanSpec,
    CompressedRelationReader::ColumnIndicesRef additionalColumns,
    const LocatedTriplesPerBlock& locatedTriples) -> ScanImplConfig {
  auto columnIndices = prepareColumnIndices(scanSpec, additionalColumns);
  // Determine the index of the graph column (which we need either for
  // filtering or for the output or both) and whether we we need it for
  // the output or not.
  //
  // NOTE: The graph column has to come directly after the triple columns and
  // before any additional payload columns. Otherwise `prepareLocatedTriples`
  // will throw an assertion.
  auto [graphColumnIndex,
        deleteGraphColumn] = [&]() -> std::pair<ColumnIndex, bool> {
    if (!scanSpec.graphsToFilter().has_value()) {
      // No filtering required, these are dummy values that are ignored by the
      // filtering logic.
      return {0, false};
    }
    auto idx = static_cast<size_t>(
        ql::ranges::find(columnIndices, ADDITIONAL_COLUMN_GRAPH_ID) -
        columnIndices.begin());
    bool deleteColumn = false;
    if (idx == columnIndices.size()) {
      idx = columnIndices.size() - additionalColumns.size();
      columnIndices.insert(columnIndices.begin() + idx,
                           ADDITIONAL_COLUMN_GRAPH_ID);
      deleteColumn = true;
    }
    return {idx, deleteColumn};
  }();
  FilterDuplicatesAndGraphs graphFilter{scanSpec.graphsToFilter(),
                                        graphColumnIndex, deleteGraphColumn};
  return {std::move(columnIndices), std::move(graphFilter), locatedTriples};
}

// _____________________________________________________________________________
void CompressedRelationReader::LazyScanMetadata::update(
    const DecompressedBlockAndMetadata& blockAndMetadata) {
  numBlocksPostprocessed_ +=
      static_cast<size_t>(blockAndMetadata.wasPostprocessed_);
  numBlocksWithUpdate_ +=
      static_cast<size_t>(blockAndMetadata.containsUpdates_);
  ++numBlocksRead_;
  numElementsRead_ += blockAndMetadata.block_.numRows();
}

// _____________________________________________________________________________
void CompressedRelationReader::LazyScanMetadata::update(
    const std::optional<DecompressedBlockAndMetadata>& blockAndMetadata) {
  if (blockAndMetadata.has_value()) {
    update(blockAndMetadata.value());
  } else {
    ++numBlocksSkippedBecauseOfGraph_;
  }
}

// _____________________________________________________________________________
void CompressedRelationReader::LazyScanMetadata::aggregate(
    const LazyScanMetadata& newValue) {
  numElementsYielded_ += newValue.numElementsYielded_;
  blockingTime_ += newValue.blockingTime_;
  numBlocksRead_ += newValue.numBlocksRead_;
  numBlocksAll_ += newValue.numBlocksAll_;
  numElementsRead_ += newValue.numElementsRead_;
  numBlocksSkippedBecauseOfGraph_ += newValue.numBlocksSkippedBecauseOfGraph_;
  numBlocksPostprocessed_ += newValue.numBlocksPostprocessed_;
  numBlocksWithUpdate_ += newValue.numBlocksWithUpdate_;
}
