// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "index/CompressedRelation.h"

#include <thread>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "index/CompressedRelationHelpersImpl.h"
#include "index/CompressedRelationPermutationWriterImpl.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/GraphComputation.h"
#include "index/IdTableUtils.h"
#include "index/LocatedTriples.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Iterators.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"

using namespace std::chrono_literals;

// A small helper function to obtain the begin and end iterator of a range
template <typename T>
static auto getBeginAndEnd(T& range) {
  return std::pair{ql::ranges::begin(range), ql::ranges::end(range)};
}

// TODO @realHannes:
// Create a separate header file CompressedRelationMetadata for all the
// metadata related helper structs and functions. This should include
// CompressedRelationMetadata, CompressedBlockMetadata, ScanSpecAndBlocks
// containing the newly introduced BlockMetadataRanges type, and the
// helper functions/methods from below (getMaskedTriple,
// containsConsistentTriples, isConsistentWith) for consistency checking.

// Extract the Ids from the given `PermutedTriple` in an array w.r.t. the
// position (column index) defined by `ignoreIndex`. The ignored positions are
// filled with Ids `Id::min()`. `Id::min()` is guaranteed
// to be smaller than Ids of all other types.
static std::array<Id, 3> getMaskedTriple(
    const CompressedBlockMetadata::PermutedTriple& triple,
    size_t ignoreIndex = 3) {
  const Id& undefined = Id::min();
  switch (ignoreIndex) {
    case 3:
      return {triple.col0Id_, triple.col1Id_, triple.col2Id_};
    case 2:
      return {triple.col0Id_, triple.col1Id_, undefined};
    case 1:
      return {triple.col0Id_, undefined, undefined};
    case 0:
      return {undefined, undefined, undefined};
    default:
      // ignoreIndex out of bound.
      AD_FAIL();
  }
}

bool CompressedBlockMetadataNoBlockIndex::containsInconsistentTriples(
    size_t columnIndex) const {
  return getMaskedTriple(firstTriple_, columnIndex) !=
         getMaskedTriple(lastTriple_, columnIndex);
}

bool CompressedBlockMetadataNoBlockIndex::isConsistentWith(
    const CompressedBlockMetadataNoBlockIndex& other,
    size_t columnIndex) const {
  return getMaskedTriple(lastTriple_, columnIndex) ==
         getMaskedTriple(other.firstTriple_, columnIndex);
}

// _____________________________________________________________________________
CompressedBlockMetadataNoBlockIndex::OffsetAndCompressedSize
CompressedBlockMetadataNoBlockIndex::getOffsetAndCompressedSizeForColumn(
    ColumnIndex columnIndex) const {
  if (!offsetsAndCompressedSize_.has_value()) {
    return {0, 0};
  }
  return offsetsAndCompressedSize_.value().at(columnIndex);
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
template <typename T>
static void pruneBlock(T& block, LimitOffsetClause& limitOffset) {
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
template <typename T>
CompressedRelationReader::IdTableGeneratorInputRange
CompressedRelationReader::asyncParallelBlockGenerator(
    T beginBlock, T endBlock, const ScanImplConfig& scanConfig,
    CancellationHandle cancellationHandle,
    LimitOffsetClause& limitOffset) const {
  // Empty range.
  if (beginBlock == endBlock) {
    return IdTableGeneratorInputRange{};
  }

  struct Generator
      : public ad_utility::InputRangeFromGet<IdTable, LazyScanMetadata> {
    const T beginBlock_;
    const T endBlock_;
    T blockMetadataIterator_;
    const ScanImplConfig& scanConfig_;
    CancellationHandle cancellationHandle_;
    LimitOffsetClause& limitOffset_;
    const CompressedRelationReader* reader_;
    ad_utility::Timer popTimer_{
        ad_utility::timer::Timer::InitialStatus::Stopped};
    std::mutex blockIteratorMutex_;
    ad_utility::InputRangeTypeErased<
        std::optional<DecompressedBlockAndMetadata>>
        queue_;
    bool needsStart_{true};

    Generator(T beginBlock, T endBlock, const ScanImplConfig& scanConfig,
              CancellationHandle cancellationHandle,
              LimitOffsetClause& limitOffset,
              const CompressedRelationReader* reader)
        : beginBlock_{beginBlock},
          endBlock_{endBlock},
          blockMetadataIterator_{beginBlock},
          scanConfig_{scanConfig},
          cancellationHandle_{cancellationHandle},
          limitOffset_{limitOffset},
          reader_{reader} {}

    void start() {
      // The rebuild's dedicated reader may override the thread count (to reduce
      // the rebuild's peak CPU); otherwise use the runtime parameter, which is
      // what all query scans use.
      auto numThreads{reader_->lazyScanNumThreadsOverride_.value_or(
          getRuntimeParameter<&RuntimeParameters::lazyIndexScanNumThreads_>())};
      auto queueSize{
          getRuntimeParameter<&RuntimeParameters::lazyIndexScanQueueSize_>()};
      auto producer{std::bind(&Generator::readAndDecompressBlock, this)};

      // Prepare queue for reading and decompressing blocks concurrently using
      // `numThreads` threads.
      queue_ = ad_utility::data_structures::queueManager<
          ad_utility::data_structures::OrderedThreadSafeQueue<
              std::optional<DecompressedBlockAndMetadata>>>(
          queueSize, numThreads, producer);
    }

    std::optional<
        std::pair<size_t, std::optional<DecompressedBlockAndMetadata>>>
    readAndDecompressBlock() {
      cancellationHandle_->throwIfCancelled();
      std::unique_lock lock{blockIteratorMutex_};
      if (blockMetadataIterator_ == endBlock_) {
        return std::nullopt;
      }

      // Note: taking a copy here is probably not necessary (the lifetime of
      // all the blocks is long enough, so a `const&` would suffice), but the
      // copy is cheap and makes the code more robust.
      auto blockMetadata = *blockMetadataIterator_;
      // Note: The order of the following two lines is important: The index
      // of the current blockMetadata depends on the current value of
      // `blockMetadataIterator`, so we have to compute it before incrementing
      // the iterator.
      auto myIndex = static_cast<size_t>(blockMetadataIterator_ - beginBlock_);
      ++blockMetadataIterator_;
      if (scanConfig_.graphFilter_.canBlockBeSkipped(blockMetadata)) {
        return std::pair{myIndex, std::nullopt};
      }
      // Note: the reading of the blockMetadata could also happen without
      // holding the lock. We still perform it inside the lock to avoid
      // contention of the file. On a fast SSD we could possibly change this,
      // but this has to be investigated.
      auto compressedBlock = reader_->readCompressedBlockFromFile(
          blockMetadata, scanConfig_.scanColumns_);

      lock.unlock();
      auto decompressedBlockAndMetadata =
          reader_->decompressAndPostprocessBlock(compressedBlock,
                                                 blockMetadata.numRows_,
                                                 scanConfig_, blockMetadata);
      return std::pair{myIndex,
                       std::optional{std::move(decompressedBlockAndMetadata)}};
    }

    std::optional<IdTable> get() override {
      if (std::exchange(needsStart_, false)) {
        start();
      }

      // Yield the blocks (in the right order) as soon as they become
      // available. Stop when all the blocks have been yielded or the LIMIT of
      // the query is reached. Keep track of various statistics.
      while (true) {
        popTimer_.cont();
        auto&& item{queue_.get()};  // copy elision
        popTimer_.stop();

        details().blockingTime_ = popTimer_.msecs();

        if (item == std::nullopt) {
          break;
        }

        if (cancellationHandle_->isCancelled()) {
          details().blockingTime_ = popTimer_.msecs();
          cancellationHandle_->throwIfCancelled();
        }

        auto& optBlock{item.value()};

        details().update(optBlock);
        if (optBlock.has_value()) {
          auto block{std::move(optBlock.value().block_)};
          pruneBlock(block, limitOffset_);

          if (!block.empty()) {
            details().numElementsYielded_ += block.numRows();
            return block;
          }

          if (limitOffset_._limit.value_or(1) == 0) {
            break;
          }
        }
      }

      return std::nullopt;
    }
  };

  // There is a std::mutex in the generator, so we cannot copy or move it,
  // that's why it is consctucted via a unique_ptr.
  std::unique_ptr<ad_utility::InputRangeFromGet<IdTable, LazyScanMetadata>>
      generator{std::make_unique<Generator>(beginBlock, endBlock, scanConfig,
                                            cancellationHandle, limitOffset,
                                            this)};

  return ad_utility::InputRangeTypeErased{std::move(generator)};
}
// _____________________________________________________________________________
auto CompressedRelationReader::FilterDuplicatesAndGraphs::isGraphAllowedLambda()
    const {
  return [this](Id graph) { return graphFilter_.isGraphAllowed(graph); };
}

// _____________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::
    blockNeedsFilteringByGraph(const CompressedBlockMetadata& metadata) const {
  if (graphFilter_.areAllGraphsAllowed()) {
    return false;
  }
  if (!metadata.graphInfo_.has_value()) {
    return true;
  }
  const auto& graphInfo = metadata.graphInfo_.value();
  return !ql::ranges::all_of(graphInfo, isGraphAllowedLambda());
}

// _____________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::
    filterByGraphIfNecessary(
        IdTable& block, const CompressedBlockMetadata& blockMetadata) const {
  bool needsFilteringByGraph = blockNeedsFilteringByGraph(blockMetadata);
  auto graphIdFromRow = [graphColumn = graphColumn_](const auto& row) {
    return row[graphColumn];
  };
  if (needsFilteringByGraph) {
    auto removedRange = ql::ranges::remove_if(
        block, std::not_fn(isGraphAllowedLambda()), graphIdFromRow);
#ifdef QLEVER_CPP_17
    block.erase(removedRange, block.end());
#else
    block.erase(removedRange.begin(), block.end());
#endif
  } else {
    AD_EXPENSIVE_CHECK(
        graphFilter_.areAllGraphsAllowed() ||
        ql::ranges::all_of(block, isGraphAllowedLambda(), graphIdFromRow));
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
void CompressedRelationReader::FilterDuplicatesAndGraphs::
    deleteGraphColumnIfNecessary(IdTable& block) const {
  if (deleteGraphColumn_) {
    block.deleteColumn(graphColumn_);
  }
}

// _____________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::postprocessBlock(
    IdTable& block, const CompressedBlockMetadata& blockMetadata) const {
  bool filteredByGraph = filterByGraphIfNecessary(block, blockMetadata);
  deleteGraphColumnIfNecessary(block);
  bool filteredByDuplicates = filterDuplicatesIfNecessary(block, blockMetadata);
  return filteredByGraph || filteredByDuplicates;
}

// ______________________________________________________________________________
bool CompressedRelationReader::FilterDuplicatesAndGraphs::canBlockBeSkipped(
    const CompressedBlockMetadata& block) const {
  if (graphFilter_.areAllGraphsAllowed()) {
    return false;
  }
  if (!block.graphInfo_.has_value()) {
    return false;
  }
  return ql::ranges::none_of(block.graphInfo_.value(), isGraphAllowedLambda());
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGeneratorInputRange
CompressedRelationReader::lazyScan(
    const ScanSpecification& scanSpec,
    std::vector<CompressedBlockMetadata> relevantBlockMetadata,
    ColumnIndices additionalColumns,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    const LimitOffsetClause& limitOffset) const {
  AD_CONTRACT_CHECK(cancellationHandle);

  if (relevantBlockMetadata.empty()) {
    return IdTableGeneratorInputRange{};
  }

  struct Generator : ad_utility::InputRangeFromGet<IdTable, LazyScanMetadata> {
    enum class State {
      yieldFirstBlocks,
      createMiddleBlocksGenerator,
      yieldMiddleBlocks,
      yieldLastBlock,
      afterLastYieldedBlock
    };

    using CompressedBlockMetadataIterator =
        std::vector<CompressedBlockMetadata>::iterator;

    ScanSpecification scanSpec_;
    std::vector<CompressedBlockMetadata> relevantBlockMetadata_;
    ColumnIndices additionalColumns_;
    const CancellationHandle& cancellationHandle_;
    const LocatedTriplesPerBlock& locatedTriplesPerBlock_;
    LimitOffsetClause limitOffset_;
    ad_utility::InputRangeTypeErased<IdTable, LazyScanMetadata>
        blockGenerator_{};
    State state_{State::yieldFirstBlocks};
    CompressedBlockMetadataIterator beginBlockMetadata_;
    CompressedBlockMetadataIterator endBlockMetadata_;
    const CompressedRelationReader* reader_;
    ScanImplConfig config_;
    IdTableGeneratorInputRange middleBlocksGenerator_{};
    // We will modify `limitOffset` as we go. We make a copy of the original
    // value for some sanity checks at the end of the function.
    const LimitOffsetClause originalLimit_{limitOffset_};
    std::size_t numBlocksTotal_;

    Generator(ScanSpecification scanSpec,
              std::vector<CompressedBlockMetadata> relevantBlockMetadata,
              ColumnIndices additionalColumns,
              const CancellationHandle& cancellationHandle,
              const LocatedTriplesPerBlock& locatedTriplesPerBlock,
              const LimitOffsetClause& limitOffset,
              const CompressedRelationReader* reader,
              const ScanImplConfig& config)
        : scanSpec_{std::move(scanSpec)},
          relevantBlockMetadata_{std::move(relevantBlockMetadata)},
          additionalColumns_{std::move(additionalColumns)},
          cancellationHandle_{cancellationHandle},
          locatedTriplesPerBlock_{locatedTriplesPerBlock},
          limitOffset_{limitOffset},
          reader_{reader},
          config_{config} {}

    void start() {
      beginBlockMetadata_ = ql::ranges::begin(relevantBlockMetadata_);
      endBlockMetadata_ = ql::ranges::end(relevantBlockMetadata_);

      numBlocksTotal_ = endBlockMetadata_ - beginBlockMetadata_;
    }

    auto getIncompleteBlock(CompressedBlockMetadataIterator it) {
      auto result = reader_->readPossiblyIncompleteBlock(
          scanSpec_, config_, *it, std::ref(details()),
          locatedTriplesPerBlock_);

      return result;
    }

    auto getPrunedBlockAndUpdateDetails(CompressedBlockMetadataIterator it) {
      auto block = getIncompleteBlock(it);
      pruneBlock(block, limitOffset_);
      if (!block.empty()) {
        details().numElementsYielded_ += block.numRows();
      }
      return block;
    }

    std::optional<IdTable> get() override {
      switch (state_) {
        case State::yieldFirstBlocks: {
          start();
          AD_CORRECTNESS_CHECK(beginBlockMetadata_ < endBlockMetadata_);

          // Get and yield the first block.
          auto block = getPrunedBlockAndUpdateDetails(beginBlockMetadata_);

          state_ = (beginBlockMetadata_ + 1 < endBlockMetadata_)
                       ? State::createMiddleBlocksGenerator
                       : State::afterLastYieldedBlock;

          if (!block.empty()) {
            return block;
          }
          // recursively go to next state because there is no data to yield
          // from this call
          return get();
        }

        case State::createMiddleBlocksGenerator: {
          middleBlocksGenerator_ = reader_->asyncParallelBlockGenerator(
              beginBlockMetadata_ + 1, endBlockMetadata_ - 1, config_,
              cancellationHandle_, limitOffset_);
          middleBlocksGenerator_.setDetailsPointer(&details());
          state_ = State::yieldMiddleBlocks;
        }
          [[fallthrough]];

        case State::yieldMiddleBlocks: {
          auto block{middleBlocksGenerator_.get()};
          if (block.has_value()) {
            return std::move(block.value());
          } else {
            state_ = State::yieldLastBlock;
          }
        }
          [[fallthrough]];

        case State::yieldLastBlock: {
          {
            auto block = getPrunedBlockAndUpdateDetails(endBlockMetadata_ - 1);
            state_ = State::afterLastYieldedBlock;

            if (!block.empty()) {
              return block;
            }
          }
        }
          [[fallthrough]];

        case State::afterLastYieldedBlock:
          checkInvariantsAtEnd();
      }

      return std::nullopt;
    }

    void checkInvariantsAtEnd() {
      // Some sanity checks.
      const auto& limit = originalLimit_._limit;

      const LazyScanMetadata& d{details()};
      AD_CORRECTNESS_CHECK(!limit.has_value() ||
                           d.numElementsYielded_ <= limit.value());
      AD_CORRECTNESS_CHECK(
          numBlocksTotal_ ==
                  (d.numBlocksRead_ + d.numBlocksSkippedBecauseOfGraph_) ||
              !limitOffset_.isUnconstrained(),
          [&]() {
            return absl::StrCat(numBlocksTotal_, " ", d.numBlocksRead_, " ",
                                d.numBlocksSkippedBecauseOfGraph_);
          });
    }
  };

  auto config =
      getScanConfig(scanSpec, additionalColumns, locatedTriplesPerBlock);

  return IdTableGeneratorInputRange{Generator{
      scanSpec, std::move(relevantBlockMetadata), additionalColumns,
      cancellationHandle, locatedTriplesPerBlock, limitOffset, this, config}};
}

// _____________________________________________________________________________
IdTable CompressedRelationReader::readBlockWithoutLocatedTriples(
    CompressedBlockMetadata block, ColumnIndices additionalColumns) const {
  auto config = getScanConfig({std::nullopt, std::nullopt, std::nullopt},
                              std::move(additionalColumns), {});
  CompressedBlock compressedColumns =
      readCompressedBlockFromFile(block, config.scanColumns_);
  auto decompressedBlock = decompressBlock(compressedColumns, block.numRows_);
  return decompressedBlock;
}

// _____________________________________________________________________________
Id CompressedRelationReader::getRelevantIdFromTriple(
    CompressedBlockMetadata::PermutedTriple triple,
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks) {
  // The `ScanSpecifcation`, which must ask for at least one column.
  const auto& scanSpec = metadataAndBlocks.scanSpec_;
  AD_CORRECTNESS_CHECK(!scanSpec.col2Id());

  // For a full scan, return the triples's `col0Id`.
  if (!scanSpec.col0Id().has_value()) {
    return triple.col0Id_;
  }

  // Compute the following range: If the `scanSpec` specifies both `col0Id`
  // and `col1Id`, the first and last `col2Id` of the blocks. If the
  // `scanSpec` specifies only `col0Id`, the first and last `col1Id` of the
  // blocks.
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
  // triples's `col2Id`. Otherwise, return `minId` (if it is smaller) or
  // `maxId` (if it is larger).
  return idForNonMatchingBlock(triple.col1Id_, scanSpec.col1Id().value(), minId,
                               maxId)
      .value_or(triple.col2Id_);
}

// _____________________________________________________________________________
auto CompressedRelationReader::getBlocksForJoin(
    ql::span<const Id> joinColumn,
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks)
    -> GetBlocksForJoinResult {
  if (joinColumn.empty() || metadataAndBlocks.getBlockMetadataView().empty()) {
    return {};
  }

  // `id < block` iff `id < block.firstTriple`
  auto idLessThanBlock = [&metadataAndBlocks](
                             Id id, const CompressedBlockMetadata& block) {
    return id < getRelevantIdFromTriple(block.firstTriple_, metadataAndBlocks);
  };

  // `block < id` iff `block.lastTriple < id`
  auto blockLessThanId = [&metadataAndBlocks](
                             const CompressedBlockMetadata& block, Id id) {
    return getRelevantIdFromTriple(block.lastTriple_, metadataAndBlocks) < id;
  };

  std::vector<CompressedBlockMetadata> result;
  const auto& mdView = metadataAndBlocks.getBlockMetadataView();

  auto [colIt, colEnd] = getBeginAndEnd(joinColumn);
  auto [blockIt, blockEnd] = getBeginAndEnd(mdView);
  GetBlocksForJoinResult res;

  // Manually count the number of blocks that have been fully processed in the
  // `mdView`. This includes blocks that are returned as part of the result as
  // well as blocks that are completely skipped, because they are
  // `< joinColumn.back()` but don't match any of the entries in the
  // `joinColumn`.
  auto& blockIdx = res.numHandledBlocks;
  while (true) {
    // Skip all IDs in the `joinColumn` that are strictly smaller than any
    // block that hasn't been handled so far.
    while (colIt != colEnd && idLessThanBlock(*colIt, *blockIt)) {
      ++colIt;
    }
    if (colIt == colEnd) {
      return res;
    }

    // At this point, `*blockIt <= *colIt`.
    // Now skip all blocks that are `< *colIt`.
    while (blockIt != blockEnd && blockLessThanId(*blockIt, *colIt)) {
      ++blockIt;
      ++blockIdx;
    }
    if (blockIt == blockEnd) {
      return res;
    }
    // Now it holds that `*blockIt >= *colIt`. As the entries in the
    // `joinColumn` as well as the blocks are sorted, it suffices to
    // additionally find the values where `*blockIt <= *colIt` to find
    // possibly matching blocks.
    while (blockIt != blockEnd && !idLessThanBlock(*colIt, *blockIt)) {
      res.matchingBlocks_.push_back(*blockIt);
      ++blockIt;
      ++blockIdx;
    }
    if (blockIt == blockEnd) {
      return res;
    }
  }
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
        auto result = metadataAndBlocks.getBlockMetadataView() |
                      ql::views::transform(getSingleBlock);
        AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(result, blockLessThanBlock));
        return result;
      };

  auto blocksWithFirstAndLastId1 =
      getBlocksWithFirstAndLastId(metadataAndBlocks1);
  auto blocksWithFirstAndLastId2 =
      getBlocksWithFirstAndLastId(metadataAndBlocks2);

  // Find the matching blocks on each side using a linear-time merge zipper.
  // Both sequences are sorted by `first_` with non-overlapping intervals
  // (i.e. consecutive blocks `b1, b2` from the same side satisfy
  // `b1.last_ < b2.first_`; invariant enforced above by the
  // `AD_CORRECTNESS_CHECK` on `is_sorted` under `blockLessThanBlock`). The
  // stateful pointer into `otherBlocks` never moves backward because
  // `a.first_` is non-decreasing, giving O(n + m) total.
  //
  // NOTE: it is tempting to reuse the `zipperJoinWithUndef` routine, but this
  // doesn't work because the implicit equality defined by `!lessThan(a,b) &&
  // !lessThan(b, a)` is not transitive.
  auto findMatchingBlocks = [&blockLessThanBlock](const auto& blocks,
                                                  const auto& otherBlocks) {
    std::vector<CompressedBlockMetadata> result;
    auto [it, end] = getBeginAndEnd(otherBlocks);
    for (const auto& a : blocks) {
      it = ql::ranges::find_if_not(it, end,
                                   [&blockLessThanBlock, &a](const auto& b) {
                                     return blockLessThanBlock(b, a);
                                   });
      if (it == end) {
        break;
      }
      if (!blockLessThanBlock(a, *it)) {
        result.push_back(a.block_);
      }
    }
    return result;
  };

  return {
      findMatchingBlocks(blocksWithFirstAndLastId1, blocksWithFirstAndLastId2),
      findMatchingBlocks(blocksWithFirstAndLastId2, blocksWithFirstAndLastId1)};
}

// _____________________________________________________________________________
IdTable CompressedRelationReader::scan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    ColumnIndicesRef additionalColumns,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    const LimitOffsetClause& limitOffset) const {
  const auto& scanSpec = scanSpecAndBlocks.scanSpec_;
  auto columnIndices = prepareColumnIndices(scanSpec, additionalColumns);
  IdTable result(columnIndices.size(), allocator_);
  // Compute an upper bound for the size and reserve enough space in the
  // result.
  auto sizes = scanSpecAndBlocks.getBlockMetadataView() |
               ql::views::transform(&CompressedBlockMetadata::numRows_);
  auto upperBoundSize = std::accumulate(sizes.begin(), sizes.end(), size_t{0});
  if (limitOffset._limit.has_value()) {
    upperBoundSize = std::min(upperBoundSize,
                              static_cast<size_t>(limitOffset._limit.value()));
  }
  result.reserve(upperBoundSize);

  for (const auto& block : lazyScan(
           scanSpec,
           convertBlockMetadataRangesToVector(scanSpecAndBlocks.blockMetadata_),
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
  bool manuallyDeleteGraphColumn = scanConfig.graphFilter_.deleteGraphColumn_;
  // We first scan the complete block including ALL columns with the following
  // exception: If `manuallyDeleteGraphColumn` is true, then the `graphColumn`
  // is contained inside `scanConfig.scanColumns`, but this function is supposed
  // to delete it. In this case we will not scan the graph column, that way the
  // `readAndDecompressBlock` function will correctly delete all duplicates from
  // the block. The downside of this approach is that further down we have to be
  // aware of this already dropped column when assembling the final result.
  std::vector<ColumnIndex> allAdditionalColumns;
  if (!manuallyDeleteGraphColumn) {
    allAdditionalColumns.push_back(ADDITIONAL_COLUMN_GRAPH_ID);
  }
  for (ColumnIndex index : scanConfig.scanColumns_) {
    if (index > ADDITIONAL_COLUMN_GRAPH_ID) {
      allAdditionalColumns.push_back(index);
    }
  }
  ScanSpecification specForAllColumns{std::nullopt,
                                      std::nullopt,
                                      std::nullopt,
                                      {},
                                      scanConfig.graphFilter_.graphFilter_};
  auto config = getScanConfig(specForAllColumns,
                              std::move(allAdditionalColumns), locatedTriples);

  // Helper lambda that returns the decompressed block or an empty block if
  // `readAndDecompressBlock` returns `std::nullopt`.
  DecompressedBlock block = [&]() {
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

  // Those are the column indices from the scanned result (which might be
  // different from the original indices, because additional columns might be
  // missing) that will become part of the final result.
  std::vector<ColumnIndex> indicesToCopy;
  indicesToCopy.reserve(scanConfig.scanColumns_.size());
  // Helper lambda that narrows down the range of the block so that all values
  // in column `columnIdx` are equal to `relevantId`. If `relevantId` is
  // `std::nullopt`, the range is not narrowed down.
  auto filterColumn = [&block, &beginIdx, &endIdx, &indicesToCopy, &scanConfig](
                          std::optional<Id> relevantId, ColumnIndex columnIdx) {
    if (!relevantId.has_value()) {
      indicesToCopy.push_back(columnIdx);
      return;
    }
    const auto& column = block.getColumn(columnIdx);
    auto matchingRange = ql::ranges::equal_range(
        column.begin() + beginIdx, column.begin() + endIdx, relevantId.value());
    beginIdx = matchingRange.begin() - column.begin();
    endIdx = matchingRange.end() - column.begin();
    // The function `getFirstAndLastTripleIgnoringGraph` is the only function
    // where the passed `scanConfig` isn't created from the passed `scanSpec`.
    // Handle this case so that we don't drop the fixed columns in that case.
    if (ad_utility::contains(scanConfig.scanColumns_, columnIdx)) {
      indicesToCopy.push_back(columnIdx);
    }
  };

  // Now narrow down the range of the block by first `scanSpec.col0Id()`,
  // then `scanSpec.col1Id()`, and then `scanSpec.col2Id()`. This order is
  // important because the rows are sorted in that order.
  filterColumn(scanSpec.col0Id(), 0);
  filterColumn(scanSpec.col1Id(), 1);
  filterColumn(scanSpec.col2Id(), 2);

  // Copy all additional columns as-is.
  for (ColumnIndex i : ad_utility::integerRange(allAdditionalColumns.size())) {
    indicesToCopy.push_back(3 + i);
  }

  // Now copy the range `[beginIdx, endIdx)` from `block` to `result`.
  DecompressedBlock result{indicesToCopy.size(), allocator_};
  result.insertAtEnd(block, beginIdx, endIdx, indicesToCopy);

  // Return the result.
  return result;
}

// ____________________________________________________________________________
template <bool exactSize>
std::pair<size_t, size_t> CompressedRelationReader::getResultSizeImpl(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  const auto& blocks = scanSpecAndBlocks.getBlockMetadataView();
  auto [beginBlock, endBlock] = getBeginAndEnd(blocks);
  const auto& scanSpec = scanSpecAndBlocks.scanSpec_;
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
      // If the first and last triple of the block match, then we know that
      // the whole block belongs to the result.
      bool isComplete = isTripleInSpecification(scanSpec, block.firstTriple_) &&
                        isTripleInSpecification(scanSpec, block.lastTriple_);
      size_t divisor =
          isComplete
              ? 1
              : getRuntimeParameter<
                    &RuntimeParameters::smallIndexScanSizeEstimateDivisor_>();
      const auto [ins, del] =
          locatedTriplesPerBlock.numTriples(block.blockIndex_);
      auto trunc = [divisor](size_t num) {
        return std::max<size_t>(std::min<size_t>(num, 1), num / divisor);
      };
      inserted += trunc(ins);
      deleted += trunc(del);
      numResults += trunc(block.numRows_);
    }
  };

  // The first and the last block might be incomplete, compute
  // and store the partial results from them.
  if (beginBlock != endBlock) {
    readSizeOfPossiblyIncompleteBlock(*beginBlock);
    ++beginBlock;
  }
  if (beginBlock != endBlock) {
    readSizeOfPossiblyIncompleteBlock(*(std::prev(endBlock)));
    --endBlock;
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
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  return getResultSizeImpl<false>(scanSpecAndBlocks, locatedTriplesPerBlock);
}

// ____________________________________________________________________________
size_t CompressedRelationReader::getResultSizeOfScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  auto [lower, upper] =
      getResultSizeImpl<true>(scanSpecAndBlocks, locatedTriplesPerBlock);
  AD_CORRECTNESS_CHECK(lower == upper);
  return lower;
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
namespace {
using ScanSpecAndBlocks = CompressedRelationReader::ScanSpecAndBlocks;

// The number of rows after which `getDistinctCol0Ids` yields a new `IdTable`.
// This is only an upper bound to keep the memory usage bounded, the sizes of
// the yielded tables don't matter otherwise.
constexpr size_t distinctCol0IdsChunkSize = 100'000;

// The index of the graph column in the blocks that `getDistinctCol0Ids` reads.
// The blocks of a full scan always consist of the three triple columns,
// followed by the graph column (if it was requested).
constexpr ColumnIndex graphColumnInBlock = 3;

// The classification of the blocks of a full scan by `BlockSelector` below.
struct SelectedBlocks {
  // The blocks that have to be read from disk.
  std::vector<CompressedBlockMetadata> toRead_;
  // The IDs (and graph IDs, if requested) that are already known from the block
  // metadata alone, in ascending order of the IDs, possibly with duplicates.
  IdTable fromMetadata_;
};

// Split the blocks of a full scan into those that have to be read from disk and
// those whose contribution to the distinct `col0Id`s is already known from
// their metadata (see `CompressedRelationReader::getDistinctCol0Ids`).
class BlockSelector {
  const ScanSpecification& scanSpec_;
  bool addGraphColumn_;
  const std::optional<std::vector<Id>>& idFilter_;
  const LocatedTriplesPerBlock& locatedTriples_;
  SelectedBlocks result_;

 public:
  BlockSelector(const ScanSpecification& scanSpec, bool addGraphColumn,
                const std::optional<std::vector<Id>>& idFilter,
                const LocatedTriplesPerBlock& locatedTriples,
                const CompressedRelationReader::Allocator& allocator)
      : scanSpec_{scanSpec},
        addGraphColumn_{addGraphColumn},
        idFilter_{idFilter},
        locatedTriples_{locatedTriples},
        result_{{}, IdTable{addGraphColumn ? 2u : 1u, allocator}} {}

  // Perform the classification described above.
  SelectedBlocks select(const ScanSpecAndBlocks& scanSpecAndBlocks) && {
    const auto& graphFilter = scanSpec_.graphFilter();
    forEachCandidateBlock(
        scanSpecAndBlocks,
        [this, &graphFilter](const CompressedBlockMetadata& block) {
          if (blockIsFilteredOut(block)) {
            return;
          }
          if (blockNeedsToBeRead(block)) {
            result_.toRead_.push_back(block);
            return;
          }
          Id id = block.firstTriple_.col0Id_;
          if (!addGraphColumn_) {
            addToMetadata(id, Id::makeUndefined());
            return;
          }
          AD_CORRECTNESS_CHECK(block.graphInfo_.has_value());
          ql::ranges::for_each(
              block.graphInfo_.value() |
                  ql::views::filter([&graphFilter](Id graph) {
                    return graphFilter.isGraphAllowed(graph);
                  }),
              [this, id](Id graph) { addToMetadata(id, graph); });
        });
    return std::move(result_);
  }

 private:
  // Return true iff none of the triples of the given block is contained in one
  // of the graphs that are allowed by the graph filter. This can only be
  // determined if the metadata of the block knows all of its graphs.
  bool blockIsFilteredOut(const CompressedBlockMetadata& block) const {
    const auto& graphFilter = scanSpec_.graphFilter();
    if (graphFilter.areAllGraphsAllowed() || !block.graphInfo_.has_value()) {
      return false;
    }
    return ql::ranges::none_of(block.graphInfo_.value(), [&graphFilter](Id id) {
      return graphFilter.isGraphAllowed(id);
    });
  }

  // Return true iff the given block has to be read to determine the IDs (and
  // graph IDs) that it contributes.
  bool blockNeedsToBeRead(const CompressedBlockMetadata& block) const {
    // Blocks with more than a single `col0Id` have to be read to get the IDs in
    // between.
    if (block.firstTriple_.col0Id_ != block.lastTriple_.col0Id_) {
      return true;
    }
    // The metadata of a block describes the triples that are stored on disk. If
    // there are updates for the block, then triples might have been deleted, so
    // the `col0Id` and the graphs from the metadata are not necessarily part of
    // the dataset anymore (and inserted triples might add new ones). We
    // therefore always read such blocks, which makes the located triples be
    // merged in and hence gives us the actual contents.
    if (locatedTriples_.containsTriples(block.blockIndex_)) {
      return true;
    }
    // At this point the single `col0Id` of the block is known, so we only have
    // to read it if we need graph IDs that the metadata doesn't know, or if we
    // cannot rule out that the graph filter removes all of its triples.
    return !block.graphInfo_.has_value() &&
           (addGraphColumn_ || !scanSpec_.graphFilter().areAllGraphsAllowed());
  }

  // Add the given `id` (together with `graph` if graph IDs are requested) to
  // `result_.fromMetadata_`, unless it duplicates the previously added row.
  void addToMetadata(Id id, Id graph) {
    IdTable& table = result_.fromMetadata_;
    size_t numRows = table.numRows();
    if (numRows != 0 && table(numRows - 1, 0) == id &&
        (!addGraphColumn_ || table(numRows - 1, 1) == graph)) {
      return;
    }
    if (addGraphColumn_) {
      table.push_back({id, graph});
    } else {
      table.push_back({id});
    }
  }

  // Call `action` for all blocks that might contain one of the requested IDs,
  // in ascending order.
  template <typename Action>
  void forEachCandidateBlock(const ScanSpecAndBlocks& scanSpecAndBlocks,
                             const Action& action) {
    if (!idFilter_.has_value()) {
      ql::ranges::for_each(scanSpecAndBlocks.getBlockMetadataView(), action);
      return;
    }
    auto firstCol0Id = [](const CompressedBlockMetadata& metadata) {
      return metadata.firstTriple_.col0Id_;
    };
    auto lastCol0Id = [](const CompressedBlockMetadata& metadata) {
      return metadata.lastTriple_.col0Id_;
    };
    const auto& ids = idFilter_.value();
    auto id = ids.begin();
    // The blocks are sorted by their `col0Id`s, so for each of the requested
    // IDs we can binary search the blocks that might contain it.
    for (const auto& blocks : scanSpecAndBlocks.blockMetadata_) {
      auto block = blocks.begin();
      auto firstUnhandledBlock = blocks.begin();
      while (id != ids.end()) {
        // Skip all the blocks that only contain smaller `col0Id`s.
        block =
            ql::ranges::lower_bound(block, blocks.end(), *id, {}, lastCol0Id);
        if (block == blocks.end()) {
          break;
        }
        // All the blocks that start with a `col0Id` that is not larger than
        // `*id` might contain it. Note that they all end with a `col0Id` that
        // is at least `*id`, because `block` does and the blocks are sorted.
        auto end =
            ql::ranges::upper_bound(block, blocks.end(), *id, {}, firstCol0Id);
        ql::ranges::for_each(
            ql::ranges::subrange{std::max(block, firstUnhandledBlock), end},
            action);
        firstUnhandledBlock = std::max(firstUnhandledBlock, end);
        ++id;
      }
      if (id == ids.end()) {
        return;
      }
    }
  }
};

// Insert `value` into the sorted vector `values`, unless it is already there.
// The vectors this is used for are tiny, so the linear insert is cheaper than
// sorting and deduplicating afterwards.
void insertSorted(std::vector<Id>& values, Id value) {
  auto it = ql::ranges::lower_bound(values, value);
  if (it == values.end() || *it != value) {
    values.insert(it, value);
  }
}

// A cursor over one of the two ascending sources of IDs that
// `getDistinctCol0Ids` merges. The tables are fetched one at a time by the
// `nextTable` function, their first column holds the IDs. If `graphColumn` is
// set, that column holds the graph IDs.
template <typename NextTable>
class IdCursor {
  NextTable nextTable_;
  std::optional<ColumnIndex> graphColumn_;
  std::optional<IdTable> table_ = std::nullopt;
  size_t row_ = 0;
  bool isExhausted_ = false;

 public:
  IdCursor(NextTable nextTable, std::optional<ColumnIndex> graphColumn)
      : nextTable_{std::move(nextTable)}, graphColumn_{graphColumn} {}

  // The ID of the next row that hasn't been consumed yet, or `std::nullopt` if
  // all rows have been consumed. Advances to the next table if necessary.
  std::optional<Id> peek() {
    while (!table_.has_value() || row_ == table_.value().numRows()) {
      if (isExhausted_) {
        return std::nullopt;
      }
      table_ = nextTable_();
      row_ = 0;
      isExhausted_ = !table_.has_value();
    }
    return table_.value()(row_, 0);
  }

  // Consume all the rows that belong to `id`, adding their graph IDs to
  // `graphs` (see `insertSorted`) if this cursor has a graph column.
  void consumeId(Id id, std::vector<Id>& graphs) {
    while (peek() == std::optional{id}) {
      if (graphColumn_.has_value()) {
        insertSorted(graphs, table_.value()(row_, graphColumn_.value()));
      }
      ++row_;
    }
  }
};

// Return a function that can be passed to `IdCursor` and that yields the given
// `table` once and nothing afterwards.
auto singleTableSource(IdTable table) {
  return [table = std::optional{std::move(table)}]() mutable {
    return std::exchange(table, std::nullopt);
  };
}

// The IDs that the caller of `getDistinctCol0Ids` requested (all of them if
// `ids` is `std::nullopt`). The IDs have to be passed to `contains` in
// ascending order, which makes it run in amortized constant time.
class RequestedIds {
  const std::optional<std::vector<Id>>& ids_;
  size_t index_ = 0;

 public:
  explicit RequestedIds(const std::optional<std::vector<Id>>& ids)
      : ids_{ids} {}

  bool contains(Id id) {
    if (!ids_.has_value()) {
      return true;
    }
    const auto& ids = ids_.value();
    while (index_ < ids.size() && ids[index_] < id) {
      ++index_;
    }
    return index_ < ids.size() && ids[index_] == id;
  }
};

// The smaller of the two IDs, or `std::nullopt` if both of them are
// `std::nullopt`.
std::optional<Id> smallerId(std::optional<Id> first, std::optional<Id> second) {
  if (!first.has_value()) {
    return second;
  }
  if (!second.has_value()) {
    return first;
  }
  return std::min(first.value(), second.value());
}

// Create an empty table for the result of `getDistinctCol0Ids`, with enough
// space reserved for one chunk (or for fewer rows if only few IDs were
// requested).
IdTable makeResultTable(bool addGraphColumn,
                        const std::optional<std::vector<Id>>& idFilter,
                        const CompressedRelationReader::Allocator& allocator) {
  IdTable table{addGraphColumn ? 2u : 1u, allocator};
  table.reserve(idFilter.has_value() ? std::min(distinctCol0IdsChunkSize,
                                                idFilter.value().size())
                                     : distinctCol0IdsChunkSize);
  return table;
}

// Append the rows for a single distinct `id` to `result`. If `result` has a
// graph column, one row per graph ID is appended, else a single row.
void appendRowsForId(IdTable& result, Id id, const std::vector<Id>& graphs) {
  if (result.numColumns() == 1) {
    result.push_back({id});
    return;
  }
  // `IdTable`s are stored in column-major order, so we write the two columns
  // separately instead of row by row.
  size_t numRows = result.numRows();
  result.resize(numRows + graphs.size());
  ql::ranges::fill(result.getColumn(0).subspan(numRows), id);
  ql::ranges::copy(graphs, result.getColumn(1).begin() + numRows);
}

}  // namespace

// ____________________________________________________________________________
cppcoro::generator<IdTable, CompressedRelationReader::LazyScanMetadata>
CompressedRelationReader::getDistinctCol0Ids(
    ScanSpecAndBlocks scanSpecAndBlocks, bool addGraphColumn,
    std::optional<std::vector<Id>> idFilter,
    CancellationHandle cancellationHandle,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  AD_CONTRACT_CHECK(cancellationHandle != nullptr);
  AD_CONTRACT_CHECK(scanSpecAndBlocks.scanSpec_.firstFreeColIndex() == 0,
                    "`getDistinctCol0Ids` only supports full scans.");
  AD_EXPENSIVE_CHECK(!idFilter.has_value() ||
                     ql::ranges::is_sorted(idFilter.value()));

  auto [blocksToRead, fromMetadata] =
      BlockSelector{scanSpecAndBlocks.scanSpec_, addGraphColumn, idFilter,
                    locatedTriplesPerBlock, allocator_}
          .select(scanSpecAndBlocks);

  // Let the inner scan write its statistics (most importantly the number of
  // blocks it actually read) directly into our own details, such that the
  // consumer of this generator sees them.
  auto& details = co_await cppcoro::getDetails;
  details.numBlocksAll_ = scanSpecAndBlocks.sizeBlockMetadata_;
  auto scan =
      lazyScan(scanSpecAndBlocks.scanSpec_, std::move(blocksToRead),
               addGraphColumn ? ColumnIndices{ADDITIONAL_COLUMN_GRAPH_ID}
                              : ColumnIndices{},
               cancellationHandle, locatedTriplesPerBlock, {});
  scan.setDetailsPointer(&details);

  // The IDs are computed by merging two ascending sources: the IDs that are
  // known from the block metadata alone, and the IDs from the blocks that had
  // to be read. We process one ID at a time and collect its graph IDs (if
  // requested) from both sources before appending it to the result.
  IdCursor fromMetadataCursor{
      singleTableSource(std::move(fromMetadata)),
      addGraphColumn ? std::optional{ColumnIndex{1}} : std::nullopt};
  IdCursor fromBlocksCursor{
      [&scan]() { return scan.get(); },
      addGraphColumn ? std::optional{graphColumnInBlock} : std::nullopt};
  RequestedIds requestedIds{idFilter};

  std::vector<Id> graphs;
  IdTable result = makeResultTable(addGraphColumn, idFilter, allocator_);
  for (;;) {
    cancellationHandle->throwIfCancelled();
    auto id = smallerId(fromMetadataCursor.peek(), fromBlocksCursor.peek());
    if (!id.has_value()) {
      break;
    }
    graphs.clear();
    fromMetadataCursor.consumeId(id.value(), graphs);
    fromBlocksCursor.consumeId(id.value(), graphs);
    // Blocks that had to be read can contain IDs that weren't requested.
    if (requestedIds.contains(id.value())) {
      appendRowsForId(result, id.value(), graphs);
    }
    if (result.numRows() >= distinctCol0IdsChunkSize) {
      co_yield std::move(result);
      result = makeResultTable(addGraphColumn, idFilter, allocator_);
    }
  }
  if (!result.empty()) {
    co_yield std::move(result);
  }
}
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// ____________________________________________________________________________
IdTable CompressedRelationReader::getDistinctColIdsAndCounts(
    ColumnIndex columnIndex, const ScanSpecAndBlocks& scanSpecAndBlocks,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    const LimitOffsetClause& limitOffset) const {
  AD_CORRECTNESS_CHECK(columnIndex <= 1, "Only column 0 and 1 are supported");
  // The result has two columns: one for the distinct `Id`s and one for their
  // counts.
  IdTableStatic<2> table(allocator_);

  // The current `colId` and its current count.
  std::optional<Id> currentColId;
  size_t currentCount = 0;
  uint64_t remainingOffset = limitOffset._offset;
  uint64_t remainingLimit = limitOffset.limitOrDefault();

  // For LIMIT 0 we need to abort early for correctness (and its also more
  // efficient).
  if (remainingLimit == 0) {
    return std::move(table).toDynamic();
  }

  // Helper lambda that processes the next `colId` and a count. If it's new, a
  // row with the previous `currentColId` and its count are added to the
  // result, and `currentColId` and its count are updated to the new `colId`.
  auto processColId = [&table, &currentColId, &currentCount, &remainingOffset,
                       &remainingLimit](std::optional<Id> colId,
                                        size_t colIdCount) {
    bool abort = false;
    if (colId != currentColId) {
      if (currentColId.has_value()) {
        // Handle `OFFSET` clause correctly.
        if (currentCount > remainingOffset) {
          currentCount -= remainingOffset;
          remainingOffset = 0;
          // Handle `LIMIT` clause correctly.
          if (remainingLimit >= currentCount) {
            remainingLimit -= currentCount;
          } else {
            currentCount = remainingLimit;
            remainingLimit = 0;
          }
          abort = remainingLimit == 0;
          table.push_back(
              {currentColId.value(), Id::makeFromInt(currentCount)});
        } else {
          remainingOffset -= currentCount;
        }
      }
      currentColId = colId;
      currentCount = 0;
    }
    currentCount += colIdCount;
    return abort;
  };

  const auto& scanSpec = scanSpecAndBlocks.scanSpec_;
  const auto& blocks = scanSpecAndBlocks.getBlockMetadataView();

  // TODO<joka921> We have to read the other columns for the merging of the
  // located triples. We could skip this for blocks with no updates, but that
  // would require more arguments to the `decompressBlock` function.
  auto scanConfig = getScanConfig(scanSpec, {}, locatedTriplesPerBlock);
  // Iterate over the blocks and only read (and decompress) those which
  // contain more than one different `colId`. For the others, we can determine
  // the count from the metadata.
  for (const auto& [i, blockMetadata] : ranges::views::enumerate(blocks)) {
    // The `numRows_` metadata shortcut is safe iff all rows of the block
    // agree on the grouped column AND the block has no delta triples. The
    // column uniformity is equivalent to `firstTriple_` and `lastTriple_`
    // agreeing on the first `columnIndex + 1` columns.
    if (!blockMetadata.containsInconsistentTriples(columnIndex + 1) &&
        !locatedTriplesPerBlock.containsTriples(blockMetadata.blockIndex_)) {
      // The whole block has the same `colId` and no delta triples ->
      // we get all the information from the metadata.
      Id colId = getMaskedTriple(blockMetadata.firstTriple_)[columnIndex];
      bool abort = processColId(colId, blockMetadata.numRows_);
      if (abort) {
        return std::move(table).toDynamic();
      }
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
      // TODO<C++23>: use `ql::views::chunk_by`.
      for (size_t j = 0; j < block.numRows(); ++j) {
        Id colId = block(j, 0);
        bool abort = processColId(colId, 1);
        if (abort) {
          return std::move(table).toDynamic();
        }
      }
    }
  }
  // Don't forget to add the last `col1Id` and its count.
  processColId(std::nullopt, 0);
  return std::move(table).toDynamic();
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
  compressAndWriteBlock(currentBlockFirstCol0_, currentBlockLastCol0_,
                        std::move(smallRelationsBuffer_), true);
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
        blockMetaData.getOffsetAndCompressedSizeForColumn(columnIndices[i]);
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
  bool wasPostprocessed = false;
  if (useGraphPostProcessing_) {
    wasPostprocessed =
        scanConfig.graphFilter_.postprocessBlock(decompressedBlock, metadata);
  } else {
    // If we do not use graph postprocessing, we might still need to remove the
    // extra column.
    scanConfig.graphFilter_.deleteGraphColumnIfNecessary(decompressedBlock);
  }
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
                                                     IdTable block,
                                                     bool invokeCallback) {
  auto timer = blockWriteQueueTimer_.startMeasurement();
  blockWriteQueue_.push([this, block = std::move(block), firstCol0Id,
                         lastCol0Id, invokeCallback]() mutable {
    std::vector<CompressedBlockMetadata::OffsetAndCompressedSize> offsets;
    for (const auto& column : block.getColumns()) {
      offsets.push_back(compressAndWriteColumn(column));
    }
    AD_CORRECTNESS_CHECK(!offsets.empty());
    auto numRows = block.numRows();
    const auto& first = block[0];
    const auto& last = block[numRows - 1];
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
size_t CompressedRelationReader::getNumberOfBlockMetadataValues(
    const BlockMetadataRanges& blockMetadata) {
  return ::ranges::accumulate(blockMetadata, 0ULL,
                              [](auto acc, const auto& block) {
                                return acc + ql::ranges::size(block);
                              });
}

// _____________________________________________________________________________
std::vector<CompressedBlockMetadata>
CompressedRelationReader::convertBlockMetadataRangesToVector(
    const BlockMetadataRanges& blockMetadata) {
  std::vector<CompressedBlockMetadata> blocksMaterialized;
  blocksMaterialized.reserve(getNumberOfBlockMetadataValues(blockMetadata));
  ql::ranges::copy(blockMetadata | ql::views::join,
                   std::back_inserter(blocksMaterialized));
  return blocksMaterialized;
}

// _____________________________________________________________________________
BlockMetadataRanges CompressedRelationReader::getRelevantBlocks(
    const ScanSpecification& scanSpec,
    const BlockMetadataRanges& blockMetadata) {
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

  // TODO:
  // Optionally implement a free function like `equal_range(YourRangeType,
  // key, comp)` that implements the equal range correctly. (1) Perform binary
  // search on the inner blocks with respect to the first and
  //     last triple.
  // (2) Perform binary search regarding the outer blocks.
  BlockMetadataRanges resultBlocks;
  ql::ranges::for_each(
      blockMetadata, [&resultBlocks, &key,
                      &comp](const BlockMetadataRange& blockMetadataSubrange) {
        auto result = ql::ranges::equal_range(blockMetadataSubrange, key, comp);
        if (result.begin() != result.end()) {
          resultBlocks.emplace_back(result.begin(), result.end());
        }
      });
  return resultBlocks;
}

// _____________________________________________________________________________
auto CompressedRelationReader::getFirstAndLastTripleIgnoringGraph(
    const ScanSpecAndBlocks& metadataAndBlocks,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const
    -> std::optional<ScanSpecAndBlocksAndBounds::FirstAndLastTriple> {
  if (metadataAndBlocks.sizeBlockMetadata_ == 0) {
    return std::nullopt;
  }
  const auto& blocks = metadataAndBlocks.getBlockMetadataView();
  const auto& scanSpec = metadataAndBlocks.scanSpec_;

  ScanSpecification scanSpecForAllColumns{std::nullopt, std::nullopt,
                                          std::nullopt};
  auto config =
      getScanConfig(scanSpecForAllColumns,
                    std::array{ColumnIndex{ADDITIONAL_COLUMN_GRAPH_ID}},
                    locatedTriplesPerBlock);
  auto scanBlock = [this, &scanSpec, &config, &locatedTriplesPerBlock](
                       const CompressedBlockMetadata& block) {
    // Note: the following call only returns the part of the block that
    // matches the `col0` and `col1`.
    return readPossiblyIncompleteBlock(scanSpec, config, block, std::nullopt,
                                       locatedTriplesPerBlock);
  };

  auto rowToTriple =
      [&](const auto& row) -> CompressedBlockMetadata::PermutedTriple {
    AD_CORRECTNESS_CHECK(!scanSpec.col0Id().has_value() ||
                         row[0] == scanSpec.col0Id().value());
    return {row[0], row[1], row[2], row[ADDITIONAL_COLUMN_GRAPH_ID]};
  };

  // NOTE: Without updates, it would suffice to look at the first and last
  // block in order to determine the first and last triple. However, with
  // updates, all triples in a block might be deleted.

  // Find the first non-empty block.
  auto [firstBlock, firstBlockIt] = [&]() {
    auto last = std::prev(blocks.end());
    for (auto it = blocks.begin(); it != blocks.end(); ++it) {
      auto block = scanBlock(*it);
      if (!block.empty() || it == last) {
        return std::pair{std::move(block), it};
      }
    }
    AD_FAIL();
  }();

  // If we did not find a non-empty block, the scan result is empty and there
  // is no first or last triple.
  if (firstBlock.empty()) {
    return std::nullopt;
  }

  // Find the last non-empty block. Avoid reading the first non-empty block
  // again.
  DecompressedBlock lastBlock{allocator_};
  for (auto it = std::prev(blocks.end());
       it != firstBlockIt && lastBlock.empty(); --it) {
    lastBlock = scanBlock(*it);
  }

  // Handle the case where the first and last non-empty block are the same.
  const auto& actualLastBlock = lastBlock.empty() ? firstBlock : lastBlock;

  AD_CORRECTNESS_CHECK(!actualLastBlock.empty());
  return ScanSpecAndBlocksAndBounds::FirstAndLastTriple{
      rowToTriple(firstBlock.front()), rowToTriple(actualLastBlock.back())};
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
    Id col0Id, size_t numDistinctC1, const IdTable& relation) {
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
                                                        IdTable relation) {
  AD_CORRECTNESS_CHECK(!relation.empty());
  AD_CORRECTNESS_CHECK(currentCol0Id_ == col0Id ||
                       currentCol0Id_.isUndefined());
  currentCol0Id_ = col0Id;
  currentRelationPreviousSize_ += relation.numRows();
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
    ql::ranges::for_each(block.getColumn(1), std::ref(distinctCol1Counter));

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

// _____________________________________________________________________________
std::optional<CompressedRelationMetadata>
CompressedRelationReader::getMetadataForSmallRelation(
    const ScanSpecAndBlocks& scanSpecAndBlocks, Id col0Id,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  CompressedRelationMetadata metadata;
  metadata.col0Id_ = col0Id;
  metadata.offsetInBlock_ = 0;
  const auto& scanSpec = scanSpecAndBlocks.scanSpec_;
  auto config = getScanConfig(scanSpec, {}, locatedTriplesPerBlock);
  const auto& blocks = scanSpecAndBlocks.getBlockMetadataView();
  // For relations that already span more than one block when the index is first
  // built, this function should never be called. With SPARQL UPDATE it might
  // happen that a relation starts in a single block, but added triples land in
  // an adjacent block (because the relation was right at the end of a block).
  // In this case we might also see two blocks here.
  AD_CONTRACT_CHECK(scanSpecAndBlocks.sizeBlockMetadata_ <= 2,
                    "Should only be called for small relations (contained in "
                    "at most one block), or relations that started in a single "
                    "block, but were extended into the adjacent block by "
                    "SPARQL UPDATE, but found a relation which spans ",
                    scanSpecAndBlocks.sizeBlockMetadata_, "block.");

  ad_utility::HashSet<Id> distinctCol2;
  size_t numRowsTotal = 0;
  size_t numDistinct = 0;
  for (const auto& blockMetadata : blocks) {
    auto block = readPossiblyIncompleteBlock(
        scanSpec, config, blockMetadata, std::nullopt, locatedTriplesPerBlock);

    numRowsTotal += block.numRows();
    // The `col1` is sorted, so we compute the multiplicity using
    // `std::unique`. Note: The distinct count might be off by one in the case
    // of two blocks, because we perform the `unique` separately for both
    // blocks. But as the multiplicity is only an approximate measure used for
    // query planning statistics, this is not an issue.
    const auto& blockCol = block.getColumn(0);
    auto endOfUnique = std::unique(blockCol.begin(), blockCol.end());
    numDistinct += endOfUnique - blockCol.begin();

    // The `col2` is unsorted, so we use a hash map.
    for (auto id : block.getColumn(1)) {
      distinctCol2.insert(id);
    }
  };

  if (numRowsTotal == 0) {
    return std::nullopt;
  }
  metadata.numRows_ = numRowsTotal;
  metadata.multiplicityCol1_ =
      CompressedRelationWriter::computeMultiplicity(numRowsTotal, numDistinct);
  metadata.multiplicityCol2_ = CompressedRelationWriter::computeMultiplicity(
      numRowsTotal, distinctCol2.size());
  return metadata;
}

// _____________________________________________________________________________
auto CompressedRelationReader::getScanConfig(
    const ScanSpecification& scanSpec, ColumnIndicesRef additionalColumns,
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
        deleteGraphColumn] = [&]() -> std::pair<size_t, bool> {
    auto it = ql::ranges::find(columnIndices, ADDITIONAL_COLUMN_GRAPH_ID);
    if (it == columnIndices.end()) {
      size_t idx = columnIndices.size() - additionalColumns.size();
      columnIndices.insert(columnIndices.begin() + idx,
                           ADDITIONAL_COLUMN_GRAPH_ID);
      return {idx, true};
    }
    return {ql::ranges::distance(columnIndices.begin(), it), false};
  }();
  FilterDuplicatesAndGraphs graphFilter{scanSpec.graphFilter(),
                                        graphColumnIndex, deleteGraphColumn};
  return {std::move(columnIndices), std::move(graphFilter), locatedTriples};
}

// _____________________________________________________________________________
// Helper to the following block-invariant-check Impls for informative error
// message construction.
auto createErrorMessage = [](const auto& b1, const auto& b2,
                             const std::string& errCause) {
  auto toString = [](const auto& b) {
    std::ostringstream oss;
    oss << b;
    return oss.str();
  };
  return absl::StrCat(errCause, "First Block:\n", toString(b1),
                      "Second Block:\n", toString(b2));
};

// _____________________________________________________________________________
// Check if the provided `Range` holds less than two `CompressedBlockMetadata`
// values.
CPP_template(typename Range)(
    requires ql::ranges::input_range<
        Range>) static bool checkBlockRangeSizeLessThanTwo(const Range&
                                                               blockMetadataRange) {
  auto begin = ql::ranges::begin(blockMetadataRange);
  auto end = ql::ranges::end(blockMetadataRange);
  return begin == end || ql::ranges::next(begin) == end;
}

// _____________________________________________________________________________
CPP_template(typename Range)(
    requires ql::ranges::input_range<
        Range>) static void checkBlockMetadataInvariantOrderAndUniquenessImpl(const Range&
                                                                                  blockMetadataRange) {
  if (checkBlockRangeSizeLessThanTwo(blockMetadataRange)) {
    return;
  }

  auto checkUniquenessAndOrder = [](const auto& blockPair) {
    const auto& [b1, b2] = blockPair;
    // Blocks must be unique.
    AD_CONTRACT_CHECK(b1 != b2 && b1.blockIndex_ != b2.blockIndex_, [&] {
      return createErrorMessage(b1, b2, "Found block metadata duplicates\n");
    });
    // Blocks must adhere to ascending order.
    AD_CONTRACT_CHECK(
        b1.lastTriple_ < b2.lastTriple_ && b1.blockIndex_ < b2.blockIndex_,
        [&] {
          return createErrorMessage(b1, b2,
                                    "Found block metadata order violation\n");
        });
  };
  auto blockMetadataRangeShifted = blockMetadataRange | ql::views::drop(1);
  auto zippedBlockPairs =
      ranges::views::zip(blockMetadataRange, blockMetadataRangeShifted);
  ql::ranges::for_each(zippedBlockPairs, checkUniquenessAndOrder);
}

// ____________________________________________________________________________
CPP_template(typename Range)(requires ql::ranges::input_range<Range>) static void checkBlockMetadataInvariantBlockConsistencyImpl(
    const Range& blockMetadataRange, size_t firstFreeColIndex) {
  if (checkBlockRangeSizeLessThanTwo(blockMetadataRange)) {
    return;
  }
  auto blockMetadataRangeShifted = blockMetadataRange | ql::views::drop(1);
  auto zippedBlockPairs =
      ranges::views::zip(blockMetadataRange, blockMetadataRangeShifted);

  for (const auto& [i, blockPair] :
       ranges::views::enumerate(zippedBlockPairs)) {
    const auto& [b1, b2] = blockPair;
    // Consecutive blocks must contain equivalent values over the fixed
    // columns.
    AD_CONTRACT_CHECK(b1.isConsistentWith(b2, firstFreeColIndex), [&] {
      return createErrorMessage(
          b1, b2, "Found column inconsistency between two blocks\n");
    });
    // All blocks, except the first and last, must contain consistent column
    // values over their triples up to the first free column.
    if (i > 0) {
      AD_CONTRACT_CHECK(
          !b1.containsInconsistentTriples(firstFreeColIndex), [&] {
            return createErrorMessage(
                b1, b2,
                absl::StrCat("The following First Block contains non-constant "
                             "column values up to defined column index: ",
                             firstFreeColIndex));
          });
    }
  }
}

// _____________________________________________________________________________
CompressedRelationReader::ScanSpecAndBlocks::ScanSpecAndBlocks(
    ScanSpecification scanSpec, const BlockMetadataRanges& blockMetadataRanges)
    : scanSpec_(std::move(scanSpec)) {
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    const auto& blockRangeView = blockMetadataRanges | ql::views::join;
    checkBlockMetadataInvariantOrderAndUniquenessImpl(blockRangeView);
  }
  blockMetadata_ = getRelevantBlocks(scanSpec_, blockMetadataRanges);
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    checkBlockMetadataInvariantBlockConsistencyImpl(
        getBlockMetadataView(), scanSpec_.firstFreeColIndex());
  }
  sizeBlockMetadata_ = getNumberOfBlockMetadataValues(blockMetadata_);
}

// _____________________________________________________________________________
ql::span<const CompressedBlockMetadata>
CompressedRelationReader::ScanSpecAndBlocks::getBlockMetadataSpan() const {
  // ScanSpecAndBlocks must contain exactly one BlockMetadataRange to be
  // accessible as a span.
  AD_CONTRACT_CHECK(blockMetadata_.size() == 1);
  // `ql::span` object requires contiguous range.
  static_assert(ql::ranges::contiguous_range<BlockMetadataRange>);
  const auto& blockMetadataRange = blockMetadata_.front();
  return ql::span(blockMetadataRange.begin(), blockMetadataRange.end());
}

// _____________________________________________________________________________
void CompressedRelationReader::ScanSpecAndBlocks::checkBlockMetadataInvariant(
    ql::span<const CompressedBlockMetadata> blocks, size_t firstFreeColIndex) {
  checkBlockMetadataInvariantOrderAndUniquenessImpl(blocks);
  checkBlockMetadataInvariantBlockConsistencyImpl(blocks, firstFreeColIndex);
}

// _____________________________________________________________________________
void CompressedRelationReader::ScanSpecAndBlocks::removePrefix(
    size_t numBlocksToRemove) {
  auto it = blockMetadata_.begin();
  auto end = blockMetadata_.end();
  for (; it != end; ++it) {
    auto& subspan = *it;
    auto sz = ql::ranges::size(subspan);
    if (numBlocksToRemove < sz) {
      // Partially remove a subspan if it contains less blocks than we have
      // to remove.
      subspan.advance(numBlocksToRemove);
      break;
    } else {
      // Completely remove the subspan (via the `erase` at the end).
      numBlocksToRemove -= sz;
    }
  }
  // Remove all the blocks that are to be erased completely.
  blockMetadata_.erase(blockMetadata_.begin(), it);
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
