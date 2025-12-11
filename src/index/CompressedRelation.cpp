// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "index/CompressedRelation.h"

#include "engine/Engine.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "index/CompressedRelationHelpersImpl.h"
#include "index/CompressedRelationPermutationWriterImpl.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/LocatedTriples.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Iterators.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/OverloadCallOperator.h"
#include "util/ProgressBar.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/TransparentFunctors.h"
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

// Extract the Ids from the given `PermutedTriple` in a tuple w.r.t. the
// position (column index) defined by `ignoreIndex`. The ignored positions are
// filled with Ids `Id::min()`. `Id::min()` is guaranteed
// to be smaller than Ids of all other types.
static auto getMaskedTriple(
    const CompressedBlockMetadata::PermutedTriple& triple,
    size_t ignoreIndex = 3) {
  const Id& undefined = Id::min();
  switch (ignoreIndex) {
    case 3:
      return std::make_tuple(triple.col0Id_, triple.col1Id_, triple.col2Id_);
    case 2:
      return std::make_tuple(triple.col0Id_, triple.col1Id_, undefined);
    case 1:
      return std::make_tuple(triple.col0Id_, undefined, undefined);
    case 0:
      return std::make_tuple(undefined, undefined, undefined);
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
      auto numThreads{
          getRuntimeParameter<&RuntimeParameters::lazyIndexScanNumThreads_>()};
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
    };

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
    };

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
  AD_CORRECTNESS_CHECK(ADDITIONAL_COLUMN_GRAPH_ID <
                       blockMetadata.offsetsAndCompressedSize_.size());

  bool manuallyDeleteGraphColumn = scanConfig.graphFilter_.deleteGraphColumn_;
  // We first scan the complete block including ALL columns with the following
  // exception: If `manuallyDeleteGraphColumn` is true, then the `graphColumn`
  // is contained inside `scanConfig.scanColumns`, but this function is supposed
  // to delete it. In this case we will not scan the graph column, that way the
  // `readAndDecompressBlock` function will correctly delete all duplicates from
  // the block. The downside of this approach is that further down we have to be
  // aware of this already dropped column when assembling the final result.
  std::vector<ColumnIndex> allAdditionalColumns;
  ql::ranges::copy(
      ql::views::iota(ADDITIONAL_COLUMN_GRAPH_ID +
                          static_cast<size_t>(manuallyDeleteGraphColumn),
                      blockMetadata.offsetsAndCompressedSize_.size()),
      std::back_inserter(allAdditionalColumns));
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

  // If `manuallyDeleteGraphColumn` is `true`, then we don't output the graph
  // column. Additionally, it means that the graph column has already been
  // dropped by the call to `readAndDecompressBlock` (see above), so we have to
  // shift the column origins above the graph column down by one.
  for (const auto& inputColIdx :
       columnIndices | ql::views::filter([&](const auto& idx) {
         return !manuallyDeleteGraphColumn || idx != ADDITIONAL_COLUMN_GRAPH_ID;
       }) | ql::views::transform([&](const auto& idx) {
         return manuallyDeleteGraphColumn && idx > ADDITIONAL_COLUMN_GRAPH_ID
                    ? idx - 1
                    : idx;
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
        return std::max(std::min(num, 1ul), num / divisor);
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

// ____________________________________________________________________________
CPP_template_def(typename IdGetter)(
    requires ad_utility::InvocableWithConvertibleReturnType<
        IdGetter, Id, const CompressedBlockMetadata::PermutedTriple&>) IdTable
    CompressedRelationReader::getDistinctColIdsAndCountsImpl(
        IdGetter idGetter, const ScanSpecAndBlocks& scanSpecAndBlocks,
        const CancellationHandle& cancellationHandle,
        const LocatedTriplesPerBlock& locatedTriplesPerBlock,
        const LimitOffsetClause& limitOffset) const {
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
    Id firstColId = std::invoke(idGetter, blockMetadata.firstTriple_);
    Id lastColId = std::invoke(idGetter, blockMetadata.lastTriple_);
    if (firstColId == lastColId) {
      // The whole block has the same `colId` -> we get all the information
      // from the metadata.
      bool abort = processColId(firstColId, blockMetadata.numRows_);
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
IdTable CompressedRelationReader::getDistinctCol0IdsAndCounts(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    const LimitOffsetClause& limitOffset) const {
  return getDistinctColIdsAndCountsImpl(
      &CompressedBlockMetadata::PermutedTriple::col0Id_, scanSpecAndBlocks,
      cancellationHandle, locatedTriplesPerBlock, limitOffset);
}

// ____________________________________________________________________________
IdTable CompressedRelationReader::getDistinctCol1IdsAndCounts(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    const LimitOffsetClause& limitOffset) const {
  return getDistinctColIdsAndCountsImpl(
      &CompressedBlockMetadata::PermutedTriple::col1Id_, scanSpecAndBlocks,
      cancellationHandle, locatedTriplesPerBlock, limitOffset);
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
CompressedRelationWriter::compressAndWriteColumn(ql::span<const Id> column) {
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
    const IdTable& block) {
  AD_CORRECTNESS_CHECK(block.numColumns() > ADDITIONAL_COLUMN_GRAPH_ID);
  // Return true iff the block contains duplicates when only considering the
  // actual triple of S, P, and O.
  auto hasDuplicates = [&block]() {
    using C = ColumnIndex;
    auto withoutGraphAndAdditionalPayload =
        block.asColumnSubsetView(std::array{C{0}, C{1}, C{2}});
    size_t numDistinct = Engine::countDistinct(withoutGraphAndAdditionalPayload,
                                               ad_utility::noop);
    return numDistinct != block.numRows();
  };

  // Return the contained graphs, or  `nullopt` if there are too many of them.
  auto graphInfo = [&block]() -> std::optional<std::vector<Id>> {
    std::vector<Id> graphColumn;
    ql::ranges::copy(block.getColumn(ADDITIONAL_COLUMN_GRAPH_ID),
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
};

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
              return tieFirstThreeColumns(lastRowFromPrevious) !=
                     tieFirstThreeColumns(row);
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
    qlever::KeyOrder permutation, const PerBlockCallbacks& perBlockCallbacks)
    -> PermutationPairResult {
  PermutationWriter<true> permutationWriter{
      basename, std::move(writerAndCallback1), std::move(writerAndCallback2),
      std::move(permutation), perBlockCallbacks};
  return permutationWriter.writePermutation(std::move(sortedTriples));
}

// _____________________________________________________________________________
auto CompressedRelationWriter::createPermutation(
    WriterAndCallback writerAndCallback,
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
    qlever::KeyOrder permutation, const PerBlockCallbacks& perBlockCallbacks)
    -> PermutationSingleResult {
  PermutationWriter<false> permutationWriter{
      std::move(writerAndCallback), std::move(permutation), perBlockCallbacks};
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
};

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
