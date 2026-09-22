// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2023 - 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2023 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2025        Hannes Baumann <baumannh@cs.uni-freiburg.de>, UFR
// 2025        Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2026        Mete Tolga Gonultas <mg885@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/idTable/IdColumn.h"
#include "index/CompressedRelationReader.h"

#include <algorithm>
#include <mutex>
#include <numeric>

#include "global/RuntimeParameters.h"
#include "index/CompressedRelationWriter.h"
#include "index/DistinctCol0Ids.h"
#include "index/LocatedTriples.h"
#include "util/Algorithm.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/HashSet.h"
#include "util/Iterators.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"

using namespace std::chrono_literals;

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
  auto beginBlock = ql::ranges::begin(blocks);
  auto endBlock = ql::ranges::end(blocks);
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
// ____________________________________________________________________________
cppcoro::generator<IdTable, CompressedRelationReader::LazyScanMetadata>
CompressedRelationReader::getDistinctCol0Ids(
    ScanSpecAndBlocks scanSpecAndBlocks, bool addGraphColumn,
    std::optional<std::vector<Id>> idFilter,
    CancellationHandle cancellationHandle,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  using namespace distinctCol0Ids;
  AD_CONTRACT_CHECK(cancellationHandle != nullptr);
  AD_CONTRACT_CHECK(scanSpecAndBlocks.scanSpec_.firstFreeColIndex() == 0,
                    "`getDistinctCol0Ids` only supports full scans.");
  AD_EXPENSIVE_CHECK(!idFilter.has_value() ||
                     ql::ranges::is_sorted(idFilter.value()));

  ColumnIndices additionalColumns =
      addGraphColumn ? ColumnIndices{ADDITIONAL_COLUMN_GRAPH_ID}
                     : ColumnIndices{};
  // Set up the same scan configuration that the actual scan below will use.
  // Out of that configuration we only need the `graphFilter_`, which knows
  // which blocks can be skipped entirely and which graphs are allowed; the
  // columns that `getScanConfig` also computes are only relevant for the scan
  // itself, which computes them again for its own blocks.
  auto scanConfig = getScanConfig(scanSpecAndBlocks.scanSpec_,
                                  additionalColumns, locatedTriplesPerBlock);
  auto [blocksToRead, fromMetadata] =
      BlockSelector{scanConfig.graphFilter_, addGraphColumn, idFilter,
                    locatedTriplesPerBlock, allocator_}
          .select(scanSpecAndBlocks);

  // Let the inner scan write its statistics (most importantly the number of
  // blocks it actually read) directly into our own details, such that the
  // consumer of this generator sees them.
  auto& details = co_await cppcoro::getDetails;
  details.numBlocksAll_ = scanSpecAndBlocks.sizeBlockMetadata_;
  auto scan = lazyScan(scanSpecAndBlocks.scanSpec_, std::move(blocksToRead),
                       std::move(additionalColumns), cancellationHandle,
                       locatedTriplesPerBlock, {});
  scan.setDetailsPointer(&details);

  // The IDs are computed by merging two ascending sources: the IDs that are
  // known from the block metadata alone, and the IDs from the blocks that had
  // to be read. We process one ID at a time and collect its graph IDs (if
  // requested) from both sources before appending it to the result.
  IdCursor fromMetadataCursor{
      std::move(fromMetadata),
      graphColumnIfRequested(addGraphColumn, graphColumnInResult)};
  IdCursor fromBlocksCursor{
      [&scan]() { return scan.get(); },
      graphColumnIfRequested(addGraphColumn, graphColumnInBlock)};
  RequestedIdsCursor requestedIds{idFilter};

  GraphSet graphs{allocator_};
  ResultBuilder result{addGraphColumn, idFilter, allocator_};
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
    if (requestedIds.advanceTo(id.value())) {
      result.addId(id.value(), graphs);
    }
    if (result.chunkIsFull()) {
      co_yield result.extractChunk();
    }
  }
  if (!result.chunkIsEmpty()) {
    co_yield result.extractChunk();
  }
}
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// ____________________________________________________________________________
bool CompressedRelationReader::columnValuesAreKnownFromMetadata(
    const CompressedBlockMetadata& block, size_t numColumns,
    const LocatedTriplesPerBlock& locatedTriples) {
  if (block.containsInconsistentTriples(numColumns)) {
    return false;
  }
  // Each of the delta triples can delete at most one of the block's triples, so
  // if there are fewer of them than the block has rows, then at least one of
  // its triples remains. Note that `numTriples` only returns an upper bound
  // (which is on the safe side here), and that the block that purely consists
  // of delta triples has `numRows_ == 0` and is thus handled correctly, too.
  return locatedTriples.numTriples(block.blockIndex_).numDeleted_ <
         block.numRows_;
}

// ____________________________________________________________________________
bool CompressedRelationReader::contentsAreKnownFromMetadata(
    const CompressedBlockMetadata& block, size_t numColumns,
    const LocatedTriplesPerBlock& locatedTriples) {
  return !block.containsInconsistentTriples(numColumns) &&
         !locatedTriples.containsTriples(block.blockIndex_);
}

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
    // The `numRows_` metadata shortcut is safe iff all rows of the block agree
    // on the grouped column AND the block has no delta triples.
    if (contentsAreKnownFromMetadata(blockMetadata, columnIndex + 1,
                                     locatedTriplesPerBlock)) {
      // The whole block has the same `colId` and no delta triples ->
      // we get all the information from the metadata.
      const auto& first = blockMetadata.firstTriple_;
      Id colId =
          std::array{first.col0Id_, first.col1Id_, first.col2Id_}[columnIndex];
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
ad_utility::HashSetWithMemoryLimit<Id::BitRepresentation>
CompressedRelationReader::computeUniqueGraphIds(
    const CompressedRelationReader::ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock,
    const CancellationHandle& cancellationHandle,
    const Allocator& allocator) const {
  ad_utility::HashSetWithMemoryLimit<Id::BitRepresentation> graphIds{
      allocator.as<Id::BitRepresentation>()};
  std::array<ColumnIndex, 1> additionalColumns{ADDITIONAL_COLUMN_GRAPH_ID};
  const auto scanConfig =
      getScanConfig(ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
                    additionalColumns, locatedTriplesPerBlock);

  for (const auto& metadata : scanSpecAndBlocks.getBlockMetadataView()) {
    bool shouldScan =
        !metadata.graphInfo_.has_value() ||
        ql::ranges::any_of(metadata.graphInfo_.value(), [&graphIds](Id id) {
          return !ad_utility::contains(graphIds, id.getBits());
        });
    if (shouldScan) {
      auto block = readAndDecompressBlock(metadata, scanConfig);
      cancellationHandle->throwIfCancelled();
      AD_CORRECTNESS_CHECK(block.has_value());
      for (Id id : block->block_.getColumn(ADDITIONAL_COLUMN_GRAPH_ID)) {
        graphIds.insert(id.getBits());
      }
    }
  }
  return graphIds;
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
