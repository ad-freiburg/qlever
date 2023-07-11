// Copyright 2021, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "CompressedRelation.h"

#include "engine/idTable/IdTable.h"
#include "util/Cache.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/ConcurrentCache.h"
#include "util/Generator.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/OverloadCallOperator.h"
#include "util/ThreadSafeQueue.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

using namespace std::chrono_literals;

// ____________________________________________________________________________
IdTable CompressedRelationReader::scan(
    const CompressedRelationMetadata& metadata,
    std::span<const CompressedBlockMetadata> blockMetadata,
    ad_utility::File& file, const TimeoutTimer& timer) const {
  IdTable result(2, allocator_);

  auto relevantBlocks =
      getBlocksFromMetadata(metadata, std::nullopt, blockMetadata);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();
  // The total size of the result is now known.
  result.resize(metadata.getNofElements());

  // The position in the result to which the next block is being
  // decompressed.
  size_t rowIndexOfNextBlock = 0;

  // The number of rows for which we still have space
  // in the result (only needed for checking of invariants).
  size_t spaceLeft = result.size();

  // We have at most one block that is incomplete and thus requires trimming.
  // Set up a lambda, that reads this block and decompresses it to
  // the result.
  auto readIncompleteBlock = [&](const auto& block) mutable {
    auto trimmedBlock = readPossiblyIncompleteBlock(metadata, std::nullopt,
                                                    file, block, std::nullopt);
    for (size_t i = 0; i < trimmedBlock.numColumns(); ++i) {
      const auto& inputCol = trimmedBlock.getColumn(i);
      auto resultColumn = result.getColumn(i);
      AD_CORRECTNESS_CHECK(inputCol.size() <= resultColumn.size());
      std::ranges::copy(inputCol, resultColumn.begin());
    }
    rowIndexOfNextBlock += trimmedBlock.size();
    spaceLeft -= trimmedBlock.size();
  };

  // Read the first block (it might be incomplete).
  readIncompleteBlock(*beginBlock);
  ++beginBlock;
  checkTimeout(timer);

  // Read all the other (complete!) blocks in parallel
  if (beginBlock < endBlock) {
#pragma omp parallel
#pragma omp single
    {
      for (; beginBlock < endBlock; ++beginBlock) {
        const auto& block = *beginBlock;
        // Read a block from disk (serially).

        CompressedBlock compressedBuffer =
            readCompressedBlockFromFile(block, file, std::nullopt);

        // This lambda decompresses the block that was just read to the
        // correct position in the result.
        auto decompressLambda = [&result, rowIndexOfNextBlock, &block,
                                 compressedBuffer =
                                     std::move(compressedBuffer)]() {
          ad_utility::TimeBlockAndLog tbl{"Decompressing a block"};

          decompressBlockToExistingIdTable(compressedBuffer, block.numRows_,
                                           result, rowIndexOfNextBlock);
        };

        // The `decompressLambda` can now run in parallel
#pragma omp task
        {
          if (!timer || !timer->wlock()->hasTimedOut()) {
            decompressLambda();
          };
        }

        // this is again serial code, set up the correct pointers
        // for the next block;
        spaceLeft -= block.numRows_;
        rowIndexOfNextBlock += block.numRows_;
      }
      AD_CORRECTNESS_CHECK(spaceLeft == 0);
    }  // End of omp parallel region, all the decompression was handled now.
  }
  return result;
}

// ____________________________________________________________________________
CompressedRelationReader::IdTableGenerator
CompressedRelationReader::asyncParallelBlockGenerator(
    auto beginBlock, auto endBlock, ad_utility::File& file,
    std::optional<std::vector<size_t>> columnIndices,
    TimeoutTimer timer) const {
  LazyScanMetadata& details = co_await cppcoro::getDetails;
  if (beginBlock == endBlock) {
    co_return;
  }
  const size_t queueSize =
      RuntimeParameters().get<"lazy-index-scan-queue-size">();
  auto blockIterator = beginBlock;
  std::mutex blockIteratorMutex;
  auto readAndDecompressBlock =
      [&]() -> std::optional<std::pair<size_t, DecompressedBlock>> {
    checkTimeout(timer);
    std::unique_lock lock{blockIteratorMutex};
    if (blockIterator == endBlock) {
      return std::nullopt;
    }
    // Note: taking a copy here is probably not necessary (the lifetime of
    // all the blocks is long enough, so a `const&` would suffice), but the
    // copy is cheap and makes the code more robust.
    auto block = *blockIterator;
    // Note: The order of the following two lines is important: The index
    // of the current block depends on the current value of `blockIterator`,
    // so we have to compute it before incrementing the iterator.
    auto myIndex = static_cast<size_t>(blockIterator - beginBlock);
    ++blockIterator;
    // Note: the reading of the block could also happen without holding the
    // lock. We still perform it inside the lock to avoid contention of the
    // file. On a fast SSD we could possibly change this, but this has to be
    // investigated.
    CompressedBlock compressedBlock =
        readCompressedBlockFromFile(block, file, columnIndices);
    lock.unlock();
    return std::pair{myIndex, decompressBlock(compressedBlock, block.numRows_)};
  };
  const size_t numThreads =
      RuntimeParameters().get<"lazy-index-scan-num-threads">();

  ad_utility::Timer popTimer{ad_utility::timer::Timer::InitialStatus::Started};
  // In case the coroutine is destroyed early we still want to have this
  // information.
  auto setTimer = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
      [&details, &popTimer]() { details.blockingTimeMs_ = popTimer.msecs(); });

  auto queue = ad_utility::data_structures::queueManager<
      ad_utility::data_structures::OrderedThreadSafeQueue<IdTable>>(
      queueSize, numThreads, readAndDecompressBlock);
  for (IdTable& block : queue) {
    popTimer.stop();
    checkTimeout(timer);
    ++details.numBlocksRead_;
    details.numElementsRead_ += block.numRows();
    co_yield block;
    popTimer.cont();
  }
  // The `OnDestruction...` above might be called too late, so we manually set
  // the timer again.
  details.blockingTimeMs_ = popTimer.msecs();
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGenerator CompressedRelationReader::lazyScan(
    CompressedRelationMetadata metadata,
    std::vector<CompressedBlockMetadata> blockMetadata, ad_utility::File& file,
    TimeoutTimer timer) const {
  auto relevantBlocks =
      getBlocksFromMetadata(metadata, std::nullopt, blockMetadata);
  const auto beginBlock = relevantBlocks.begin();
  const auto endBlock = relevantBlocks.end();

  LazyScanMetadata& details = co_await cppcoro::getDetails;
  size_t numBlocksTotal = endBlock - beginBlock;

  if (beginBlock == endBlock) {
    co_return;
  }

  // Read the first block, it might be incomplete
  auto firstBlock = readPossiblyIncompleteBlock(metadata, std::nullopt, file,
                                                *beginBlock, std::ref(details));
  co_yield firstBlock;
  checkTimeout(timer);

  auto blockGenerator = asyncParallelBlockGenerator(beginBlock + 1, endBlock,
                                                    file, std::nullopt, timer);
  blockGenerator.setDetailsPointer(&details);
  for (auto& block : blockGenerator) {
    co_yield block;
  }
  AD_CORRECTNESS_CHECK(numBlocksTotal == details.numBlocksRead_);
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGenerator CompressedRelationReader::lazyScan(
    CompressedRelationMetadata metadata, Id col1Id,
    std::vector<CompressedBlockMetadata> blockMetadata, ad_utility::File& file,
    TimeoutTimer timer) const {
  auto relevantBlocks = getBlocksFromMetadata(metadata, col1Id, blockMetadata);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();

  LazyScanMetadata& details = co_await cppcoro::getDetails;
  size_t numBlocksTotal = endBlock - beginBlock;

  if (beginBlock == endBlock) {
    co_return;
  }

  // Invariant: The col0Id is completely stored in a single block, or it is
  // contained in multiple blocks that only contain this col0Id,
  bool col0IdHasExclusiveBlocks =
      metadata.offsetInBlock_ == std::numeric_limits<uint64_t>::max();
  if (!col0IdHasExclusiveBlocks) {
    // This might also be zero if no block was found at all.
    AD_CORRECTNESS_CHECK(endBlock - beginBlock <= 1);
  }

  auto getIncompleteBlock = [&](auto it) {
    auto result = readPossiblyIncompleteBlock(metadata, col1Id, file, *it,
                                              std::ref(details));
    result.setColumnSubset(std::array<ColumnIndex, 1>{1});
    checkTimeout(timer);
    return result;
  };

  if (beginBlock < endBlock) {
    auto block = getIncompleteBlock(beginBlock);
    co_yield block;
  }

  if (beginBlock + 1 < endBlock) {
    auto blockGenerator = asyncParallelBlockGenerator(
        beginBlock + 1, endBlock - 1, file, std::vector{1UL}, timer);
    blockGenerator.setDetailsPointer(&details);
    for (auto& block : blockGenerator) {
      co_yield block;
    }
    auto lastBlock = getIncompleteBlock(endBlock - 1);
    co_yield lastBlock;
  }
  AD_CORRECTNESS_CHECK(numBlocksTotal == details.numBlocksRead_);
}

namespace {
// An internal helper function for the `getBlocksForJoin` functions below.
// Get the ID from the `triple` that pertains to the join column (the col2 if
// the col1 is specified, else the col1). There are two special cases: If the
// triple doesn't match the `col0` and (if specified) the `col1` then a sentinel
// value is returned that is `Id::min` if the triple is lower than all matching
// triples, and `Id::max` if is higher. That way we can consistently compare a
// single ID from a join column with a complete triple from a block.
auto getRelevantIdFromTriple(
    CompressedBlockMetadata::PermutedTriple triple,
    const CompressedRelationReader::MetadataAndBlocks& metadataAndBlocks) {
  auto idForNonMatchingBlock = [](Id fromTriple, Id key, Id minKey,
                                  Id maxKey) -> std::optional<Id> {
    if (fromTriple < key) {
      return minKey;
    }
    if (fromTriple > key) {
      return maxKey;
    }
    return std::nullopt;
  };

  auto [minKey, maxKey] = [&]() {
    if (!metadataAndBlocks.firstAndLastTriple_.has_value()) {
      return std::array{Id::min(), Id::max()};
    }
    const auto& [first, last] = metadataAndBlocks.firstAndLastTriple_.value();
    if (metadataAndBlocks.col1Id_.has_value()) {
      return std::array{first.col2Id_, last.col2Id_};
    } else {
      return std::array{first.col1Id_, last.col1Id_};
    }
  }();

  if (auto optId = idForNonMatchingBlock(
          triple.col0Id_, metadataAndBlocks.relationMetadata_.col0Id_, minKey,
          maxKey)) {
    return optId.value();
  }
  if (!metadataAndBlocks.col1Id_.has_value()) {
    return triple.col1Id_;
  }

  return idForNonMatchingBlock(
             triple.col1Id_, metadataAndBlocks.col1Id_.value(), minKey, maxKey)
      .value_or(triple.col2Id_);
}
}  // namespace

// _____________________________________________________________________________
std::vector<CompressedBlockMetadata> CompressedRelationReader::getBlocksForJoin(
    std::span<const Id> joinColumn,
    const MetadataAndBlocks& metadataAndBlocks) {
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
  // fulfill a concept for the `std::ranges` algorithms.
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
  // this doesn't work because the implicit equality defined by `!lessThan(a,b)
  // && !lessThan(b, a)` is not transitive.
  std::vector<CompressedBlockMetadata> result;

  auto blockIsNeeded = [&joinColumn, &lessThan](const auto& block) {
    return !std::ranges::equal_range(joinColumn, block, lessThan).empty();
  };
  std::ranges::copy(relevantBlocks | std::views::filter(blockIsNeeded),
                    std::back_inserter(result));
  // The following check is cheap as there are only few blocks.
  AD_CORRECTNESS_CHECK(std::ranges::unique(result).empty());
  return result;
}

// _____________________________________________________________________________
std::array<std::vector<CompressedBlockMetadata>, 2>
CompressedRelationReader::getBlocksForJoin(
    const MetadataAndBlocks& metadataAndBlocks1,
    const MetadataAndBlocks& metadataAndBlocks2) {
  auto relevantBlocks1 = getBlocksFromMetadata(metadataAndBlocks1);
  auto relevantBlocks2 = getBlocksFromMetadata(metadataAndBlocks2);

  auto metadataForBlock =
      [&](const CompressedBlockMetadata& block) -> decltype(auto) {
    if (relevantBlocks1.data() <= &block &&
        &block < relevantBlocks1.data() + relevantBlocks1.size()) {
      return metadataAndBlocks1;
    } else {
      return metadataAndBlocks2;
    }
  };

  auto blockLessThanBlock = [&](const CompressedBlockMetadata& block1,
                                const CompressedBlockMetadata& block2) {
    return getRelevantIdFromTriple(block1.lastTriple_,
                                   metadataForBlock(block1)) <
           getRelevantIdFromTriple(block2.firstTriple_,
                                   metadataForBlock(block2));
  };

  std::array<std::vector<CompressedBlockMetadata>, 2> result;

  AD_CONTRACT_CHECK(
      std::ranges::is_sorted(relevantBlocks1, blockLessThanBlock));
  AD_CONTRACT_CHECK(
      std::ranges::is_sorted(relevantBlocks2, blockLessThanBlock));

  // Find the matching blocks on each side by performing binary search on the
  // other side. Note that it is tempting to reuse the `zipperJoinWithUndef`
  // routine, but this doesn't work because the implicit equality defined by
  // `!lessThan(a,b) && !lessThan(b, a)` is not transitive.
  for (const auto& block : relevantBlocks1) {
    if (!std::ranges::equal_range(relevantBlocks2, block, blockLessThanBlock)
             .empty()) {
      result[0].push_back(block);
    }
  }
  for (const auto& block : relevantBlocks2) {
    if (!std::ranges::equal_range(relevantBlocks1, block, blockLessThanBlock)
             .empty()) {
      result[1].push_back(block);
    }
  }

  // The following check shouldn't be too expensive as there are only few
  // blocks.
  for (auto& vec : result) {
    AD_CORRECTNESS_CHECK(std::ranges::unique(vec).begin() == vec.end());
  }
  return result;
}

// _____________________________________________________________________________
IdTable CompressedRelationReader::scan(
    const CompressedRelationMetadata& metadata, Id col1Id,
    std::span<const CompressedBlockMetadata> blocks, ad_utility::File& file,
    const TimeoutTimer& timer) const {
  IdTable result(1, allocator_);

  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  auto relevantBlocks = getBlocksFromMetadata(metadata, col1Id, blocks);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();

  // Invariant: The col0Id is completely stored in a single block, or it is
  // contained in multiple blocks that only contain this col0Id,
  bool col0IdHasExclusiveBlocks =
      metadata.offsetInBlock_ == std::numeric_limits<uint64_t>::max();
  if (!col0IdHasExclusiveBlocks) {
    // This might also be zero if no block was found at all.
    AD_CORRECTNESS_CHECK(endBlock - beginBlock <= 1);
  }

  // The first and the last block might be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the result as a vector.
  auto readIncompleteBlock = [&](const auto& block) {
    return readPossiblyIncompleteBlock(metadata, col1Id, file, block,
                                       std::nullopt);
  };

  // The first and the last block might be incomplete, compute
  // and store the partial results from them.
  std::optional<DecompressedBlock> firstBlockResult;
  std::optional<DecompressedBlock> lastBlockResult;
  size_t totalResultSize = 0;
  if (beginBlock < endBlock) {
    firstBlockResult = readIncompleteBlock(*beginBlock);
    totalResultSize += firstBlockResult.value().size();
    ++beginBlock;
    checkTimeout(timer);
  }
  if (beginBlock < endBlock) {
    lastBlockResult = readIncompleteBlock(*(endBlock - 1));
    totalResultSize += lastBlockResult.value().size();
    endBlock--;
    checkTimeout(timer);
  }

  // Determine the total size of the result.
  // First accumulate the complete blocks in the "middle"
  totalResultSize += std::accumulate(beginBlock, endBlock, 0UL,
                                     [](const auto& count, const auto& block) {
                                       return count + block.numRows_;
                                     });
  result.resize(totalResultSize);

  size_t rowIndexOfNextBlockStart = 0;
  // Insert the first block into the result;
  if (firstBlockResult.has_value()) {
    std::ranges::copy(firstBlockResult.value().getColumn(1),
                      result.getColumn(0).data());
    rowIndexOfNextBlockStart = firstBlockResult.value().numRows();
  }

  // Insert the complete blocks from the middle in parallel
  if (beginBlock < endBlock) {
#pragma omp parallel
#pragma omp single
    for (; beginBlock < endBlock; ++beginBlock) {
      const auto& block = *beginBlock;

      // Read the block serially, only read the second column.
      AD_CORRECTNESS_CHECK(block.offsetsAndCompressedSize_.size() == 2);
      CompressedBlock compressedBuffer =
          readCompressedBlockFromFile(block, file, std::vector{1UL});

      // A lambda that owns the compressed block decompresses it to the
      // correct position in the result. It may safely be run in parallel
      auto decompressLambda = [rowIndexOfNextBlockStart, &block, &result,
                               compressedBuffer =
                                   std::move(compressedBuffer)]() mutable {
        ad_utility::TimeBlockAndLog tbl{"Decompression a block"};

        decompressBlockToExistingIdTable(compressedBuffer, block.numRows_,
                                         result, rowIndexOfNextBlockStart);
      };

      // Register an OpenMP task that performs the decompression of this
      // block in parallel
#pragma omp task
      {
        if (!timer || !timer->wlock()->hasTimedOut()) {
          decompressLambda();
        }
      }

      // update the pointers
      rowIndexOfNextBlockStart += block.numRows_;
    }  // end of parallel region
  }
  // Add the last block.
  if (lastBlockResult.has_value()) {
    std::ranges::copy(lastBlockResult.value().getColumn(1),
                      result.getColumn(0).data() + rowIndexOfNextBlockStart);
    rowIndexOfNextBlockStart += lastBlockResult.value().size();
  }
  AD_CORRECTNESS_CHECK(rowIndexOfNextBlockStart == result.size());
  return result;
}

// _____________________________________________________________________________
DecompressedBlock CompressedRelationReader::readPossiblyIncompleteBlock(
    const CompressedRelationMetadata& relationMetadata,
    std::optional<Id> col1Id, ad_utility::File& file,
    const CompressedBlockMetadata& blockMetadata,
    std::optional<std::reference_wrapper<LazyScanMetadata>> scanMetadata)
    const {
  // A block is uniquely identified by its start position in the file.
  auto cacheKey = blockMetadata.offsetsAndCompressedSize_.at(0).offsetInFile_;
  DecompressedBlock block =
      blockCache_
          .computeOnce(cacheKey,
                       [&]() {
                         return readAndDecompressBlock(blockMetadata, file,
                                                       std::nullopt);
                       })
          ._resultPointer->clone();
  AD_CORRECTNESS_CHECK(block.numColumns() == 2);
  const auto& col1Column = block.getColumn(0);
  const auto& col2Column = block.getColumn(1);
  AD_CORRECTNESS_CHECK(col1Column.size() == col2Column.size());

  // Find the range in the blockMetadata, that belongs to the same relation
  // `col0Id`
  bool containedInOnlyOneBlock =
      relationMetadata.offsetInBlock_ != std::numeric_limits<uint64_t>::max();
  auto begin = col1Column.begin();
  if (containedInOnlyOneBlock) {
    begin += relationMetadata.offsetInBlock_;
  }
  auto end = containedInOnlyOneBlock ? begin + relationMetadata.numRows_
                                     : col1Column.end();

  // If the `col1Id` was specified, we additionally have to filter by it.
  auto subBlock = [&]() {
    if (col1Id.has_value()) {
      return std::ranges::equal_range(begin, end, col1Id.value());
    } else {
      return std::ranges::subrange{begin, end};
    }
  }();
  auto numResults = subBlock.size();
  block.erase(block.begin(),
              block.begin() + (subBlock.begin() - col1Column.begin()));
  block.resize(numResults);

  if (scanMetadata.has_value()) {
    auto& details = scanMetadata.value().get();
    ++details.numBlocksRead_;
    details.numElementsRead_ += block.numRows();
  }
  return block;
};

// _____________________________________________________________________________
size_t CompressedRelationReader::getResultSizeOfScan(
    const CompressedRelationMetadata& metadata, Id col1Id,
    const vector<CompressedBlockMetadata>& blocks,
    ad_utility::File& file) const {
  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  auto relevantBlocks = getBlocksFromMetadata(metadata, col1Id, blocks);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();

  // The first and the last block might be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the size of the result.
  auto readSizeOfPossiblyIncompleteBlock = [&](const auto& block) {
    return readPossiblyIncompleteBlock(metadata, col1Id, file, block,
                                       std::nullopt)
        .numRows();
  };

  size_t numResults = 0;
  // The first and the last block might be incomplete, compute
  // and store the partial results from them.
  if (beginBlock < endBlock) {
    numResults += readSizeOfPossiblyIncompleteBlock(*beginBlock);
    ++beginBlock;
  }
  if (beginBlock < endBlock) {
    numResults += readSizeOfPossiblyIncompleteBlock(*(endBlock - 1));
    --endBlock;
  }

  // Determine the total size of the result.
  // First accumulate the complete blocks in the "middle"
  numResults += std::accumulate(beginBlock, endBlock, 0UL,
                                [](const auto& count, const auto& block) {
                                  return count + block.numRows_;
                                });
  return numResults;
}

// _____________________________________________________________________________
float CompressedRelationWriter::computeMultiplicity(
    size_t numElements, size_t numDistinctElements) {
  bool functional = numElements == numDistinctElements;
  float multiplicity =
      functional ? 1.0f
                 : static_cast<float>(numElements) / float(numDistinctElements);
  // Ensure that the multiplicity is only exactly 1.0 if the relation is indeed
  // functional to prevent numerical instabilities;
  if (!functional && multiplicity == 1.0f) [[unlikely]] {
    multiplicity = std::nextafter(1.0f, 2.0f);
  }
  return multiplicity;
}

// ___________________________________________________________________________
CompressedRelationMetadata CompressedRelationWriter::addRelation(
    Id col0Id, const BufferedIdTable& col1And2Ids, size_t numDistinctCol1) {
  AD_CONTRACT_CHECK(!col1And2Ids.empty());
  float multC1 = computeMultiplicity(col1And2Ids.numRows(), numDistinctCol1);
  // Dummy value that will be overwritten later
  float multC2 = 42.42;
  // This sets everything except the offsetInBlock_, which will be set
  // explicitly below.
  CompressedRelationMetadata metadata{col0Id, col1And2Ids.numRows(), multC1,
                                      multC2};

  // Determine the number of bytes the IDs stored in an IdTable consume.
  // The return type is double because we use the result to compare it with
  // other doubles below.
  auto sizeInBytes = [](const auto& table) {
    return static_cast<double>(table.numRows() * table.numColumns() *
                               sizeof(Id));
  };

  // If this is a large relation, or the currrently buffered relations +
  // this relation are too large, we will write the buffered relations to file
  // and start a new block.
  bool relationHasExclusiveBlocks =
      sizeInBytes(col1And2Ids) > 0.8 * static_cast<double>(numBytesPerBlock_);
  if (relationHasExclusiveBlocks ||
      sizeInBytes(col1And2Ids) + sizeInBytes(buffer_) >
          static_cast<double>(numBytesPerBlock_) * 1.5) {
    writeBufferedRelationsToSingleBlock();
  }

  if (relationHasExclusiveBlocks) {
    // The relation is large, immediately write the relation to a set of
    // exclusive blocks.
    writeRelationToExclusiveBlocks(col0Id, col1And2Ids);
    metadata.offsetInBlock_ = std::numeric_limits<uint64_t>::max();
  } else {
    // Append to the current buffered block.
    metadata.offsetInBlock_ = buffer_.numRows();
    static_assert(sizeof(col1And2Ids[0][0]) == sizeof(Id));
    if (buffer_.numRows() == 0) {
      currentBlockData_.firstTriple_ = {col0Id, col1And2Ids(0, 0),
                                        col1And2Ids(0, 1)};
    }
    currentBlockData_.lastTriple_ = {col0Id,
                                     col1And2Ids(col1And2Ids.numRows() - 1, 0),
                                     col1And2Ids(col1And2Ids.numRows() - 1, 1)};
    AD_CORRECTNESS_CHECK(buffer_.numColumns() == col1And2Ids.numColumns());
    auto bufferOldSize = buffer_.numRows();
    buffer_.resize(buffer_.numRows() + col1And2Ids.numRows());
    for (size_t i = 0; i < col1And2Ids.numColumns(); ++i) {
      const auto& column = col1And2Ids.getColumn(i);
      std::ranges::copy(column, buffer_.getColumn(i).begin() + bufferOldSize);
    }
  }
  return metadata;
}

// _____________________________________________________________________________
void CompressedRelationWriter::writeRelationToExclusiveBlocks(
    Id col0Id, const BufferedIdTable& data) {
  const size_t numRowsPerBlock = numBytesPerBlock_ / (NumColumns * sizeof(Id));
  AD_CORRECTNESS_CHECK(numRowsPerBlock > 0);
  AD_CORRECTNESS_CHECK(data.numColumns() == NumColumns);
  const auto totalSize = data.numRows();
  for (size_t i = 0; i < totalSize; i += numRowsPerBlock) {
    size_t actualNumRowsPerBlock = std::min(numRowsPerBlock, totalSize - i);

    std::vector<CompressedBlockMetadata::OffsetAndCompressedSize> offsets;
    for (const auto& column : data.getColumns()) {
      offsets.push_back(compressAndWriteColumn(
          {column.begin() + i, column.begin() + i + actualNumRowsPerBlock}));
    }

    blockBuffer_.push_back(
        CompressedBlockMetadata{std::move(offsets),
                                actualNumRowsPerBlock,
                                {col0Id, data[i][0], data[i][1]},
                                {col0Id, data[i + actualNumRowsPerBlock - 1][0],
                                 data[i + actualNumRowsPerBlock - 1][1]}});
  }
}

// ___________________________________________________________________________
void CompressedRelationWriter::writeBufferedRelationsToSingleBlock() {
  if (buffer_.empty()) {
    return;
  }

  AD_CORRECTNESS_CHECK(buffer_.numColumns() == NumColumns);
  // Convert from bytes to number of ID pairs.
  size_t numRows = buffer_.numRows();

  // TODO<joka921, C++23> This is
  // `ranges::to<vector>(ranges::transform_view(buffer_.getColumns(),
  // compressAndWriteColumn))`;
  std::ranges::for_each(buffer_.getColumns(),
                        [this](const auto& column) mutable {
                          currentBlockData_.offsetsAndCompressedSize_.push_back(
                              compressAndWriteColumn(column));
                        });

  currentBlockData_.numRows_ = numRows;
  // The `firstId` and `lastId` of `currentBlockData_` were already set
  // correctly by `addRelation()`.
  blockBuffer_.push_back(currentBlockData_);
  // Reset the data of the current block.
  currentBlockData_ = CompressedBlockMetadata{};
  buffer_.clear();
}

// _____________________________________________________________________________
CompressedBlock CompressedRelationReader::readCompressedBlockFromFile(
    const CompressedBlockMetadata& blockMetaData, ad_utility::File& file,
    std::optional<std::vector<size_t>> columnIndices) {
  // If we have no column indices specified, we read all the columns.
  // TODO<joka921> This should be some kind of `smallVector` for performance
  // reasons.
  if (!columnIndices.has_value()) {
    columnIndices.emplace();
    // TODO<joka921, C++23> this is ranges::to<vector>(std::iota).
    columnIndices->reserve(NumColumns);
    for (size_t i = 0; i < NumColumns; ++i) {
      columnIndices->push_back(i);
    }
  }
  CompressedBlock compressedBuffer;
  compressedBuffer.resize(columnIndices->size());
  // TODO<C++23> Use `std::views::zip`
  for (size_t i = 0; i < compressedBuffer.size(); ++i) {
    const auto& offset =
        blockMetaData.offsetsAndCompressedSize_.at(columnIndices->at(i));
    auto& currentCol = compressedBuffer[i];
    currentCol.resize(offset.compressedSize_);
    file.read(currentCol.data(), offset.compressedSize_, offset.offsetInFile_);
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
void CompressedRelationReader::decompressBlockToExistingIdTable(
    const CompressedBlock& compressedBlock, size_t numRowsToRead,
    IdTable& table, size_t offsetInTable) {
  AD_CORRECTNESS_CHECK(table.numRows() >= offsetInTable + numRowsToRead);
  // TODO<joka921, C++23> use zip_view.
  AD_CORRECTNESS_CHECK(compressedBlock.size() == table.numColumns());
  for (size_t i = 0; i < compressedBlock.size(); ++i) {
    auto col = table.getColumn(i);
    decompressColumn(compressedBlock[i], numRowsToRead,
                     col.data() + offsetInTable);
  }
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

// _____________________________________________________________________________
DecompressedBlock CompressedRelationReader::readAndDecompressBlock(
    const CompressedBlockMetadata& blockMetaData, ad_utility::File& file,
    std::optional<std::vector<size_t>> columnIndices) const {
  CompressedBlock compressedColumns = readCompressedBlockFromFile(
      blockMetaData, file, std::move(columnIndices));
  const auto numRowsToRead = blockMetaData.numRows_;
  return decompressBlock(compressedColumns, numRowsToRead);
}

// _____________________________________________________________________________
CompressedBlockMetadata::OffsetAndCompressedSize
CompressedRelationWriter::compressAndWriteColumn(std::span<const Id> column) {
  std::vector<char> compressedBlock = ZstdWrapper::compress(
      (void*)(column.data()), column.size() * sizeof(column[0]));
  auto offsetInFile = outfile_.tell();
  auto compressedSize = compressedBlock.size();
  outfile_.write(compressedBlock.data(), compressedBlock.size());
  return {offsetInFile, compressedSize};
};

// _____________________________________________________________________________
std::span<const CompressedBlockMetadata>
CompressedRelationReader::getBlocksFromMetadata(
    const CompressedRelationMetadata& metadata, std::optional<Id> col1Id,
    std::span<const CompressedBlockMetadata> blockMetadata) {
  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  Id col0Id = metadata.col0Id_;
  CompressedBlockMetadata key;
  key.firstTriple_.col0Id_ = col0Id;
  key.lastTriple_.col0Id_ = col0Id;
  key.firstTriple_.col1Id_ = col1Id.value_or(Id::min());
  key.lastTriple_.col1Id_ = col1Id.value_or(Id::max());

  auto comp = [](const auto& a, const auto& b) {
    bool endBeforeBegin = a.lastTriple_.col0Id_ < b.firstTriple_.col0Id_;
    endBeforeBegin =
        endBeforeBegin || (a.lastTriple_.col0Id_ == b.firstTriple_.col0Id_ &&
                           a.lastTriple_.col1Id_ < b.firstTriple_.col1Id_);
    return endBeforeBegin;
  };

  auto result = std::ranges::equal_range(blockMetadata, key, comp);

  // Invariant: The col0Id is completely stored in a single block, or it is
  // contained in multiple blocks that only contain this col0Id,
  bool col0IdHasExclusiveBlocks =
      metadata.offsetInBlock_ == std::numeric_limits<uint64_t>::max();
  // `result` might also be empty if no block was found at all.
  AD_CORRECTNESS_CHECK(col0IdHasExclusiveBlocks || result.size() <= 1);

  if (!result.empty()) {
    // Check some invariants of the splitting of the relations.
    bool firstIncomplete = result.front().firstTriple_.col0Id_ < col0Id ||
                           result.front().lastTriple_.col0Id_ > col0Id;

    auto lastBlock = result.end() - 1;

    bool lastIncomplete = result.begin() < lastBlock &&
                          (lastBlock->firstTriple_.col0Id_ < col0Id ||
                           lastBlock->lastTriple_.col0Id_ > col0Id);

    // Invariant: A relation spans multiple blocks exclusively or several
    // entities are stored completely in the same Block.
    if (!col1Id.has_value()) {
      AD_CORRECTNESS_CHECK(!firstIncomplete || (result.begin() == lastBlock));
      AD_CORRECTNESS_CHECK(!lastIncomplete);
    }
    if (firstIncomplete) {
      AD_CORRECTNESS_CHECK(!col0IdHasExclusiveBlocks);
    }
  }
  return result;
}

// _____________________________________________________________________________
std::span<const CompressedBlockMetadata>
CompressedRelationReader::getBlocksFromMetadata(
    const MetadataAndBlocks& metadata) {
  return getBlocksFromMetadata(metadata.relationMetadata_, metadata.col1Id_,
                               metadata.blockMetadata_);
}

// _____________________________________________________________________________
auto CompressedRelationReader::getFirstAndLastTriple(
    const CompressedRelationReader::MetadataAndBlocks& metadataAndBlocks,
    ad_utility::File& file) const -> MetadataAndBlocks::FirstAndLastTriple {
  auto relevantBlocks = getBlocksFromMetadata(metadataAndBlocks);
  AD_CONTRACT_CHECK(!relevantBlocks.empty());

  auto scanBlock = [&](const CompressedBlockMetadata& block) {
    // Note: the following call only returns the part of the block that actually
    // matches the col0 and col1.
    return readPossiblyIncompleteBlock(metadataAndBlocks.relationMetadata_,
                                       metadataAndBlocks.col1Id_, file, block,
                                       std::nullopt);
  };

  auto rowToTriple =
      [&](const auto& row) -> CompressedBlockMetadata::PermutedTriple {
    return {metadataAndBlocks.relationMetadata_.col0Id_, row[0], row[1]};
  };

  auto firstBlock = scanBlock(relevantBlocks.front());
  auto lastBlock = scanBlock(relevantBlocks.back());
  AD_CORRECTNESS_CHECK(!firstBlock.empty());
  AD_CORRECTNESS_CHECK(!lastBlock.empty());
  return {rowToTriple(firstBlock.front()), rowToTriple(lastBlock.back())};
}
