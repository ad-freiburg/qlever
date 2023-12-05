// Copyright 2021, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "CompressedRelation.h"

#include <ranges>

#include "engine/idTable/IdTable.h"
#include "util/Cache.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/ConcurrentCache.h"
#include "util/Generator.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/OverloadCallOperator.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

using namespace std::chrono_literals;

// A small helper function to obtain the begin and end iterator of a range
static auto getBeginAndEnd(auto& range) {
  return std::pair{std::ranges::begin(range), std::ranges::end(range)};
}

// ____________________________________________________________________________
CompressedRelationReader::IdTableGenerator
CompressedRelationReader::asyncParallelBlockGenerator(
    auto beginBlock, auto endBlock, ColumnIndices columnIndices,
    std::shared_ptr<ad_utility::CancellationHandle> cancellationHandle) const {
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
    checkCancellation(cancellationHandle);
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
        readCompressedBlockFromFile(block, columnIndices);
    lock.unlock();
    return std::pair{myIndex, decompressBlock(compressedBlock, block.numRows_)};
  };
  const size_t numThreads =
      RuntimeParameters().get<"lazy-index-scan-num-threads">();

  ad_utility::Timer popTimer{ad_utility::timer::Timer::InitialStatus::Started};
  // In case the coroutine is destroyed early we still want to have this
  // information.
  auto setTimer = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
      [&details, &popTimer]() { details.blockingTime_ = popTimer.msecs(); });

  auto queue = ad_utility::data_structures::queueManager<
      ad_utility::data_structures::OrderedThreadSafeQueue<IdTable>>(
      queueSize, numThreads, readAndDecompressBlock);
  for (IdTable& block : queue) {
    popTimer.stop();
    checkCancellation(cancellationHandle);
    ++details.numBlocksRead_;
    details.numElementsRead_ += block.numRows();
    co_yield block;
    popTimer.cont();
  }
  // The `OnDestruction...` above might be called too late, so we manually stop
  // the timer here in case it wasn't already.
  popTimer.stop();
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGenerator CompressedRelationReader::lazyScan(
    CompressedRelationMetadata metadata, std::optional<Id> col1Id,
    std::vector<CompressedBlockMetadata> blockMetadata,
    ColumnIndices additionalColumns,
    std::shared_ptr<ad_utility::CancellationHandle> cancellationHandle) const {
  AD_CONTRACT_CHECK(cancellationHandle);
  auto relevantBlocks = getBlocksFromMetadata(metadata, col1Id, blockMetadata);
  auto [beginBlock, endBlock] = getBeginAndEnd(relevantBlocks);

  LazyScanMetadata& details = co_await cppcoro::getDetails;
  size_t numBlocksTotal = endBlock - beginBlock;

  if (beginBlock == endBlock) {
    co_return;
  }

  auto columnIndices = prepareColumnIndices(col1Id, additionalColumns);

  auto getIncompleteBlock = [&](auto it) {
    auto result = readPossiblyIncompleteBlock(metadata, col1Id, *it,
                                              std::ref(details), columnIndices);
    checkCancellation(cancellationHandle);
    return result;
  };

  if (beginBlock < endBlock) {
    auto block = getIncompleteBlock(beginBlock);
    co_yield block;
  }

  if (beginBlock + 1 < endBlock) {
    // We copy the cancellationHandle because it is still captured by reference
    // inside the `getIncompleteBlock` lambda.
    auto blockGenerator = asyncParallelBlockGenerator(
        beginBlock + 1, endBlock - 1, columnIndices, cancellationHandle);
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
    const CompressedRelationMetadata& metadata, std::optional<Id> col1Id,
    std::span<const CompressedBlockMetadata> blocks,
    ColumnIndicesRef additionalColumns,
    std::shared_ptr<ad_utility::CancellationHandle> cancellationHandle) const {
  auto columnIndices = prepareColumnIndices(col1Id, additionalColumns);
  IdTable result(columnIndices.size(), allocator_);

  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  auto relevantBlocks = getBlocksFromMetadata(metadata, col1Id, blocks);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();

  // The first and the last block might be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the result as a vector.
  auto readIncompleteBlock = [&](const auto& block) {
    return readPossiblyIncompleteBlock(metadata, col1Id, block, std::nullopt,
                                       columnIndices);
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
    checkCancellation(cancellationHandle);
  }
  if (beginBlock < endBlock) {
    lastBlockResult = readIncompleteBlock(*(endBlock - 1));
    totalResultSize += lastBlockResult.value().size();
    endBlock--;
    checkCancellation(cancellationHandle);
  }

  // Determine the total size of the result.
  // First accumulate the complete blocks in the "middle"
  totalResultSize += std::accumulate(beginBlock, endBlock, 0UL,
                                     [](const auto& count, const auto& block) {
                                       return count + block.numRows_;
                                     });
  result.resize(totalResultSize);

  size_t rowIndexOfNextBlockStart = 0;
  // Lambda that appends a possibly incomplete block (the first or last block)
  // to the `result`.
  auto addIncompleteBlockIfExists =
      [&rowIndexOfNextBlockStart, &result](
          const std::optional<DecompressedBlock>& incompleteBlock) mutable {
        if (!incompleteBlock.has_value()) {
          return;
        }
        AD_CORRECTNESS_CHECK(incompleteBlock->numColumns() ==
                             result.numColumns());
        for (auto i : ad_utility::integerRange(result.numColumns())) {
          std::ranges::copy(
              incompleteBlock->getColumn(i),
              result.getColumn(i).data() + rowIndexOfNextBlockStart);
        }
        rowIndexOfNextBlockStart += incompleteBlock->numRows();
      };

  addIncompleteBlockIfExists(firstBlockResult);

  // Insert the complete blocks from the middle in parallel
  if (beginBlock < endBlock) {
#pragma omp parallel
#pragma omp single
    for (; beginBlock < endBlock; ++beginBlock) {
      const auto& block = *beginBlock;

      // Read the block serially, only read the second column.
      AD_CORRECTNESS_CHECK(block.offsetsAndCompressedSize_.size() >= 2);
      CompressedBlock compressedBuffer =
          readCompressedBlockFromFile(block, columnIndices);

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
        if (!cancellationHandle->isCancelled()) {
          decompressLambda();
        }
      }

      // update the pointers
      rowIndexOfNextBlockStart += block.numRows_;
    }  // end of parallel region
  }
  // Add the last block.
  addIncompleteBlockIfExists(lastBlockResult);
  AD_CORRECTNESS_CHECK(rowIndexOfNextBlockStart == result.size());
  checkCancellation(cancellationHandle);
  return result;
}

// _____________________________________________________________________________
DecompressedBlock CompressedRelationReader::readPossiblyIncompleteBlock(
    const CompressedRelationMetadata& relationMetadata,
    std::optional<Id> col1Id, const CompressedBlockMetadata& blockMetadata,
    std::optional<std::reference_wrapper<LazyScanMetadata>> scanMetadata,
    ColumnIndicesRef columnIndices) const {
  std::vector<ColumnIndex> allColumns;
  std::ranges::copy(
      ad_utility::integerRange(blockMetadata.offsetsAndCompressedSize_.size()),
      std::back_inserter(allColumns));
  // A block is uniquely identified by its start position in the file.
  auto cacheKey = blockMetadata.offsetsAndCompressedSize_.at(0).offsetInFile_;
  DecompressedBlock block = blockCache_
                                .computeOnce(cacheKey,
                                             [&]() {
                                               return readAndDecompressBlock(
                                                   blockMetadata, allColumns);
                                             })
                                ._resultPointer->clone();
  const auto& col1Column = block.getColumn(0);

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
  block.setColumnSubset(columnIndices);
  return block;
};

// _____________________________________________________________________________
size_t CompressedRelationReader::getResultSizeOfScan(
    const CompressedRelationMetadata& metadata, Id col1Id,
    const vector<CompressedBlockMetadata>& blocks) const {
  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  auto relevantBlocks = getBlocksFromMetadata(metadata, col1Id, blocks);
  auto [beginBlock, endBlock] = getBeginAndEnd(relevantBlocks);
  std::array<ColumnIndex, 1> columnIndices{0u};

  // The first and the last block might be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the size of the result.
  auto readSizeOfPossiblyIncompleteBlock = [&](const auto& block) {
    return readPossiblyIncompleteBlock(metadata, col1Id, block, std::nullopt,
                                       columnIndices)
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
void CompressedRelationWriter::writeBufferedRelationsToSingleBlock() {
  if (smallRelationsBuffer_.empty()) {
    return;
  }

  AD_CORRECTNESS_CHECK(smallRelationsBuffer_.numColumns() == numColumns());
  compressAndWriteBlock(
      currentBlockFirstCol0_, currentBlockLastCol0_,
      std::make_shared<IdTable>(std::move(smallRelationsBuffer_).toDynamic()));
  smallRelationsBuffer_.clear();
  smallRelationsBuffer_.reserve(2 * blocksize());
}

// _____________________________________________________________________________
CompressedBlock CompressedRelationReader::readCompressedBlockFromFile(
    const CompressedBlockMetadata& blockMetaData,
    ColumnIndicesRef columnIndices) const {
  CompressedBlock compressedBuffer;
  compressedBuffer.resize(columnIndices.size());
  // TODO<C++23> Use `std::views::zip`
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
    const CompressedBlockMetadata& blockMetaData,
    ColumnIndicesRef columnIndices) const {
  CompressedBlock compressedColumns =
      readCompressedBlockFromFile(blockMetaData, columnIndices);
  const auto numRowsToRead = blockMetaData.numRows_;
  return decompressBlock(compressedColumns, numRowsToRead);
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
void CompressedRelationWriter::compressAndWriteBlock(
    Id firstCol0Id, Id lastCol0Id, std::shared_ptr<IdTable> block) {
  blockWriteQueue_.push(
      [this, buf = std::move(block), firstCol0Id, lastCol0Id]() {
        std::vector<CompressedBlockMetadata::OffsetAndCompressedSize> offsets;
        for (const auto& column : buf->getColumns()) {
          offsets.push_back(compressAndWriteColumn(column));
        }
        AD_CORRECTNESS_CHECK(!offsets.empty());
        auto numRows = buf->numRows();
        const auto& first = (*buf)[0];
        const auto& last = (*buf)[buf->numRows() - 1];

        blockBuffer_.wlock()->push_back(
            CompressedBlockMetadata{std::move(offsets),
                                    numRows,
                                    {firstCol0Id, first[0], first[1]},
                                    {lastCol0Id, last[0], last[1]}});
      });
}

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
    const CompressedRelationReader::MetadataAndBlocks& metadataAndBlocks) const
    -> MetadataAndBlocks::FirstAndLastTriple {
  auto relevantBlocks = getBlocksFromMetadata(metadataAndBlocks);
  AD_CONTRACT_CHECK(!relevantBlocks.empty());

  auto scanBlock = [&](const CompressedBlockMetadata& block) {
    // Note: the following call only returns the part of the block that actually
    // matches the col0 and col1.
    return readPossiblyIncompleteBlock(
        metadataAndBlocks.relationMetadata_, metadataAndBlocks.col1Id_, block,
        std::nullopt, std::array<const ColumnIndex, 2>{0, 1});
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

// ____________________________________________________________________________
std::vector<ColumnIndex> CompressedRelationReader::prepareColumnIndices(
    std::initializer_list<ColumnIndex> baseColumns,
    ColumnIndicesRef additionalColumns) {
  std::vector<ColumnIndex> result;
  result.reserve(baseColumns.size() + additionalColumns.size());
  std::ranges::copy(baseColumns, std::back_inserter(result));
  std::ranges::copy(additionalColumns, std::back_inserter(result));
  return result;
}

// ____________________________________________________________________________
std::vector<ColumnIndex> CompressedRelationReader::prepareColumnIndices(
    const std::optional<Id>& col1Id, ColumnIndicesRef additionalColumns) {
  if (col1Id.has_value()) {
    return prepareColumnIndices({1}, additionalColumns);
  } else {
    return prepareColumnIndices({0, 1}, additionalColumns);
  }
}

// _____________________________________________________________________________
CompressedRelationMetadata CompressedRelationWriter::addSmallRelation(
    Id col0Id, size_t numDistinctC1, IdTableView<0> relation) {
  AD_CORRECTNESS_CHECK(!relation.empty());
  size_t numRows = relation.numRows();
  // Make sure that the blocks don't become too large: If the previously
  // buffered small relations together with the new relations would exceed `1.5
  // * blocksize` then we start a new block for the current relation. Note:
  // there are some unit tests that rely on this factor being `1.5`.
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
    std::ranges::copy(
        relation.getColumn(i),
        smallRelationsBuffer_.getColumn(i).begin() + offsetInBlock);
  }
  // Note: the multiplicity of the `col2` (where we set the dummy here) will be
  // set later in `createPermutationPair`.
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
  compressAndWriteBlock(currentCol0Id_, currentCol0Id_, std::move(relation));
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

// A class that is called for all pairs of `CompressedRelationMetadata` for the
// same `col0Id` and the "twin permutations" (e.g. PSO and POS). The
// multiplicity of the last column is exchanged and then the metadata are passed
// on to the respective `MetadataCallback`.
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
    std::ranges::for_each(block.getColumn(0), std::ref(distinctCol1Counter));
    addBlockForLargeRelation(
        col0Id, std::make_shared<IdTable>(std::move(block).toDynamic()));
  }
  return finishLargeRelation(distinctCol1Counter.getAndReset());
}

// _____________________________________________________________________________
std::pair<std::vector<CompressedBlockMetadata>,
          std::vector<CompressedBlockMetadata>>
CompressedRelationWriter::createPermutationPair(
    const std::string& basename, WriterAndCallback writerAndCallback1,
    WriterAndCallback writerAndCallback2,
    cppcoro::generator<IdTableStatic<0>> sortedTriples,
    std::array<size_t, 3> permutation,
    const std::vector<std::function<void(const IdTableStatic<0>&)>>&
        perBlockCallbacks) {
  auto [c0, c1, c2] = permutation;
  auto& writer1 = writerAndCallback1.writer_;
  auto& writer2 = writerAndCallback2.writer_;
  const size_t blocksize = writer1.blocksize();
  AD_CORRECTNESS_CHECK(writer2.blocksize() == writer1.blocksize());
  const size_t numColumns = writer1.numColumns();
  AD_CORRECTNESS_CHECK(writer1.numColumns() == writer2.numColumns());
  MetadataWriter writeMetadata{std::move(writerAndCallback1.callback_),
                               std::move(writerAndCallback2.callback_),
                               writer1.blocksize()};

  // A queue for the callbacks that have to be applied for each triple.
  // The second argument is the number of threads. It is crucial that this queue
  // is single threaded.
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
    return std::tie(a[0], a[1]) < std::tie(b[0], b[1]);
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
    twinRelation.swapColumns(0, 1);
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

  auto finishRelation = [&twinRelationSorter, &writer2, &writer1,
                         &numBlocksCurrentRel, &col0IdCurrentRelation,
                         &relation, &distinctCol1Counter,
                         &addBlockForLargeRelation, &compare, &blocksize,
                         &writeMetadata, &largeTwinRelationTimer]() {
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
      auto md1 = writer1.addSmallRelation(col0IdCurrentRelation.value(),
                                          distinctCol1Counter.getAndReset(),
                                          relation.asStaticView<0>());
      // We don't use the parallel twinRelationSorter to create the twin
      // relation as its overhead is far too high for small relations.
      relation.swapColumns(0, 1);
      std::ranges::sort(relation, compare);
      std::ranges::for_each(relation.getColumn(0),
                            std::ref(distinctCol1Counter));
      auto md2 = writer2.addSmallRelation(col0IdCurrentRelation.value(),
                                          distinctCol1Counter.getAndReset(),
                                          relation.asStaticView<0>());
      writeMetadata(md1, md2);
    }
    relation.clear();
    numBlocksCurrentRel = 0;
  };
  size_t i = 0;
  // All columns but the `col0` in the order in which they have to be added to
  // the relation.
  std::vector<ColumnIndex> remainingColIndices{c1, c2};
  for (size_t colIdx = 2; colIdx < numColumns; ++colIdx) {
    remainingColIndices.push_back(colIdx + 1);
  }
  inputWaitTimer.cont();
  for (auto& block : AD_FWD(sortedTriples)) {
    AD_CORRECTNESS_CHECK(block.numColumns() == numColumns + 1);
    inputWaitTimer.stop();
    // This only happens when the index is completely empty.
    if (block.empty()) {
      continue;
    }
    auto firstCol = block.getColumn(c0);
    auto remainingCols = block.asColumnSubsetView(remainingColIndices);
    if (!col0IdCurrentRelation.has_value()) {
      col0IdCurrentRelation = firstCol[0];
    }
    // TODO<C++23> Use `views::zip`
    for (size_t idx : ad_utility::integerRange(block.numRows())) {
      Id col0Id = firstCol[idx];
      decltype(auto) curRemainingCols = remainingCols[idx];
      if (col0Id != col0IdCurrentRelation) {
        finishRelation();
        col0IdCurrentRelation = col0Id;
      }
      distinctCol1Counter(curRemainingCols[0]);
      relation.push_back(curRemainingCols);
      if (relation.size() >= blocksize) {
        addBlockForLargeRelation();
      }
      ++i;
      if (i % 100'000'000 == 0) {
        LOG(INFO) << "Triples processed: " << i << std::endl;
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
  LOG(TIMING) << "Time spent waiting for large twin relations "
              << ad_utility::Timer::toSeconds(largeTwinRelationTimer.msecs())
              << "s" << std::endl;
  LOG(TIMING)
      << "Time spent waiting for triple callbacks (e.g. the next sorter) "
      << ad_utility::Timer::toSeconds(blockCallbackTimer.msecs()) << "s"
      << std::endl;
  return std::pair{std::move(writer1).getFinishedBlocks(),
                   std::move(writer2).getFinishedBlocks()};
}
