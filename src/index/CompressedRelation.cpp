// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include "CompressedRelation.h"

#include "engine/idTable/IdTable.h"
#include "util/Cache.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/ConcurrentCache.h"
#include "util/Generator.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/OverloadCallOperator.h"
#include "util/TypeTraits.h"

using namespace std::chrono_literals;

// ____________________________________________________________________________
void CompressedRelationReader::scan(
    const CompressedRelationMetadata& metadata,
    std::span<const CompressedBlockMetadata> blockMetadata,
    ad_utility::File& file, IdTable* result,
    ad_utility::SharedConcurrentTimeoutTimer timer) const {
  AD_CONTRACT_CHECK(result->numColumns() == NumColumns);

  auto relevantBlocks =
      getBlocksFromMetadata(metadata, std::nullopt, blockMetadata);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();
  Id col0Id = metadata.col0Id_;
  // The total size of the result is now known.
  result->resize(metadata.getNofElements());

  // The position in the result to which the next block is being
  // decompressed.
  size_t rowIndexOfNextBlock = 0;

  // The number of rows for which we still have space
  // in the result (only needed for checking of invariants).
  size_t spaceLeft = result->size();

  // The first block might contain entries that are not part of our
  // actual scan result.
  bool firstBlockIsIncomplete =
      beginBlock < endBlock && (beginBlock->firstTriple_[0] < col0Id ||
                                beginBlock->lastTriple_[0] > col0Id);
  auto lastBlock = endBlock - 1;

  bool lastBlockIsIncomplete =
      beginBlock < lastBlock && (lastBlock->firstTriple_[0] < col0Id ||
                                 lastBlock->lastTriple_[0] > col0Id);

  // Invariant: A relation spans multiple blocks exclusively or several
  // entities are stored completely in the same Block.
  AD_CORRECTNESS_CHECK(!firstBlockIsIncomplete || (beginBlock == lastBlock));
  AD_CORRECTNESS_CHECK(!lastBlockIsIncomplete);
  if (firstBlockIsIncomplete) {
    AD_CORRECTNESS_CHECK(metadata.offsetInBlock_ !=
                         std::numeric_limits<uint64_t>::max());
  }

  // We have at most one block that is incomplete and thus requires trimming.
  // Set up a lambda, that reads this block and decompresses it to
  // the result.
  auto readIncompleteBlock = [&](const auto& block) mutable {
    // A block is uniquely identified by its start position in the file.
    auto cacheKey = block.offsetsAndCompressedSize_.at(0).offsetInFile_;
    auto uncompressedBuffer = blockCache_
                                  .computeOnce(cacheKey,
                                               [&]() {
                                                 return readAndDecompressBlock(
                                                     block, file, std::nullopt);
                                               })
                                  ._resultPointer;

    // Extract the part of the block that actually belongs to the relation
    auto numElements = metadata.numRows_;
    AD_CORRECTNESS_CHECK(uncompressedBuffer->numColumns() ==
                         metadata.numColumns());
    for (size_t i = 0; i < uncompressedBuffer->numColumns(); ++i) {
      const auto& inputCol = uncompressedBuffer->getColumn(i);
      auto begin = inputCol.begin() + metadata.offsetInBlock_;
      auto resultColumn = result->getColumn(i);
      AD_CORRECTNESS_CHECK(numElements <= spaceLeft);
      std::copy(begin, begin + numElements, resultColumn.begin());
    }
    rowIndexOfNextBlock += numElements;
    spaceLeft -= numElements;
  };

  // Read the first block if it is incomplete
  if (firstBlockIsIncomplete) {
    readIncompleteBlock(*beginBlock);
    ++beginBlock;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan :");
    }
  }

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
                                           *result, rowIndexOfNextBlock);
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
}

// _____________________________________________________________________________
cppcoro::generator<IdTable> CompressedRelationReader::lazyScan(
    CompressedRelationMetadata metadata,
    std::vector<CompressedBlockMetadata> blockMetadata, ad_utility::File& file,
    ad_utility::AllocatorWithLimit<Id> allocator,
    ad_utility::SharedConcurrentTimeoutTimer timer) const {
  auto relevantBlocks =
      getBlocksFromMetadata(metadata, std::nullopt, blockMetadata);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();
  Id col0Id = metadata.col0Id_;
  // The first block might contain entries that are not part of our
  // actual scan result.
  bool firstBlockIsIncomplete =
      beginBlock < endBlock && (beginBlock->firstTriple_[0] < col0Id ||
                                beginBlock->lastTriple_[0] > col0Id);
  auto lastBlock = endBlock - 1;

  bool lastBlockIsIncomplete =
      beginBlock < lastBlock && (lastBlock->firstTriple_[0] < col0Id ||
                                 lastBlock->lastTriple_[0] > col0Id);

  // Invariant: A relation spans multiple blocks exclusively or several
  // entities are stored completely in the same Block.
  AD_CORRECTNESS_CHECK(!firstBlockIsIncomplete || (beginBlock == lastBlock));
  AD_CORRECTNESS_CHECK(!lastBlockIsIncomplete);
  if (firstBlockIsIncomplete) {
    AD_CORRECTNESS_CHECK(metadata.offsetInBlock_ !=
                         std::numeric_limits<uint64_t>::max());
  }

  // We have at most one block that is incomplete and thus requires trimming.
  // Set up a lambda, that reads this block and decompresses it to
  // the result.
  auto readIncompleteBlock = [&](const auto& block) {
    // A block is uniquely identified by its start position in the file.
    auto cacheKey = block.offsetsAndCompressedSize_.at(0).offsetInFile_;
    auto uncompressedBuffer = blockCache_
                                  .computeOnce(cacheKey,
                                               [&]() {
                                                 return readAndDecompressBlock(
                                                     block, file, std::nullopt);
                                               })
                                  ._resultPointer;

    // Extract the part of the block that actually belongs to the relation
    auto numElements = metadata.numRows_;
    AD_CORRECTNESS_CHECK(uncompressedBuffer->numColumns() ==
                         metadata.numColumns());
    IdTable result(uncompressedBuffer->numColumns(), allocator);
    result.resize(numElements);
    for (size_t i = 0; i < uncompressedBuffer->numColumns(); ++i) {
      const auto& inputCol = uncompressedBuffer->getColumn(i);
      auto begin = inputCol.begin() + metadata.offsetInBlock_;
      decltype(auto) resultColumn = result.getColumn(i);
      std::copy(begin, begin + numElements, resultColumn.begin());
    }
    return result;
  };

  // Read the first block if it is incomplete
  if (firstBlockIsIncomplete) {
    co_yield readIncompleteBlock(*beginBlock);
    ++beginBlock;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan :");
    }
  }

  // Read all the other (complete!) blocks in parallel
  for (; beginBlock < endBlock; ++beginBlock) {
    const auto& block = *beginBlock;
    // Read a block from disk (serially).

    CompressedBlock compressedBuffer =
        readCompressedBlockFromFile(block, file, std::nullopt);
    co_yield decompressBlock(compressedBuffer, block.numRows_);
    // The `decompressLambda` can now run in parallel
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow();
    };
  }
}

cppcoro::generator<IdTable> CompressedRelationReader::lazyScan(
    CompressedRelationMetadata metadata, Id col1Id,
    std::vector<CompressedBlockMetadata> blockMetadata, ad_utility::File& file,
    ad_utility::AllocatorWithLimit<Id> allocator,
    ad_utility::SharedConcurrentTimeoutTimer timer) const {
  auto relevantBlocks = getBlocksFromMetadata(metadata, col1Id, blockMetadata);
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
  auto readPossiblyIncompleteBlock = [&](const auto& block) {
    DecompressedBlock uncompressedBuffer =
        readAndDecompressBlock(block, file, std::nullopt);
    AD_CORRECTNESS_CHECK(uncompressedBuffer.numColumns() == 2);
    const auto& col1Column = uncompressedBuffer.getColumn(0);
    const auto& col2Column = uncompressedBuffer.getColumn(1);
    AD_CORRECTNESS_CHECK(col1Column.size() == col2Column.size());

    // Find the range in the block, that belongs to the same relation `col0Id`
    bool containedInOnlyOneBlock =
        metadata.offsetInBlock_ != std::numeric_limits<uint64_t>::max();
    auto begin = col1Column.begin();
    if (containedInOnlyOneBlock) {
      begin += metadata.offsetInBlock_;
    }
    auto end =
        containedInOnlyOneBlock ? begin + metadata.numRows_ : col1Column.end();

    // Find the range in the block, where also the col1Id matches (the second
    // ID in the `std::array` does not matter).
    std::tie(begin, end) = std::equal_range(begin, end, col1Id);

    size_t beginIndex = begin - col1Column.begin();
    size_t endIndex = end - col1Column.begin();

    // Only extract the relevant portion of the second column.
    IdTable result{1, allocator};
    result.resize(endIndex - beginIndex);
    std::copy(col2Column.begin() + beginIndex, col2Column.begin() + endIndex,
              result.getColumn(0).begin());
    return result;
  };

  // The first and the last block might be incomplete, compute
  // and store the partial results from them.
  std::optional<IdTable> firstBlockResult, lastBlockResult;
  if (beginBlock < endBlock) {
    firstBlockResult = readPossiblyIncompleteBlock(*beginBlock);
    ++beginBlock;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan: ");
    }
  }
  if (beginBlock < endBlock) {
    lastBlockResult = readPossiblyIncompleteBlock(*(endBlock - 1));
    endBlock--;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan: ");
    }
  }

  if (firstBlockResult.has_value()) {
    co_yield std::move(firstBlockResult.value());
  }

  for (; beginBlock < endBlock; ++beginBlock) {
    const auto& block = *beginBlock;

    // Read the block serially, only read the second column.
    AD_CORRECTNESS_CHECK(block.offsetsAndCompressedSize_.size() == 2);
    CompressedBlock compressedBuffer =
        readCompressedBlockFromFile(block, file, std::vector{1ul});
    co_yield decompressBlock(compressedBuffer, block.numRows_);
  }
  if (timer) {
    timer->wlock()->checkTimeoutAndThrow();
  }
  if (lastBlockResult.has_value()) {
    co_yield lastBlockResult.value();
  }
}

// TODO<joka921> Comment those helpers. Should we register them in the header as
// private static functions?
namespace {
auto getJoinColumnRangeValue = [](std::array<Id, 3> block, Id col0Id,
                                  std::optional<Id> col1Id) {
  auto minId = Id::makeUndefined();
  auto maxId = Id::fromBits(std::numeric_limits<uint64_t>::max());
  if (block[0] < col0Id) {
    return minId;
  }
  if (block[0] > col0Id) {
    return maxId;
  }
  if (!col1Id.has_value()) {
    return block[1];
  }

  if (block[1] < col1Id.value()) {
    return minId;
  }
  if (block[1] > col1Id.value()) {
    return maxId;
  }
  return block[2];
};

using IdPair = std::pair<Id, Id>;
auto blocksToIdRanges = [](std::span<const CompressedBlockMetadata> blocks,
                           Id col0Id, std::optional<Id> col1Id) {
  std::vector<IdPair> result;
  for (const auto& block : blocks) {
    result.emplace_back(
        getJoinColumnRangeValue(block.firstTriple_, col0Id, col1Id),
        getJoinColumnRangeValue(block.lastTriple_, col0Id, col1Id));
  }
  return result;
};
}  // namespace

// _____________________________________________________________________________
std::vector<CompressedBlockMetadata> CompressedRelationReader::getBlocksForJoin(
    std::span<const Id> joinColum, const CompressedRelationMetadata& metadata,
    std::optional<Id> col1Id,
    std::span<const CompressedBlockMetadata> blockMetadata) {
  // get all the blocks where col0FirstId_ <= col0Id <= col0LastId_
  auto relevantBlocks =
      getBlocksFromMetadata(metadata, std::nullopt, blockMetadata);
  // TODO<joka921> Is this condition necessary?
  if (relevantBlocks.size() < 2) {
    return {relevantBlocks.begin(), relevantBlocks.end()};
  }

  auto idRanges = blocksToIdRanges(relevantBlocks, metadata.col0Id_, col1Id);

  auto idLessThanBlock = [](Id id, const IdPair& block) {
    return id < block.first;
  };

  auto blockLessThanId = [](const IdPair& block, Id id) {
    return block.second < id;
  };

  auto idLessThanId = [](Id id, Id id2) { return id < id2; };

  auto lessThan = ad_utility::OverloadCallOperator{
      idLessThanBlock, blockLessThanId, idLessThanId};

  std::vector<CompressedBlockMetadata> result;
  auto addRow = [&result, begBlocks = relevantBlocks.begin(),
                 begRanges = idRanges.begin()]([[maybe_unused]] auto it1,
                                               auto it2) {
    result.push_back(*(begBlocks + (it2 - begRanges)));
  };

  // TODO<joka921> is the copy really necessary?
  // TODO<joka921> Should we respect the memory limit?
  // TODO<joka921> The correct way indeed WAS to remove the duplicates inside
  // the `zipperJoinWithUndef` function.
  std::vector<Id> joinColumnUnique;
  joinColumnUnique.reserve(joinColum.size());
  std::ranges::unique_copy(joinColum, std::back_inserter(joinColumnUnique));

  if (joinColum.empty()) {
    return result;
  }

  auto noop = ad_utility::noop;
  [[maybe_unused]] auto numOutOfOrder = ad_utility::zipperJoinWithUndef(
      joinColumnUnique, idRanges, lessThan, addRow, noop, noop);
  return result;
}

// _____________________________________________________________________________
std::array<std::vector<CompressedBlockMetadata>, 2>
CompressedRelationReader::getBlocksForJoin(
    const CompressedRelationMetadata& md1,
    const CompressedRelationMetadata& md2, std::optional<Id> col1Id1,
    std::optional<Id> col1Id2,
    std::span<const CompressedBlockMetadata> blockMetadata1,
    std::span<const CompressedBlockMetadata> blockMetadata2) {
  // get all the blocks where col0FirstId_ <= col0Id <= col0LastId_
  struct KeyLhs {
    Id col0FirstId_;
    Id col0LastId_;
  };

  auto relevantBlocks1 =
      getBlocksFromMetadata(md1, std::nullopt, blockMetadata1);
  auto beginBlock1 = relevantBlocks1.begin();
  auto endBlock1 = relevantBlocks1.end();

  auto relevantBlocks2 =
      getBlocksFromMetadata(md2, std::nullopt, blockMetadata2);
  auto beginBlock2 = relevantBlocks2.begin();
  auto endBlock2 = relevantBlocks2.end();

  auto blockLessThanBlock = [](const auto& block1, const auto& block2) {
    return block1.second < block2.first;
  };

  std::array<std::vector<CompressedBlockMetadata>, 2> result;
  auto idRanges1 = blocksToIdRanges(
      std::ranges::subrange{beginBlock1, endBlock1}, md1.col0Id_, col1Id1);
  auto idRanges2 = blocksToIdRanges(
      std::ranges::subrange{beginBlock2, endBlock2}, md2.col0Id_, col1Id2);
  auto addRow = [&result, &beginBlock1, &beginBlock2, &idRanges1, &idRanges2](
                    [[maybe_unused]] auto it1, auto it2) {
    result[0].push_back(*(beginBlock1 + (it1 - idRanges1.begin())));
    result[1].push_back(*(beginBlock2 + (it2 - idRanges2.begin())));
  };

  auto noop = ad_utility::noop;
  [[maybe_unused]] auto numOutOfOrder = ad_utility::zipperJoinWithUndef(
      idRanges1, idRanges2, blockLessThanBlock, addRow, noop, noop);
  for (auto& vec : result) {
    vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
  }
  return result;
}

// _____________________________________________________________________________
void CompressedRelationReader::scan(
    const CompressedRelationMetadata& metaData, Id col1Id,
    const vector<CompressedBlockMetadata>& blocks, ad_utility::File& file,
    IdTable* result, ad_utility::SharedConcurrentTimeoutTimer timer) const {
  AD_CONTRACT_CHECK(result->numColumns() == 1);

  auto relevantBlocks = getBlocksFromMetadata(metaData, col1Id, blocks);
  auto beginBlock = relevantBlocks.begin();
  auto endBlock = relevantBlocks.end();

  // Invariant: The col0Id is completely stored in a single block, or it is
  // contained in multiple blocks that only contain this col0Id,
  bool col0IdHasExclusiveBlocks =
      metaData.offsetInBlock_ == std::numeric_limits<uint64_t>::max();
  if (!col0IdHasExclusiveBlocks) {
    // This might also be zero if no block was found at all.
    AD_CORRECTNESS_CHECK(endBlock - beginBlock <= 1);
  }

  // The first and the last block might be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the result as a vector.
  auto readPossiblyIncompleteBlock = [&](const auto& block) {
    DecompressedBlock uncompressedBuffer =
        readAndDecompressBlock(block, file, std::nullopt);
    AD_CORRECTNESS_CHECK(uncompressedBuffer.numColumns() == 2);
    const auto& col1Column = uncompressedBuffer.getColumn(0);
    const auto& col2Column = uncompressedBuffer.getColumn(1);
    AD_CORRECTNESS_CHECK(col1Column.size() == col2Column.size());

    // Find the range in the block, that belongs to the same relation `col0Id`
    bool containedInOnlyOneBlock =
        metaData.offsetInBlock_ != std::numeric_limits<uint64_t>::max();
    auto begin = col1Column.begin();
    if (containedInOnlyOneBlock) {
      begin += metaData.offsetInBlock_;
    }
    auto end =
        containedInOnlyOneBlock ? begin + metaData.numRows_ : col1Column.end();

    // Find the range in the block, where also the col1Id matches (the second
    // ID in the `std::array` does not matter).
    std::tie(begin, end) = std::equal_range(begin, end, col1Id);

    size_t beginIndex = begin - col1Column.begin();
    size_t endIndex = end - col1Column.begin();

    // Only extract the relevant portion of the second column.
    std::vector<Id> result(col2Column.begin() + beginIndex,
                           col2Column.begin() + endIndex);
    return result;
  };

  // The first and the last block might be incomplete, compute
  // and store the partial results from them.
  std::vector<Id> firstBlockResult, lastBlockResult;
  if (beginBlock < endBlock) {
    firstBlockResult = readPossiblyIncompleteBlock(*beginBlock);
    ++beginBlock;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan: ");
    }
  }
  if (beginBlock < endBlock) {
    lastBlockResult = readPossiblyIncompleteBlock(*(endBlock - 1));
    endBlock--;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan: ");
    }
  }

  // Determine the total size of the result.
  // First accumulate the complete blocks in the "middle"
  auto totalResultSize = std::accumulate(
      beginBlock, endBlock, 0ul, [](const auto& count, const auto& block) {
        return count + block.numRows_;
      });
  // Add the possibly incomplete blocks from the beginning and end;
  totalResultSize += firstBlockResult.size() + lastBlockResult.size();

  result->resize(totalResultSize);

  // Insert the first block into the result;
  std::copy(firstBlockResult.begin(), firstBlockResult.end(),
            result->getColumn(0).data());
  size_t rowIndexOfNextBlockStart = firstBlockResult.size();

  // Insert the complete blocks from the middle in parallel
  if (beginBlock < endBlock) {
#pragma omp parallel
#pragma omp single
    for (; beginBlock < endBlock; ++beginBlock) {
      const auto& block = *beginBlock;

      // Read the block serially, only read the second column.
      AD_CORRECTNESS_CHECK(block.offsetsAndCompressedSize_.size() == 2);
      CompressedBlock compressedBuffer =
          readCompressedBlockFromFile(block, file, std::vector{1ul});

      // A lambda that owns the compressed block decompresses it to the
      // correct position in the result. It may safely be run in parallel
      auto decompressLambda = [rowIndexOfNextBlockStart, &block, result,
                               compressedBuffer =
                                   std::move(compressedBuffer)]() mutable {
        ad_utility::TimeBlockAndLog tbl{"Decompression a block"};

        decompressBlockToExistingIdTable(compressedBuffer, block.numRows_,
                                         *result, rowIndexOfNextBlockStart);
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
  std::copy(lastBlockResult.begin(), lastBlockResult.end(),
            result->getColumn(0).data() + rowIndexOfNextBlockStart);
  AD_CORRECTNESS_CHECK(rowIndexOfNextBlockStart + lastBlockResult.size() ==
                       result->size());
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
  CompressedRelationMetadata metaData{col0Id, col1And2Ids.numRows(), multC1,
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
    metaData.offsetInBlock_ = std::numeric_limits<uint64_t>::max();
  } else {
    // Append to the current buffered block.
    metaData.offsetInBlock_ = buffer_.numRows();
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
  return metaData;
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
  DecompressedBlock decompressedBlock{allocator};
  decompressedBlock.setNumColumns(compressedBlock.size());
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
  if (!col1Id.has_value()) {
    // get all the blocks where col0FirstId_ <= col0Id <= col0LastId_
    Id col0Id = metadata.col0Id_;
    CompressedBlockMetadata key;
    key.firstTriple_[0] = col0Id;
    key.lastTriple_[0] = col0Id;
    // TODO<joka921, Clang16> Use a structured binding. Structured bindings are
    // currently not supported by clang when using OpenMP because clang
    // internally transforms the `#pragma`s into lambdas, and capturing
    // structured bindings is only supported in clang >= 16.
    auto [beginBlock, endBlock] = std::equal_range(
        // TODO<joka921> For some reason we can't use
        // `std::ranges::equal_range`, find out why. Note: possibly it has
        // something to do with the limited support of ranges in clang with
        // versions < 16. Revisit this when we use clang 16.
        blockMetadata.begin(), blockMetadata.end(), key,
        [](const auto& a, const auto& b) {
          return a.firstTriple_[0] < b.firstTriple_[0] &&
                 a.lastTriple_[0] < b.lastTriple_[0];
        });
    return {beginBlock, endBlock};
  } else {
    // Get all the blocks  that possibly might contain our pair of col0Id and
    // col1Id
    Id col0Id = metadata.col0Id_;
    CompressedBlockMetadata key;
    key.firstTriple_[0] = col0Id;
    key.lastTriple_[0] = col0Id;
    key.firstTriple_[1] = col1Id.value();
    key.lastTriple_[1] = col1Id.value();

    auto comp = [](const auto& a, const auto& b) {
      bool endBeforeBegin = a.lastTriple_[0] < b.firstTriple_[0];
      endBeforeBegin |= (a.lastTriple_[0] == b.firstTriple_[0] &&
                         a.lastTriple_[1] < b.firstTriple_[1]);
      return endBeforeBegin;
    };

    // Note: See the comment in the other overload for `scan` above for the
    // reason why we (currently) can't use a structured binding here.
    auto [beginBlock, endBlock] =
        std::equal_range(blockMetadata.begin(), blockMetadata.end(), key, comp);
    return {beginBlock, endBlock};
  }
}
