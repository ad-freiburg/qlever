// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include "CompressedRelation.h"

#include "engine/idTable/IdTable.h"
#include "util/AllocatorWithLimit.h"
#include "util/Cache.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/ConcurrentCache.h"
#include "util/Generator.h"
#include "util/TypeTraits.h"

using namespace std::chrono_literals;

// ____________________________________________________________________________
void CompressedRelationReader::scan(
    const CompressedRelationMetadata& metadataForRelation,
    const vector<CompressedBlockMetadata>& metadataForAllBlocks,
    ad_utility::File& file, IdTable* result,
    ad_utility::SharedConcurrentTimeoutTimer timer,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  AD_CONTRACT_CHECK(result->numColumns() == NumColumns);

  // get all the blocks where _col0FirstId <= col0Id <= _col0LastId
  struct KeyLhs {
    Id _col0FirstId;
    Id _col0LastId;
  };
  Id col0Id = metadataForRelation._col0Id;
  // TODO<joka921, Clang16> Use a structured binding. Structured bindings are
  // currently not supported by clang when using OpenMP because clang internally
  // transforms the `#pragma`s into lambdas, and capturing structured bindings
  // is only supported in clang >= 16.
  decltype(metadataForAllBlocks.begin()) beginBlock, endBlock;
  std::tie(beginBlock, endBlock) = std::equal_range(
      // TODO<joka921> For some reason we can't use `std::ranges::equal_range`,
      // find out why. Note: possibly it has something to do with the limited
      // support of ranges in clang with versions < 16. Revisit this when
      // we use clang 16.
      metadataForAllBlocks.begin(), metadataForAllBlocks.end(),
      KeyLhs{col0Id, col0Id}, [](const auto& a, const auto& b) {
        return a._col0FirstId < b._col0FirstId && a._col0LastId < b._col0LastId;
      });

  // The first block might contain entries that are not part of our
  // actual scan result.
  bool firstBlockIsIncomplete =
      beginBlock < endBlock &&
      (beginBlock->_col0FirstId < col0Id || beginBlock->_col0LastId > col0Id);
  auto lastBlock = endBlock - 1;

  bool lastBlockIsIncomplete =
      beginBlock < lastBlock &&
      (lastBlock->_col0FirstId < col0Id || lastBlock->_col0LastId > col0Id);

  // Invariant: A relation spans multiple blocks exclusively or several
  // entities are stored completely in the same Block.
  AD_CORRECTNESS_CHECK(!firstBlockIsIncomplete || (beginBlock == lastBlock));
  AD_CORRECTNESS_CHECK(!lastBlockIsIncomplete);
  if (firstBlockIsIncomplete) {
    AD_CORRECTNESS_CHECK(metadataForRelation._offsetInBlock !=
                         std::numeric_limits<uint64_t>::max());
  }

  // Compute the numer of inserted and deleted triples per block and overall.
  // note the `<=` so that we don't forget the block beyond the last (which may
  // have information about delta triples at the vey end of a relation).
  std::vector<std::pair<size_t, size_t>> numInsAndDelPerBlock;
  size_t numInsTotal = 0;
  size_t numDelTotal = 0;
  for (auto block = beginBlock; block <= endBlock; ++block) {
    size_t blockIndex = block - metadataForAllBlocks.begin();
    auto [numIns, numDel] =
        block == beginBlock || block == endBlock
            ? locatedTriplesPerBlock.numTriples(blockIndex, col0Id)
            : locatedTriplesPerBlock.numTriples(blockIndex);
    numInsTotal += numIns;
    numDelTotal += numDel;
    numInsAndDelPerBlock.push_back({numIns, numDel});
  }
  if (numInsTotal > 0 || numDelTotal > 0) {
    LOG(INFO) << "Index scan with delta triples: #inserts = " << numInsTotal
              << ", #deletes = " << numDelTotal
              << ", #blocks = " << (endBlock - beginBlock) << std::endl;
    AD_CORRECTNESS_CHECK(numDelTotal < metadataForRelation.getNofElements());
  }

  // The total size of the result is now known.
  result->resize(metadataForRelation.getNofElements() + numInsTotal -
                 numDelTotal);

  // The position in the result to which the next block is being
  // decompressed.
  size_t offsetInResult = 0;

  // The number of rows for which we still have space
  // in the result (only needed for checking of invariants).
  size_t spaceLeft = result->size();

  // We have at most one block that is incomplete and thus requires trimming.
  // Set up a lambda, that reads this block and decompresses it to
  // the result.
  auto processIncompleteBlock = [&](const auto& blockMetadata) {
    // A block is uniquely identified by its start position in the file.
    //
    // NOTE: We read these blocks via a cache in order to speed up the unit
    // tests (which make many requests to the same block, so we don't want
    // to decompress it again and again).
    auto cacheKey =
        blockMetadata->_offsetsAndCompressedSize.at(0)._offsetInFile;
    auto block = blockCache_
                     .computeOnce(cacheKey,
                                  [&]() {
                                    return readAndDecompressBlock(
                                        *blockMetadata, file, std::nullopt);
                                  })
                     ._resultPointer;

    // Determine (via the metadata for the relation), exactly which part of the
    // block belongs to the relation.
    auto numInsAndDel = numInsAndDelPerBlock.at(blockMetadata - beginBlock);
    size_t rowIndexBegin = metadataForRelation._offsetInBlock;
    size_t rowIndexEnd = rowIndexBegin + metadataForRelation._numRows;
    AD_CORRECTNESS_CHECK(rowIndexBegin < block->size());
    AD_CORRECTNESS_CHECK(rowIndexEnd <= block->size());
    AD_CORRECTNESS_CHECK(block->numColumns() ==
                         metadataForRelation.numColumns());
    size_t numRowsWrittenToResult = rowIndexEnd - rowIndexBegin;

    // Without delta triples, just copy the part of the block to `result`.
    // Otherwise use `mergeTriples`.
    if (numInsAndDel == std::pair<size_t, size_t>{0, 0}) {
      for (size_t i = 0; i < numRowsWrittenToResult; ++i) {
        (*result)(offsetInResult + i, 0) = (*block)(rowIndexBegin + i, 0);
        (*result)(offsetInResult + i, 1) = (*block)(rowIndexBegin + i, 1);
      }
    } else {
      // TODO: First copy `*block` to an object of class `IdTable`. This copy
      // would be avoidable, see the related comment in `getBlockPart` in the
      // other `CompressedRelationReader::scan` below.
      ad_utility::AllocatorWithLimit<Id> allocator{
          ad_utility::makeAllocationMemoryLeftThreadsafeObject(
              std::numeric_limits<size_t>::max())};
      IdTable blockAsIdTable(2, allocator);
      blockAsIdTable.resize(block->size());
      for (size_t i = 0; i < block->size(); ++i) {
        blockAsIdTable(i, 0) = (*block)(i, 0);
        blockAsIdTable(i, 1) = (*block)(i, 1);
      }
      // Now call `mergeTriples` on `blockAsIdTable`.
      size_t blockIndex = blockMetadata - metadataForAllBlocks.begin();
      size_t numRowsWrittenExpected = numRowsWrittenToResult;
      numRowsWrittenExpected += numInsAndDel.first - numInsAndDel.second;
      numRowsWrittenToResult = locatedTriplesPerBlock.mergeTriples(
          blockIndex, std::move(blockAsIdTable), *result, offsetInResult,
          col0Id, rowIndexBegin);
      AD_CORRECTNESS_CHECK(numRowsWrittenToResult == numRowsWrittenExpected);
    }

    AD_CORRECTNESS_CHECK(numRowsWrittenToResult <= spaceLeft);
    offsetInResult += numRowsWrittenToResult;
    spaceLeft -= numRowsWrittenToResult;
  };

  // Read the first block if it is incomplete
  auto completeBlocksBegin = beginBlock;
  auto completeBlocksEnd = endBlock;
  if (firstBlockIsIncomplete) {
    processIncompleteBlock(beginBlock);
    ++completeBlocksBegin;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan :");
    }
  }

  // Process all the other (complete) blocks. The compressed blocks are
  // read sequentially from disk and then decompressed in parallel.
  if (completeBlocksBegin < completeBlocksEnd) {
#pragma omp parallel
#pragma omp single
    for (auto block = completeBlocksBegin; block < completeBlocksEnd; ++block) {
      size_t blockIndex = block - metadataForAllBlocks.begin();
      auto numInsAndDel = numInsAndDelPerBlock.at(block - beginBlock);

      // Read the compressed block from disk (both columns).
      CompressedBlock compressedBuffer =
          readCompressedBlockFromFile(*block, file, std::nullopt);

      // This lambda decompresses the block that was just read to the
      // correct position in the result.
      auto decompressLambda = [&result, &locatedTriplesPerBlock, &block,
                               numInsAndDel, offsetInResult, blockIndex,
                               compressedBuffer =
                                   std::move(compressedBuffer)]() {
        ad_utility::TimeBlockAndLog tbl{"Decompressing a block"};

        decompressBlockToExistingIdTable(compressedBuffer, block->_numRows,
                                         *result, offsetInResult, numInsAndDel,
                                         locatedTriplesPerBlock, blockIndex);
      };

      // This `decompressLambda` can run concurrently.
#pragma omp task
      {
        if (!timer || !timer->wlock()->hasTimedOut()) {
          decompressLambda();
        };
      }

      // Update the counters.
      AD_CORRECTNESS_CHECK(numInsAndDel.second <= block->_numRows);
      size_t numRowsOfThisBlock =
          block->_numRows + numInsAndDel.first - numInsAndDel.second;
      AD_CORRECTNESS_CHECK(numRowsOfThisBlock <= spaceLeft);
      spaceLeft -= numRowsOfThisBlock;
      offsetInResult += numRowsOfThisBlock;
    }
    // End of omp parallel region, all blocks are decompressed now.
  }

  // Add delta triples from beyond last block, if any.
  AD_CORRECTNESS_CHECK(numInsAndDelPerBlock.size() > 0);
  auto numInsBeyondLastBlock = numInsAndDelPerBlock.back().first;
  if (numInsBeyondLastBlock > 0) {
    size_t blockIndex = endBlock - metadataForAllBlocks.begin();
    size_t numRowsWrittenToResult = locatedTriplesPerBlock.mergeTriples(
        blockIndex, std::nullopt, *result, offsetInResult, col0Id);
    AD_CORRECTNESS_CHECK(numRowsWrittenToResult == numInsBeyondLastBlock);
    spaceLeft -= numRowsWrittenToResult;
  }
  AD_CORRECTNESS_CHECK(spaceLeft == 0);
}

// _____________________________________________________________________________
void CompressedRelationReader::scan(
    const CompressedRelationMetadata& metadataForRelation, Id col1Id,
    const vector<CompressedBlockMetadata>& metadataForAllBlocks,
    ad_utility::File& file, IdTable* result,
    ad_utility::SharedConcurrentTimeoutTimer timer,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock) const {
  AD_CONTRACT_CHECK(result->numColumns() == 1);

  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  struct KeyLhs {
    Id _col0FirstId;
    Id _col0LastId;
    Id _col1FirstId;
    Id _col1LastId;
  };

  auto comp = [](const auto& a, const auto& b) {
    bool endBeforeBegin = a._col0LastId < b._col0FirstId;
    endBeforeBegin |=
        (a._col0LastId == b._col0FirstId && a._col1LastId < b._col1FirstId);
    return endBeforeBegin;
  };

  Id col0Id = metadataForRelation._col0Id;

  // Note: See the comment in the other overload for `scan` above for the
  // reason why we (currently) can't use a structured binding here.
  decltype(metadataForAllBlocks.begin()) beginBlock, endBlock;
  std::tie(beginBlock, endBlock) =
      std::equal_range(metadataForAllBlocks.begin(), metadataForAllBlocks.end(),
                       KeyLhs{col0Id, col0Id, col1Id, col1Id}, comp);

  // Compute the number of inserted and deleted triples per block and overall.
  // note the `<=` so that we don't forget the block beyond the last (which
  // may have information about delta triples at the vey end of a relation).
  std::vector<std::pair<size_t, size_t>> numInsAndDelPerBlock;
  size_t numInsTotal = 0;
  size_t numDelTotal = 0;
  for (auto block = beginBlock; block <= endBlock; ++block) {
    size_t blockIndex = block - metadataForAllBlocks.begin();
    auto [numIns, numDel] =
        block == beginBlock || block == endBlock - 1 || block == endBlock
            ? locatedTriplesPerBlock.numTriples(blockIndex, col0Id, col1Id)
            : locatedTriplesPerBlock.numTriples(blockIndex);
    numInsTotal += numIns;
    numDelTotal += numDel;
    numInsAndDelPerBlock.push_back({numIns, numDel});
  }
  if (numInsTotal > 0 || numDelTotal > 0) {
    LOG(INFO) << "Index scan with delta triples: #inserts = " << numInsTotal
              << ", #deletes = " << numDelTotal
              << ", #blocks = " << (endBlock - beginBlock) << std::endl;
    AD_CORRECTNESS_CHECK(numDelTotal < metadataForRelation.getNofElements());
  }

  // Invariant: The col0Id is completely stored in a single block, or it is
  // contained in multiple blocks that only contain this col0Id,
  bool col0IdHasExclusiveBlocks = metadataForRelation._offsetInBlock ==
                                  std::numeric_limits<uint64_t>::max();
  if (!col0IdHasExclusiveBlocks) {
    // This might also be zero if no block was found at all.
    AD_CORRECTNESS_CHECK(endBlock - beginBlock <= 1);
  }

  // Helper class for a part of a block (needed for the first and last block
  // in the following). These are small objects, so an unlimited allocator is
  // OK.
  struct BlockPart {
    std::unique_ptr<IdTable> idTable = nullptr;
    size_t rowIndexBegin = 0;
    size_t rowIndexEnd = 0;
    size_t blockIndex = 0;
    std::pair<size_t, size_t> numInsAndDel;
    size_t size() const { return rowIndexEnd - rowIndexBegin; }
  };

  // Helper lambda that extracts the relevant `Id`s from the given
  // `blockMetadata` iterator. Returns the corresponding part and its (row
  // index) begin and end index in the original block.
  //
  // NOTE: This is used for the first and last block below because these may
  // contain triples that do not match `col0Id` and `col1Id`. We cannot
  // directly merge these into `result` because we first need to know its
  // total size and resize it before we can write to it.
  auto getBlockPart = [&](auto blockMetadata) -> BlockPart {
    DecompressedBlock block =
        readAndDecompressBlock(*blockMetadata, file, std::nullopt);

    // First find the range with matching `col0Id`. The `if` condition asks if
    // the relation is contained in a single block (this one).
    auto blockPartBegin = block.begin();
    auto blockPartEnd = block.end();
    if (metadataForRelation._offsetInBlock !=
        std::numeric_limits<uint64_t>::max()) {
      blockPartBegin += metadataForRelation._offsetInBlock;
      blockPartEnd = blockPartBegin + metadataForRelation._numRows;
      AD_CORRECTNESS_CHECK(blockPartBegin < block.end());
      AD_CORRECTNESS_CHECK(blockPartEnd <= block.end());
    }

    // Within that range find the subrange, where also `col1Id` matches.
    std::tie(blockPartBegin, blockPartEnd) = std::equal_range(
        blockPartBegin, blockPartEnd, std::array<Id, 1>{col1Id},
        [](const auto& x, const auto& y) { return x[0] < y[0]; });
    // std::cout << "Block part: ";
    // std::transform(blockPartBegin, blockPartEnd,
    //                std::ostream_iterator<std::string>(std::cout, " "),
    //                [](const auto& row) {
    //                  return absl::StrCat("{", row[0], " ", row[1], "}");
    //                });
    // std::cout << std::endl;

    // Variables for the index of this block and the range.
    //
    // TODO: `IndexTest.scanTest` failes if we check `rowIndexEnd >
    // rowIndexBegin` instead of just `>='. Can this really happen?
    size_t rowIndexBegin = blockPartBegin - block.begin();
    size_t rowIndexEnd = blockPartEnd - block.begin();
    AD_CORRECTNESS_CHECK(rowIndexBegin < block.size());
    AD_CORRECTNESS_CHECK(rowIndexEnd <= block.size());
    AD_CORRECTNESS_CHECK(rowIndexEnd >= rowIndexBegin);
    size_t blockIndex = blockMetadata - metadataForAllBlocks.begin();
    auto numInsAndDel = numInsAndDelPerBlock.at(blockMetadata - beginBlock);

    // Copy `block` to an `IdTable`.
    //
    // TODO: This is an unecessary copy. Extend the `IdTable` class so that we
    // can move the data from the second column of `block` to
    // `blockAsIdTable`.
    ad_utility::AllocatorWithLimit<Id> allocator{
        ad_utility::makeAllocationMemoryLeftThreadsafeObject(
            std::numeric_limits<size_t>::max())};
    IdTable result(1, allocator);
    result.resize(block.size());
    for (size_t i = 0; i < block.size(); ++i) {
      result(i, 0) = block(i, 1);
    }
    return {std::make_unique<IdTable>(std::move(result)), rowIndexBegin,
            rowIndexEnd, blockIndex, numInsAndDel};
  };

  // The first and the last block might be incomplete. We process them
  // separately from the complete blocks inbetween.
  BlockPart firstBlockPart;
  BlockPart lastBlockPart;
  auto completeBlocksBegin = beginBlock;
  auto completeBlocksEnd = endBlock;
  if (beginBlock < endBlock) {
    firstBlockPart = getBlockPart(beginBlock);
    ++completeBlocksBegin;
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan: ");
    }
  }
  if (completeBlocksBegin < endBlock) {
    --completeBlocksEnd;
    lastBlockPart = getBlockPart(completeBlocksEnd);
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan: ");
    }
  }

  // The total result size is the size of complete blocks plus the size of the
  // possibly incomplete blocks at the beginning and end, plus the number of
  // inserted triples minus the number of deleted triples.
  auto totalResultSize =
      std::accumulate(completeBlocksBegin, completeBlocksEnd, 0ul,
                      [](const auto& count, const auto& block) {
                        return count + block._numRows;
                      });
  totalResultSize += firstBlockPart.size() + lastBlockPart.size();
  AD_CORRECTNESS_CHECK(numDelTotal <= totalResultSize);
  totalResultSize += numInsTotal - numDelTotal;
  result->resize(totalResultSize);
  size_t spaceLeft = result->size();
  size_t offsetInResult = 0;

  // Helper lambda for processing the first or last block.
  //
  // NOTE: This should only be called once for a given `BlockPart` because the
  // `idTable` is moved away from it.
  auto processBlockPart = [&](BlockPart& blockPart) {
    if (blockPart.idTable) {
      size_t numRowsWrittenToResult = 0;
      // If there are no delta triples, copy directly to the result, otherwise
      // use (the slightly more expensive) `mergeTriples`.
      if (blockPart.numInsAndDel == std::pair<size_t, size_t>{0, 0}) {
        for (size_t i = 0; i < blockPart.size(); ++i) {
          (*result)(offsetInResult + i, 0) =
              (*blockPart.idTable)(blockPart.rowIndexBegin + i, 0);
        }
        numRowsWrittenToResult = blockPart.size();
      } else {
        numRowsWrittenToResult = locatedTriplesPerBlock.mergeTriples(
            blockPart.blockIndex, std::move(*(blockPart.idTable)), *result,
            offsetInResult, col0Id, col1Id, blockPart.rowIndexBegin,
            blockPart.rowIndexEnd);
      }
      // Check that `numRowsWrittenToResult` is as expected.
      {
        size_t expected = blockPart.size();
        AD_CORRECTNESS_CHECK(blockPart.numInsAndDel.second <= expected);
        expected += blockPart.numInsAndDel.first;
        expected -= blockPart.numInsAndDel.second;
        AD_CORRECTNESS_CHECK(numRowsWrittenToResult == expected);
      }
      AD_CORRECTNESS_CHECK(numRowsWrittenToResult <= spaceLeft);
      offsetInResult += numRowsWrittenToResult;
      spaceLeft -= numRowsWrittenToResult;
    }
  };

  // Process the first block part, then all the complete blocks, then the last
  // block part. The complete blocks are read sequentially from disk and then
  // (after we know their position in `result`) decompressed and merged into
  // `result` in parallel.
  processBlockPart(firstBlockPart);
  if (completeBlocksBegin < completeBlocksEnd) {
#pragma omp parallel
#pragma omp single
    for (auto block = completeBlocksBegin; block < completeBlocksEnd; ++block) {
      size_t blockIndex = block - metadataForAllBlocks.begin();
      auto numInsAndDel = numInsAndDelPerBlock.at(block - beginBlock);

      // Read the compressed block from disk (second column only).
      AD_CORRECTNESS_CHECK(block->_offsetsAndCompressedSize.size() == 2);
      CompressedBlock compressedBuffer =
          readCompressedBlockFromFile(*block, file, std::vector{1ul});

      // A lambda that owns the compressed block decompresses it to the
      // correct position in the result. It may safely be run in parallel
      auto decompressLambda = [&result, &block, &locatedTriplesPerBlock,
                               offsetInResult, numInsAndDel, blockIndex,
                               compressedBuffer =
                                   std::move(compressedBuffer)]() mutable {
        ad_utility::TimeBlockAndLog tbl{"Decompression a block"};

        decompressBlockToExistingIdTable(compressedBuffer, block->_numRows,
                                         *result, offsetInResult, numInsAndDel,
                                         locatedTriplesPerBlock, blockIndex);
      };

      // Register an OpenMP task that performs the decompression of this
      // block in parallel
#pragma omp task
      {
        if (!timer || !timer->wlock()->hasTimedOut()) {
          decompressLambda();
        }
      }

      // Update the counters.
      AD_CORRECTNESS_CHECK(numInsAndDel.second <= block->_numRows);
      size_t numRowsOfThisBlock =
          block->_numRows + numInsAndDel.first - numInsAndDel.second;
      AD_CORRECTNESS_CHECK(numRowsOfThisBlock <= spaceLeft);
      spaceLeft -= numRowsOfThisBlock;
      offsetInResult += numRowsOfThisBlock;
    }
    // End of omp parallel region, all blocks are decompressed now.
  }
  processBlockPart(lastBlockPart);

  // Add delta triples from beyond last block, if any.
  AD_CORRECTNESS_CHECK(numInsAndDelPerBlock.size() > 0);
  auto numInsBeyondLastBlock = numInsAndDelPerBlock.back().first;
  if (numInsBeyondLastBlock > 0) {
    size_t blockIndex = endBlock - metadataForAllBlocks.begin();
    size_t numRowsWrittenToResult = locatedTriplesPerBlock.mergeTriples(
        blockIndex, std::nullopt, *result, offsetInResult, col0Id, col1Id);
    AD_CORRECTNESS_CHECK(numRowsWrittenToResult == numInsBeyondLastBlock);
    spaceLeft -= numRowsWrittenToResult;
  }
  AD_CORRECTNESS_CHECK(spaceLeft == 0);
}

// _____________________________________________________________________________
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
void CompressedRelationWriter::addRelation(Id col0Id,
                                           const BufferedIdTable& col1And2Ids,
                                           size_t numDistinctCol1) {
  AD_CONTRACT_CHECK(!col1And2Ids.empty());
  float multC1 = computeMultiplicity(col1And2Ids.numRows(), numDistinctCol1);
  // Dummy value that will be overwritten later
  float multC2 = 42.42;
  // This sets everything except the _offsetInBlock, which will be set
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
      sizeInBytes(col1And2Ids) > 0.8 * static_cast<double>(_numBytesPerBlock);
  if (relationHasExclusiveBlocks ||
      sizeInBytes(col1And2Ids) + sizeInBytes(_buffer) >
          static_cast<double>(_numBytesPerBlock) * 1.5) {
    writeBufferedRelationsToSingleBlock();
  }

  if (relationHasExclusiveBlocks) {
    // The relation is large, immediately write the relation to a set of
    // exclusive blocks.
    writeRelationToExclusiveBlocks(col0Id, col1And2Ids);
    metaData._offsetInBlock = std::numeric_limits<uint64_t>::max();
  } else {
    // Append to the current buffered block.
    metaData._offsetInBlock = _buffer.numRows();
    static_assert(sizeof(col1And2Ids[0][0]) == sizeof(Id));
    if (_buffer.numRows() == 0) {
      _currentBlockData._col0FirstId = col0Id;
      _currentBlockData._col1FirstId = col1And2Ids(0, 0);
    }
    _currentBlockData._col0LastId = col0Id;
    _currentBlockData._col1LastId = col1And2Ids(col1And2Ids.numRows() - 1, 0);
    _currentBlockData._col2LastId = col1And2Ids(col1And2Ids.numRows() - 1, 1);
    AD_CORRECTNESS_CHECK(_buffer.numColumns() == col1And2Ids.numColumns());
    auto bufferOldSize = _buffer.numRows();
    _buffer.resize(_buffer.numRows() + col1And2Ids.numRows());
    for (size_t i = 0; i < col1And2Ids.numColumns(); ++i) {
      const auto& column = col1And2Ids.getColumn(i);
      std::ranges::copy(column, _buffer.getColumn(i).begin() + bufferOldSize);
    }
  }
  _metaDataBuffer.push_back(metaData);
}

// _____________________________________________________________________________
void CompressedRelationWriter::writeRelationToExclusiveBlocks(
    Id col0Id, const BufferedIdTable& data) {
  const size_t numRowsPerBlock = _numBytesPerBlock / (NumColumns * sizeof(Id));
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

    _blockBuffer.push_back(CompressedBlockMetadata{
        std::move(offsets), actualNumRowsPerBlock, col0Id, col0Id, data[i][0],
        data[i + actualNumRowsPerBlock - 1][0],
        data[i + actualNumRowsPerBlock - 1][1]});
  }
}

// ___________________________________________________________________________
void CompressedRelationWriter::writeBufferedRelationsToSingleBlock() {
  if (_buffer.empty()) {
    return;
  }

  AD_CORRECTNESS_CHECK(_buffer.numColumns() == NumColumns);
  // Convert from bytes to number of ID pairs.
  size_t numRows = _buffer.numRows();

  // TODO<joka921, C++23> This is
  // `ranges::to<vector>(ranges::transform_view(_buffer.getColumns(),
  // compressAndWriteColumn))`;
  std::ranges::for_each(_buffer.getColumns(),
                        [this](const auto& column) mutable {
                          _currentBlockData._offsetsAndCompressedSize.push_back(
                              compressAndWriteColumn(column));
                        });

  _currentBlockData._numRows = numRows;
  // The `firstId` and `lastId` of `_currentBlockData` were already set
  // correctly by `addRelation()`.
  _blockBuffer.push_back(_currentBlockData);
  // Reset the data of the current block.
  _currentBlockData = CompressedBlockMetadata{};
  _buffer.clear();
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
        blockMetaData._offsetsAndCompressedSize.at(columnIndices->at(i));
    auto& currentCol = compressedBuffer[i];
    currentCol.resize(offset._compressedSize);
    file.read(currentCol.data(), offset._compressedSize, offset._offsetInFile);
  }
  return compressedBuffer;
}

// ____________________________________________________________________________
DecompressedBlock CompressedRelationReader::decompressBlock(
    const CompressedBlock& compressedBlock, size_t numRowsToRead) {
  DecompressedBlock decompressedBlock{compressedBlock.size()};
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
    IdTable& result, size_t offsetInResult,
    std::pair<size_t, size_t> numInsAndDel,
    const LocatedTriplesPerBlock& locatedTriplesPerBlock, size_t blockIndex) {
  // Check that the given arguments are consistent (they should always be,
  // given that this method is `private`). LOG(INFO) << "numRowsToRead: " <<
  // numRowsToRead << std::endl; LOG(INFO) << "numInsAndDel.first: " <<
  // numInsAndDel.first << std::endl; LOG(INFO) << "numInsAndDel.second: " <<
  // numInsAndDel.second << std::endl;
  AD_CORRECTNESS_CHECK(numInsAndDel.second <= numRowsToRead);
  AD_CORRECTNESS_CHECK(result.numRows() + numInsAndDel.second >=
                       offsetInResult + numRowsToRead + numInsAndDel.first);
  AD_CORRECTNESS_CHECK(compressedBlock.size() == result.numColumns());

  // Helper lambda that decompresses `numRowsToRead` from `compressedBlock`
  // to the given `IdTable` iterator.
  //
  // TODO: It would be more natural to pass an `IdTable::iterator` here, but
  // it seems that we can't get from that an iterator into an `IdTable`
  // column.
  //
  // TODO<joka921, C++23> use zip_view.
  auto decompressToIdTable = [&compressedBlock, &numRowsToRead](
                                 IdTable& idTable, size_t offsetInIdTable) {
    size_t numColumns = compressedBlock.size();
    for (size_t i = 0; i < numColumns; ++i) {
      const auto& columnFromBlock = compressedBlock[i];
      auto columnFromIdTable = idTable.getColumn(i);
      decompressColumn(columnFromBlock, numRowsToRead,
                       columnFromIdTable.data() + offsetInIdTable);
    }
  };

  // If there are no delta triples for this block, just decompress directly to
  // the `result` table. Otherwise decompress to an intermediate table and
  // merge from there to `result`.
  //
  // TODO: In the second case, we use an unlimited allocator for the space
  // allocation for the intermediate table. This looks OK because our blocks
  // are small, but it might be better to allocate also this table from the
  // memory pool available to the server (to which we don't have acces here).
  if (numInsAndDel == std::pair<size_t, size_t>{0, 0}) {
    decompressToIdTable(result, offsetInResult);
  } else {
    ad_utility::AllocatorWithLimit<Id> allocator{
        ad_utility::makeAllocationMemoryLeftThreadsafeObject(
            std::numeric_limits<size_t>::max())};
    IdTable blockAsIdTable(compressedBlock.size(), allocator);
    blockAsIdTable.resize(numRowsToRead);
    decompressToIdTable(blockAsIdTable, 0);
    locatedTriplesPerBlock.mergeTriples(blockIndex, std::move(blockAsIdTable),
                                        result, offsetInResult);
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
    std::optional<std::vector<size_t>> columnIndices) {
  CompressedBlock compressedColumns = readCompressedBlockFromFile(
      blockMetaData, file, std::move(columnIndices));
  const auto numRowsToRead = blockMetaData._numRows;
  return decompressBlock(compressedColumns, numRowsToRead);
}

// _____________________________________________________________________________
CompressedBlockMetadata::OffsetAndCompressedSize
CompressedRelationWriter::compressAndWriteColumn(std::span<const Id> column) {
  std::vector<char> compressedBlock = ZstdWrapper::compress(
      (void*)(column.data()), column.size() * sizeof(column[0]));
  auto offsetInFile = _outfile.tell();
  auto compressedSize = compressedBlock.size();
  _outfile.write(compressedBlock.data(), compressedBlock.size());
  return {offsetInFile, compressedSize};
};
