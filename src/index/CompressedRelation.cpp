// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include "CompressedRelation.h"

#include "engine/idTable/IdTable.h"
#include "index/Permutations.h"
#include "util/Cache.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/ConcurrentCache.h"
#include "util/Generator.h"
#include "util/TypeTraits.h"

using namespace std::chrono_literals;

// This cache stores a small number of decompressed blocks. Its only purpose
// currently is to make the e2e-tests run fast. They contain many Sparql queries
// with ?s ?p ?o triples in the body.
// TODO<joka921> Improve the performance of these triples also for large
// knowledge bases.
auto& globalBlockCache() {
  static ad_utility::ConcurrentCache<
      ad_utility::HeapBasedLRUCache<std::string, DecompressedBlock>>
      globalCache(20ul);
  return globalCache;
}

// ____________________________________________________________________________
void CompressedRelationMetadata::scan(
    const CompressedRelationMetadata& metadata,
    const vector<CompressedBlockMetadata>& blockMetadata,
    const std::string& permutationName, ad_utility::File& file, IdTable* result,
    ad_utility::SharedConcurrentTimeoutTimer timer) {
  AD_CHECK(result->numColumns() == NumColumns);

  // get all the blocks where _col0FirstId <= col0Id <= _col0LastId
  struct KeyLhs {
    Id _col0FirstId;
    Id _col0LastId;
  };
  Id col0Id = metadata._col0Id;
  // TODO<joka921, Clang16> Use a structured binding. Structured bindings are
  // currently not supported by clang when using OpenMP because clang internally
  // transforms the `#pragma`s into lambdas, and capturing structured bindings
  // is only supported in clang >= 16.
  decltype(blockMetadata.begin()) beginBlock, endBlock;
  std::tie(beginBlock, endBlock) = std::equal_range(
      // TODO<joka921> For some reason we can't use `std::ranges::equal_range`,
      // find out why. Note: possibly it has something to do with the limited
      // support of ranges in clang with versions < 16. Revisit this when
      // we use clang 16.
      blockMetadata.begin(), blockMetadata.end(), KeyLhs{col0Id, col0Id},
      [](const auto& a, const auto& b) {
        return a._col0FirstId < b._col0FirstId && a._col0LastId < b._col0LastId;
      });

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
      beginBlock < endBlock &&
      (beginBlock->_col0FirstId < col0Id || beginBlock->_col0LastId > col0Id);
  auto lastBlock = endBlock - 1;

  bool lastBlockIsIncomplete =
      beginBlock < lastBlock &&
      (lastBlock->_col0FirstId < col0Id || lastBlock->_col0LastId > col0Id);

  // Invariant: A relation spans multiple blocks exclusively or several
  // entities are stored completely in the same Block.
  AD_CHECK(!firstBlockIsIncomplete || (beginBlock == lastBlock));
  AD_CHECK(!lastBlockIsIncomplete);
  if (firstBlockIsIncomplete) {
    AD_CHECK(metadata._offsetInBlock != std::numeric_limits<uint64_t>::max());
  }

  // We have at most one block that is incomplete and thus requires trimming.
  // Set up a lambda, that reads this block and decompresses it to
  // the result.
  auto readIncompleteBlock = [&](const auto& block) {
    auto cacheKey =
        permutationName +
        std::to_string(block._offsetsAndCompressedSize.at(0)._offsetInFile);

    auto uncompressedBuffer = globalBlockCache()
                                  .computeOnce(cacheKey,
                                               [&]() {
                                                 return readAndDecompressBlock(
                                                     block, file, std::nullopt);
                                               })
                                  ._resultPointer;

    // Extract the part of the block that actually belongs to the relation
    auto numElements = metadata._numRows;
    AD_CHECK(uncompressedBuffer->numColumns() == numColumns());
    for (size_t i = 0; i < uncompressedBuffer->numColumns(); ++i) {
      const auto& inputCol = uncompressedBuffer->getColumn(i);
      auto begin = inputCol.begin() + metadata._offsetInBlock;
      auto resultColumn = result->getColumn(i);
      AD_CHECK(numElements <= spaceLeft);
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
          ad_utility::TimeBlockAndLog("Decompressing a block");

          decompressBlockToExistingIdTable(compressedBuffer, block._numRows,
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
        spaceLeft -= block._numRows;
        rowIndexOfNextBlock += block._numRows;
      }
      AD_CHECK(spaceLeft == 0);
    }  // End of omp parallel region, all the decompression was handled now.
  }
}

// _____________________________________________________________________________
void CompressedRelationMetadata::scan(
    const CompressedRelationMetadata& metaData, Id col1Id,
    const vector<CompressedBlockMetadata>& blocks, ad_utility::File& file,
    IdTable* result, ad_utility::SharedConcurrentTimeoutTimer timer) {
  AD_CHECK(result->numColumns() == 1);

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

  Id col0Id = metaData._col0Id;

  // Note: See the comment in the other overload for `scan` above for the
  // reason why we (currently) can't use a structured binding here.
  decltype(blocks.begin()) beginBlock, endBlock;
  std::tie(beginBlock, endBlock) =
      std::equal_range(blocks.begin(), blocks.end(),
                       KeyLhs{col0Id, col0Id, col1Id, col1Id}, comp);

  // Invariant: The col0Id is completely stored in a single block, or it is
  // contained in multiple blocks that only contain this col0Id,
  bool col0IdHasExclusiveBlocks =
      metaData._offsetInBlock == std::numeric_limits<uint64_t>::max();
  if (!col0IdHasExclusiveBlocks) {
    // This might also be zero if no block was found at all.
    AD_CHECK(endBlock - beginBlock <= 1);
  }

  // The first and the last block might be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the result as a vector.
  auto readPossiblyIncompleteBlock = [&](const auto& block) {
    DecompressedBlock uncompressedBuffer =
        readAndDecompressBlock(block, file, std::nullopt);
    AD_CHECK(uncompressedBuffer.numColumns() == 2);
    const auto& col1Column = uncompressedBuffer.getColumn(0);
    const auto& col2Column = uncompressedBuffer.getColumn(1);
    AD_CHECK(col1Column.size() == col2Column.size());

    // Find the range in the block, that belongs to the same relation `col0Id`
    bool containedInOnlyOneBlock =
        metaData._offsetInBlock != std::numeric_limits<uint64_t>::max();
    auto begin = col1Column.begin();
    if (containedInOnlyOneBlock) {
      begin += metaData._offsetInBlock;
    }
    auto end =
        containedInOnlyOneBlock ? begin + metaData._numRows : col1Column.end();

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
        return count + block._numRows;
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
      AD_CHECK(block._offsetsAndCompressedSize.size() == 2);
      CompressedBlock compressedBuffer =
          readCompressedBlockFromFile(block, file, std::vector{1ul});

      // A lambda that owns the compressed block decompresses it to the
      // correct position in the result. It may safely be run in parallel
      auto decompressLambda = [rowIndexOfNextBlockStart, &block, result,
                               compressedBuffer =
                                   std::move(compressedBuffer)]() mutable {
        ad_utility::TimeBlockAndLog("Decompression a block");

        decompressBlockToExistingIdTable(compressedBuffer, block._numRows,
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
      rowIndexOfNextBlockStart += block._numRows;
    }  // end of parallel region
  }
  // Add the last block.
  std::copy(lastBlockResult.begin(), lastBlockResult.end(),
            result->getColumn(0).data() + rowIndexOfNextBlockStart);
  AD_CHECK(rowIndexOfNextBlockStart + lastBlockResult.size() == result->size());
}

// _____________________________________________________________________________
float CompressedRelationWriter::computeMultiplicity(
    size_t numElements, size_t numDistinctElements) {
  bool functional = numElements == numDistinctElements;
  float multiplicity =
      functional ? 1.0f : numElements / float(numDistinctElements);
  // Ensure that the multiplicity is only exactly 1.0 if the relation is indeed
  // functional to prevent numerical instabilities;
  if (!functional && multiplicity == 1.0f) [[unlikely]] {
    multiplicity = std::nextafter(1.0f, 2.0f);
  }
  return multiplicity;
}

// ___________________________________________________________________________
void CompressedRelationWriter::addRelation(Id col0Id,
                                           const BufferedIdTable& col1And2Ids,
                                           size_t numDistinctCol1) {
  AD_CHECK(!col1And2Ids.empty());
  float multC1 = computeMultiplicity(col1And2Ids.numRows(), numDistinctCol1);
  // Dummy value that will be overwritten later
  float multC2 = 42.42;
  // This sets everything except the _offsetInBlock, which will be set
  // explicitly below.
  CompressedRelationMetadata metaData{col0Id, col1And2Ids.numRows(), multC1,
                                      multC2};
  auto sizeOfRelation =
      col1And2Ids.numRows() * col1And2Ids.numColumns() * sizeof(Id);

  // If this is a large relation, or the currrently buffered relations +
  // this relation are too large, we will write the buffered relations to file
  // and start a new block.
  if (sizeOfRelation > _numBytesPerBlock * 8 / 10 ||
      sizeOfRelation + _buffer.numRows() > 1.5 * _numBytesPerBlock) {
    writeBufferedRelationsToSingleBlock();
  }

  if (sizeOfRelation > _numBytesPerBlock * 8 / 10) {
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
    AD_CHECK(_buffer.numColumns() == col1And2Ids.numColumns());
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
  AD_CHECK(numRowsPerBlock > 0);
  AD_CHECK(data.numColumns() == NumColumns);
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
        data[i + actualNumRowsPerBlock - 1][0]});
  }
}

// ___________________________________________________________________________
void CompressedRelationWriter::writeBufferedRelationsToSingleBlock() {
  if (_buffer.empty()) {
    return;
  }

  AD_CHECK(_buffer.numColumns() == NumColumns);
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
CompressedBlock CompressedRelationMetadata::readCompressedBlockFromFile(
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
DecompressedBlock CompressedRelationMetadata::decompressBlock(
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
void CompressedRelationMetadata::decompressBlockToExistingIdTable(
    const CompressedBlock& compressedBlock, size_t numRowsToRead,
    IdTable& table, size_t offsetInTable) {
  AD_CHECK(table.numRows() >= offsetInTable + numRowsToRead);
  // TODO<joka921, C++23> use zip_view.
  AD_CHECK(compressedBlock.size() == table.numColumns());
  for (size_t i = 0; i < compressedBlock.size(); ++i) {
    auto col = table.getColumn(i);
    decompressColumn(compressedBlock[i], numRowsToRead,
                     col.data() + offsetInTable);
  }
}

// ____________________________________________________________________________
template <typename Iterator>
void CompressedRelationMetadata::decompressColumn(
    const std::vector<char>& compressedBlock, size_t numRowsToRead,
    Iterator iterator) {
  auto numBytesActuallyRead = ZstdWrapper::decompressToBuffer(
      compressedBlock.data(), compressedBlock.size(), iterator,
      numRowsToRead * sizeof(*iterator));
  static_assert(sizeof(Id) == sizeof(*iterator));
  AD_CHECK(numRowsToRead * sizeof(Id) == numBytesActuallyRead);
}

// _____________________________________________________________________________
DecompressedBlock CompressedRelationMetadata::readAndDecompressBlock(
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
