// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include "CompressedRelation.h"

#include "engine/idTable/IdTable.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Permutations.h"
#include "util/Cache.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/ConcurrentCache.h"
#include "util/TypeTraits.h"

using namespace std::chrono_literals;

// This cache stores a small number of decompressed blocks. Its only purpose
// currently is to make the e2e-tests run fast. They contain many Sparql queries
// with ?s ?p ?o triples in the body.
// TODO<joka921> Improve the performance of these triples also for large
// knowledge bases.
auto& globalBlockCache() {
  static ad_utility::ConcurrentCache<ad_utility::HeapBasedLRUCache<
      std::string, std::vector<std::array<Id, 2>>>>
      globalCache(20ul);
  return globalCache;
}

// ____________________________________________________________________________
template <class Permutation, typename IdTableImpl>
void CompressedRelationMetaData::scan(
    Id col0Id, IdTableImpl* result, const Permutation& permutation,
    ad_utility::SharedConcurrentTimeoutTimer timer) {
  if (!permutation._isLoaded) {
    throw std::runtime_error("This query requires the permutation " +
                             permutation._readableName +
                             ", which was not loaded");
  }
  if constexpr (!ad_utility::isVector<IdTableImpl>) {
    AD_CHECK(result->numColumns() == 2);
  }
  if (!permutation._meta.col0IdExists(col0Id)) {
    return;
  }
  const auto& metaData = permutation._meta.getMetaData(col0Id);

  // get all the blocks where _col0FirstId <= col0Id <= _col0LastId
  struct KeyLhs {
    Id _col0FirstId;
    Id _col0LastId;
  };
  auto [beginBlock, endBlock] = std::equal_range(
      permutation._meta.blockData().begin(),
      permutation._meta.blockData().end(), KeyLhs{col0Id, col0Id},
      [](const auto& a, const auto& b) {
        return a._col0FirstId < b._col0FirstId && a._col0LastId < b._col0LastId;
      });

  // The total size of the result is now known.
  result->resize(metaData.getNofElements());

  // The position in the result, to which the next block is being
  // decompressed.
  size_t currentIndex = 0;
  // auto* position = reinterpret_cast<std::array<Id, 2>*>(result->data());

  // The number of Id Pairs for which we still have space
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
    AD_CHECK(metaData._offsetInBlock != std::numeric_limits<uint64_t>::max());
  }

  auto get = [result](size_t row, size_t col) -> Id& {
    if constexpr (requires { (*result)(0, 0); }) {
      return (*result)(row, col);
    } else {
      return (*result)[row][col];
    }
  };

  // We have at most one block that is incomplete and thus requires trimming.
  // Set up a lambda, that reads this block and decompresses it to
  // the result.
  auto readIncompleteBlock = [&](const auto& block) {
    auto cacheKey =
        permutation._readableName + std::to_string(block._offsetInFile);

    auto uncompressedBuffer =
        globalBlockCache()
            .computeOnce(
                cacheKey,
                [&]() { return readAndDecompressBlock(block, permutation); })
            ._resultPointer;

    // Extract the part of the block that actually belongs to the relation
    auto begin = uncompressedBuffer->begin() + metaData._offsetInBlock;
    auto end = begin + metaData._numRows;
    auto numElements = static_cast<size_t>(end - begin);
    AD_CHECK(numElements <= spaceLeft);
    for (size_t i = 0; i < numElements; ++i) {
      get(currentIndex + i, 0) = (*(begin + i))[0];
      get(currentIndex + i, 1) = (*(begin + i))[1];
    }
    currentIndex += numElements;
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

        std::vector<char> compressedBuffer =
            readCompressedBlockFromFile(block, permutation);

        // This lambda decompresses the block that was just read to the
        // correct position in the result.
        auto decompressLambda = [&get, currentIndex, &block,
                                 compressedBuffer =
                                     std::move(compressedBuffer)]() {
          ad_utility::TimeBlockAndLog("Decompressing a block");
          std::vector<std::array<Id, 2>> buffer;
          buffer.resize(block._numRows);
          decompressBlock(compressedBuffer, block._numRows, buffer.data());
          for (size_t i = 0; i < block._numRows; ++i) {
            get(currentIndex + i, 0) = buffer[i][0];
            get(currentIndex + i, 1) = buffer[i][1];
          }
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
        currentIndex += block._numRows;
      }
      AD_CHECK(spaceLeft == 0);
    }  // End of omp parallel region, all the decompression was handled now.
  }
}

// Explicit instantiations for all six permutations
template void CompressedRelationMetaData::scan<Permutation::POS_T, IdTable>(
    Id key, IdTable* result, const Permutation::POS_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::PSO_T, IdTable>(
    Id key, IdTable* result, const Permutation::PSO_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::SPO_T, IdTable>(
    Id key, IdTable* result, const Permutation::SPO_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::SOP_T, IdTable>(
    Id key, IdTable* result, const Permutation::SOP_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::OPS_T, IdTable>(
    Id key, IdTable* result, const Permutation::OPS_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::OSP_T, IdTable>(
    Id key, IdTable* result, const Permutation::OSP_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);

using V = std::vector<std::array<Id, 2>,
                      ad_utility::AllocatorWithLimit<std::array<Id, 2>>>;
template void CompressedRelationMetaData::scan<Permutation::POS_T, V>(
    Id key, V* result, const Permutation::POS_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::PSO_T, V>(
    Id key, V* result, const Permutation::PSO_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::SPO_T, V>(
    Id key, V* result, const Permutation::SPO_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::SOP_T, V>(
    Id key, V* result, const Permutation::SOP_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::OPS_T, V>(
    Id key, V* result, const Permutation::OPS_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::OSP_T, V>(
    Id key, V* result, const Permutation::OSP_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);

using V2 = std::vector<std::array<Id, 2>>;
template void CompressedRelationMetaData::scan<Permutation::POS_T, V2>(
    Id key, V2* result, const Permutation::POS_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::PSO_T, V2>(
    Id key, V2* result, const Permutation::PSO_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::SPO_T, V2>(
    Id key, V2* result, const Permutation::SPO_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::SOP_T, V2>(
    Id key, V2* result, const Permutation::SOP_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::OPS_T, V2>(
    Id key, V2* result, const Permutation::OPS_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);
template void CompressedRelationMetaData::scan<Permutation::OSP_T, V2>(
    Id key, V2* result, const Permutation::OSP_T& p,
    ad_utility::SharedConcurrentTimeoutTimer timer);

// _____________________________________________________________________________
template <class Permutation, typename IdTableImpl>
void CompressedRelationMetaData::scan(
    const Id col0Id, const Id& col1Id, IdTableImpl* result,
    const Permutation& permutation,
    ad_utility::SharedConcurrentTimeoutTimer timer) {
  AD_CHECK(result->numColumns() == 1);

  if (!permutation._meta.col0IdExists(col0Id)) {
    return;
  }
  const auto& metaData = permutation._meta.getMetaData(col0Id);

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

  auto [beginBlock, endBlock] =
      std::equal_range(permutation._meta.blockData().begin(),
                       permutation._meta.blockData().end(),
                       KeyLhs{col0Id, col0Id, col1Id, col1Id}, comp);

  // Invariant: The col0Id is completely stored in a single block, or it is
  // contained in multiple blocks that only contain this col0Id,
  bool col0IdHasExclusiveBlocks =
      metaData._offsetInBlock == std::numeric_limits<uint64_t>::max();
  if (!col0IdHasExclusiveBlocks) {
    // This might also be zero if no block was found at all.
    AD_CHECK(endBlock - beginBlock <= 1);
  }

  // The first and the last block might possibly be incomplete (that is, only
  // a part of these blocks is actually part of the result,
  // set up a lambda which allows us to read these blocks, and returns
  // the result as a vector.
  auto readPossiblyIncompleteBlock = [&](const auto& block) {
    std::vector<std::array<Id, 2>> uncompressedBuffer =
        readAndDecompressBlock(block, permutation);

    // Find the range in the block, that belongs to the same relation `col0Id`
    bool containedInOnlyOneBlock =
        metaData._offsetInBlock != std::numeric_limits<uint64_t>::max();
    auto begin = uncompressedBuffer.begin();
    if (containedInOnlyOneBlock) {
      begin += metaData._offsetInBlock;
    }
    auto end = containedInOnlyOneBlock ? begin + metaData._numRows
                                       : uncompressedBuffer.end();

    // Find the range in the block, where also the col1Id matches (the second
    // ID in the `std::array` does not matter).
    std::tie(begin, end) = std::equal_range(
        begin, end, std::array<Id, 2>{col1Id, Id{}},
        [](const auto& a, const auto& b) { return a[0] < b[0]; });

    // Extract the one column result from the two column block.
    std::vector<Id> result;
    result.reserve(end - begin);
    std::for_each(begin, end, [&](const auto& p) { result.push_back(p[1]); });
    return result;
  };

  // The first and the last block might possibly be incomplete, compute
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
  // Only one column, so there is only one Vector
  Id* position = &(*result)(0, 0);
  size_t spaceLeft = result->size();

  // Insert the first block into the result;
  position =
      std::copy(firstBlockResult.begin(), firstBlockResult.end(), position);
  spaceLeft -= firstBlockResult.size();

  // Insert the complete blocks from the middle in parallel
  if (beginBlock < endBlock) {
#pragma omp parallel
#pragma omp single
    for (; beginBlock < endBlock; ++beginBlock) {
      const auto& block = *beginBlock;

      // Read the block serially
      std::vector<char> compressedBuffer =
          readCompressedBlockFromFile(block, permutation);

      // A lambda that owns the compressed block decompresses it to the
      // correct position in the result. It may safely be run in parallel
      auto decompressLambda = [position, &block,
                               compressedBuffer =
                                   std::move(compressedBuffer)]() mutable {
        ad_utility::TimeBlockAndLog("Decompression a block");
        std::vector<std::array<Id, 2>> uncompressedBuffer =
            decompressBlock(compressedBuffer, block._numRows);

        // Extract the single result column from the two column block;
        std::for_each(uncompressedBuffer.begin(), uncompressedBuffer.end(),
                      [&](const auto& p) { *position++ = p[1]; });
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
      spaceLeft -= block._numRows;
      position += block._numRows;
    }  // end of parallel region
  }
  // Add the last block.
  std::copy(lastBlockResult.begin(), lastBlockResult.end(), position);
  spaceLeft -= lastBlockResult.size();
  AD_CHECK(spaceLeft == 0);
}

// Explicit instantiations for all six permutations
template void CompressedRelationMetaData::scan<Permutation::POS_T, IdTable>(
    const Id, const Id&, IdTable*, const Permutation::POS_T&,
    ad_utility::SharedConcurrentTimeoutTimer);
template void CompressedRelationMetaData::scan<Permutation::PSO_T, IdTable>(
    const Id, const Id&, IdTable*, const Permutation::PSO_T&,
    ad_utility::SharedConcurrentTimeoutTimer);
template void CompressedRelationMetaData::scan<Permutation::SOP_T, IdTable>(
    const Id, const Id&, IdTable*, const Permutation::SOP_T&,
    ad_utility::SharedConcurrentTimeoutTimer);
template void CompressedRelationMetaData::scan<Permutation::SPO_T, IdTable>(
    const Id, const Id&, IdTable*, const Permutation::SPO_T&,
    ad_utility::SharedConcurrentTimeoutTimer);
template void CompressedRelationMetaData::scan<Permutation::OPS_T, IdTable>(
    const Id, const Id&, IdTable*, const Permutation::OPS_T&,
    ad_utility::SharedConcurrentTimeoutTimer);
template void CompressedRelationMetaData::scan<Permutation::OSP_T, IdTable>(
    const Id, const Id&, IdTable*, const Permutation::OSP_T&,
    ad_utility::SharedConcurrentTimeoutTimer);

// ___________________________________________________________________________
void CompressedRelationWriter::addRelation(
    Id col0Id, const ad_utility::BufferedVector<std::array<Id, 2>>& data,
    size_t numDistinctCol1, bool functional) {
  LOG(TRACE) << "Writing a relation ...\n";
  AD_CHECK_GT(data.size(), 0);
  LOG(TRACE) << "Calculating multiplicities ...\n";
  float multC1 = functional ? 1.0 : data.size() / float(numDistinctCol1);
  // Dummy value that will be overwritten later
  float multC2 = 42.42;
  LOG(TRACE) << "Done calculating multiplicities.\n";
  // This sets everything except the _offsetInBlock, which will be set
  // explicitly below.
  CompressedRelationMetaData metaData{col0Id, data.size(), multC1, multC2};
  auto sizeOfRelation = data.size() * 2 * sizeof(Id);

  // If this is a large relation, or the currrently buffered relations +
  // this relation are too large, we will write the buffered relations to file
  // and start a new block.
  if (sizeOfRelation > BLOCKSIZE_COMPRESSED_METADATA * 8 / 10 ||
      sizeOfRelation + _buffer.data().size() >
          1.5 * BLOCKSIZE_COMPRESSED_METADATA) {
    writeBufferedRelationsToSingleBlock();
  }

  if (sizeOfRelation > BLOCKSIZE_COMPRESSED_METADATA * 8 / 10) {
    // The relation is large, immediately write the relation to a set of
    // exclusive blocks.
    writeRelationToExclusiveBlocks(col0Id, data);
    metaData._offsetInBlock = std::numeric_limits<uint64_t>::max();
  } else {
    // Append to the current buffered block.
    metaData._offsetInBlock = _buffer.data().size() / (2 * sizeof(Id));
    static_assert(sizeof(data[0]) == 2 * sizeof(Id));
    if (_buffer.data().empty()) {
      _currentBlockData._col0FirstId = col0Id;
      _currentBlockData._col1FirstId = data.front()[0];
    }
    _currentBlockData._col0LastId = col0Id;
    _currentBlockData._col1LastId = data.back()[0];
    _buffer.serializeBytes(reinterpret_cast<const char*>(data.data()),
                           data.size() * 2 * sizeof(Id));
  }
  _metaDataBuffer.push_back(metaData);
}

// _____________________________________________________________________________
void CompressedRelationWriter::writeRelationToExclusiveBlocks(
    Id col0Id, const ad_utility::BufferedVector<std::array<Id, 2>>& data) {
  constexpr size_t NUM_ROWS_PER_BLOCK =
      BLOCKSIZE_COMPRESSED_METADATA / (2 * sizeof(Id));
  for (size_t i = 0; i < data.size(); i += NUM_ROWS_PER_BLOCK) {
    size_t actualNumRowsPerBlock =
        std::min(NUM_ROWS_PER_BLOCK, size_t(data.size() - i));

    std::vector<char> compressedBlock = ZstdWrapper::compress(
        (void*)(data.data() + i), actualNumRowsPerBlock * 2 * sizeof(Id));
    _blockBuffer.push_back(CompressedBlockMetaData{
        _outfile.tell(), compressedBlock.size(), actualNumRowsPerBlock, col0Id,
        col0Id, data[i][0], data[i + actualNumRowsPerBlock - 1][0]});
    _outfile.write(compressedBlock.data(), compressedBlock.size());
  }
  LOG(TRACE) << "Done writing relation.\n";
}

// ___________________________________________________________________________
void CompressedRelationWriter::writeBufferedRelationsToSingleBlock() {
  if (_buffer.data().empty()) {
    return;
  }

  auto bytesFromBuffer = std::move(_buffer).data();
  _buffer.clear();

  // Convert from bytes to number of Id pairs.
  size_t numRows = bytesFromBuffer.size() / (2 * sizeof(Id));

  std::vector<char> compressedBlock = ZstdWrapper::compress(
      (void*)(bytesFromBuffer.data()), bytesFromBuffer.size());
  _currentBlockData._offsetInFile = _outfile.tell();
  _currentBlockData._compressedSize = compressedBlock.size();
  _currentBlockData._numRows = numRows;
  // The `firstId` and `lastId` of `_currentBlockData` were already set
  // correctly by `addRelation()`.
  _blockBuffer.push_back(_currentBlockData);
  _outfile.write(compressedBlock.data(), compressedBlock.size());
  LOG(TRACE) << "Done writing relation.\n";
}

// _____________________________________________________________________________
template <class Permutation>
std::vector<char> CompressedRelationMetaData::readCompressedBlockFromFile(
    const CompressedBlockMetaData& block, const Permutation& permutation) {
  std::vector<char> compressedBuffer;
  compressedBuffer.resize(block._compressedSize);
  permutation._file.read(compressedBuffer.data(), block._compressedSize,
                         block._offsetInFile);
  return compressedBuffer;
}

// ____________________________________________________________________________
std::vector<std::array<Id, 2>> CompressedRelationMetaData::decompressBlock(
    const std::vector<char>& compressedBlock, size_t numRowsToRead) {
  std::vector<std::array<Id, 2>> uncompressedBuffer;
  uncompressedBuffer.resize(numRowsToRead);
  decompressBlock(compressedBlock, numRowsToRead, uncompressedBuffer.data());
  return uncompressedBuffer;
}

// ____________________________________________________________________________
template <typename Iterator>
void CompressedRelationMetaData::decompressBlock(
    const std::vector<char>& compressedBlock, size_t numRowsToRead,
    Iterator iterator) {
  auto numBytesActuallyRead = ZstdWrapper::decompressToBuffer(
      compressedBlock.data(), compressedBlock.size(), iterator,
      numRowsToRead * sizeof(*iterator));
  static_assert(2 * sizeof(Id) == sizeof(*iterator));
  AD_CHECK(numRowsToRead * 2 * sizeof(Id) == numBytesActuallyRead);
}

// _____________________________________________________________________________
template <class Permutation>
std::vector<std::array<Id, 2>>
CompressedRelationMetaData::readAndDecompressBlock(
    const CompressedBlockMetaData& block, const Permutation& permutation) {
  auto compressed = readCompressedBlockFromFile(block, permutation);
  auto numRowsToRead = block._numRows;
  return decompressBlock(compressed, numRowsToRead);
}
