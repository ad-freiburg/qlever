// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/ParallelBlockMergeTestHelpers.h"
#include "../util/SpillingBlockStorageTestHelpers.h"
#include "backports/filesystem.h"
#include "util/CompressedBlockFile.h"
#include "util/File.h"
#include "util/Random.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"
#include "util/parallelBlockMerge/SpillingBlockStorage.h"

// The tests of the generic `SpillingBlockStorage`, run with a simple codec for
// blocks of strings. The storage for `IdTable`s (and hence most of the
// behavior of the storage) is tested extensively in
// `test/engine/idTable/CompressedIdTableBlockStorageTest.cpp`, and the codec of
// the vocabulary merger in
// `test/index/vocabulary_merger/QueueWordBlockCodecTest.cpp`.
using namespace ad_utility::parallelBlockMerge;
using namespace parallelBlockMergeTestHelpers;
using namespace spillingBlockStorageTestHelpers;
namespace net = boost::asio;

namespace {
// A `SpillingBlockCodec` for blocks of strings, which stores a whole block as a
// single block of the `CompressedBlockFile`.
struct StringBlockCodec {
  using Block = std::vector<std::string>;
  using BlockMetadata = ad_utility::CompressedBlockFile::BlockMetadata;

  // ___________________________________________________________________________
  BlockMetadata writeBlock(ad_utility::CompressedBlockFile& file,
                           const Block& block) const {
    ad_utility::serialization::ByteBufferWriteSerializer serializer;
    serializer << block;
    const auto& bytes = serializer.data();
    return file.appendBlock(bytes.data(), bytes.size());
  }

  // ___________________________________________________________________________
  Block readBlock(const ad_utility::CompressedBlockFile& file,
                  const BlockMetadata& metadata) const {
    std::vector<char> bytes(metadata.uncompressedSize_);
    file.readBlock(metadata, bytes.data());
    ad_utility::serialization::ByteBufferReadSerializer serializer{
        std::move(bytes)};
    Block block;
    serializer >> block;
    return block;
  }
};
static_assert(SpillingBlockCodec<StringBlockCodec>);

using StringStorage = SpillingBlockStorage<StringBlockCodec>;
static_assert(BlockStorageConcept<StringStorage, std::vector<std::string>>);

// Return the blocks of the given `readBlocks` and whether each of them was
// still in memory, as two separate vectors.
std::pair<std::vector<std::vector<std::string>>, std::vector<bool>> split(
    std::vector<ReadBlock<std::vector<std::string>>> readBlocks) {
  std::vector<std::vector<std::string>> blocks;
  std::vector<bool> wasInMemory;
  for (auto& readBlock : readBlocks) {
    blocks.push_back(std::move(readBlock.block_));
    wasInMemory.push_back(readBlock.wasInMemory_);
  }
  return {std::move(blocks), std::move(wasInMemory)};
}
}  // namespace

// _____________________________________________________________________________
TEST(SpillingBlockStorage, roundTripOfSpilledAndBufferedBlocks) {
  std::vector<std::vector<std::string>> blocks{
      {"alpha", "", "gamma"},
      {std::string(100'000, 'x')},
      {},
      {"single"},
  };
  for (auto compressionLevel :
       {ad_utility::CompressedBlockFile::CompressionLevel{
            ad_utility::ZSTD_DEFAULT_LEVEL},
        ad_utility::NO_BLOCK_COMPRESSION}) {
    std::string prefix = gtestCurrentTestName();
    net::io_context ioContext;
    StringStorage storage{ioContext.get_executor(), prefix, StringBlockCodec{},
                          1, compressionLevel};
    // NOTE: The storage deletes the spill file of a chunk as soon as that
    // chunk is done, this cleanup is only a safeguard for a failing test.
    absl::Cleanup cleanup{[&storage] {
      for (size_t chunkIndex : {0u, 1u}) {
        ad_utility::deleteFile(storage.spillFilename(chunkIndex), false);
      }
    }};
    // The consumer reads chunk `0`, so that chunk keeps a single block in
    // memory and spills the rest, whereas chunk `1` spills every block.
    storeChunk(ioContext, storage, 0, blocks);
    storeChunk(ioContext, storage, 1, blocks);
    EXPECT_TRUE(ql::filesystem::exists(storage.spillFilename(1)));
    auto [chunkZero, chunkZeroInMemory] =
        split(readChunk(ioContext, storage, 0));
    EXPECT_THAT(chunkZero, ::testing::ElementsAreArray(blocks));
    EXPECT_THAT(chunkZeroInMemory,
                ::testing::ElementsAre(true, false, false, false));
    auto [chunkOne, chunkOneInMemory] = split(readChunk(ioContext, storage, 1));
    EXPECT_THAT(chunkOne, ::testing::ElementsAreArray(blocks));
    EXPECT_THAT(chunkOneInMemory, ::testing::Each(false));
    // A chunk that is done deletes its spill file.
    ad_utility::testing::pollUntilQuiescent(ioContext);
    EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(0)));
    EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(1)));
  }
}

// _____________________________________________________________________________
TEST(SpillingBlockStorage, cancelAllWakesUpAWaitingConsumer) {
  net::io_context ioContext;
  StringStorage storage{ioContext.get_executor(), gtestCurrentTestName(),
                        StringBlockCodec{}, 1};
  expectCancelAllWakesUpAWaitingConsumer(ioContext, storage);
}

// _____________________________________________________________________________
TEST(SpillingBlockStorage, parallelMergeOfLazyBlocksWithSpilling) {
  // Random sorted runs of strings, which are merged in parallel from lazy
  // input blocks into a spilling storage.
  ad_utility::SlowRandomIntGenerator<size_t> lengthGenerator{0, 30};
  ad_utility::FastRandomIntGenerator<uint64_t> charGenerator;
  std::vector<std::vector<std::string>> runs(8);
  for (auto& run : runs) {
    run.resize(300);
    for (auto& word : run) {
      size_t length = lengthGenerator();
      for (size_t i = 0; i < length; ++i) {
        word.push_back(static_cast<char>('a' + charGenerator() % 26));
      }
    }
    ql::ranges::sort(run);
  }
  auto expected = concatenation(runs);
  ql::ranges::sort(expected);

  for (size_t maxBufferedBlocksPerChunk : {0u, 2u}) {
    std::string prefix = gtestCurrentTestName();
    MergeOptions options = optionsWithBlockSize(16);
    options.parallelismHint = 4;
    options.targetChunksPerThread = 3;
    // Force the parallel code path also for this small input.
    options.serialNumElementsThreshold = 0;
    net::thread_pool pool{4};
    // All the coroutines have to finish before the pool is destroyed, and only
    // after the range is destroyed, hence the cleanup before the range.
    absl::Cleanup joinPool = [&pool] { pool.join(); };
    std::vector<std::string> result;
    {
      auto blocks = parallelBlockMergeToRange</*moveElements=*/true>(
          pool.get_executor(), makeLazyVectorInput(runs, 5), std::less<>{},
          makeSpillingBlockStorageFactory(pool.get_executor(), prefix,
                                          StringBlockCodec{},
                                          maxBufferedBlocksPerChunk),
          options);
      for (auto& block : blocks) {
        EXPECT_FALSE(block.empty());
        for (auto& word : block) {
          result.push_back(std::move(word));
        }
      }
    }
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
    // Every chunk deletes its spill file once it is done. The deletion is
    // posted to the pool, so it is only guaranteed to be done once the pool
    // has been joined.
    pool.join();
    for (size_t chunkIndex = 0; chunkIndex < 100; ++chunkIndex) {
      EXPECT_FALSE(
          ql::filesystem::exists(absl::StrCat(prefix, ".", chunkIndex)));
    }
  }
}
