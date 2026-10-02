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

#include <atomic>
#include <cstdint>
#include <fstream>
#include <range/v3/view/cartesian_product.hpp>
#include <string>
#include <vector>

#include "./util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "util/CompressedBlockFile.h"
#include "util/File.h"
#include "util/Random.h"
#include "util/Views.h"
#include "util/jthread.h"

namespace {

using ad_utility::CompressedBlockFile;
using ad_utility::integerRange;
using ::ranges::views::cartesian_product;

// Create a deterministic but not trivially compressible sequence of
// `numBytes` bytes, seeded by `seed`.
std::vector<char> makeBytes(size_t numBytes, uint64_t seed) {
  ad_utility::FastRandomIntGenerator<char> generator{
      ad_utility::RandomSeed::make(seed)};
  std::vector<char> result;
  // Reserve one byte more than needed, such that `data()` is never `nullptr`,
  // also for the empty block.
  result.reserve(numBytes + 1);
  for ([[maybe_unused]] size_t i : integerRange(numBytes)) {
    result.push_back(generator());
  }
  return result;
}

// Read the block that is described by `metadata` from `file` and return the
// decompressed bytes.
std::vector<char> readBytes(
    const CompressedBlockFile& file,
    const CompressedBlockFile::BlockMetadata& metadata) {
  // Add a canary byte at the end, so that we also notice if `readBlock` writes
  // more bytes than it should.
  std::vector<char> result(metadata.uncompressedSize_ + 1, 'X');
  file.readBlock(metadata, result.data());
  EXPECT_EQ(result.back(), 'X');
  result.pop_back();
  return result;
}

// The raw bytes of the file with the given `filename`, straight from disk and
// without going through the `CompressedBlockFile`.
std::vector<char> rawFileContents(const std::string& filename) {
  std::ifstream stream{filename, std::ios::binary};
  EXPECT_TRUE(stream.good());
  return std::vector<char>{std::istreambuf_iterator<char>{stream},
                           std::istreambuf_iterator<char>{}};
}

// The blocks that a round trip has appended: the bytes that went in, and the
// metadata that `checkRoundTrip` has returned for them.
struct RoundTrip {
  std::vector<std::vector<char>> expected_;
  std::vector<CompressedBlockFile::BlockMetadata> metadata_;
};

// Append a fixed sequence of blocks to the `file`, read them all back, and
// check that they arrive unchanged and that the file is laid out as promised.
// Return what was appended, so that the caller can check more.
RoundTrip checkRoundTrip(CompressedBlockFile& file) {
  // The sizes deliberately include an empty block, and blocks that are much
  // larger and much smaller than each other.
  std::vector<size_t> sizes{17, 0, 1, 100'000, 3, 0, 4096};
  RoundTrip roundTrip;
  for (size_t i : integerRange(sizes.size())) {
    size_t numBytes = sizes.at(i);
    roundTrip.expected_.push_back(makeBytes(numBytes, i + 1));
    roundTrip.metadata_.push_back(
        file.appendBlock(roundTrip.expected_.back().data(), numBytes));
    EXPECT_EQ(roundTrip.metadata_.back().uncompressedSize_, numBytes);
  }

  // The blocks are stored one after the other, without gaps or overlaps.
  size_t expectedOffset = 0;
  for (const auto& block : roundTrip.metadata_) {
    EXPECT_EQ(block.offsetInFile_, expectedOffset);
    expectedOffset += block.compressedSize_;
  }
  EXPECT_EQ(ql::filesystem::file_size(file.filename()), expectedOffset);

  // Read the blocks back, both in order and in reverse order, to make sure
  // that reading doesn't depend on the shared file offset.
  for (size_t i : integerRange(sizes.size())) {
    EXPECT_EQ(readBytes(file, roundTrip.metadata_.at(i)),
              roundTrip.expected_.at(i))
        << "block " << i;
  }
  for (size_t i : integerRange(sizes.size())) {
    size_t idx = sizes.size() - 1 - i;
    EXPECT_EQ(readBytes(file, roundTrip.metadata_.at(idx)),
              roundTrip.expected_.at(idx))
        << "block " << idx;
  }
  return roundTrip;
}

// The compression levels that the round trip below is run with: no compression
// at all, the fastest ZSTD level, the default level, and a slow one.
const std::vector<CompressedBlockFile::CompressionLevel>& compressionLevels() {
  static const std::vector<CompressedBlockFile::CompressionLevel> result{
      ad_utility::NO_BLOCK_COMPRESSION, 1, ad_utility::ZSTD_DEFAULT_LEVEL, 9};
  return result;
}

}  // namespace

// _____________________________________________________________________________
TEST(CompressedBlockFile, appendAndReadBlocks) {
  std::string filename = gtestCurrentTestName();
  {
    // A file that is created without an explicit compression uses the default
    // ZSTD level.
    CompressedBlockFile file{filename};
    ASSERT_EQ(file.filename(), filename);
    EXPECT_EQ(file.compressionLevel(), ad_utility::ZSTD_DEFAULT_LEVEL);
    checkRoundTrip(file);
    ASSERT_TRUE(ql::filesystem::exists(filename));
  }
  // The destructor has deleted the file.
  EXPECT_FALSE(ql::filesystem::exists(filename));
}

// _____________________________________________________________________________
// The very same round trip, but with each of the compressions that a caller may
// choose, including `NO_BLOCK_COMPRESSION`.
TEST(CompressedBlockFile, appendAndReadBlocksWithExplicitCompression) {
  for (size_t i : integerRange(compressionLevels().size())) {
    CompressedBlockFile::CompressionLevel compression =
        compressionLevels().at(i);
    std::string filename = absl::StrCat(gtestCurrentTestName(), ".", i);
    {
      CompressedBlockFile file{filename, compression};
      EXPECT_EQ(file.compressionLevel(), compression);
      checkRoundTrip(file);
    }
    EXPECT_FALSE(ql::filesystem::exists(filename));
  }
}

// _____________________________________________________________________________
// A file that was created with `NO_BLOCK_COMPRESSION` stores its blocks exactly
// as they are: the two sizes of a block are equal, and the bytes at the
// recorded offset are byte for byte the bytes that were appended.
TEST(CompressedBlockFile, uncompressedBlocksAreStoredVerbatim) {
  std::string filename = gtestCurrentTestName();
  {
    CompressedBlockFile file{filename, ad_utility::NO_BLOCK_COMPRESSION};
    EXPECT_EQ(file.compressionLevel(), ad_utility::NO_BLOCK_COMPRESSION);
    RoundTrip roundTrip = checkRoundTrip(file);
    std::vector<char> contents = rawFileContents(filename);
    for (size_t i : integerRange(roundTrip.metadata_.size())) {
      const auto& metadata = roundTrip.metadata_.at(i);
      const std::vector<char>& expected = roundTrip.expected_.at(i);
      EXPECT_EQ(metadata.compressedSize_, metadata.uncompressedSize_)
          << "block " << i;
      ASSERT_LE(metadata.offsetInFile_ + metadata.compressedSize_,
                contents.size());
      auto begin = contents.begin() +
                   static_cast<std::ptrdiff_t>(metadata.offsetInFile_);
      std::vector<char> stored{
          begin, begin + static_cast<std::ptrdiff_t>(metadata.compressedSize_)};
      EXPECT_EQ(stored, expected) << "block " << i;
    }
  }
  EXPECT_FALSE(ql::filesystem::exists(filename));
}

// _____________________________________________________________________________
// The compression really is the one that the caller has asked for: a
// compressible block shrinks by a lot at any ZSTD level, it shrinks at least as
// much at a higher level, and it does not shrink at all without compression.
TEST(CompressedBlockFile, theRequestedCompressionIsApplied) {
  // A block of a single repeated byte, so that every ZSTD level compresses it
  // by a large factor.
  std::vector<char> block(100'000, 'a');
  auto appendedSize = [&block](
                          CompressedBlockFile::CompressionLevel compression,
                          const std::string& filename) {
    CompressedBlockFile file{filename, compression};
    return file.appendBlock(block.data(), block.size()).compressedSize_;
  };
  size_t uncompressed =
      appendedSize(ad_utility::NO_BLOCK_COMPRESSION,
                   absl::StrCat(gtestCurrentTestName(), ".none"));
  size_t fast = appendedSize(1, absl::StrCat(gtestCurrentTestName(), ".fast"));
  size_t slow = appendedSize(9, absl::StrCat(gtestCurrentTestName(), ".slow"));
  EXPECT_EQ(uncompressed, block.size());
  EXPECT_LT(fast, block.size() / 2);
  EXPECT_LE(slow, fast);
}

// _____________________________________________________________________________
TEST(CompressedBlockFile, clearTruncatesAndAllowsReuse) {
  std::string filename = gtestCurrentTestName();
  {
    CompressedBlockFile file{filename};
    auto firstBytes = makeBytes(50'000, 1);
    auto firstBlock = file.appendBlock(firstBytes.data(), firstBytes.size());
    // Append a second block, such that the offset of the next append is far
    // away from 0 when the file is cleared below.
    auto moreBytes = makeBytes(20'000, 2);
    auto moreBlock = file.appendBlock(moreBytes.data(), moreBytes.size());
    ASSERT_EQ(moreBlock.offsetInFile_, firstBlock.compressedSize_);
    ASSERT_GT(ql::filesystem::file_size(filename), 0u);
    ASSERT_EQ(readBytes(file, firstBlock), firstBytes);
    ASSERT_EQ(readBytes(file, moreBlock), moreBytes);

    file.clear();
    EXPECT_EQ(ql::filesystem::file_size(filename), 0u);

    // The file can be reused: the new blocks again start at offset 0 and again
    // follow each other without a gap.
    auto secondBytes = makeBytes(1234, 3);
    auto secondBlock = file.appendBlock(secondBytes.data(), secondBytes.size());
    EXPECT_EQ(secondBlock.offsetInFile_, 0u);
    EXPECT_EQ(readBytes(file, secondBlock), secondBytes);
    auto thirdBytes = makeBytes(2345, 4);
    auto thirdBlock = file.appendBlock(thirdBytes.data(), thirdBytes.size());
    EXPECT_EQ(thirdBlock.offsetInFile_, secondBlock.compressedSize_);
    EXPECT_EQ(readBytes(file, thirdBlock), thirdBytes);
    EXPECT_EQ(ql::filesystem::file_size(filename),
              secondBlock.compressedSize_ + thirdBlock.compressedSize_);
  }
  EXPECT_FALSE(ql::filesystem::exists(filename));
}

// _____________________________________________________________________________
TEST(CompressedBlockFile, destructorDeletesTheFile) {
  std::string filename = gtestCurrentTestName();
  // If the destructor should ever fail to delete the file, then don't leave it
  // behind.
  absl::Cleanup cleanup = [&filename] {
    if (ql::filesystem::exists(filename)) {
      ad_utility::deleteFile(filename);
    }
  };
  {
    CompressedBlockFile file{filename};
    auto bytes = makeBytes(100, 42);
    file.appendBlock(bytes.data(), bytes.size());
    ASSERT_TRUE(ql::filesystem::exists(filename));
  }
  EXPECT_FALSE(ql::filesystem::exists(filename));
}

// _____________________________________________________________________________
TEST(CompressedBlockFile, failedWriteThrows) {
  // Writing to `/dev/full` fails with `ENOSPC`, exactly like writing to a disk
  // that ran full.
  const std::string devFull = "/dev/full";
  if (!ql::filesystem::exists(devFull)) {
    GTEST_SKIP() << "no " << devFull << " on this platform";
  }
  CompressedBlockFile file{devFull, ad_utility::NO_BLOCK_COMPRESSION};
  auto bytes = makeBytes(100, 7);
  AD_EXPECT_THROW_WITH_MESSAGE(
      file.appendBlock(bytes.data(), bytes.size()),
      ::testing::AllOf(::testing::HasSubstr("Writing 100 bytes"),
                       ::testing::HasSubstr(devFull),
                       ::testing::HasSubstr("No space left on device")));
}

// _____________________________________________________________________________
// Test that several threads may append at the same time, that they get
// distinct and non-overlapping ranges of the file, and that every block is
// readable afterwards. The appends reserve their range of the file with an
// atomic counter, so they do not exclude each other.
TEST(CompressedBlockFile, concurrentAppends) {
  std::string filename = gtestCurrentTestName();
  CompressedBlockFile file{filename};
  static constexpr size_t numThreads = 8;
  static constexpr size_t numBlocksPerThread = 25;
  std::vector<std::vector<std::vector<char>>> expected(numThreads);
  std::vector<std::vector<CompressedBlockFile::BlockMetadata>> metadata(
      numThreads);
  std::vector<ad_utility::JThread> threads;
  for (size_t threadIdx : integerRange(numThreads)) {
    threads.emplace_back([&file, &expected, &metadata, threadIdx]() {
      for (size_t i : integerRange(numBlocksPerThread)) {
        expected.at(threadIdx).push_back(
            makeBytes(500 + 13 * i, threadIdx * numBlocksPerThread + i + 1));
        metadata.at(threadIdx).push_back(
            file.appendBlock(expected.at(threadIdx).back().data(),
                             expected.at(threadIdx).back().size()));
      }
    });
  }
  threads.clear();

  // The appends have reserved distinct ranges of the file: sorted by their
  // offset, the blocks tile the file from 0 up to its size, with neither gaps
  // nor overlaps between them.
  std::vector<CompressedBlockFile::BlockMetadata> allBlocks;
  for (const auto& blocksOfThread : metadata) {
    allBlocks.insert(allBlocks.end(), blocksOfThread.begin(),
                     blocksOfThread.end());
  }
  ASSERT_EQ(allBlocks.size(), numThreads * numBlocksPerThread);
  ql::ranges::sort(allBlocks, [](const auto& a, const auto& b) {
    return a.offsetInFile_ < b.offsetInFile_;
  });
  size_t expectedOffset = 0;
  for (const auto& block : allBlocks) {
    EXPECT_EQ(block.offsetInFile_, expectedOffset);
    expectedOffset += block.compressedSize_;
  }
  EXPECT_EQ(ql::filesystem::file_size(filename), expectedOffset);

  // Every block holds exactly the bytes that were appended for it.
  for (auto [threadIdx, i] : cartesian_product(
           integerRange(numThreads), integerRange(numBlocksPerThread))) {
    EXPECT_EQ(readBytes(file, metadata.at(threadIdx).at(i)),
              expected.at(threadIdx).at(i))
        << "thread " << threadIdx << ", block " << i;
  }
}

// _____________________________________________________________________________
// Test that appends may run while other threads read blocks that were appended
// before. An append neither moves nor rewrites an existing block, so the
// readers have to see the unchanged bytes all the time.
TEST(CompressedBlockFile, appendsConcurrentWithReads) {
  std::string filename = gtestCurrentTestName();
  CompressedBlockFile file{filename};
  static constexpr size_t numInitialBlocks = 20;
  static constexpr size_t numWriterThreads = 4;
  static constexpr size_t numReaderThreads = 4;
  static constexpr size_t numBlocksPerWriter = 25;
  static constexpr size_t numReadRounds = 20;

  // The blocks that the reader threads below read over and over again.
  std::vector<std::vector<char>> initialBytes;
  std::vector<CompressedBlockFile::BlockMetadata> initialMetadata;
  for (size_t i : integerRange(numInitialBlocks)) {
    initialBytes.push_back(makeBytes(1000 + 37 * i, i + 1));
    initialMetadata.push_back(file.appendBlock(initialBytes.back().data(),
                                               initialBytes.back().size()));
  }

  std::vector<std::vector<std::vector<char>>> appended(numWriterThreads);
  std::vector<std::vector<CompressedBlockFile::BlockMetadata>> appendedMetadata(
      numWriterThreads);
  std::vector<ad_utility::JThread> threads;
  for (size_t threadIdx : integerRange(numWriterThreads)) {
    threads.emplace_back([&file, &appended, &appendedMetadata, threadIdx]() {
      for (size_t i : integerRange(numBlocksPerWriter)) {
        appended.at(threadIdx).push_back(
            makeBytes(777 + 11 * i, 10'000 * (threadIdx + 1) + i));
        appendedMetadata.at(threadIdx).push_back(
            file.appendBlock(appended.at(threadIdx).back().data(),
                             appended.at(threadIdx).back().size()));
      }
    });
  }
  for (size_t threadIdx : integerRange(numReaderThreads)) {
    threads.emplace_back([&file, &initialBytes, &initialMetadata, threadIdx]() {
      for (auto [round, i] : cartesian_product(
               integerRange(numReadRounds), integerRange(numInitialBlocks))) {
        size_t idx = (i + threadIdx + round) % numInitialBlocks;
        EXPECT_EQ(readBytes(file, initialMetadata.at(idx)),
                  initialBytes.at(idx))
            << "thread " << threadIdx << ", block " << idx;
      }
    });
  }
  threads.clear();

  // The blocks that were appended while the readers were running are readable,
  // too.
  for (auto [threadIdx, i] : cartesian_product(
           integerRange(numWriterThreads), integerRange(numBlocksPerWriter))) {
    EXPECT_EQ(readBytes(file, appendedMetadata.at(threadIdx).at(i)),
              appended.at(threadIdx).at(i))
        << "thread " << threadIdx << ", block " << i;
  }
}

// _____________________________________________________________________________
TEST(CompressedBlockFile, concurrentReads) {
  std::string filename = gtestCurrentTestName();
  CompressedBlockFile file{filename};
  static constexpr size_t numBlocks = 20;
  std::vector<std::vector<char>> expected;
  std::vector<CompressedBlockFile::BlockMetadata> metadata;
  for (size_t i : integerRange(numBlocks)) {
    expected.push_back(makeBytes(1000 + 37 * i, i + 1));
    metadata.push_back(
        file.appendBlock(expected.back().data(), expected.back().size()));
  }

  // Each of the threads reads all the blocks, in a different order.
  static constexpr size_t numThreads = 8;
  std::vector<ad_utility::JThread> threads;
  for (size_t threadIdx : integerRange(numThreads)) {
    threads.emplace_back([&file, &expected, &metadata, threadIdx]() {
      for (size_t i : integerRange(numBlocks)) {
        size_t idx = (i + threadIdx) % numBlocks;
        EXPECT_EQ(readBytes(file, metadata.at(idx)), expected.at(idx))
            << "thread " << threadIdx << ", block " << idx;
      }
    });
  }
  threads.clear();
}

// _____________________________________________________________________________
TEST(CompressedBlockFile, concurrentAppendsAndReads) {
  std::string filename = gtestCurrentTestName();
  CompressedBlockFile file{filename};
  static constexpr size_t numThreads = 8;
  static constexpr size_t numBlocksPerThread = 20;

  // Each thread appends its own blocks and immediately reads them back again.
  // Appending from one thread must not invalidate the blocks that another
  // thread has already appended.
  std::vector<ad_utility::JThread> threads;
  for (size_t threadIdx : integerRange(numThreads)) {
    threads.emplace_back([&file, threadIdx]() {
      std::vector<std::vector<char>> expected;
      std::vector<CompressedBlockFile::BlockMetadata> metadata;
      for (size_t i : integerRange(numBlocksPerThread)) {
        expected.push_back(makeBytes(500 + i, 1000 * (threadIdx + 1) + i));
        metadata.push_back(
            file.appendBlock(expected.back().data(), expected.back().size()));
      }
      for (size_t i : integerRange(numBlocksPerThread)) {
        EXPECT_EQ(readBytes(file, metadata.at(i)), expected.at(i))
            << "thread " << threadIdx << ", block " << i;
      }
    });
  }
  threads.clear();
}

// _____________________________________________________________________________
// Test that `clear` may run while other threads append. Which blocks survive
// depends on the timing, so the appended blocks are not read back. What is
// checked instead is that the file stays consistent: an append that reserved
// its range before a `clear` must not write it to the file after the `clear`.
// If it did, then the file would be larger than the ranges that were reserved
// since the last `clear`, and the next append would not start at its end.
TEST(CompressedBlockFile, clearConcurrentWithAppends) {
  std::string filename = gtestCurrentTestName();
  CompressedBlockFile file{filename, ad_utility::NO_BLOCK_COMPRESSION};
  static constexpr size_t numThreads = 8;
  static constexpr size_t numBlocksPerThread = 50;
  std::atomic<size_t> numAppends = 0;
  {
    std::vector<ad_utility::JThread> threads;
    for (size_t threadIdx : integerRange(numThreads)) {
      threads.emplace_back([&file, &numAppends, threadIdx]() {
        for (size_t i : integerRange(numBlocksPerThread)) {
          auto bytes = makeBytes(100 + 7 * i, 1000 * (threadIdx + 1) + i);
          file.appendBlock(bytes.data(), bytes.size());
          ++numAppends;
        }
      });
    }
    // Keep clearing the file until all the appends are done, such that the
    // clears are interleaved with the appends.
    do {
      file.clear();
    } while (numAppends.load() < numThreads * numBlocksPerThread);
  }

  // All the appends have finished, so every range that was reserved since the
  // last `clear` has been written, and the next block starts at the end of the
  // file.
  auto sizeBefore = ql::filesystem::file_size(filename);
  auto bytes = makeBytes(1234, 42);
  auto block = file.appendBlock(bytes.data(), bytes.size());
  EXPECT_EQ(block.offsetInFile_, sizeBefore);
  EXPECT_EQ(readBytes(file, block), bytes);
  EXPECT_EQ(ql::filesystem::file_size(filename), sizeBefore + bytes.size());
}
