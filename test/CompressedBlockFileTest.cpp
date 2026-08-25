// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "./util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "util/CompressedBlockFile.h"
#include "util/File.h"

namespace {

using ad_utility::CompressedBlockFile;

// Create a deterministic but not trivially compressible sequence of
// `numBytes` bytes, seeded by `seed`.
std::vector<char> makeBytes(size_t numBytes, uint64_t seed) {
  std::vector<char> result;
  // Reserve one byte more than needed, such that `data()` is never `nullptr`,
  // also for the empty block.
  result.reserve(numBytes + 1);
  uint64_t state = seed * 2654435761u + 1;
  for ([[maybe_unused]] size_t i : ql::views::iota(size_t{0}, numBytes)) {
    // A simple xorshift, so that the test does not depend on any RNG
    // implementation.
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    result.push_back(static_cast<char>(state & 0xFFu));
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
  for (size_t i : ql::views::iota(size_t{0}, sizes.size())) {
    size_t numBytes = sizes.at(i);
    roundTrip.expected_.push_back(makeBytes(numBytes, i + 1));
    roundTrip.metadata_.push_back(
        file.appendBlock(roundTrip.expected_.back().data(), numBytes));
    EXPECT_EQ(roundTrip.metadata_.back().uncompressedSize_, numBytes);
  }
  file.flush();

  // The blocks are stored one after the other, without gaps or overlaps.
  size_t expectedOffset = 0;
  for (const auto& block : roundTrip.metadata_) {
    EXPECT_EQ(block.offsetInFile_, expectedOffset);
    expectedOffset += block.compressedSize_;
  }
  EXPECT_EQ(ql::filesystem::file_size(file.filename()), expectedOffset);

  // Read the blocks back, both in order and in reverse order, to make sure
  // that reading doesn't depend on the shared file offset.
  for (size_t i : ql::views::iota(size_t{0}, sizes.size())) {
    EXPECT_EQ(readBytes(file, roundTrip.metadata_.at(i)),
              roundTrip.expected_.at(i))
        << "block " << i;
  }
  for (size_t i : ql::views::iota(size_t{0}, sizes.size())) {
    size_t idx = sizes.size() - 1 - i;
    EXPECT_EQ(readBytes(file, roundTrip.metadata_.at(idx)),
              roundTrip.expected_.at(idx))
        << "block " << idx;
  }
  return roundTrip;
}

// The compressions that the round trip below is run with: no compression at
// all, the fastest ZSTD level, the default level, and a slow one.
const std::vector<CompressedBlockFile::Compression>& compressions() {
  static const std::vector<CompressedBlockFile::Compression> result{
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
    EXPECT_EQ(file.compression(), ad_utility::ZSTD_DEFAULT_LEVEL);
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
  for (size_t i : ql::views::iota(size_t{0}, compressions().size())) {
    CompressedBlockFile::Compression compression = compressions().at(i);
    std::string filename = gtestCurrentTestName() + "." + std::to_string(i);
    {
      CompressedBlockFile file{filename, compression};
      EXPECT_EQ(file.compression(), compression);
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
    EXPECT_EQ(file.compression(), ad_utility::NO_BLOCK_COMPRESSION);
    RoundTrip roundTrip = checkRoundTrip(file);
    std::vector<char> contents = rawFileContents(filename);
    for (size_t i : ql::views::iota(size_t{0}, roundTrip.metadata_.size())) {
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
  auto appendedSize = [&block](CompressedBlockFile::Compression compression,
                               const std::string& filename) {
    CompressedBlockFile file{filename, compression};
    return file.appendBlock(block.data(), block.size()).compressedSize_;
  };
  size_t uncompressed = appendedSize(ad_utility::NO_BLOCK_COMPRESSION,
                                     gtestCurrentTestName() + ".none");
  size_t fast = appendedSize(1, gtestCurrentTestName() + ".fast");
  size_t slow = appendedSize(9, gtestCurrentTestName() + ".slow");
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
    file.flush();
    ASSERT_GT(ql::filesystem::file_size(filename), 0u);
    ASSERT_EQ(readBytes(file, firstBlock), firstBytes);

    file.clear();
    EXPECT_EQ(ql::filesystem::file_size(filename), 0u);

    // The file can be reused, and the new blocks again start at offset 0.
    auto secondBytes = makeBytes(1234, 2);
    auto secondBlock = file.appendBlock(secondBytes.data(), secondBytes.size());
    EXPECT_EQ(secondBlock.offsetInFile_, 0u);
    file.flush();
    EXPECT_EQ(readBytes(file, secondBlock), secondBytes);
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
    file.flush();
    ASSERT_TRUE(ql::filesystem::exists(filename));
  }
  EXPECT_FALSE(ql::filesystem::exists(filename));
}

// _____________________________________________________________________________
TEST(CompressedBlockFile, concurrentReads) {
  std::string filename = gtestCurrentTestName();
  CompressedBlockFile file{filename};
  static constexpr size_t numBlocks = 20;
  std::vector<std::vector<char>> expected;
  std::vector<CompressedBlockFile::BlockMetadata> metadata;
  for (size_t i : ql::views::iota(size_t{0}, numBlocks)) {
    expected.push_back(makeBytes(1000 + 37 * i, i + 1));
    metadata.push_back(
        file.appendBlock(expected.back().data(), expected.back().size()));
  }
  file.flush();

  // Each of the threads reads all the blocks, in a different order.
  static constexpr size_t numThreads = 8;
  std::vector<std::thread> threads;
  for (size_t threadIdx : ql::views::iota(size_t{0}, numThreads)) {
    threads.emplace_back([&file, &expected, &metadata, threadIdx]() {
      for (size_t i : ql::views::iota(size_t{0}, numBlocks)) {
        size_t idx = (i + threadIdx) % numBlocks;
        EXPECT_EQ(readBytes(file, metadata.at(idx)), expected.at(idx))
            << "thread " << threadIdx << ", block " << idx;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
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
  std::vector<std::thread> threads;
  for (size_t threadIdx : ql::views::iota(size_t{0}, numThreads)) {
    threads.emplace_back([&file, threadIdx]() {
      std::vector<std::vector<char>> expected;
      std::vector<CompressedBlockFile::BlockMetadata> metadata;
      for (size_t i : ql::views::iota(size_t{0}, numBlocksPerThread)) {
        expected.push_back(makeBytes(500 + i, 1000 * (threadIdx + 1) + i));
        metadata.push_back(
            file.appendBlock(expected.back().data(), expected.back().size()));
        file.flush();
      }
      for (size_t i : ql::views::iota(size_t{0}, numBlocksPerThread)) {
        EXPECT_EQ(readBytes(file, metadata.at(i)), expected.at(i))
            << "thread " << threadIdx << ", block " << i;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
