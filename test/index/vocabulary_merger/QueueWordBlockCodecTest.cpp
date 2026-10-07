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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/SpillingBlockStorageTestHelpers.h"
#include "backports/filesystem.h"
#include "index/vocabulary_merger/QueueWordBlockCodec.h"
#include "util/CompressedBlockFile.h"
#include "util/File.h"

using ad_utility::CompressedBlockFile;
using ad_utility::vocabulary_merger::QueueWordBlockCodec;
using ad_utility::vocabulary_merger::detail::QueueWord;
using Block = QueueWordBlockCodec::Block;

namespace {
// Return a `QueueWord` with the given fields.
QueueWord makeWord(std::string word, bool isExternal, uint64_t localIndex,
                   size_t partialFileId) {
  return QueueWord{
      TripleComponentWithIndex{std::move(word), isExternal, localIndex},
      partialFileId};
}

// Return all the fields of the `word`, such that two words can be compared.
auto fields(const QueueWord& word) {
  return std::tuple{word.iriOrLiteral(), word.isExternal(), word.id(),
                    word.partialFileId_};
}

// Return the fields of all the words of the `block`, see `fields`.
auto fields(const Block& block) {
  std::vector<decltype(fields(std::declval<const QueueWord&>()))> result;
  for (const auto& word : block) {
    result.push_back(fields(word));
  }
  return result;
}

// Return blocks with all kinds of words: empty words, long words, words with
// arbitrary bytes, the extreme values of the integer fields, and an empty
// block.
std::vector<Block> makeBlocks() {
  std::vector<Block> blocks(4);
  blocks.at(0).push_back(makeWord("<http://example.org/a>", false, 0, 0));
  blocks.at(0).push_back(makeWord("", true, 17, 3));
  blocks.at(0).push_back(makeWord("\"literal\"@en", true,
                                  std::numeric_limits<uint64_t>::max(),
                                  std::numeric_limits<size_t>::max()));
  blocks.at(1).push_back(makeWord(std::string(200'000, 'y'), false, 42, 1));
  blocks.at(1).push_back(
      makeWord(std::string("with\0null\nbytes", 15), true, 43, 2));
  // `blocks.at(2)` is deliberately empty.
  for (size_t i = 0; i < 1000; ++i) {
    blocks.at(3).push_back(
        makeWord(absl::StrCat("\"word", i, "\""), i % 3 == 0, i * 7, i % 5));
  }
  return blocks;
}

// The compression levels that the round trips below are run with: the level
// of the merge phase (which is also the default of the factory), the default
// level of ZSTD, and no compression at all.
const std::vector<CompressedBlockFile::CompressionLevel> compressionLevels{
    ad_utility::compressedExternalIdTable::MERGE_PHASE_SPILL_COMPRESSION,
    ad_utility::ZSTD_DEFAULT_LEVEL, ad_utility::NO_BLOCK_COMPRESSION};
}  // namespace

// _____________________________________________________________________________
TEST(QueueWordBlockCodec, roundTripThroughAFile) {
  auto blocks = makeBlocks();
  for (const auto& compressionLevel : compressionLevels) {
    std::string filename = gtestCurrentTestName();
    absl::Cleanup cleanup{[&filename] { ad_utility::deleteFile(filename); }};
    CompressedBlockFile file{filename, compressionLevel};
    QueueWordBlockCodec codec;
    std::vector<QueueWordBlockCodec::BlockMetadata> metadata;
    for (const auto& block : blocks) {
      metadata.push_back(codec.writeBlock(file, block));
    }
    // The blocks can be read back in any order.
    for (size_t i = blocks.size(); i-- > 0;) {
      EXPECT_THAT(fields(codec.readBlock(file, metadata.at(i))),
                  ::testing::ElementsAreArray(fields(blocks.at(i))));
    }
  }
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// _____________________________________________________________________________
TEST(QueueWordBlockCodec, storageRoundTripOfSpilledAndBufferedBlocks) {
  using namespace spillingBlockStorageTestHelpers;
  auto blocks = makeBlocks();
  for (const auto& compressionLevel : compressionLevels) {
    std::string prefix = gtestCurrentTestName();
    boost::asio::io_context ioContext;
    auto makeStorage =
        ad_utility::vocabulary_merger::makeQueueWordBlockStorageFactory(
            ioContext.get_executor(), prefix, 2, compressionLevel);
    auto storage = makeStorage(ad_utility::parallelBlockMerge::Strand{
        boost::asio::any_io_executor{ioContext.get_executor()}});
    // NOTE: The storage deletes the spill file of a chunk as soon as that
    // chunk is done, this cleanup is only a safeguard for a failing test.
    absl::Cleanup cleanup{[&storage] {
      for (size_t chunkIndex : {0u, 1u}) {
        ad_utility::deleteFile(storage.spillFilename(chunkIndex), false);
      }
    }};
    // The consumer reads chunk `0`, so that chunk keeps two blocks in memory
    // and spills the others, whereas chunk `1` spills every block.
    storeChunk(ioContext, storage, 0, makeBlocks());
    storeChunk(ioContext, storage, 1, makeBlocks());
    EXPECT_TRUE(ql::filesystem::exists(storage.spillFilename(1)));
    for (size_t chunkIndex : {0u, 1u}) {
      auto readBlocks = readChunk(ioContext, storage, chunkIndex);
      ASSERT_EQ(readBlocks.size(), blocks.size());
      for (size_t i = 0; i < blocks.size(); ++i) {
        EXPECT_THAT(fields(readBlocks.at(i).block_),
                    ::testing::ElementsAreArray(fields(blocks.at(i))));
        EXPECT_EQ(readBlocks.at(i).wasInMemory_, chunkIndex == 0 && i < 2);
      }
    }
    ad_utility::testing::pollUntilQuiescent(ioContext);
    EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(0)));
    EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(1)));
  }
}

// _____________________________________________________________________________
TEST(QueueWordBlockCodec, cancelAllWakesUpAWaitingConsumer) {
  boost::asio::io_context ioContext;
  auto makeStorage =
      ad_utility::vocabulary_merger::makeQueueWordBlockStorageFactory(
          ioContext.get_executor(), gtestCurrentTestName(), 1);
  auto storage = makeStorage(ad_utility::parallelBlockMerge::Strand{
      boost::asio::any_io_executor{ioContext.get_executor()}});
  spillingBlockStorageTestHelpers::expectCancelAllWakesUpAWaitingConsumer(
      ioContext, storage);
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
