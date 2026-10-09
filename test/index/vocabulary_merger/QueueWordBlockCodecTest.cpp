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
#include <type_traits>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/SpillingBlockStorageTestHelpers.h"
#include "./VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/QueueWordBlockCodec.h"
#include "util/CompressedBlockFile.h"
#include "util/File.h"

using ad_utility::CompressedBlockFile;
using ad_utility::vocabulary_merger::QueueWordBlockCodec;
using ad_utility::vocabulary_merger::detail::QueueWord;
using vocabularyMergerTestHelpers::makeQueueWord;
using Block = QueueWordBlockCodec::Block;

namespace {
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
  blocks.at(0).push_back(makeQueueWord("<http://example.org/a>", false, 0, 0));
  blocks.at(0).push_back(makeQueueWord("", true, 3, 17));
  blocks.at(0).push_back(makeQueueWord("\"literal\"@en", true,
                                       std::numeric_limits<size_t>::max(),
                                       std::numeric_limits<uint64_t>::max()));
  blocks.at(1).push_back(
      makeQueueWord(std::string(200'000, 'y'), false, 1, 42));
  blocks.at(1).push_back(
      makeQueueWord(std::string("with\0null\nbytes", 15), true, 2, 43));
  // `blocks.at(2)` is deliberately empty.
  for (size_t i = 0; i < 1000; ++i) {
    blocks.at(3).push_back(makeQueueWord(absl::StrCat("\"word", i, "\""),
                                         i % 3 == 0, i % 5, i * 7));
  }
  return blocks;
}

// The compression levels that the round trips below are run with: the default
// level of the spilled blocks (which is also the default of the factory), the
// default level of ZSTD, and no compression at all.
const std::vector<CompressedBlockFile::CompressionLevel> compressionLevels{
    ad_utility::parallelBlockMerge::DEFAULT_SPILL_COMPRESSION_LEVEL,
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

namespace {
using ad_utility::vocabulary_merger::QueueWordBlockStorage;

// The factory creates exactly the storage that the tests below construct
// directly.
static_assert(std::is_same_v<
              std::invoke_result_t<
                  decltype(ad_utility::vocabulary_merger::
                               makeQueueWordBlockStorageFactory(
                                   std::declval<boost::asio::any_io_executor>(),
                                   std::declval<std::string>(), size_t{0}))&,
                  const ad_utility::parallelBlockMerge::Strand&>,
              QueueWordBlockStorage>);

// Return a storage that runs on the `ioContext`, spills to files with the given
// `prefix`, keeps `numBufferedBlocks` blocks of the chunk of the consumer in
// memory, and spills with the given `compressionLevel`.
QueueWordBlockStorage makeStorage(
    boost::asio::io_context& ioContext, std::string prefix,
    size_t numBufferedBlocks,
    CompressedBlockFile::CompressionLevel compressionLevel =
        ad_utility::parallelBlockMerge::DEFAULT_SPILL_COMPRESSION_LEVEL) {
  return QueueWordBlockStorage{ioContext.get_executor(), std::move(prefix),
                               QueueWordBlockCodec{}, numBufferedBlocks,
                               compressionLevel};
}
}  // namespace

// _____________________________________________________________________________
TEST(QueueWordBlockCodec, storageRoundTripOfSpilledAndBufferedBlocks) {
  using namespace spillingBlockStorageTestHelpers;
  for (const auto& compressionLevel : compressionLevels) {
    boost::asio::io_context ioContext;
    auto storage =
        makeStorage(ioContext, gtestCurrentTestName(), 2, compressionLevel);
    auto cleanup = makeSpillFileCleanup(storage, 2);
    expectRoundTripOfSpilledAndBufferedBlocks(
        ioContext, storage, makeBlocks(), 2,
        [](const Block& block) { return fields(block); });
  }
}

// _____________________________________________________________________________
TEST(QueueWordBlockCodec, cancelAllWakesUpAWaitingConsumer) {
  boost::asio::io_context ioContext;
  auto storage = makeStorage(ioContext, gtestCurrentTestName(), 1);
  spillingBlockStorageTestHelpers::expectCancelAllWakesUpAWaitingConsumer(
      ioContext, storage);
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
