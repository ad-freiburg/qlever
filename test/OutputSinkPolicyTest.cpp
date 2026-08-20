// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <exception>
#include <stdexcept>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/jthread.h"
#include "util/parallelBlockMerge/OutputSinkPolicy.h"

using namespace ad_utility::parallelBlockMerge;

namespace {
using Block = std::vector<int>;
using Sink = InOrderBlockSink<Block>;

// A type that does not fulfill any of the requirements of the `BlockSink`
// concept.
struct NotASink {};

static_assert(BlockSink<Sink, Block>);
static_assert(!BlockSink<NotASink, Block>);

std::vector<Block> collect(Sink& sink) {
  std::vector<Block> result;
  for (auto& block : sink.blocks()) {
    result.push_back(std::move(block));
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(OutputSinkPolicy, SingleThreaded) {
  Sink sink{2};
  sink.setNumChunks(2);
  sink(0, Block{1, 2});
  sink.finishChunk(0);
  sink(1, Block{3});
  sink(1, Block{4});
  sink.finishChunk(1);
  EXPECT_THAT(collect(sink),
              ::testing::ElementsAre(Block{1, 2}, Block{3}, Block{4}));
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, Empty) {
  Sink sink;
  sink.setNumChunks(0);
  EXPECT_THAT(collect(sink), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, MultiThreadedOutOfOrder) {
  static constexpr size_t numChunks = 8;
  static constexpr size_t numBlocksPerChunk = 20;
  Sink sink{3};
  sink.setNumChunks(numChunks);

  // The producers deliberately start with the *highest* chunk index, so that
  // the consumer has to reorder them.
  std::vector<ad_utility::JThread> producers;
  for (size_t i = 0; i < numChunks; ++i) {
    size_t chunkIndex = numChunks - 1 - i;
    producers.emplace_back([&sink, chunkIndex] {
      for (size_t j = 0; j < numBlocksPerChunk; ++j) {
        sink(chunkIndex,
             Block{static_cast<int>(chunkIndex), static_cast<int>(j)});
      }
      sink.finishChunk(chunkIndex);
    });
  }

  auto blocks = collect(sink);
  producers.clear();

  ASSERT_EQ(blocks.size(), numChunks * numBlocksPerChunk);
  // The blocks appear in strict chunk order, and in the order of their
  // creation within a chunk.
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    for (size_t j = 0; j < numBlocksPerChunk; ++j) {
      const auto& block = blocks.at(chunkIndex * numBlocksPerChunk + j);
      EXPECT_THAT(block, ::testing::ElementsAre(static_cast<int>(chunkIndex),
                                                static_cast<int>(j)));
    }
  }
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, PushExceptionSurfaces) {
  Sink sink{2};
  sink.setNumChunks(2);
  sink(0, Block{1});
  ad_utility::JThread producer{[&sink] {
    try {
      throw std::runtime_error{"expected error"};
    } catch (...) {
      sink.pushException(std::current_exception());
    }
  }};
  producer.join();
  auto blocks = sink.blocks();
  AD_EXPECT_THROW_WITH_MESSAGE(([&blocks] {
                                 for ([[maybe_unused]] auto& block : blocks) {
                                 }
                               })(),
                               ::testing::HasSubstr("expected error"));
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, ExceptionUnblocksProducers) {
  // A producer that is blocked on a full buffer is unblocked by an exception
  // that is pushed by another producer.
  Sink sink{1};
  sink.setNumChunks(2);
  ad_utility::JThread blockedProducer{[&sink] {
    for (size_t i = 0; i < 100; ++i) {
      sink(1, Block{static_cast<int>(i)});
    }
  }};
  try {
    throw std::runtime_error{"other error"};
  } catch (...) {
    sink.pushException(std::current_exception());
  }
  // This must terminate.
  blockedProducer.join();
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, BackPressure) {
  // With a buffer of a single block per chunk, the producers of the higher
  // chunks are blocked most of the time. The test must still terminate.
  static constexpr size_t numChunks = 4;
  static constexpr size_t numBlocksPerChunk = 50;
  Sink sink{1};
  sink.setNumChunks(numChunks);

  std::vector<ad_utility::JThread> producers;
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    producers.emplace_back([&sink, chunkIndex] {
      for (size_t j = 0; j < numBlocksPerChunk; ++j) {
        sink(chunkIndex, Block{static_cast<int>(chunkIndex)});
      }
      sink.finishChunk(chunkIndex);
    });
  }

  auto blocks = collect(sink);
  producers.clear();
  ASSERT_EQ(blocks.size(), numChunks * numBlocksPerChunk);
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    for (size_t j = 0; j < numBlocksPerChunk; ++j) {
      EXPECT_THAT(blocks.at(chunkIndex * numBlocksPerChunk + j),
                  ::testing::ElementsAre(static_cast<int>(chunkIndex)));
    }
  }
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, AbortUnblocksProducers) {
  // A consumer that stops early must not leave a blocked producer behind. The
  // destructor of the sink aborts, so the producer terminates.
  std::atomic<bool> producerIsDone = false;
  {
    Sink sink{1};
    sink.setNumChunks(1);
    ad_utility::JThread producer{[&sink, &producerIsDone] {
      for (size_t i = 0; i < 1000; ++i) {
        sink(0, Block{static_cast<int>(i)});
      }
      producerIsDone = true;
    }};
    // Consume a single block, then abort.
    auto blocks = sink.blocks();
    auto it = blocks.begin();
    ASSERT_NE(it, blocks.end());
    sink.abort();
    producer.join();
  }
  EXPECT_TRUE(producerIsDone);
}
