// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <stdexcept>
#include <string>
#include <vector>

#include "./util/GTestHelpers.h"
#include "engine/AsyncChunkPipeline.h"
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "util/Generator.h"
#endif

namespace {

using qlever::export_pipeline::AsyncChunkPipeline;

// Push/pop preserves order; finish drains to nullopt.
TEST(AsyncChunkPipelineTest, PushPopRoundtripInOrder) {
  AsyncChunkPipeline<std::string> pipeline(2);
  EXPECT_TRUE(pipeline.push("first"));
  EXPECT_TRUE(pipeline.push("second"));
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_TRUE(pipeline.push("third"));
  pipeline.finish();
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"third"});
  EXPECT_EQ(pipeline.pop(), std::nullopt);
}

// Cancelling unblocks a consumer waiting in pop: the flag update and the
// wakeups hold the mutex, so no wakeup is missed between the predicate
// check and the wait.
TEST(AsyncChunkPipelineTest, CancelUnblocksWaitingPop) {
  AsyncChunkPipeline<std::string> pipeline(2);
  auto consumer =
      std::async(std::launch::async, [&pipeline]() { return pipeline.pop(); });
  EXPECT_EQ(consumer.wait_for(std::chrono::seconds(5)),
            std::future_status::timeout);
  pipeline.cancel();
  ASSERT_EQ(consumer.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_EQ(consumer.get(), std::nullopt);
  EXPECT_TRUE(pipeline.isCancelled());
}

// No production after cancellation.
TEST(AsyncChunkPipelineTest, PushAfterCancelReturnsFalse) {
  AsyncChunkPipeline<std::string> pipeline(2);
  pipeline.cancel();
  EXPECT_FALSE(pipeline.push("late"));
}

// Zero capacity violates the constructor precondition.
TEST(AsyncChunkPipelineTest, ZeroCapacityThrows) {
  AD_EXPECT_THROW_WITH_MESSAGE(AsyncChunkPipeline<std::string>(0),
                               ::testing::HasSubstr("capacity_ >= 1"));
}

// Buffered chunks drain first; the producer exception is rethrown once the
// buffer is empty.
TEST(AsyncChunkPipelineTest, ProducerExceptionPropagatesAfterDrain) {
  AsyncChunkPipeline<std::string> pipeline(2);
  EXPECT_TRUE(pipeline.push("buffered"));
  pipeline.setException(
      std::make_exception_ptr(std::runtime_error("producer failed")));
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"buffered"});
  EXPECT_THROW(pipeline.pop(), std::runtime_error);
}

// Cancelling unblocks a producer waiting in push on a full buffer: the
// blocked push returns false instead of hanging.
TEST(AsyncChunkPipelineTest, CancelUnblocksProducerBlockedOnFullBuffer) {
  AsyncChunkPipeline<std::string> pipeline(1);
  EXPECT_TRUE(pipeline.push("fills the only slot"));
  auto producer = std::async(std::launch::async,
                             [&pipeline]() { return pipeline.push("late"); });
  EXPECT_EQ(producer.wait_for(std::chrono::seconds(5)),
            std::future_status::timeout);
  pipeline.cancel();
  ASSERT_EQ(producer.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_FALSE(producer.get());
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// The generator adapter streams all source chunks in order.
TEST(AsyncChunkPipelineTest, MakeDoubleBufferedStreamsChunksInOrder) {
  auto source = []() -> cppcoro::generator<std::string> {
    co_yield "first";
    co_yield "second";
    co_yield "third";
  };
  auto buffered = AsyncChunkPipeline<std::string>::makeDoubleBuffered(source());
  std::vector<std::string> chunks;
  for (auto&& chunk : buffered) {
    chunks.push_back(std::move(chunk));
  }
  ASSERT_EQ(chunks.size(), 3u);
  EXPECT_EQ(chunks.at(0), "first");
  EXPECT_EQ(chunks.at(1), "second");
  EXPECT_EQ(chunks.at(2), "third");
}

// Destroying the adapter without draining cancels the worker and joins it
// instead of hanging or terminating the process.
TEST(AsyncChunkPipelineTest, MakeDoubleBufferedEarlyExitJoinsWorker) {
  auto source = []() -> cppcoro::generator<std::string> {
    for (int i = 0; i < 100; ++i) {
      co_yield "chunk" + std::to_string(i);
    }
  };
  auto buffered = AsyncChunkPipeline<std::string>::makeDoubleBuffered(source());
  size_t count = 0;
  for ([[maybe_unused]] auto&& chunk : buffered) {
    if (++count == 2) {
      break;
    }
  }
  EXPECT_EQ(count, 2u);
}

// The producer-callable adapter delivers pushed chunks in order.
TEST(AsyncChunkPipelineTest, PipelineStreamDeliversProducerChunks) {
  auto stream = AsyncChunkPipeline<std::string>::pipelineStream(
      [](qlever::export_pipeline::ChunkSink<std::string>& sink) {
        EXPECT_TRUE(sink.push("first"));
        EXPECT_TRUE(sink.push("second"));
      });
  std::vector<std::string> chunks;
  for (auto&& chunk : stream) {
    chunks.push_back(std::move(chunk));
  }
  ASSERT_EQ(chunks.size(), 2u);
  EXPECT_EQ(chunks.at(0), "first");
  EXPECT_EQ(chunks.at(1), "second");
}
#endif

}  // namespace
