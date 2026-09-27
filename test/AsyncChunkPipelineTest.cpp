// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "util/GTestHelpers.h"

namespace {
using namespace qlever::export_v2;

static_assert(exportV2CompiledIn);
static_assert(numRingSlots == 2);

TEST(AsyncChunkPipelineTest, RuntimeKillSwitchLeavesPipelineClosed) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = false}};
  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_EQ(pipeline.push("ignored"), PushResult::Closed);
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, EmptyCompletedPipelineReturnsNoChunk) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  pipeline.finish();
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, MovesChunksWithoutCopyingTheirBuffer) {
  AsyncChunkPipeline<std::unique_ptr<std::string>> pipeline{
      {.runtimeEnabled_ = true}};
  auto chunk = std::make_unique<std::string>("payload");
  const auto* allocation = chunk.get();

  EXPECT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);
  auto received = pipeline.pop();

  ASSERT_TRUE(received.has_value());
  EXPECT_EQ(received->get(), allocation);
  EXPECT_EQ(**received, "payload");
}

TEST(AsyncChunkPipelineTest, CompletionDrainsQueuedChunksInOrder) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  pipeline.finish();
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
}

TEST(AsyncChunkPipelineTest, FullRingSignalsBackpressureWithoutBlocking) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  // Both ring slots hold undrained chunks, so the producer must suspend
  // generation instead of blocking.
  EXPECT_EQ(pipeline.push("third"), PushResult::Full);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  // The drained slot is immediately reusable for the next chunk.
  EXPECT_EQ(pipeline.push("third"), PushResult::Accepted);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"third"});
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
}

TEST(AsyncChunkPipelineTest, SlotsAlternateAcrossWraparound) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  // Rotate the ring several times so both slot indices wrap around and the
  // freed slot is reused on every iteration.
  for (int i = 0; i < 5; ++i) {
    const std::string chunk = "chunk-" + std::to_string(i);
    ASSERT_EQ(pipeline.push(std::string{chunk}), PushResult::Accepted)
        << "iteration " << i;
    EXPECT_EQ(pipeline.pop(), std::optional<std::string>{chunk})
        << "iteration " << i;
  }
  EXPECT_EQ(pipeline.stats().chunksProduced_, 5);
  EXPECT_EQ(pipeline.stats().chunksConsumed_, 5);
}

TEST(AsyncChunkPipelineTest, CancellationDiscardsBothSlots) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 2);
}

TEST(AsyncChunkPipelineTest, CancellationReleasesQueuedBuffer) {
  AsyncChunkPipeline<std::shared_ptr<std::string>> pipeline{
      {.runtimeEnabled_ = true}};
  auto chunk = std::make_shared<std::string>("payload");
  std::weak_ptr<std::string> lifetime = chunk;
  ASSERT_EQ(pipeline.push(std::move(chunk)), PushResult::Accepted);

  pipeline.cancel();

  EXPECT_TRUE(lifetime.expired());
}

TEST(AsyncChunkPipelineTest, PropagatesFailureAfterQueuedChunks) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("before-error"), PushResult::Accepted);
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"producer failed"}));
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"before-error"});
  // Use a lambda to discard the nodiscard return value while still checking
  // the exception
  EXPECT_THROW([&] { static_cast<void>(pipeline.pop()); }(),
               std::runtime_error);
}

TEST(AsyncChunkPipelineTest, FailAfterFinishIsNoOp) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("only"), PushResult::Accepted);
  pipeline.finish();
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"too late"}));
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"only"});
  EXPECT_FALSE(static_cast<bool>(pipeline.pop().has_value()));
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
}

TEST(AsyncChunkPipelineTest, CancelAfterFinishIsNoOp) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("only"), PushResult::Accepted);
  pipeline.finish();
  pipeline.cancel();
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"only"});
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, SecondFailKeepsFirstException) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"first"}));
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"second"}));
  try {
    static_cast<void>(pipeline.pop());
    FAIL() << "pop must rethrow the recorded failure";
  } catch (const std::runtime_error& e) {
    EXPECT_STREQ(e.what(), "first");
  }
}

TEST(AsyncChunkPipelineTest, DefaultConstructedPipelineIsDisabled) {
  AsyncChunkPipeline<std::string> pipeline;
  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  // All lifecycle calls are no-ops and nothing is counted.
  pipeline.finish();
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"ignored"}));
  pipeline.cancel();
  std::string chunk = "kept";
  EXPECT_EQ(pipeline.push(std::move(chunk)), PushResult::Closed);
  EXPECT_EQ(chunk, "kept");
  EXPECT_FALSE(pipeline.pop().has_value());
  auto stats = pipeline.stats();
  EXPECT_EQ(stats.chunksProduced_, 0);
  EXPECT_EQ(stats.chunksConsumed_, 0);
  EXPECT_EQ(stats.chunksDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, IsRunningTracksLifecycleWhileIsEnabledStays) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_TRUE(pipeline.isRunning());
  // An empty running pipeline has no chunk yet but is not over.
  EXPECT_FALSE(pipeline.pop().has_value());
  EXPECT_TRUE(pipeline.isRunning());
  pipeline.finish();
  EXPECT_TRUE(pipeline.isEnabled());
  EXPECT_FALSE(pipeline.isRunning());
  // A second `finish` is a no-op.
  pipeline.finish();
  EXPECT_FALSE(pipeline.isRunning());
}

TEST(AsyncChunkPipelineTest, RejectedPushLeavesChunkAndStatsUntouched) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  std::string third = "third";
  EXPECT_EQ(pipeline.push(std::move(third)), PushResult::Full);
  // The caller keeps the chunk and can retry it after a `pop`.
  EXPECT_EQ(third, "third");
  EXPECT_EQ(pipeline.stats().chunksProduced_, 2);
  EXPECT_EQ(pipeline.stats().bytesProduced_, 11);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_EQ(pipeline.push(std::move(third)), PushResult::Accepted);
  EXPECT_EQ(pipeline.stats().chunksProduced_, 3);
  EXPECT_EQ(pipeline.stats().bytesProduced_, 16);
  EXPECT_EQ(pipeline.stats().bytesConsumed_, 5);
}

TEST(AsyncChunkPipelineTest, FailureDrainsBothSlotsThenRethrowsRepeatedly) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("first"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("second"), PushResult::Accepted);
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"producer failed"}));
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"first"});
  EXPECT_EQ(pipeline.pop(), std::optional<std::string>{"second"});
  for (int i = 0; i < 2; ++i) {
    EXPECT_THROW([&] { static_cast<void>(pipeline.pop()); }(),
                 std::runtime_error);
  }
  // A failed export cannot be cancelled afterwards.
  pipeline.cancel();
  EXPECT_THROW([&] { static_cast<void>(pipeline.pop()); }(),
               std::runtime_error);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
}

TEST(AsyncChunkPipelineTest, FailRequiresAnException) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  EXPECT_ANY_THROW(pipeline.fail(nullptr));
  EXPECT_TRUE(pipeline.isRunning());
}

TEST(AsyncChunkPipelineTest, CancelOfEmptyPipelineCloses) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  pipeline.cancel();
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_FALSE(pipeline.pop().has_value());
  EXPECT_EQ(pipeline.push("late"), PushResult::Closed);
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 0);
  // Late `fail` and a second `cancel` are no-ops.
  pipeline.fail(std::make_exception_ptr(std::runtime_error{"too late"}));
  pipeline.cancel();
  EXPECT_FALSE(pipeline.pop().has_value());
}

TEST(AsyncChunkPipelineTest, CancellationCountsDiscardedBytes) {
  AsyncChunkPipeline<std::string> pipeline{{.runtimeEnabled_ = true}};
  ASSERT_EQ(pipeline.push("abc"), PushResult::Accepted);
  ASSERT_EQ(pipeline.push("de"), PushResult::Accepted);
  pipeline.cancel();
  EXPECT_EQ(pipeline.stats().chunksDiscarded_, 2);
  EXPECT_EQ(pipeline.stats().bytesDiscarded_, 5);
}

}  // namespace
