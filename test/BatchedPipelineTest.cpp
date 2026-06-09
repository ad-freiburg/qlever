// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <tuple>

#include "util/BatchedPipeline.h"

using ad_pipeline::makePipelineExecutor;

TEST(BatcherTest, MoveOnlyCreator) {
  auto exec = makePipelineExecutor(1);
  auto pipeline = ad_pipeline::detail::Batcher(
      20, [ptr = std::unique_ptr<int>()]() { return std::optional(25); },
      &exec.batcherPool_);
  auto batch = pipeline.pickupBatch();
  ASSERT_TRUE(batch.isPipelineGood_);
  ASSERT_EQ(20u, batch.content_.size());
  ASSERT_EQ(batch.content_[0], 25);
}

// A move-only creator (it captures a `unique_ptr`) must also work through the
// full `BatchExtractor` interface returned by `setupParallelPipeline`.
TEST(BatcherTest, MoveOnlyCreator2) {
  auto exec = makePipelineExecutor(1);
  auto pipeline = ad_pipeline::setupParallelPipeline<1>(
      exec, 20,
      [ptr = std::make_unique<int>(23)]() { return std::optional(*ptr); },
      std::tuple([](int x) { return x; }));
  auto batch = pipeline.getNextValue();
  ASSERT_TRUE(batch);
  ASSERT_EQ(23, batch.value());
}

TEST(BatchedPipelineTest, BasicPipeline) {
  // A single transforming stage on top of a `Batcher`.
  {
    auto exec = makePipelineExecutor(1);
    auto pipeline = ad_pipeline::detail::Batcher(
        20, []() { return std::optional(25); }, &exec.batcherPool_);
    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<1>(
        exec, std::move(pipeline), [](const auto x) { return x + 3; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.isPipelineGood_);
    ASSERT_EQ(20u, batch2.content_.size());
    ASSERT_EQ(batch2.content_[0], 28);
  }

  // A full pipeline through the `BatchExtractor` interface. This also exercises
  // the exhaustion of the creator and the move semantics of the extractor.
  {
    auto exec = makePipelineExecutor(1);
    auto finalPipeline = ad_pipeline::setupParallelPipeline<1>(
        exec, 20,
        [i = 0]() mutable -> std::optional<int> {
          if (i < 100) {
            ++i;
            return std::optional(i);
          }
          return std::nullopt;
        },
        std::tuple([](const auto& x) { return x * x; }));
    int n = 1;
    while (auto opt = finalPipeline.getNextValue()) {
      ASSERT_EQ(n * n, opt.value());
      ++n;
      if (n == 50) break;
    }

    auto pipelineMoved = std::move(finalPipeline);
    while (auto opt = pipelineMoved.getNextValue()) {
      ASSERT_EQ(n * n, opt.value());
      ++n;
    }
    ASSERT_EQ(n, 101);
  }
}

TEST(BatchedPipelineTest, SimpleParallelism) {
  // A single transformer that is broadcast over several worker threads.
  {
    auto exec = makePipelineExecutor(3);
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); },
        &exec.batcherPool_);

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<3>(
        exec, std::move(pipeline), [](const auto x) { return x * 3; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.isPipelineGood_);
    ASSERT_EQ(20u, batch2.content_.size());
    for (size_t i = 0; i < 20u; ++i) {
      ASSERT_EQ(batch2.content_[i], i * 3);
    }
  }
  // More threads than elements in the batch.
  {
    auto exec = makePipelineExecutor(40);
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); },
        &exec.batcherPool_);

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<40>(
        exec, std::move(pipeline), [](const auto x) { return x * 3; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.isPipelineGood_);
    ASSERT_EQ(20u, batch2.content_.size());
    for (size_t i = 0; i < 20u; ++i) {
      ASSERT_EQ(batch2.content_[i], i * 3);
    }
  }

  // The same, but through the `BatchExtractor` interface. A tuple with a single
  // transformer is broadcast over all `<Parallelism>` worker threads.
  {
    auto exec = makePipelineExecutor(4);
    auto pipeline = ad_pipeline::setupParallelPipeline<4>(
        exec, 20,
        [i = 0ull]() mutable -> std::optional<size_t> {
          if (i >= 67) {
            return std::nullopt;
          }
          return std::optional(i++);
        },
        std::tuple([](const auto x) { return x * 3; }));

    size_t j = 0;
    while (auto opt = pipeline.getNextValue()) {
      ASSERT_EQ(opt.value(), j * 3);
      j++;
    }
    ASSERT_EQ(j, 67u);
  }
}

TEST(BatchedPipelineTest, BranchedParallelism) {
  // Two different transformers, each applied to one half of the batch.
  {
    auto exec = makePipelineExecutor(2);
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); },
        &exec.batcherPool_);

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<2>(
        exec, std::move(pipeline), [](const auto x) { return x * 3; },
        [](const auto x) { return x * 2; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.isPipelineGood_);
    ASSERT_EQ(20u, batch2.content_.size());
    for (size_t i = 0; i < 10u; ++i) {
      ASSERT_EQ(batch2.content_[i], i * 3);
    }
    for (size_t i = 10; i < 20u; ++i) {
      ASSERT_EQ(batch2.content_[i], i * 2);
    }
  }
  // A single transformer that is broadcast over more threads than elements.
  {
    auto exec = makePipelineExecutor(40);
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); },
        &exec.batcherPool_);

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<40>(
        exec, std::move(pipeline), [](const auto x) { return x * 3; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.isPipelineGood_);
    ASSERT_EQ(20u, batch2.content_.size());
    for (size_t i = 0; i < 20u; ++i) {
      ASSERT_EQ(batch2.content_[i], i * 3);
    }
  }

  // The branched case through the `BatchExtractor` interface, using a tuple of
  // two transformers (this mirrors the production usage in `IndexImpl`).
  {
    auto exec = makePipelineExecutor(2);
    auto pipeline = ad_pipeline::setupParallelPipeline<2>(
        exec, 20,
        [i = 0ull]() mutable -> std::optional<size_t> {
          if (i >= 67) {
            return std::nullopt;
          }
          return std::optional(i++);
        },
        std::tuple([](const auto x) { return x * 3; },
                   [](const auto x) { return x * 2; }));

    size_t j = 0;
    while (auto opt = pipeline.getNextValue()) {
      ASSERT_TRUE(opt.value() == j * 3 || opt.value() == j * 2);
      j++;
    }
    ASSERT_EQ(j, 67u);
  }
}
