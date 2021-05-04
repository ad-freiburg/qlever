// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include <optional>

#include "../src/util/BatchedPipeline.h"

TEST(BatcherTest, MoveOnlyCreator) {
  auto pipeline = ad_pipeline::detail::Batcher(
      20, [ptr = std::unique_ptr<int>()]() { return std::optional(25); });
  auto batch = pipeline.pickupBatch();
  ASSERT_TRUE(batch.m_isPipelineGood);
  ASSERT_EQ(20u, batch.m_content.size());
  ASSERT_EQ(batch.m_content[0], 25);
}

TEST(BatcherTest, MoveOnlyCreator2) {
  auto pipeline = ad_pipeline::setupPipeline(
      20, [ptr = std::make_unique<int>(23)]() { return std::optional(*ptr); });
  auto batch = pipeline.getNextValue();
  ASSERT_TRUE(batch);
  ASSERT_EQ(23, batch.value());
}

TEST(BatchedPipelineTest, BasicPipeline) {
  auto pipeline =
      ad_pipeline::detail::Batcher(20, []() { return std::optional(25); });
  auto batch = pipeline.pickupBatch();
  ASSERT_TRUE(batch.m_isPipelineGood);
  ASSERT_EQ(20u, batch.m_content.size());
  ASSERT_EQ(batch.m_content[0], 25);

  auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<1>(
      std::move(pipeline), [](const auto x) { return x + 3; });
  auto batch2 = pipeline2.pickupBatch();
  ASSERT_TRUE(batch2.m_isPipelineGood);
  ASSERT_EQ(20u, batch2.m_content.size());
  ASSERT_EQ(batch2.m_content[0], 28);

  auto pipeline3 = ad_pipeline::detail::makeBatchedPipeline<1>(
      std::move(pipeline2), [](const auto x) { return std::to_string(x); });
  auto batch3 = pipeline3.pickupBatch();
  ASSERT_TRUE(batch3.m_isPipelineGood);
  ASSERT_EQ(20u, batch3.m_content.size());
  ASSERT_EQ(batch3.m_content[0], std::string("28"));

  {
    auto finalPipeline = ad_pipeline::setupPipeline(
        20,
        [i = 0]() mutable -> std::optional<int> {
          if (i < 100) {
            ++i;
            return std::optional(i);
          }
          return std::nullopt;
        },
        [a = 0](const auto& x) mutable {
          a += 3;
          return (x + a) * (x + a);
        },
        [a = int(0)](const auto& x) mutable {
          a += 2;
          return x + a;
        });
    int n = 1;
    while (auto opt = finalPipeline.getNextValue()) {
      ASSERT_EQ((n + 3 * n) * (n + 3 * n) + 2 * n, opt.value());
      ++n;
      if (n == 50) break;
    }

    auto pipelineMoved = std::move(finalPipeline);
    while (auto opt = pipelineMoved.getNextValue()) {
      ASSERT_EQ((n + 3 * n) * (n + 3 * n) + 2 * n, opt.value());
      ++n;
    }
    ASSERT_EQ(n, 101);
  }
}

TEST(BatchedPipelineTest, SimpleParallelism) {
  {
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); });

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<3>(
        std::move(pipeline), [](const auto x) { return x * 3; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.m_isPipelineGood);
    ASSERT_EQ(20u, batch2.m_content.size());
    for (size_t i = 0; i < 20u; ++i) {
      ASSERT_EQ(batch2.m_content[i], i * 3);
    }
  }
  {
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); });

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<40>(
        std::move(pipeline), [](const auto x) { return x * 3; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.m_isPipelineGood);
    ASSERT_EQ(20u, batch2.m_content.size());
    for (size_t i = 0; i < 20u; ++i) {
      ASSERT_EQ(batch2.m_content[i], i * 3);
    }
  }

  {
    auto pipeline = ad_pipeline::setupParallelPipeline<4>(
        20,
        [i = 0ull]() mutable -> std::optional<size_t> {
          if (i >= 67) {
            return std::nullopt;
          }
          return std::optional(i++);
        },
        [](const auto x) { return x * 3; });

    size_t j = 0;
    while (auto opt = pipeline.getNextValue()) {
      ASSERT_EQ(opt.value(), j * 3);
      j++;
    }
    ASSERT_EQ(j, 67u);
  }
}

TEST(BatchedPipelineTest, BranchedParallelism) {
  {
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); });

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<2>(
        std::move(pipeline), [](const auto x) { return x * 3; },
        [](const auto x) { return x * 2; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.m_isPipelineGood);
    ASSERT_EQ(20u, batch2.m_content.size());
    for (size_t i = 0; i < 10u; ++i) {
      ASSERT_EQ(batch2.m_content[i], i * 3);
    }
    for (size_t i = 10; i < 20u; ++i) {
      ASSERT_EQ(batch2.m_content[i], i * 2);
    }
  }
  {
    auto pipeline = ad_pipeline::detail::Batcher(
        20, [i = 0ull]() mutable { return std::optional(i++); });

    auto pipeline2 = ad_pipeline::detail::makeBatchedPipeline<40>(
        std::move(pipeline), [](const auto x) { return x * 3; });
    auto batch2 = pipeline2.pickupBatch();
    ASSERT_TRUE(batch2.m_isPipelineGood);
    ASSERT_EQ(20u, batch2.m_content.size());
    for (size_t i = 0; i < 20u; ++i) {
      ASSERT_EQ(batch2.m_content[i], i * 3);
    }
  }

  {
    auto pipeline = ad_pipeline::setupParallelPipeline<2>(
        20,
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
      if (j % 20 < 10) {
        ASSERT_EQ(opt.value(), j * 3);
      } else {
        ASSERT_EQ(opt.value(), j * 2);
      }
      j++;
    }
    ASSERT_EQ(j, 67u);
  }
}
