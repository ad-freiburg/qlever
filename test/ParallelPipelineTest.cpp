//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 23.09.21.
//

#ifndef QLEVER_PARALLELPIPELINETEST_H
#define QLEVER_PARALLELPIPELINETEST_H

#include <gtest/gtest.h>

#include <string>

#include "../src/util/Log.h"
#include "../src/util/ParallelPipeline.h"
#include "../src/util/ResourcePool.h"

using namespace ad_pipeline;
TEST(ParallelPipeline, First) {
  std::atomic<int> result = 0;
  auto starter = [i = 0]() mutable -> std::optional<int> {
    if (i < 1500) {
      return i++;
    } else {
      return std::nullopt;
    }
  };

  auto middle = [](int i) { return i * 2; };

  auto finisher = [&result](int i) { result += i; };

  Pipeline p{false, {1, 5, 5, 20}, starter, middle, middle, finisher};
  p.finish();
  ASSERT_EQ(result, 4497000);
}

TEST(ParallelPipeline, MoveOnly) {
  auto starter = [i = 0]() mutable -> std::optional<std::string> {
    if (i < 20) {
      i++;
      return "hallo";
    } else {
      return std::nullopt;
    }
  };

  auto middle = [](std::string&& s) { return std::move(s); };
  auto end = [](std::string s) { LOG(INFO) << s << std::endl; };

  Pipeline p{false, {1, 5, 1}, starter, middle, end};
}

TEST(ResourcePool, First) {
  ad_utility::ResourcePool<int> p{};
  p.addResource(0);
  *p.acquire() += 5;
  auto handle = p.acquire();
  *handle += 2;
  p.release(std::move(handle));
  ASSERT_EQ(7, *p.acquire());
}

#endif  // QLEVER_PARALLELPIPELINETEST_H
