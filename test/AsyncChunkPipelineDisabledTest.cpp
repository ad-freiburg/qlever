// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>

#include "engine/export_v2/AsyncChunkPipeline.h"

namespace {

TEST(AsyncChunkPipelineDisabledTest, CompileTimeSwitchOverridesRuntimeOptIn) {
  static_assert(!qlever::export_v2::exportV2CompiledIn);
  qlever::export_v2::AsyncChunkPipeline<std::string> pipeline{
      {.runtimeEnabled_ = true}};

  EXPECT_FALSE(pipeline.isEnabled());
  EXPECT_EQ(pipeline.push("ignored"), qlever::export_v2::PushResult::Closed);
  EXPECT_FALSE(pipeline.pop().has_value());
}

}  // namespace
