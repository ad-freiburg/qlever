//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 23.09.21.
//

#ifndef QLEVER_PARALLELPIPELINETEST_H
#define QLEVER_PARALLELPIPELINETEST_H

#include <gtest/gtest.h>
#include "../src/util/ParallelPipeline.h"

using namespace ad_pipeline;
TEST(ParallelPipeline, First) {
  ad_pipeline::Pipeline<int, short, bool> z;
}

#endif  // QLEVER_PARALLELPIPELINETEST_H
