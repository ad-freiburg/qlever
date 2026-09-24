// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/metrics/Metrics.h"

// _____________________________________________________________________________
TEST(Metrics, initialize) {
  EXPECT_EQ(ad_utility::metrics::initialize(false), nullptr);
  EXPECT_NE(ad_utility::metrics::initialize(true), nullptr);
}
