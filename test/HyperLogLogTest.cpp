// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "util/HyperLogLog.h"

using ad_utility::HyperLogLog;

// _____________________________________________________________________________
TEST(HyperLogLog, Estimate) {
  // Empty sketch.
  EXPECT_EQ(HyperLogLog{}.estimate(), 0.0);

  // Small cardinalities are estimated (almost) exactly, and duplicates don't
  // count.
  for (uint64_t numDistinct : {1, 2, 10, 1000}) {
    HyperLogLog hll;
    for (size_t repetition = 0; repetition < 3; ++repetition) {
      for (uint64_t i = 0; i < numDistinct; ++i) {
        hll.add(i);
      }
    }
    EXPECT_NEAR(hll.estimate(), numDistinct, 0.01 * numDistinct);
  }

  // Large cardinalities are estimated with an error of a few percent at most,
  // also with interleaved duplicates.
  for (uint64_t numDistinct : {100'000, 3'000'000}) {
    HyperLogLog hll;
    for (uint64_t i = 0; i < numDistinct; ++i) {
      hll.add(i * 7 + 3);
      hll.add((i / 2) * 7 + 3);
    }
    EXPECT_NEAR(hll.estimate(), numDistinct, 0.03 * numDistinct);
  }
}
