//
// Created by johannes on 21.04.21.
//

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "../../src/engine/SortPerformanceEstimator.h"
#include "../../src/util/Log.h"

TEST(SortPerformanceEstimator, construct) {
  // only allow the test to use 1 Gig of RAM
  auto allocator = ad_utility::AllocatorWithLimit<Id>{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(8 * 1ull << 30ul)};
  SortPerformanceEstimator t{allocator};

  for (size_t numColumns = 7; numColumns < 15; numColumns++) {
    for (size_t i = 5000000; i < 100000000; i = static_cast<size_t>(i * 1.5)) {
      try {
        double measurement =
            SortPerformanceEstimator::measureSortingTimeInSeconds(i, numColumns,
                                                                  allocator, 0);
        double estimate = t.EstimateSortTimeInSeconds(i, numColumns);
        LOG(INFO) << "input of size " << i << "with " << numColumns
                  << " columns took " << measurement
                  << " seconds, estimate was " << estimate << " seconds"
                  << std::endl;
        EXPECT_LE(0.5 * measurement, estimate);
        ASSERT_GE(2 * measurement, estimate);
      } catch (ad_utility::detail::AllocationExceedsLimitException&) {
        break;
      }
    }
  }
}