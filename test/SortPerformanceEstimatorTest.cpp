//
// Created by johannes on 21.04.21.
//

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "../../src/engine/SortPerformanceEstimator.h"
#include "../../src/util/Log.h"

TEST(SortPerformanceEstimator, TestManyEstimates) {
  // only allow the test to use 1 Gig of RAM
  auto allocator = ad_utility::AllocatorWithLimit<Id>{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1ull << 30ul)};
  SortPerformanceEstimator t{allocator};

  for (size_t numColumns = 7; numColumns < 15; numColumns++) {
    bool isFirst = true;
    for (size_t i = 1000000; i < 100000000; i = static_cast<size_t>(i * 1.5)) {
      try {
        double measurement =
            SortPerformanceEstimator::measureSortingTimeInSeconds(i, numColumns,
                                                                  allocator);
        double estimate = t.EstimateSortTimeInSeconds(i, numColumns);
        LOG(INFO) << std::fixed << std::setprecision(3) << "input of size " << i
                  << "with " << numColumns << " columns took " << measurement
                  << " seconds, estimate was " << estimate << " seconds"
                  << std::endl;
        ASSERT_GE(2 * measurement, estimate);
        if (!isFirst) {
          EXPECT_LE(0.5 * measurement, estimate);
        } else if (0.5 * measurement > estimate) {
          LOG(WARN) << "The first measurement with a new column size took much "
                       "longer than the estimate. This happens "
                       "deterministically for unknown reasons"
                    << std::endl;
        }
        isFirst = false;
      } catch (ad_utility::detail::AllocationExceedsLimitException&) {
        break;
      }
    }
  }
}