//
// Created by johannes on 21.04.21.
//

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "../../src/engine/SortPerformanceEstimator.h"
#include "../../src/util/Log.h"
#include "../../src/util/Random.h"

TEST(SortPerformanceEstimator, TestManyEstimates) {
  // only allow the test to use 1 Gig of RAM
  auto allocator = ad_utility::AllocatorWithLimit<Id>{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1ull << 30ul)};
  auto t = SortPerformanceEstimator::CreateEstimatorExpensively(allocator);

  SlowRandomIntGenerator<int> dice(1, 6);

  for (size_t numColumns = 1; numColumns < 15; numColumns++) {
    bool isFirst = true;
    for (size_t i = 1'000'000; i < 100'000'000;
         i = static_cast<size_t>(i * 1.5)) {
      if (dice() != 6) {
        // only actually perform every 6th test, to obtain an acceptable
        // performance.
        continue;
      }
      try {
        double measurement =
            SortPerformanceEstimator::measureSortingTimeInSeconds(i, numColumns,
                                                                  allocator);
        double estimate = t.estimatedSortTimeInSeconds(i, numColumns);
        LOG(INFO) << std::fixed << std::setprecision(3) << "input of size " << i
                  << "with " << numColumns << " columns took " << measurement
                  << " seconds, estimate was " << estimate << " seconds"
                  << std::endl;
        ASSERT_GE(2 * measurement, estimate);
        if (!isFirst) {
          EXPECT_LE(0.5 * measurement, estimate);
        } else if (0.5 * measurement > estimate) {
          LOG(WARN) << "The first measurement with a new column size took "
                       "twice as long as estimated. This is not unusual (even "
                       "typical) and hence does not count as a failed test."
                    << std::endl;
        }
        isFirst = false;
      } catch (ad_utility::detail::AllocationExceedsLimitException&) {
        break;
      }
    }
  }
}