//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "engine/SortPerformanceEstimator.h"
#include "util/Log.h"
#include "util/Random.h"

TEST(SortPerformanceEstimator, TestManyEstimates) {
  // only allow the test to use 1 Gig of RAM
  auto allocator = ad_utility::AllocatorWithLimit<Id>{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1ull << 30ul)};
  auto t =
      SortPerformanceEstimator{allocator, std::numeric_limits<size_t>::max()};

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
        using ad_utility::Timer;
        Timer::Duration measurement =
            SortPerformanceEstimator::measureSortingTime(i, numColumns,
                                                         allocator);
        Timer::Duration estimate = t.estimatedSortTime(i, numColumns);
        LOG(INFO) << std::fixed << std::setprecision(3) << "input of size " << i
                  << "with " << numColumns << " columns took "
                  << Timer::toSeconds(measurement) << " seconds, estimate was "
                  << Timer::toSeconds(estimate) << " seconds" << std::endl;
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