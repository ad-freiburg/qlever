//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/strings/str_format.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "engine/SortPerformanceEstimator.h"
#include "util/Log.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"

using namespace ad_utility::memory_literals;

TEST(SortPerformanceEstimator, TestManyEstimates) {
  // only allow the test to use 1 Gig of RAM
  auto allocator = qlever::makeAllocatorWithLimit<Id>(1_GB);
  auto t =
      SortPerformanceEstimator{allocator, std::numeric_limits<size_t>::max()};

  ad_utility::SlowRandomIntGenerator<int> dice(1, 6);

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
        // Only report the measurement in the case of a failure.
        auto message = [&i, &numColumns, &measurement, &estimate]() {
          return absl::StrFormat(
              "Input of size %d with %d columns took %.3f seconds, estimate "
              "was %.3f seconds.",
              i, numColumns, Timer::toSeconds(measurement),
              Timer::toSeconds(estimate));
        };
        ASSERT_GE(2 * measurement, estimate) << message();
        if (!isFirst) {
          EXPECT_LE(0.5 * measurement, estimate) << message();
        } else if (0.5 * measurement > estimate) {
          AD_LOG_WARN
              << "The first measurement with a new column size took "
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
