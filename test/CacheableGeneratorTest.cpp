//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <atomic>

#include "util/CacheableGenerator.h"
#include "util/Generator.h"
#include "util/jthread.h"

using ad_utility::CacheableGenerator;
using cppcoro::generator;
using namespace std::chrono_literals;

generator<uint32_t> testGenerator(uint32_t range) {
  for (uint32_t i = 0; i < range; i++) {
    co_yield i;
  }
}

// _____________________________________________________________________________
TEST(CacheableGenerator, allowsMultiConsumption) {
  CacheableGenerator generator{testGenerator(3)};

  auto iterator1 = generator.begin(true);

  ASSERT_NE(iterator1, generator.end());
  EXPECT_EQ(**iterator1, 0);
  ++iterator1;

  ASSERT_NE(iterator1, generator.end());
  EXPECT_EQ(**iterator1, 1);
  ++iterator1;

  ASSERT_NE(iterator1, generator.end());
  EXPECT_EQ(**iterator1, 2);
  ++iterator1;

  EXPECT_EQ(iterator1, generator.end());

  auto iterator2 = generator.begin(false);

  ASSERT_NE(iterator2, generator.end());
  EXPECT_EQ(**iterator2, 0);
  ++iterator2;

  ASSERT_NE(iterator2, generator.end());
  EXPECT_EQ(**iterator2, 1);
  ++iterator2;

  ASSERT_NE(iterator2, generator.end());
  EXPECT_EQ(**iterator2, 2);
  ++iterator2;
  EXPECT_EQ(iterator2, generator.end());
}

// _____________________________________________________________________________
TEST(CacheableGenerator, masterBlocksSlaves) {
  CacheableGenerator generator{testGenerator(3)};

  // Verify slave is not blocked indefinitely if master has not been started yet
  EXPECT_THROW(generator.begin(false), ad_utility::Exception);

  auto masterIterator = generator.begin(true);
  std::mutex counterMutex;
  std::condition_variable cv;
  std::atomic_int counter = 0;
  uint32_t proceedStage = 0;

  ad_utility::JThread thread1{[&]() {
    auto iterator = generator.begin(false);

    ASSERT_NE(iterator, generator.end());
    {
      std::lock_guard guard{counterMutex};
      EXPECT_EQ(counter, 0);
      proceedStage = 1;
    }
    cv.notify_all();

    EXPECT_EQ(**iterator, 0);
    ++iterator;

    ASSERT_NE(iterator, generator.end());
    {
      std::lock_guard guard{counterMutex};
      EXPECT_EQ(counter, 1);
      proceedStage = 2;
    }
    cv.notify_all();

    EXPECT_EQ(**iterator, 1);
    ++iterator;

    ASSERT_NE(iterator, generator.end());
    {
      std::lock_guard guard{counterMutex};
      EXPECT_EQ(counter, 2);
      proceedStage = 3;
    }
    cv.notify_all();

    EXPECT_EQ(**iterator, 2);
    ++iterator;

    EXPECT_EQ(iterator, generator.end());
    {
      std::lock_guard guard{counterMutex};
      EXPECT_EQ(counter, 3);
    }
  }};

  ad_utility::JThread thread2{[&]() {
    auto iterator = generator.begin(false);

    ASSERT_NE(iterator, generator.end());
    EXPECT_GE(counter, 0);

    EXPECT_EQ(**iterator, 0);
    ++iterator;

    ASSERT_NE(iterator, generator.end());
    EXPECT_GE(counter, 1);

    EXPECT_EQ(**iterator, 1);
    ++iterator;

    ASSERT_NE(iterator, generator.end());
    EXPECT_GE(counter, 2);

    EXPECT_EQ(**iterator, 2);
    ++iterator;

    EXPECT_EQ(iterator, generator.end());
    EXPECT_GE(counter, 3);
  }};

  EXPECT_EQ(**masterIterator, 0);

  {
    std::unique_lock guard{counterMutex};
    cv.wait(guard, [&]() { return proceedStage == 1; });
    ++counter;
    ++masterIterator;
  }
  ASSERT_NE(masterIterator, generator.end());

  EXPECT_EQ(**masterIterator, 1);
  {
    std::unique_lock guard{counterMutex};
    cv.wait(guard, [&]() { return proceedStage == 2; });
    ++counter;
    ++masterIterator;
  }
  ASSERT_NE(masterIterator, generator.end());

  EXPECT_EQ(**masterIterator, 2);
  {
    std::unique_lock guard{counterMutex};
    cv.wait(guard, [&]() { return proceedStage == 3; });
    ++counter;
    ++masterIterator;
  }
  EXPECT_EQ(masterIterator, generator.end());
}

// _____________________________________________________________________________
TEST(CacheableGenerator, verifyExhaustedMasterCausesFreeForAll) {
  CacheableGenerator generator{testGenerator(3)};

  (void)generator.begin(true);

  auto iterator1 = generator.begin(false);
  auto iterator2 = generator.begin(false);

  ASSERT_NE(iterator1, generator.end());
  ASSERT_NE(iterator2, generator.end());

  EXPECT_EQ(**iterator1, 0);
  EXPECT_EQ(**iterator2, 0);

  ++iterator1;
  ASSERT_NE(iterator1, generator.end());
  EXPECT_EQ(**iterator1, 1);

  ++iterator2;
  ASSERT_NE(iterator2, generator.end());
  EXPECT_EQ(**iterator2, 1);

  ++iterator2;
  ASSERT_NE(iterator2, generator.end());
  EXPECT_EQ(**iterator2, 2);

  ++iterator1;
  ASSERT_NE(iterator1, generator.end());
  EXPECT_EQ(**iterator1, 2);

  ++iterator1;
  EXPECT_EQ(iterator1, generator.end());

  ++iterator2;
  EXPECT_EQ(iterator2, generator.end());
}

// _____________________________________________________________________________
TEST(CacheableGenerator, verifyOnSizeChangedIsCalledWithCorrectTimingInfo) {
  auto timedGenerator = []() -> generator<int> {
    while (true) {
#ifndef _QLEVER_NO_TIMING_TESTS
      std::this_thread::sleep_for(2ms);
#endif
      co_yield 0;
    }
  }();

  uint32_t callCounter = 0;

  CacheableGenerator generator{std::move(timedGenerator)};

  generator.setOnSizeChanged([&](auto duration) {
#ifndef _QLEVER_NO_TIMING_TESTS
    using ::testing::AllOf;
    using ::testing::Le;
    using ::testing::Ge;
    EXPECT_THAT(duration, AllOf(Le(3ms), Ge(1ms)));
#endif
    ++callCounter;
  });

  {
    auto masterIterator = generator.begin(true);
    EXPECT_EQ(callCounter, 1);
    ASSERT_NE(masterIterator, generator.end());

    ++masterIterator;

    EXPECT_EQ(callCounter, 2);
    ASSERT_NE(masterIterator, generator.end());
  }

  {
    auto slaveIterator1 = generator.begin();
    EXPECT_EQ(callCounter, 2);
    ASSERT_NE(slaveIterator1, generator.end());

    auto slaveIterator2 = generator.begin();
    EXPECT_EQ(callCounter, 2);
    ASSERT_NE(slaveIterator2, generator.end());

    ++slaveIterator2;

    EXPECT_EQ(callCounter, 2);
    ASSERT_NE(slaveIterator2, generator.end());

    ++slaveIterator2;

    EXPECT_EQ(callCounter, 3);
    ASSERT_NE(slaveIterator2, generator.end());

    ++slaveIterator1;

    EXPECT_EQ(callCounter, 3);
    ASSERT_NE(slaveIterator1, generator.end());

    ++slaveIterator1;

    EXPECT_EQ(callCounter, 3);
    ASSERT_NE(slaveIterator1, generator.end());

    ++slaveIterator1;

    EXPECT_EQ(callCounter, 4);
    ASSERT_NE(slaveIterator1, generator.end());
  }

  auto slaveIterator3 = generator.begin();
  EXPECT_EQ(callCounter, 4);
  ASSERT_NE(slaveIterator3, generator.end());

  ++slaveIterator3;

  EXPECT_EQ(callCounter, 4);
  ASSERT_NE(slaveIterator3, generator.end());

  ++slaveIterator3;

  EXPECT_EQ(callCounter, 4);
  ASSERT_NE(slaveIterator3, generator.end());

  ++slaveIterator3;

  EXPECT_EQ(callCounter, 4);
  ASSERT_NE(slaveIterator3, generator.end());

  ++slaveIterator3;

  EXPECT_EQ(callCounter, 5);
  ASSERT_NE(slaveIterator3, generator.end());
}

// _____________________________________________________________________________
TEST(CacheableGenerator, verifyOnSizeChangedIsCalledAndRespectsShrink) {
  CacheableGenerator generator{testGenerator(3)};
  uint32_t callCounter = 0;
  generator.setOnSizeChanged([&](auto) { ++callCounter; });

  auto iterator = generator.begin(true);
  EXPECT_EQ(callCounter, 1);
  ASSERT_NE(iterator, generator.end());

  auto slaveIterator1 = generator.begin();
  EXPECT_EQ(callCounter, 1);
  ASSERT_NE(slaveIterator1, generator.end());
  EXPECT_EQ(**slaveIterator1, 0);

  ++iterator;
  EXPECT_EQ(callCounter, 2);
  ASSERT_NE(iterator, generator.end());

  generator.setMaxSize(1);

  ++slaveIterator1;
  EXPECT_EQ(callCounter, 2);
  ASSERT_NE(slaveIterator1, generator.end());
  EXPECT_EQ(**slaveIterator1, 1);

  auto slaveIterator2 = generator.begin();
  EXPECT_EQ(callCounter, 2);
  ASSERT_NE(slaveIterator2, generator.end());
  EXPECT_EQ(**slaveIterator2, 0);

  ++iterator;
  EXPECT_EQ(callCounter, 5);
  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(**iterator, 2);

  ++iterator;
  EXPECT_EQ(callCounter, 5);
  EXPECT_EQ(iterator, generator.end());

  ++slaveIterator1;
  ASSERT_NE(slaveIterator1, generator.end());
  EXPECT_EQ(**slaveIterator1, 2);

  EXPECT_THROW(++slaveIterator2, ad_utility::IteratorExpired);
}

// _____________________________________________________________________________
TEST(CacheableGenerator, verifyShrinkKeepsSingleElement) {
  CacheableGenerator generator{testGenerator(3)};
  uint32_t callCounter = 0;
  generator.setOnSizeChanged([&](auto) { ++callCounter; });

  auto iterator = generator.begin(true);
  EXPECT_EQ(callCounter, 1);
  ASSERT_NE(iterator, generator.end());

  auto slaveIterator = generator.begin();
  EXPECT_EQ(callCounter, 1);
  ASSERT_NE(slaveIterator, generator.end());

  ++iterator;
  EXPECT_EQ(callCounter, 2);
  ASSERT_NE(iterator, generator.end());

  generator.setMaxSize(0);

  ++slaveIterator;
  EXPECT_EQ(callCounter, 2);
  ASSERT_NE(slaveIterator, generator.end());

  ++iterator;
  EXPECT_EQ(callCounter, 5);
  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(**iterator, 2);

  ++iterator;
  EXPECT_EQ(callCounter, 5);
  EXPECT_EQ(iterator, generator.end());

  ++slaveIterator;
  ASSERT_NE(slaveIterator, generator.end());
  EXPECT_EQ(**slaveIterator, 2);
}

// _____________________________________________________________________________
TEST(CacheableGenerator, verifySlavesCantBlockMasterIterator) {
  CacheableGenerator generator{testGenerator(3)};
  generator.setMaxSize(1);

  auto masterIterator = generator.begin(true);
  ASSERT_NE(masterIterator, generator.end());
  EXPECT_EQ(**masterIterator, 0);

  auto slaveIterator = generator.begin(false);
  ASSERT_NE(slaveIterator, generator.end());
  EXPECT_EQ(**slaveIterator, 0);

  ++masterIterator;
  ASSERT_NE(masterIterator, generator.end());
  EXPECT_EQ(**masterIterator, 1);

  ++masterIterator;
  ASSERT_NE(masterIterator, generator.end());
  EXPECT_EQ(**masterIterator, 2);

  EXPECT_THROW(**slaveIterator, ad_utility::IteratorExpired);

  ++masterIterator;
  EXPECT_EQ(masterIterator, generator.end());
}
