//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>

#include "util/Timer.h"

using ad_utility::Timer;
using namespace std::chrono_literals;

// On macOS the timer seems to work, but the `sleep_for` is too imprecise.
// That's why we have deactivated all the tests via `GTEST_SKIP` on macOS.

void testTime(Timer::Duration duration, std::chrono::milliseconds msecs,
              std::chrono::milliseconds expected) {
  auto lowerBound = 0.9 * expected;
  auto upperBound = 1.1 * expected + 3ms;
  EXPECT_GE(duration, lowerBound);
  EXPECT_LE(duration, upperBound);

  EXPECT_GE(msecs, lowerBound);
  EXPECT_LE(msecs, upperBound);
}

void testTime(const ad_utility::Timer& timer,
              std::chrono::milliseconds expected) {
  testTime(timer.value(), timer.msecs(), expected);
}

TEST(Timer, BasicWorkflow) {
#ifdef _QLEVER_NO_TIMING_TESTS
  GTEST_SKIP_("because _QLEVER_NO_TIMING_TESTS defined");
#endif
  Timer t{Timer::Started};
  ASSERT_TRUE(t.isRunning());
  std::this_thread::sleep_for(10ms);
  testTime(t, 10ms);

  // After stopping the timer, the value remains unchanged.
  t.stop();
  ASSERT_FALSE(t.isRunning());
  auto v = t.value();
  auto m = t.msecs();
  std::this_thread::sleep_for(10ms);
  ASSERT_EQ(v, t.value());
  ASSERT_EQ(m, t.msecs());

  // Stopping an already stopped timer also is a noop.
  t.stop();
  std::this_thread::sleep_for(5ms);
  ASSERT_FALSE(t.isRunning());
  ASSERT_EQ(v, t.value());
  ASSERT_EQ(m, t.msecs());

  // Continue the timer.
  t.cont();
  ASSERT_TRUE(t.isRunning());
  std::this_thread::sleep_for(15ms);
  // Continuing a running timer is a noop;
  t.cont();
  ASSERT_TRUE(t.isRunning());
  std::this_thread::sleep_for(5ms);
  testTime(t, 30ms);

  // Measure the time after stopping the timer
  t.stop();
  testTime(t, 30ms);

  t.cont();
  // Reset leads to a stopped timer.
  t.reset();
  ASSERT_FALSE(t.isRunning());
  ASSERT_EQ(t.value(), Timer::Duration::zero());
  ASSERT_EQ(t.msecs(), 0ms);
  std::this_thread::sleep_for(5ms);

  // Start leads to a running timer.
  t.start();
  ASSERT_TRUE(t.isRunning());
  testTime(t, 0ms);
}

TEST(Timer, InitiallyStopped) {
#ifdef _QLEVER_NO_TIMING_TESTS
  GTEST_SKIP_("because _QLEVER_NO_TIMING_TESTS defined");
#endif
  Timer t{Timer::Stopped};
  ASSERT_FALSE(t.isRunning());
  ASSERT_EQ(t.value(), Timer::Duration::zero());
  ASSERT_EQ(t.msecs(), 0ms);
  std::this_thread::sleep_for(15ms);
  ASSERT_EQ(t.value(), Timer::Duration::zero());
  ASSERT_EQ(t.msecs(), 0ms);

  t.cont();
  std::this_thread::sleep_for(15ms);
  testTime(t, 15ms);
}

TEST(TimeBlockAndLog, TimeBlockAndLog) {
#ifdef _QLEVER_NO_TIMING_TESTS
  GTEST_SKIP_("because _QLEVER_NO_TIMING_TESTS defined");
#endif
  std::string s;
  {
    auto callback = [&s](std::chrono::milliseconds msecs,
                         std::string_view message) {
      s = absl::StrCat(message, ": ", msecs.count());
    };
    ad_utility::TimeBlockAndLog t{"message", callback};
    std::this_thread::sleep_for(25ms);
  }
  ASSERT_THAT(s, ::testing::MatchesRegex("message: 2[5-9]"));
}
