//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>

#include "util/Timer.h"

using ad_utility::TimeoutTimer;
using ad_utility::Timer;
using namespace std::chrono_literals;

// On macOS the timer seems to work, but the `sleep_for` is too imprecise.
#ifndef __APPLE__

void testTime(Timer::Duration duration, size_t msecs,
              std::chrono::milliseconds expected) {
  auto lowerBound = 0.9 * expected;
  auto upperBound = 1.1 * expected + 3ms;
  EXPECT_GE(duration, lowerBound);
  EXPECT_LE(duration, upperBound);

  EXPECT_GE(msecs, lowerBound.count());
  EXPECT_LE(msecs, upperBound.count());

  EXPECT_GE(Timer::toSeconds(duration), 0.001 * lowerBound.count());
  EXPECT_LE(Timer::toSeconds(duration), 0.001 * upperBound.count());
}

void testTime(const ad_utility::Timer& timer,
              std::chrono::milliseconds expected) {
  testTime(timer.value(), timer.msecs(), expected);
}

TEST(Timer, BasicWorkflow) {
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
  ASSERT_EQ(t.msecs(), 0u);
  std::this_thread::sleep_for(5ms);

  // Start leads to a running timer.
  t.start();
  ASSERT_TRUE(t.isRunning());
  testTime(t, 0ms);
}

TEST(Timer, InitiallyStopped) {
  Timer t{Timer::Stopped};
  ASSERT_FALSE(t.isRunning());
  ASSERT_EQ(t.value(), Timer::Duration::zero());
  ASSERT_EQ(t.msecs(), 0u);
  std::this_thread::sleep_for(15ms);
  ASSERT_EQ(t.value(), Timer::Duration::zero());
  ASSERT_EQ(t.msecs(), 0u);

  t.cont();
  std::this_thread::sleep_for(15ms);
  testTime(t, 15ms);
}

TEST(TimeoutTimer, Unlimited) {
  auto timer = TimeoutTimer::unlimited();
  for (size_t i = 0; i < 10; ++i) {
    std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(timer.hasTimedOut());
    ASSERT_NO_THROW(timer.checkTimeoutAndThrow("error1"));
    // When no timeout occurs, the lambda is not executed.
    ASSERT_NO_THROW(
        timer.checkTimeoutAndThrow([]() -> std::string { throw 42; }));
  }
}

TEST(TimeoutTimer, Limited) {
  auto timer = TimeoutTimer{10ms, Timer::Started};
  std::this_thread::sleep_for(5ms);
  ASSERT_FALSE(timer.hasTimedOut());
  ASSERT_NO_THROW(timer.checkTimeoutAndThrow("error1"));
  ASSERT_NO_THROW(timer.checkTimeoutAndThrow([]() { return "error2"; }));
  std::this_thread::sleep_for(7ms);

  ASSERT_TRUE(timer.hasTimedOut());
  ASSERT_THROW(timer.checkTimeoutAndThrow("hi"), ad_utility::TimeoutException);
  try {
    timer.checkTimeoutAndThrow([]() { return "Testing. "; });
    FAIL() << "Expected a timeout exception, but no exception was thrown";
  } catch (const ad_utility::TimeoutException& ex) {
    ASSERT_STREQ(
        ex.what(),
        "Testing. A Timeout occured. The time limit was 0.010 seconds");
  }
}

TEST(TimeBlockAndLog, TimeBlockAndLog) {
  std::string s;
  {
    auto callback = [&s](size_t msecs, std::string_view message) {
      s = absl::StrCat(message, ": ", msecs);
    };
    ad_utility::TimeBlockAndLog t{"message", callback};
    std::this_thread::sleep_for(25ms);
  }
  ASSERT_THAT(s, ::testing::MatchesRegex("message: 2[5-9]"));
}

#endif
