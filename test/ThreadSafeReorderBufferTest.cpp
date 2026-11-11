// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include "./util/GTestHelpers.h"
#include "util/ThreadSafeReorderBuffer.h"
#include "util/TransparentFunctors.h"

using ad_utility::ThreadSafeReorderBuffer;
using namespace std::chrono_literals;

// Test that results which are delivered in an arbitrary order are taken in
// the order of the slots, and that the end is signalled by `std::nullopt`.
TEST(ThreadSafeReorderBuffer, deliverOutOfOrderTakeInOrder) {
  ThreadSafeReorderBuffer<std::string> buffer;
  buffer.setNumSlots(3);
  buffer.deliver(2, "two");
  buffer.deliver(0, "zero");
  buffer.deliver(1, "one");
  EXPECT_EQ(buffer.takeNext(1ms, ad_utility::noop), "zero");
  EXPECT_EQ(buffer.takeNext(1ms, ad_utility::noop), "one");
  EXPECT_EQ(buffer.takeNext(1ms, ad_utility::noop), "two");
  EXPECT_EQ(buffer.takeNext(1ms, ad_utility::noop), std::nullopt);
  EXPECT_EQ(buffer.takeNext(1ms, ad_utility::noop), std::nullopt);
  EXPECT_FALSE(buffer.stopRequested());
  EXPECT_NO_THROW(buffer.rethrowIfException());
}

// Test that a buffer without slots is immediately at its end.
TEST(ThreadSafeReorderBuffer, noSlots) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(0);
  EXPECT_EQ(buffer.takeNext(1h, ad_utility::noop), std::nullopt);
}

// Test that `takeNext` waits for a result that is delivered by another thread,
// and calls the tick function while it waits.
TEST(ThreadSafeReorderBuffer, takeWaitsAndTicks) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(1);
  std::atomic<size_t> numTicks = 0;
  std::thread deliverer{[&buffer, &numTicks]() {
    // Wait until the consumer has ticked a few times.
    while (numTicks.load() < 3) {
      std::this_thread::sleep_for(1ms);
    }
    buffer.deliver(0, 42);
  }};
  EXPECT_EQ(buffer.takeNext(1ms, [&numTicks]() { ++numTicks; }), 42);
  deliverer.join();
  EXPECT_GE(numTicks.load(), 3u);
}

// Test that the delivery of the slot that the consumer waits for wakes it up,
// even if slots after it are delivered first.
TEST(ThreadSafeReorderBuffer, deliveryWakesUpTheConsumer) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(2);
  std::thread deliverer{[&buffer]() {
    buffer.deliver(1, 1);
    std::this_thread::sleep_for(5ms);
    buffer.deliver(0, 0);
  }};
  // The long tick interval makes sure that the consumer is woken up by the
  // notification, not by the timeout.
  EXPECT_EQ(buffer.takeNext(1h, ad_utility::noop), 0);
  EXPECT_EQ(buffer.takeNext(1h, ad_utility::noop), 1);
  EXPECT_EQ(buffer.takeNext(1h, ad_utility::noop), std::nullopt);
  deliverer.join();
}

// Test that each slot can be delivered only once, also after it has been
// taken, and that slots out of range cannot be delivered.
TEST(ThreadSafeReorderBuffer, deliverTwiceThrows) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(2);
  buffer.deliver(1, 3);
  EXPECT_ANY_THROW(buffer.deliver(1, 4));
  EXPECT_ANY_THROW(buffer.deliver(2, 4));
  buffer.deliver(0, 5);
  EXPECT_EQ(buffer.takeNext(1ms, ad_utility::noop), 5);
  EXPECT_ANY_THROW(buffer.deliver(0, 6));
}

// Test that only the first stored exception is kept and rethrown by
// `takeNext`, also if the next slot is delivered, and that a stop is
// requested.
TEST(ThreadSafeReorderBuffer, storeExceptionStopsAndRethrows) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(2);
  EXPECT_FALSE(buffer.storeException(nullptr));
  EXPECT_FALSE(buffer.stopRequested());
  buffer.deliver(0, 1);
  EXPECT_TRUE(buffer.storeException(
      std::make_exception_ptr(std::runtime_error{"first"})));
  EXPECT_TRUE(buffer.storeException(
      std::make_exception_ptr(std::runtime_error{"second"})));
  EXPECT_TRUE(buffer.stopRequested());
  AD_EXPECT_THROW_WITH_MESSAGE(buffer.takeNext(1ms, ad_utility::noop),
                               ::testing::StrEq("first"));
  AD_EXPECT_THROW_WITH_MESSAGE(buffer.takeNext(1ms, ad_utility::noop),
                               ::testing::StrEq("first"));
  AD_EXPECT_THROW_WITH_MESSAGE(buffer.rethrowIfException(),
                               ::testing::StrEq("first"));
}

// Test that an exception which is stored by another thread wakes up a waiting
// consumer.
TEST(ThreadSafeReorderBuffer, exceptionWakesUpTheConsumer) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(1);
  std::thread thrower{[&buffer]() {
    std::this_thread::sleep_for(5ms);
    buffer.storeException(
        std::make_exception_ptr(std::runtime_error{"failed"}));
  }};
  // The long tick interval makes sure that the consumer is woken up by the
  // notification, not by the timeout.
  AD_EXPECT_THROW_WITH_MESSAGE(buffer.takeNext(1h, ad_utility::noop),
                               ::testing::StrEq("failed"));
  thrower.join();
}

// Test that a stop without an exception makes `takeNext` throw an
// `ThreadSafeReorderBufferStopped`, even if the next slot is delivered.
TEST(ThreadSafeReorderBuffer, stopMakesTakeThrow) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(2);
  buffer.deliver(0, 1);
  EXPECT_EQ(buffer.takeNext(1ms, ad_utility::noop), 1);
  buffer.deliver(1, 2);
  buffer.requestStop();
  EXPECT_TRUE(buffer.stopRequested());
  EXPECT_NO_THROW(buffer.rethrowIfException());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      buffer.takeNext(1ms, ad_utility::noop),
      ::testing::HasSubstr("stopped before slot 1"),
      ad_utility::ThreadSafeReorderBufferStopped);
}

// Test that a stop which is requested by another thread wakes up a waiting
// consumer.
TEST(ThreadSafeReorderBuffer, stopWakesUpTheConsumer) {
  ThreadSafeReorderBuffer<int> buffer;
  buffer.setNumSlots(1);
  std::thread stopper{[&buffer]() {
    std::this_thread::sleep_for(5ms);
    buffer.requestStop();
  }};
  EXPECT_THROW(buffer.takeNext(1h, ad_utility::noop),
               ad_utility::ThreadSafeReorderBufferStopped);
  stopper.join();
}
