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
#include "util/OrderedResultMailbox.h"

using ad_utility::OrderedResultMailbox;
using namespace std::chrono_literals;

namespace {
// A tick function that does nothing.
auto noTick = []() {};
}  // namespace

// Test that results which are delivered in an arbitrary order are taken in
// the order of the slots.
TEST(OrderedResultMailbox, deliverOutOfOrderTakeInOrder) {
  OrderedResultMailbox<std::string> mailbox;
  mailbox.setNumSlots(3);
  mailbox.deliver(2, "two");
  mailbox.deliver(0, "zero");
  mailbox.deliver(1, "one");
  EXPECT_EQ(mailbox.take(0, 1ms, noTick), "zero");
  EXPECT_EQ(mailbox.take(1, 1ms, noTick), "one");
  EXPECT_EQ(mailbox.take(2, 1ms, noTick), "two");
  EXPECT_FALSE(mailbox.stopRequested());
  EXPECT_NO_THROW(mailbox.rethrowIfException());
}

// Test that `take` waits for a result that is delivered by another thread,
// and calls the tick function while it waits.
TEST(OrderedResultMailbox, takeWaitsAndTicks) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(1);
  std::atomic<size_t> numTicks = 0;
  std::thread deliverer{[&mailbox, &numTicks]() {
    // Wait until the consumer has ticked a few times.
    while (numTicks.load() < 3) {
      std::this_thread::sleep_for(1ms);
    }
    mailbox.deliver(0, 42);
  }};
  EXPECT_EQ(mailbox.take(0, 1ms, [&numTicks]() { ++numTicks; }), 42);
  deliverer.join();
  EXPECT_GE(numTicks.load(), 3u);
}

// Test that a slot cannot be delivered twice.
TEST(OrderedResultMailbox, deliverTwiceThrows) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(2);
  mailbox.deliver(1, 3);
  EXPECT_ANY_THROW(mailbox.deliver(1, 4));
  EXPECT_ANY_THROW(mailbox.deliver(2, 4));
}

// Test that only the first stored exception is kept and rethrown by `take`,
// also for slots that are done, and that a stop is requested.
TEST(OrderedResultMailbox, storeExceptionStopsAndRethrows) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(2);
  EXPECT_FALSE(mailbox.storeException(nullptr));
  EXPECT_FALSE(mailbox.stopRequested());
  mailbox.deliver(0, 1);
  EXPECT_TRUE(mailbox.storeException(
      std::make_exception_ptr(std::runtime_error{"first"})));
  EXPECT_TRUE(mailbox.storeException(
      std::make_exception_ptr(std::runtime_error{"second"})));
  EXPECT_TRUE(mailbox.stopRequested());
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.take(0, 1ms, noTick),
                               ::testing::StrEq("first"));
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.take(1, 1ms, noTick),
                               ::testing::StrEq("first"));
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.rethrowIfException(),
                               ::testing::StrEq("first"));
}

// Test that an exception which is stored by another thread wakes up a waiting
// consumer.
TEST(OrderedResultMailbox, exceptionWakesUpTheConsumer) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(1);
  std::thread thrower{[&mailbox]() {
    std::this_thread::sleep_for(5ms);
    mailbox.storeException(
        std::make_exception_ptr(std::runtime_error{"failed"}));
  }};
  // The long tick interval makes sure that the consumer is woken up by the
  // notification, not by the timeout.
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.take(0, 1h, noTick),
                               ::testing::StrEq("failed"));
  thrower.join();
}

// Test that a stop without an exception makes `take` fail, even if the slot
// itself is done, and also for a slot without a result.
TEST(OrderedResultMailbox, stopMakesTakeFail) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(3);
  mailbox.deliver(0, 1);
  mailbox.deliver(1, std::nullopt);
  mailbox.requestStop();
  EXPECT_TRUE(mailbox.stopRequested());
  EXPECT_NO_THROW(mailbox.rethrowIfException());
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.take(0, 1ms, noTick),
                               ::testing::HasSubstr("stopped before slot 0"));
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.take(1, 1ms, noTick),
                               ::testing::HasSubstr("stopped before slot 1"));
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.take(2, 1ms, noTick),
                               ::testing::HasSubstr("stopped before slot 2"));
}

// Test that a slot without a result cannot be taken, even without a stop.
TEST(OrderedResultMailbox, takeSlotWithoutResultThrows) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(1);
  mailbox.deliver(0, std::nullopt);
  EXPECT_ANY_THROW(mailbox.take(0, 1ms, noTick));
}
