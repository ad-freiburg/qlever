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
// the order of the slots, and that the end is signalled by `std::nullopt`.
TEST(OrderedResultMailbox, deliverOutOfOrderTakeInOrder) {
  OrderedResultMailbox<std::string> mailbox;
  mailbox.setNumSlots(3);
  mailbox.deliver(2, "two");
  mailbox.deliver(0, "zero");
  mailbox.deliver(1, "one");
  EXPECT_EQ(mailbox.takeNext(1ms, noTick), "zero");
  EXPECT_EQ(mailbox.takeNext(1ms, noTick), "one");
  EXPECT_EQ(mailbox.takeNext(1ms, noTick), "two");
  EXPECT_EQ(mailbox.takeNext(1ms, noTick), std::nullopt);
  EXPECT_EQ(mailbox.takeNext(1ms, noTick), std::nullopt);
  EXPECT_FALSE(mailbox.stopRequested());
  EXPECT_NO_THROW(mailbox.rethrowIfException());
}

// Test that a mailbox without slots is immediately at its end.
TEST(OrderedResultMailbox, noSlots) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(0);
  EXPECT_EQ(mailbox.takeNext(1h, noTick), std::nullopt);
}

// Test that `takeNext` waits for a result that is delivered by another thread,
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
  EXPECT_EQ(mailbox.takeNext(1ms, [&numTicks]() { ++numTicks; }), 42);
  deliverer.join();
  EXPECT_GE(numTicks.load(), 3u);
}

// Test that the delivery of the slot that the consumer waits for wakes it up,
// even if slots after it are delivered first.
TEST(OrderedResultMailbox, deliveryWakesUpTheConsumer) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(2);
  std::thread deliverer{[&mailbox]() {
    mailbox.deliver(1, 1);
    std::this_thread::sleep_for(5ms);
    mailbox.deliver(0, 0);
  }};
  // The long tick interval makes sure that the consumer is woken up by the
  // notification, not by the timeout.
  EXPECT_EQ(mailbox.takeNext(1h, noTick), 0);
  EXPECT_EQ(mailbox.takeNext(1h, noTick), 1);
  EXPECT_EQ(mailbox.takeNext(1h, noTick), std::nullopt);
  deliverer.join();
}

// Test that each slot can be delivered only once, also after it has been
// taken, and that slots out of range cannot be delivered.
TEST(OrderedResultMailbox, deliverTwiceThrows) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(2);
  mailbox.deliver(1, 3);
  EXPECT_ANY_THROW(mailbox.deliver(1, 4));
  EXPECT_ANY_THROW(mailbox.deliver(2, 4));
  mailbox.deliver(0, 5);
  EXPECT_EQ(mailbox.takeNext(1ms, noTick), 5);
  EXPECT_ANY_THROW(mailbox.deliver(0, 6));
}

// Test that only the first stored exception is kept and rethrown by
// `takeNext`, also if the next slot is delivered, and that a stop is
// requested.
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
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.takeNext(1ms, noTick),
                               ::testing::StrEq("first"));
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.takeNext(1ms, noTick),
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
  AD_EXPECT_THROW_WITH_MESSAGE(mailbox.takeNext(1h, noTick),
                               ::testing::StrEq("failed"));
  thrower.join();
}

// Test that a stop without an exception makes `takeNext` throw an
// `OrderedResultMailboxStopped`, even if the next slot is delivered.
TEST(OrderedResultMailbox, stopMakesTakeThrow) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(2);
  mailbox.deliver(0, 1);
  EXPECT_EQ(mailbox.takeNext(1ms, noTick), 1);
  mailbox.deliver(1, 2);
  mailbox.requestStop();
  EXPECT_TRUE(mailbox.stopRequested());
  EXPECT_NO_THROW(mailbox.rethrowIfException());
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      mailbox.takeNext(1ms, noTick),
      ::testing::HasSubstr("stopped before slot 1"),
      ad_utility::OrderedResultMailboxStopped);
}

// Test that a stop which is requested by another thread wakes up a waiting
// consumer.
TEST(OrderedResultMailbox, stopWakesUpTheConsumer) {
  OrderedResultMailbox<int> mailbox;
  mailbox.setNumSlots(1);
  std::thread stopper{[&mailbox]() {
    std::this_thread::sleep_for(5ms);
    mailbox.requestStop();
  }};
  EXPECT_THROW(mailbox.takeNext(1h, noTick),
               ad_utility::OrderedResultMailboxStopped);
  stopper.join();
}
