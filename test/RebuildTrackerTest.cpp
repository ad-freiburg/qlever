// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <utility>

#include "engine/RebuildTracker.h"

using ad_utility::RebuildTracker;
using ::testing::Optional;

// _____________________________________________________________________________
TEST(RebuildTracker, ARebuildIsReportedOnlyWhileItRuns) {
  RebuildTracker tracker;
  // No rebuild has run yet.
  EXPECT_FALSE(tracker.poll().has_value());

  {
    auto runningRebuild = tracker.tryBegin();
    EXPECT_TRUE(runningRebuild.has_value());
    EXPECT_THAT(tracker.poll(), Optional(1u));
    // The closing brace destroys the guard, which is what ends the rebuild.
  }
  EXPECT_FALSE(tracker.poll().has_value());
}

// _____________________________________________________________________________
TEST(RebuildTracker, RebuildsAreNumberedFromOne) {
  RebuildTracker tracker;
  {
    auto first = tracker.tryBegin();
    EXPECT_THAT(tracker.poll(), Optional(1u));
  }
  // Each rebuild gets the next number, so the log can tell them apart.
  auto second = tracker.tryBegin();
  EXPECT_THAT(tracker.poll(), Optional(2u));
}

// _____________________________________________________________________________
TEST(RebuildTracker, ASecondRebuildIsRefusedWhileOneRuns) {
  RebuildTracker tracker;
  {
    auto runningRebuild = tracker.tryBegin();
    ASSERT_TRUE(runningRebuild.has_value());

    auto refused = tracker.tryBegin();
    EXPECT_FALSE(refused.has_value());
    // The refused attempt left the running rebuild untouched.
    EXPECT_THAT(tracker.poll(), Optional(1u));
  }
  // A refused attempt takes no number, so the numbering has no gap in it.
  auto next = tracker.tryBegin();
  EXPECT_THAT(tracker.poll(), Optional(2u));
}

// _____________________________________________________________________________
TEST(RebuildTracker, OnlyTheMovedToGuardEndsTheRebuild) {
  RebuildTracker tracker;
  auto firstRebuild = tracker.tryBegin();
  ASSERT_TRUE(firstRebuild.has_value());
  {
    auto handedOver = std::move(firstRebuild.value());
    EXPECT_THAT(tracker.poll(), Optional(1u));
  }
  // The guard it was handed to ended the rebuild.
  EXPECT_FALSE(tracker.poll().has_value());

  // `firstRebuild` was moved out of, so destroying it must not end anything.
  // It is destroyed by hand rather than by a scope, because the second rebuild
  // has to still be running at that moment.
  auto secondRebuild = tracker.tryBegin();
  ASSERT_THAT(tracker.poll(), Optional(2u));
  firstRebuild.reset();
  EXPECT_THAT(tracker.poll(), Optional(2u));
}
