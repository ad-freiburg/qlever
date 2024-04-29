//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "global/Id.h"
#include "util/Log.h"

template <uint64_t distance>
void testMilestoneIds() {
  ad_utility::MilestoneIdManager<distance> m;
  for (size_t i = 0; i < distance; ++i) {
    auto id = m.getNextMilestoneId();
    ASSERT_EQ(i * distance, id);
    ASSERT_TRUE(m.isMilestoneId(id));
    ASSERT_EQ(i, m.milestoneIdToLocal(id));
    ASSERT_EQ(id, m.milestoneIdFromLocal(m.milestoneIdToLocal(id)));
  }
}

TEST(MilestoneId, OnlyMilestoneIds) {
  testMilestoneIds<256>();
  testMilestoneIds<257>();
#ifdef QLEVER_RUN_EXPENSIVE_TESTS
  testMilestoneIds<1024 * 1024>();
  testMilestoneIds<1024 * 1024 + 53>();
#endif
}

template <uint64_t distance>
void testConsecutiveIds() {
  ad_utility::MilestoneIdManager<distance> m;
  for (size_t i = 0; i < 3 * distance; ++i) {
    auto id = m.getNextId();
    ASSERT_EQ(i, id);
    if (id == 0 || id == distance || id == 2 * distance) {
      ASSERT_TRUE(m.isMilestoneId(id));
      ASSERT_EQ(id, m.milestoneIdFromLocal(m.milestoneIdToLocal(id)));
    } else {
      ASSERT_FALSE(m.isMilestoneId(id));
      ASSERT_NE(id, m.milestoneIdFromLocal(m.milestoneIdToLocal(id)));
    }
  }
}

TEST(MilestoneId, ConsecutiveIds) {
  testConsecutiveIds<256>();
  testConsecutiveIds<257>();

#ifdef QLEVER_RUN_EXPENSIVE_TESTS
  testConsecutiveIds<1024 * 1024>();
  testConsecutiveIds<1024 * 1024 + 53>();
#endif
}

template <uint64_t distance>
void testMixedIds() {
  ad_utility::MilestoneIdManager<distance> m;
  m.getNextId();
  for (size_t i = 0; i < 680; ++i) {
    for (size_t j = 0; j < 123; ++j) {
      auto id = m.getNextId();
      ASSERT_EQ(i * distance + j + 1, id);
    }
    auto id = m.getNextMilestoneId();
    ASSERT_TRUE(m.isMilestoneId(id));
    ASSERT_EQ(id, (i + 1) * distance);
    ASSERT_EQ(m.milestoneIdToLocal(id), i + 1);
    ASSERT_EQ(id, m.milestoneIdFromLocal(m.milestoneIdToLocal(id)));
  }
}

TEST(MilestoneId, MixedIds) {
  testMixedIds<256>();
  testMixedIds<257>();
  testMixedIds<1024 * 1024>();
  testMixedIds<1024 * 1024 + 53>();
}

TEST(MilestoneId, Overflow) {
  constexpr static uint64_t distance = 1ull << 63;
  auto m = ad_utility::MilestoneIdManager<distance>{};
  auto id = m.getNextMilestoneId();
  ASSERT_EQ(id, 0u);
  id = m.getNextMilestoneId();
  ASSERT_EQ(id, distance);
  ASSERT_THROW(m.getNextMilestoneId(),
               ad_utility::MilestoneIdOverflowException);
}
