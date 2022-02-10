//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/global/Id.h"
#include "../src/util/Log.h"

template <uint64_t distance>
void testOnlyMilestoneIds() {
  ad_utility::MilestoneIdManager<distance> m;
  for (size_t i = 0; i < distance; ++i) {
    auto id = m.getNextMilestoneId();
    ASSERT_EQ(i * distance, id);
    ASSERT_TRUE(m.isMilestoneId(id));
    ASSERT_EQ(i, m.MilestoneIdToLocal(id));
    ASSERT_EQ(id, m.MilestoneIdFromLocal(m.MilestoneIdToLocal(id)));
  }
}

TEST(MilestoneId, RoundedIds) {
  testOnlyMilestoneIds<256>();
  testOnlyMilestoneIds<257>();
  testOnlyMilestoneIds<1024 * 1024>();
  testOnlyMilestoneIds<1024 * 1024 + 53>();
}

template <uint64_t distance>
void testConsecutiveIds() {
  ad_utility::MilestoneIdManager<distance> m;
  for (size_t i = 0; i < 3 * distance; ++i) {
    auto id = m.getNextId();
    ASSERT_EQ(i, id);
    if (id == 0 || id == distance || id == 2 * distance) {
      ASSERT_TRUE(m.isMilestoneId(id));
      ASSERT_EQ(id, m.MilestoneIdFromLocal(m.MilestoneIdToLocal(id)));
    } else {
      ASSERT_FALSE(m.isMilestoneId(id));
      ASSERT_NE(id, m.MilestoneIdFromLocal(m.MilestoneIdToLocal(id)));
    }
  }
}

TEST(MilestoneId, ConsecutiveIds) {
  testConsecutiveIds<256>();
  testConsecutiveIds<257>();
  testConsecutiveIds<1024 * 1024>();
  testConsecutiveIds<1024 * 1024 + 53>();
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
    ASSERT_EQ(m.MilestoneIdToLocal(id), i + 1);
    ASSERT_EQ(id, m.MilestoneIdFromLocal(m.MilestoneIdToLocal(id)));
  }
}

TEST(Id, MixedIds) {
  testMixedIds<256>();
  testMixedIds<257>();
  testMixedIds<1024 * 1024>();
  testMixedIds<1024 * 1024 + 53>();
}
