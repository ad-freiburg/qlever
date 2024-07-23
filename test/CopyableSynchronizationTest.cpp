// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "util/CopyableSynchronization.h"

using namespace ad_utility;

// _________________________________________________
TEST(CopyableSynchronization, CopyableMutex) {
  // Not much to test here.
  CopyableMutex m1;
  m1.lock();
  [[maybe_unused]] CopyableMutex m2{m1};
  // m2 is still unlocked.
  EXPECT_TRUE(m2.try_lock());
  m2.unlock();
  m1 = m2;
  // m1 is still locked;
  EXPECT_FALSE(m1.try_lock());
  m1.unlock();
}

// _________________________________________________
TEST(CopyableSynchronization, CopyableAtomic) {
  CopyableAtomic<int> i1 = 42;
  CopyableAtomic<int> i2{i1};
  EXPECT_EQ(i2, 42);
  ++i2;
  EXPECT_EQ(i2, 43);
  EXPECT_EQ(i1, 42);
  i1 = i2;
  EXPECT_EQ(i1, 43);
}
