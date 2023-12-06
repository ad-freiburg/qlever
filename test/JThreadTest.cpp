//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "util/jthread.h"

using ad_utility::JThread;

TEST(JThread, ensureJoinOnDestruction) {
#ifdef __cpp_lib_jthread
  GTEST_SKIP_("std::jthread does not require unit tests");
#endif
  std::atomic_bool flag = false;
  {
    JThread thread{[&flag]() {
      // Make sure the thread doesn't end before even attempting to join
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
      flag = true;
    }};
  }
  EXPECT_TRUE(flag);
}

// _____________________________________________________________________________
TEST(JThread, ensureDefaultConstructedObjectIsDestroyedNormally) {
#ifdef __cpp_lib_jthread
  GTEST_SKIP_("std::jthread does not require unit tests");
#endif
  JThread thread{};
}

// _____________________________________________________________________________
TEST(JThread, ensureCorrectMoveSemantics) {
#ifdef __cpp_lib_jthread
  GTEST_SKIP_("std::jthread does not require unit tests");
#endif
  std::atomic_bool flag1 = false;
  std::atomic_bool flag2 = false;
  {
    JThread thread{[&flag1]() {
      // Make sure the thread doesn't end before even attempting to join
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
      flag1 = true;
    }};

    thread = JThread{[&flag2]() {
      // Make sure the thread doesn't end before even attempting to join
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
      flag2 = true;
    }};
    EXPECT_TRUE(flag1);
  }
  EXPECT_TRUE(flag2);
}
