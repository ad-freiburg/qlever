//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/CoroToStateMachine.h"
#include "../src/util/Log.h"

using ad_utility::CoroToStateMachine;

CoroToStateMachine<int, false>intStateMachine(int initial, int& target) {
  target += initial;

  while (co_await ad_utility::valueWasPushedTag) {
    target += co_await ad_utility::nextValueTag;
  }

  target += initial;
}

TEST(CoroToStateMachine, IntStateMachine) {
  int target = 0;
  int compare = 0;

  auto z = intStateMachine(42, target);
  compare += 42;
  EXPECT_EQ(target, compare);

  for (int i = 0; i < 2000; ++i) {
    compare += i;
    z.push(i);
    //EXPECT_EQ(target, compare);
  }

  z.finish();
  compare += 42;
  ASSERT_EQ(target, compare);
}

// _______________________________________________________________________________________
CoroToStateMachine<std::string, false> moveStringStateMachine(
    std::string initial, std::vector<std::string>& target) {
  target.push_back(initial);

  while (co_await ad_utility::valueWasPushedTag) {
    target.push_back(std::move(co_await ad_utility::nextValueTag));
  }

  target.push_back(initial);
}

TEST(CoroToStateMachine, MoveStringStateMachine) {
  std::vector<std::string> target;
  std::vector<std::string> compare;

  auto stateMachine = moveStringStateMachine("hello", target);
  compare.push_back("hello");
  EXPECT_EQ(target, compare);

  compare.push_back("alpha");
  std::string str = "alpha";
  // Push an lvalue reference, which the coroutine will move.
  stateMachine.push(str);
  EXPECT_TRUE(str.empty());
  EXPECT_EQ(target, compare);

  compare.push_back("beta");
  str = "beta";
  // Push an rvalue reference, which the coroutine will move.
  stateMachine.push(std::move(str));
  // also a move
  EXPECT_TRUE(str.empty());
  EXPECT_EQ(target, compare);

  compare.push_back("gamma");
  // Push a temporary, which the coroutine will also move (but we cannot actually test this).
  stateMachine.push("gamma");
  EXPECT_EQ(target, compare);

  stateMachine.finish();
  compare.push_back("hello");
  ASSERT_EQ(target, compare);
}

// _______________________________________________________________________________________
CoroToStateMachine<std::string, true> constStringStateMachine(
    std::string initial, std::vector<std::string>& target) {
  target.push_back(initial);

  while (co_await ad_utility::valueWasPushedTag) {
    target.push_back(std::move(co_await ad_utility::nextValueTag));
  }

  target.push_back(initial);
}

TEST(CoroToStateMachine, ConstStringStateMachine) {
  std::vector<std::string> target;
  std::vector<std::string> compare;

  auto stateMachine = constStringStateMachine("hello", target);
  compare.push_back("hello");
  EXPECT_EQ(target, compare);

  compare.push_back("alpha");
  std::string str = "alpha";
  stateMachine.push(str);
  // We called `move` on the string, but the const state machine can't actually
  // move.
  EXPECT_EQ(str, "alpha");
  EXPECT_EQ(target, compare);

  compare.push_back("beta");
  str = "beta";
  stateMachine.push(std::move(str));
  EXPECT_EQ(str, "beta");
  EXPECT_EQ(target, compare);

  compare.push_back("gamma");
  stateMachine.push("gamma");
  EXPECT_EQ(target, compare);

  stateMachine.finish();
  compare.push_back("hello");
  ASSERT_EQ(target, compare);
}

struct TestException : public std::exception {};

CoroToStateMachine<bool> stateMachineWithExceptions(bool throwInitial,
                                                    bool throwFinal) {
  if (throwInitial) {
    throw TestException{};
  }

  while (co_await ad_utility::valueWasPushedTag) {
    // `push(true)` will throw a `TestException`
    if (co_await ad_utility::nextValueTag) {
      throw TestException{};
    }
  }

  if (throwFinal) {
    throw TestException{};
  }
}

TEST(CoroToStateMachine, StateMachineWithExceptions) {
  ASSERT_THROW(stateMachineWithExceptions(true, false), TestException);

  {
    auto throwOnPush = stateMachineWithExceptions(false, false);
    for (size_t i = 0; i < 120; ++i) {
      throwOnPush.push(false);
    }
    ASSERT_THROW(throwOnPush.push(true), TestException);
  }

  {
    auto throwOnEnd = stateMachineWithExceptions(false, true);
    for (size_t i = 0; i < 120; ++i) {
      throwOnEnd.push(false);
    }
    ASSERT_THROW(throwOnEnd.finish(), TestException);
  }
}

TEST(CoroToStateMachine, DefaultConstructor) {
  // The only thing we can legally do with an default constructed
  // `CoroToStateMachine` is to destroy it or to move something in.
  { ad_utility::CoroToStateMachine<int> x; }
  {
    ad_utility::CoroToStateMachine<int> x;
    x.finish();
  }
}

ad_utility::CoroToStateMachine<int> simpleStateMachine(int& result) {
  while (co_await ad_utility::valueWasPushedTag) {
    result = co_await ad_utility::nextValueTag;
  }
  result = 0;
}

TEST(CoroToStateMachine, MoveAssignment) {
  {
    int target = 0;
    ad_utility::CoroToStateMachine<int> a;
    {
      auto b = simpleStateMachine(target);
      b.push(42);
      ASSERT_EQ(target, 42);
      a = std::move(b);
      ASSERT_EQ(target, 42);
      a.push(12);
      ASSERT_EQ(target, 12);
      b.finish();
      ASSERT_EQ(target, 12);
    }
    ASSERT_EQ(target, 12);
    a.push(15);
    ASSERT_EQ(target, 15);
    a.finish();
    ASSERT_EQ(target, 0);
  }
}

TEST(CoroToStateMachine, MoveConstructor) {
  {
    int target = 0;
    {
      auto b = simpleStateMachine(target);
      b.push(42);
      ASSERT_EQ(target, 42);
      auto a{std::move(b)};
      ASSERT_EQ(target, 42);
      a.push(12);
      ASSERT_EQ(target, 12);
      b.finish();
      ASSERT_EQ(target, 12);
      ASSERT_EQ(target, 12);
      a.push(15);
      ASSERT_EQ(target, 15);
      a.finish();
      ASSERT_EQ(target, 0);
    }
  }
}

TEST(CoroToStateMachine, Swap) {
  {
    int target = 0;
    int target2 = 0;
    {
      auto a = simpleStateMachine(target);
      auto b = simpleStateMachine(target2);
      ASSERT_EQ(target, 0);
      ASSERT_EQ(target2, 0);
      b.push(42);
      ASSERT_EQ(target, 0);
      ASSERT_EQ(target2, 42);
      a.push(19);
      ASSERT_EQ(target, 19);
      ASSERT_EQ(target2, 42);
      std::swap(a, b);
      a.push(20);
      ASSERT_EQ(target, 19);
      ASSERT_EQ(target2, 20);
      b.push(3);
      ASSERT_EQ(target, 3);
      ASSERT_EQ(target2, 20);
      b.finish();
      ASSERT_EQ(target, 0);
      ASSERT_EQ(target2, 20);
    }
    ASSERT_EQ(target, 0);
    ASSERT_EQ(target2, 0);
  }
}
