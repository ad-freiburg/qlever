// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "util/Consumer.h"

using ad_utility::Consumer;
using ad_utility::ConsumerImpl;

template <auto make>
constexpr auto makeWrapper =
    [](auto&&... args) { return ad_utility::makeConsumer(make(args...)); };

ConsumerImpl<int> intStateMachineImpl(int initial, int& target) {
  target += initial;

  while (co_await ad_utility::valueWasPushedTag) {
    target += co_await ad_utility::nextValueTag;
  }

  target += initial;
}
auto intStateMachine = makeWrapper<&intStateMachineImpl>;

TEST(ConsumerImpl, IntStateMachine) {
  int target = 0;
  int compare = 0;

  auto z = intStateMachine(42, target);
  compare += 42;
  EXPECT_EQ(target, compare);

  for (int i = 0; i < 2000; ++i) {
    compare += i;
    z(i);
    // EXPECT_EQ(target, compare);
  }

  z.finish();
  compare += 42;
  ASSERT_EQ(target, compare);
}

// _______________________________________________________________________________________
ConsumerImpl<std::string&&> moveStringStateMachineImpl(
    std::string initial, std::vector<std::string>& target) {
  target_back(initial);

  while (co_await ad_utility::valueWasPushedTag) {
    target_back(std::move(co_await ad_utility::nextValueTag));
  }

  target_back(initial);
}
auto moveStringStateMachine = makeWrapper<&moveStringStateMachineImpl>;

TEST(ConsumerImpl, MoveStringStateMachine) {
  std::vector<std::string> target;
  std::vector<std::string> compare;

  auto stateMachine = moveStringStateMachine("hello", target);
  compare_back("hello");
  EXPECT_EQ(target, compare);

  compare_back("alpha");
  std::string str = "alpha";
  // Push an lvalue reference, which the coroutine will move.
  stateMachine(std::move(str));
  EXPECT_TRUE(str.empty());
  EXPECT_EQ(target, compare);

  compare_back("beta");
  str = "beta";
  // Push an rvalue reference, which the coroutine will move.
  stateMachine(std::move(str));
  // also a move
  EXPECT_TRUE(str.empty());
  EXPECT_EQ(target, compare);

  compare_back("gamma");
  // Push a temporary, which the coroutine will also move (but we cannot
  // actually test this).
  stateMachine("gamma");
  EXPECT_EQ(target, compare);

  stateMachine.finish();
  compare_back("hello");
  ASSERT_EQ(target, compare);
}

// _______________________________________________________________________________________
ConsumerImpl<const std::string&> constStringStateMachineImpl(
    std::string initial, std::vector<std::string>& target) {
  target_back(initial);

  while (co_await ad_utility::valueWasPushedTag) {
    target_back(std::move(co_await ad_utility::nextValueTag));
  }

  target_back(initial);
}

auto constStringStateMachine = makeWrapper<&constStringStateMachineImpl>;

TEST(ConsumerImpl, ConstStringStateMachine) {
  std::vector<std::string> target;
  std::vector<std::string> compare;

  auto stateMachine = constStringStateMachine("hello", target);
  compare_back("hello");
  EXPECT_EQ(target, compare);

  compare_back("alpha");
  std::string str = "alpha";
  stateMachine(str);
  // We called `move` on the string, but the const state machine can't actually
  // move.
  EXPECT_EQ(str, "alpha");
  EXPECT_EQ(target, compare);

  compare_back("beta");
  str = "beta";
  stateMachine(std::move(str));
  EXPECT_EQ(str, "beta");
  EXPECT_EQ(target, compare);

  compare_back("gamma");
  stateMachine("gamma");
  EXPECT_EQ(target, compare);

  stateMachine.finish();
  compare_back("hello");
  ASSERT_EQ(target, compare);
}

struct TestException : public std::exception {};

ConsumerImpl<bool> stateMachineWithExceptionsImpl(bool throwInitial,
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
auto stateMachineWithExceptions = makeWrapper<&stateMachineWithExceptionsImpl>;

TEST(ConsumerImpl, StateMachineWithExceptions) {
  EXPECT_THROW(stateMachineWithExceptions(true, false), TestException);

  {
    auto throwOnPush = stateMachineWithExceptions(false, false);
    for (size_t i = 0; i < 120; ++i) {
      throwOnPush(false);
    }
    ASSERT_THROW(throwOnPush(true), TestException);
  }

  {
    auto throwOnEnd = stateMachineWithExceptions(false, true);
    for (size_t i = 0; i < 120; ++i) {
      throwOnEnd(false);
    }
    ASSERT_THROW(throwOnEnd.finish(), TestException);
  }

  // Throwing destructor.
  {
    using T = Consumer<bool>;
    alignas(T) unsigned char buf[sizeof(T)];
    T* throwOnEnd = nullptr;
    auto assign = [&]() {
      throwOnEnd = new (buf) T{ad_utility::makeConsumer(

          stateMachineWithExceptionsImpl(false, true))};
    };
    ASSERT_NO_THROW(assign());
    EXPECT_ANY_THROW(throwOnEnd->~T());
  }
  // No throwing destrutor if it wasn't safe.
  {
    auto dontThrowInDestructor = []() {
      auto throwOnEnd = stateMachineWithExceptions(false, true);
      throw std::runtime_error{"blim"};
    };
    // We neither see the `TestException` from the destructor, Nor does it crash
    // the test executable.
    EXPECT_THROW(dontThrowInDestructor(), std::runtime_error);
  }
}

TEST(ConsumerImpl, DefaultConstructor) {
  // The only thing we can legally do with an default constructed
  // `ConsumerImpl` is to destroy it or to move something in.
  { ad_utility::ConsumerImpl<int> x; }
  {
    auto x = ad_utility::makeConsumer(ConsumerImpl<int>{});
    x.finish();
  }
}
