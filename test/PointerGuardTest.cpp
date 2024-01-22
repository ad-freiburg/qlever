//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <optional>
#include <thread>

#include "util/PointerGuard.h"

using namespace std::chrono_literals;
using ad_utility::PointerGuard;

constexpr std::chrono::milliseconds DEFAULT_TIMEOUT = 10ms;

template <std::invocable Func>
auto runAsynchronously(Func func) {
  std::packaged_task<void()> task{std::move(func)};
  std::future<void> future = task.get_future();

  std::thread{std::move(task)}.detach();

  return future;
}

// ____________________________________________________________________________
TEST(PointerGuard, checkEmptyPointerThrowsException) {
  EXPECT_THROW(PointerGuard<int> guard{nullptr}, ad_utility::Exception);
}

// ____________________________________________________________________________
TEST(PointerGuard, checkGuardProvidesCorrectAccessToReference) {
  PointerGuard<int> guard{std::make_shared<int>(42)};
  EXPECT_EQ(guard.get(), 42);
  EXPECT_EQ(*guard.getWeak().lock(), 42);
}

// ____________________________________________________________________________
TEST(PointerGuard, checkExpiredPointerDoesntBlock) {
  auto future = runAsynchronously(
      []() { PointerGuard<int> guard{std::make_shared<int>(0)}; });
  ASSERT_EQ(future.wait_for(DEFAULT_TIMEOUT), std::future_status::ready)
      << "Destructor of a PointerGuard with expired shared pointer should not "
         "block";
}

// ____________________________________________________________________________
TEST(PointerGuard, verifyCorrectBlockingBehaviour) {
  auto ptr = std::make_shared<int>(1337);
  auto future = runAsynchronously(
      [ptr]() mutable { PointerGuard<int> guard{std::move(ptr)}; });

  // Pointer should block here
  ASSERT_EQ(future.wait_for(DEFAULT_TIMEOUT), std::future_status::timeout);

  // Pointer should no longer block
  ptr.reset();
  ASSERT_EQ(future.wait_for(DEFAULT_TIMEOUT), std::future_status::ready);
}

// ____________________________________________________________________________
TEST(PointerGuard, verifyCorrectMoveSemantics) {
  auto ptr = std::make_shared<int>(1337);
  auto outerGuard = std::make_shared<std::optional<PointerGuard<int>>>();
  auto future1 = runAsynchronously([ptr, outerGuard]() mutable {
    PointerGuard<int> guard{std::move(ptr)};
    *outerGuard = std::move(guard);
    outerGuard.reset();
  });

  ASSERT_EQ(future1.wait_for(DEFAULT_TIMEOUT), std::future_status::ready);
  ASSERT_EQ(outerGuard.use_count(), 1);

  auto future2 = runAsynchronously(
      [outerGuard = std::move(outerGuard)]() mutable { outerGuard.reset(); });
  ASSERT_EQ(future2.wait_for(DEFAULT_TIMEOUT), std::future_status::timeout);

  // Pointer should no longer block
  ptr.reset();
  ASSERT_EQ(future2.wait_for(100ms), std::future_status::ready);
}

static_assert(!std::is_copy_constructible_v<PointerGuard<int>>);
static_assert(!std::is_copy_assignable_v<PointerGuard<int>>);
