//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <string_view>
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
TEST(PointerGuard, checkUninitializedDoesntBlock) {
  auto future = runAsynchronously([]() { PointerGuard<int> guard; });
  ASSERT_EQ(future.wait_for(DEFAULT_TIMEOUT), std::future_status::ready)
      << "Destructor did not stop blocking";
}

// ____________________________________________________________________________
TEST(PointerGuard, verifyCorrectBlockingBehaviour) {
  std::shared_ptr<int> ptr = std::make_shared<int>(1337);
  auto future = runAsynchronously([weakPtr = std::weak_ptr{ptr}]() mutable {
    PointerGuard<int> guard;
    guard.set(std::move(weakPtr));
  });

  // Pointer should block here
  ASSERT_EQ(future.wait_for(DEFAULT_TIMEOUT), std::future_status::timeout);

  // Pointer should no longer block
  ptr.reset();
  ASSERT_EQ(future.wait_for(DEFAULT_TIMEOUT), std::future_status::ready);
}

// ____________________________________________________________________________
TEST(PointerGuard, verifyCorrectMoveSemantics) {
  std::shared_ptr<int> ptr = std::make_shared<int>(1337);
  std::shared_ptr<PointerGuard<int>> outerGuard =
      std::make_shared<PointerGuard<int>>();
  auto future1 =
      runAsynchronously([weakPtr = std::weak_ptr{ptr}, outerGuard]() mutable {
        PointerGuard<int> guard;
        guard.set(std::move(weakPtr));
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
