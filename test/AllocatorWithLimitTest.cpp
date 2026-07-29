//
// Created by johannes on 27.04.20.
//

#include <stdexcept>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/AllocatorWithLimit.h"

// This is a white-box test of the limit-enforcing allocator implementation, so
// it always targets `ad_utility::allocatorImpl::AllocatorWithLimit` directly
// (rather than the public `ad_utility::AllocatorWithLimit` name, which is an
// alias for the compile-time selected backend and may be the PMR allocator).
using ad_utility::makeAllocationMemoryLeftThreadsafeObject;
using ad_utility::allocatorImpl::AllocatorWithLimit;
using namespace ad_utility::memory_literals;

TEST(AllocatorWithLimit, unlikelyExceptionsDuringCopyingAndMoving) {
  struct ThrowOnCopy {
    ThrowOnCopy() = default;
    ThrowOnCopy& operator=(const ThrowOnCopy&) {
      throw std::runtime_error("unexpected copy assign");
    }
    ThrowOnCopy(const ThrowOnCopy&) {
      throw std::runtime_error("unexpected copy construct");
    }
    ThrowOnCopy& operator=(ThrowOnCopy&&) noexcept = default;
    ThrowOnCopy(ThrowOnCopy&&) noexcept = default;
    void operator()(ad_utility::MemorySize) const {}
  };
  AllocatorWithLimit<int> a1{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B),
      ThrowOnCopy{}};
  auto copy = [&a1]() { [[maybe_unused]] auto a2 = a1; };
  auto copyAssign = [&a1]() {
    AllocatorWithLimit<int> a2{
        ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B)};
    a2 = a1;
  };
  ASSERT_THROW(copy(), std::runtime_error);
  ASSERT_THROW(copyAssign(), std::runtime_error);
  auto move = [&a1]() { auto a2 = std::move(a1); };
  auto moveAssign = [&a1]() {
    AllocatorWithLimit<int> a2{
        ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B)};
    a2 = std::move(a1);
  };
  // The move operations call the copy operations which throw, but are declared
  // `noexcept`, so the program dies when they are called.
  ASSERT_DEATH_IF_SUPPORTED(move(),
                            "MemoryLimitTracker.*move constructor");
  ASSERT_DEATH_IF_SUPPORTED(
      moveAssign(), "MemoryLimitTracker.*move assignment");
}
