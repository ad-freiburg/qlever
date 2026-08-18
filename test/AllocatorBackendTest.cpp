// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Typed tests covering the shared public surface of both allocator backends.
// The same test body is compiled and run for both:
//   ad_utility::allocatorImpl::AllocatorWithLimit<int>  (limit backend)
//   ad_utility::PmrAllocator<int>                       (PMR backend)
//
// Backend-specific behaviour (if any) lives in AllocatorPmrTest.cpp.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <new>
#include <utility>
#include <vector>

#include "backports/memory_resource.h"
#include "util/AllocatorPmr.h"
#include "util/AllocatorWithLimitImpl.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"

namespace {

using namespace ad_utility::memory_literals;

template <typename T>
class AllocatorBackendTest : public ::testing::Test {};

using AllocatorBackendTypes =
    ::testing::Types<ad_utility::allocatorImpl::AllocatorWithLimit<int>,
                     ad_utility::PmrAllocator<int>>;

TYPED_TEST_SUITE(AllocatorBackendTest, AllocatorBackendTypes);

// A `ql::pmr::memory_resource` for which every allocation fails. Used below to
// simulate an upstream resource (e.g. an injected bounded arena) that cannot
// satisfy a request although the memory limit would allow it.
class AlwaysThrowingResource : public ql::pmr::memory_resource {
 protected:
  void* do_allocate(std::size_t, std::size_t) override {
    throw std::bad_alloc{};
  }
  void do_deallocate(void*, std::size_t, std::size_t) override {
    // Never called, as `do_allocate` always throws.
    AD_FAIL();
  }
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

// Both backends reserve the requested memory from their tracker *before* they
// hand the request to the underlying allocator, and have to give that
// reservation back when the underlying allocation fails. Provoking such a
// failure necessarily differs between the backends, so it is described by the
// following traits, which the shared test below then uses.
template <typename Allocator>
struct FailingUnderlyingAllocation;

// For the limit backend, the underlying allocator is a `std::allocator<int>`,
// which cannot be replaced, so the failure has to be provoked by requesting an
// absurd amount of memory: 8 EB, which `std::allocator` rejects because it
// exceeds its `max_size()`. The memory limit is set to the maximum, so that the
// reservation for those bytes still succeeds. Note that the number of bytes
// must not overflow, as the test below checks that exactly those bytes are
// given back to the tracker.
//
// Only libstdc++ rejects such a request by its size alone; libc++ instead
// attempts the allocation, which then fails in an implementation-defined way,
// so the test is skipped there.
template <>
struct FailingUnderlyingAllocation<
    ad_utility::allocatorImpl::AllocatorWithLimit<int>> {
#ifdef _LIBCPP_VERSION
  static constexpr bool isSupported = false;
#else
  static constexpr bool isSupported = true;
#endif
  static size_t numElements() { return (size_t{1} << 63) / sizeof(int); }
  static ad_utility::allocatorImpl::AllocatorWithLimit<int> makeAllocator() {
    return ad_utility::allocatorImpl::AllocatorWithLimit<int>::makeLimited(
        ad_utility::MemorySize::max());
  }
};

// For the PMR backend, the underlying allocator is the upstream resource, so a
// resource that always throws can simply be injected.
template <>
struct FailingUnderlyingAllocation<ad_utility::PmrAllocator<int>> {
  static constexpr bool isSupported = true;
  static size_t numElements() { return 1; }
  static ad_utility::PmrAllocator<int> makeAllocator() {
    // The allocator only stores a pointer to the upstream resource, which
    // therefore has to outlive it.
    static AlwaysThrowingResource throwingUpstream;
    return ad_utility::makePmrAllocatorWithLimit<int>(100_B, &throwingUpstream);
  }
};

// Allocating within the budget works; exceeding it throws
// AllocationExceedsLimitException; amountMemoryLeft() tracks allocate and
// deallocate correctly.
TYPED_TEST(AllocatorBackendTest, LimitEnforced) {
  static_assert(sizeof(int) == 4);
  auto alloc = TypeParam::makeLimited(12_B);
  EXPECT_EQ(alloc.amountMemoryLeft(), 12_B);

  int* p = alloc.allocate(1);  // 4 bytes consumed
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);

  int* p2 = alloc.allocate(2);  // 8 more bytes consumed
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);

  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);

  alloc.deallocate(p2, 2);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);
  alloc.deallocate(p, 1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 12_B);
}

// The clearOnAllocation hook is invoked when the budget is exceeded and may
// free room; since the hook here frees nothing, the allocation still throws
// afterwards. This behaviour is shared by both backends.
TYPED_TEST(AllocatorBackendTest, ClearOnAllocationHook) {
  bool called = false;
  auto alloc = TypeParam::makeLimited(
      4_B, [&called](ad_utility::MemorySize) { called = true; });
  int* p1 = alloc.allocate(1);  // consumes the whole 4-byte budget
  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_TRUE(called);
  alloc.deallocate(p1, 1);
}

// .as<char>() yields an allocator sharing the same budget: consuming bytes
// through the char allocator reduces the int allocator's amountMemoryLeft().
TYPED_TEST(AllocatorBackendTest, AsSharesBudget) {
  static_assert(sizeof(int) == 4);
  auto allocInt = TypeParam::makeLimited(8_B);
  auto allocChar = allocInt.template as<char>();

  EXPECT_EQ(allocChar.amountMemoryLeft(), 8_B);

  char* p = allocChar.allocate(3);
  EXPECT_EQ(allocInt.amountMemoryLeft(), 5_B);
  allocChar.deallocate(p, 3);
  EXPECT_EQ(allocInt.amountMemoryLeft(), 8_B);
}

// Copies and rebound allocators share the same backing state.
TYPED_TEST(AllocatorBackendTest, AsSharesBackingState) {
  auto allocInt = TypeParam::makeLimited(8_B);
  auto allocChar = allocInt.template as<char>();
  EXPECT_EQ(allocInt, allocInt.template as<int>());
  EXPECT_EQ(allocInt, allocChar.template as<int>());
}

// Larger allocations track memory correctly and report the same amount via a
// const allocator. Exceeding the remaining budget reports the requested and
// available memory.
TYPED_TEST(AllocatorBackendTest, LargeAllocationTracksMemory) {
  TypeParam alloc = TypeParam::makeLimited(2_MB);
  static_assert(sizeof(int) == 4);
  [[maybe_unused]] auto ptr = alloc.allocate(250'000);
  EXPECT_EQ(alloc.amountMemoryLeft(), 1_MB);
  EXPECT_EQ(std::as_const(alloc).amountMemoryLeft(), 1_MB);
  AD_EXPECT_THROW_WITH_MESSAGE(
      alloc.allocate(500'000),
      ::testing::StrEq("Tried to allocate 2 MB, but only 1 MB were available"));
  alloc.deallocate(ptr, 250'000);
}

// Both backends satisfy the standard allocator requirements when used
// with std::vector.
TYPED_TEST(AllocatorBackendTest, WorksWithVector) {
  auto alloc = TypeParam::makeLimited(ad_utility::MemorySize::megabytes(1));
  std::vector<int, TypeParam> v{alloc};
  for (int i = 0; i < 1000; ++i) {
    v.push_back(i);
  }
  EXPECT_EQ(v.size(), 1000u);
  EXPECT_EQ(v.front(), 0);
  EXPECT_EQ(v.back(), 999);
  EXPECT_LT(alloc.amountMemoryLeft(), ad_utility::MemorySize::megabytes(1));
}

// Multiple vectors using copies of the same allocator share the same budget.
TYPED_TEST(AllocatorBackendTest, VectorsShareBudgetAndThrowOnGrowth) {
  auto allocator = TypeParam::makeLimited(18_B);
  std::vector<int, TypeParam> v{allocator};
  std::vector<int, TypeParam> u{allocator};
  v.push_back(5);  // allocate 4 bytes -> works
  u.push_back(5);
  v.push_back(4);  // allocate 8 bytes, then free 4 bytes -> works
  ASSERT_EQ(v.size(), 2u);
  ASSERT_EQ(v[1], 4);

  ASSERT_THROW(u.push_back(1),
               ad_utility::detail::AllocationExceedsLimitException);
}

// makeUnlimited() never throws for reasonable allocations and reports max().
TYPED_TEST(AllocatorBackendTest, UnlimitedAllocator) {
  auto alloc = TypeParam::makeUnlimited();
  EXPECT_EQ(alloc.amountMemoryLeft(), ad_utility::MemorySize::max());
  int* p = alloc.allocate(1000);
  ASSERT_NE(p, nullptr);
  alloc.deallocate(p, 1000);
}

// If the underlying allocator fails although the memory limit would have
// allowed the allocation, the reserved memory is given back to the tracker, so
// that it remains available for later allocations.
TYPED_TEST(AllocatorBackendTest, UnderlyingAllocationFailureReleasesMemory) {
  using Failing = FailingUnderlyingAllocation<TypeParam>;
  if (!Failing::isSupported) {
    GTEST_SKIP() << "The failure of the underlying allocation cannot be "
                    "provoked for this backend and standard library, see the "
                    "comment at `FailingUnderlyingAllocation`";
  }
  auto alloc = Failing::makeAllocator();
  const auto memoryLeftBefore = alloc.amountMemoryLeft();
  EXPECT_THROW(alloc.allocate(Failing::numElements()), std::bad_alloc);
  EXPECT_EQ(alloc.amountMemoryLeft(), memoryLeftBefore);
}

// Copy construction and move construction let the new allocator share the
// budget of the source. As both backends deliberately copy rather than steal on
// move, a moved-from allocator stays valid and keeps sharing that budget.
TYPED_TEST(AllocatorBackendTest, Construction) {
  static_assert(sizeof(int) == 4);
  auto alloc = TypeParam::makeLimited(8_B);

  TypeParam copyConstructed{alloc};
  EXPECT_EQ(copyConstructed, alloc);
  EXPECT_EQ(copyConstructed.amountMemoryLeft(), 8_B);
  // Allocating via the copy counts towards the budget of `alloc`.
  int* p = copyConstructed.allocate(1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 4_B);
  copyConstructed.deallocate(p, 1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);

  TypeParam moveConstructed{std::move(copyConstructed)};
  EXPECT_EQ(moveConstructed, alloc);
  // The moved-from allocator is still usable and still shares the same budget.
  int* q = copyConstructed.allocate(1);
  EXPECT_EQ(moveConstructed.amountMemoryLeft(), 4_B);
  copyConstructed.deallocate(q, 1);
  EXPECT_EQ(moveConstructed.amountMemoryLeft(), 8_B);
}

// Copy assignment and move assignment let the target allocator share the budget
// of the source. As both backends deliberately copy rather than steal on move,
// a moved-from allocator stays valid and keeps sharing that budget.
TYPED_TEST(AllocatorBackendTest, Assignment) {
  static_assert(sizeof(int) == 4);
  auto alloc = TypeParam::makeLimited(8_B);
  auto copyAssigned = TypeParam::makeLimited(20_B);
  ASSERT_NE(copyAssigned, alloc);

  copyAssigned = alloc;
  EXPECT_EQ(copyAssigned, alloc);
  EXPECT_EQ(copyAssigned.amountMemoryLeft(), 8_B);
  // Allocating via the copy now counts towards the budget of `alloc`.
  int* p = copyAssigned.allocate(1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 4_B);
  copyAssigned.deallocate(p, 1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);

  auto moveAssigned = TypeParam::makeLimited(20_B);
  moveAssigned = std::move(copyAssigned);
  EXPECT_EQ(moveAssigned, alloc);
  // The moved-from allocator is still usable and still shares the same budget.
  int* q = copyAssigned.allocate(1);
  EXPECT_EQ(moveAssigned.amountMemoryLeft(), 4_B);
  copyAssigned.deallocate(q, 1);
  EXPECT_EQ(moveAssigned.amountMemoryLeft(), 8_B);
}

// operator==: an allocator equals itself and its own .as<int>() (same backing
// state). Two independently-created allocators are not equal.
TYPED_TEST(AllocatorBackendTest, Equality) {
  auto a1 = TypeParam::makeLimited(20_B);
  auto a2 = TypeParam::makeLimited(20_B);

  EXPECT_EQ(a1, a1);
  EXPECT_EQ(a1, a1.template as<int>());
  EXPECT_NE(a1, a2);
}

}  // namespace
