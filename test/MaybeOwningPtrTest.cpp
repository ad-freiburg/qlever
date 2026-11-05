// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "util/GTestHelpers.h"
#include "util/MaybeOwningPtr.h"

namespace {

using ad_utility::MaybeOwningPtr;
using ::testing::HasSubstr;

// Without the lifetime checks, a `MaybeOwningPtr` is as large as a
// `shared_ptr`.
#ifndef QL_ENABLE_LIFETIME_CHECKS
static_assert(sizeof(MaybeOwningPtr<int>) == sizeof(std::shared_ptr<int>));
#endif

// Test that an owning pointer keeps its object alive and that a non-owning copy
// doesn't.
TEST(MaybeOwningPtr, ownership) {
  auto shared = std::make_shared<int>(42);
  std::weak_ptr<int> weak = shared;
  std::optional<MaybeOwningPtr<int>> owning{MaybeOwningPtr<int>{shared}};
  EXPECT_TRUE(owning->isOwning());
  EXPECT_EQ(shared.use_count(), 2);

  auto nonOwning = owning->nonOwningCopy();
  EXPECT_FALSE(nonOwning.isOwning());
  EXPECT_EQ(shared.use_count(), 2);
  EXPECT_EQ(nonOwning.get(), shared.get());
  EXPECT_EQ(*nonOwning, 42);

  // Copies of a non-owning pointer don't touch the reference count.
  auto copy = nonOwning;
  auto copyOfCopy = copy.nonOwningCopy();
  EXPECT_FALSE(copyOfCopy.isOwning());
  EXPECT_EQ(shared.use_count(), 2);

  // Copies of an owning pointer share the ownership.
  auto owningCopy = *owning;
  EXPECT_TRUE(owningCopy.isOwning());
  EXPECT_EQ(shared.use_count(), 3);

  // Moving copies, so the moved-from pointer still owns its object.
  static_assert(std::is_nothrow_move_constructible_v<MaybeOwningPtr<int>>);
  static_assert(std::is_nothrow_move_assignable_v<MaybeOwningPtr<int>>);
  auto moved = std::move(owningCopy);
  EXPECT_TRUE(owningCopy.isOwning());
  EXPECT_EQ(owningCopy.get(), shared.get());
  EXPECT_EQ(shared.use_count(), 4);

  // Assignment.
  copy = moved;
  EXPECT_TRUE(copy.isOwning());
  copy = nonOwning;
  EXPECT_FALSE(copy.isOwning());
  EXPECT_EQ(shared.use_count(), 4);
}

// Test a non-owning pointer to an object whose lifetime is managed elsewhere.
TEST(MaybeOwningPtr, uncheckedNonOwning) {
  static std::pair<int, int> value{3, 4};
  using Ptr = MaybeOwningPtr<std::pair<int, int>>;
  auto ptr = Ptr::makeUncheckedNonOwning(&value);
  EXPECT_FALSE(ptr.isOwning());
  EXPECT_EQ(ptr.get(), &value);
  EXPECT_EQ(ptr->first, 3);
  EXPECT_EQ((*ptr).second, 4);
  auto copy = ptr.nonOwningCopy();
  EXPECT_FALSE(copy.isOwning());
  EXPECT_EQ(copy.get(), &value);
}

// Test that the pointer must not be null and that an owning pointer needs a
// control block.
TEST(MaybeOwningPtr, nullptrIsRejected) {
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      MaybeOwningPtr<int>{std::shared_ptr<int>{}},
      HasSubstr("ptr_ != nullptr && isOwning()"), ad_utility::Exception);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      MaybeOwningPtr<int>::makeUncheckedNonOwning(nullptr),
      HasSubstr("ptr != nullptr"), ad_utility::Exception);
  int value = 42;
  std::shared_ptr<int> withoutControlBlock{std::shared_ptr<int>{}, &value};
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      MaybeOwningPtr<int>{withoutControlBlock},
      HasSubstr("ptr_ != nullptr && isOwning()"), ad_utility::Exception);
}

// Test that destroying or overwriting a non-owning pointer that outlived its
// owner terminates the program when the lifetime checks are enabled.
TEST(MaybeOwningPtr, lifetimeCheck) {
#ifdef QL_ENABLE_LIFETIME_CHECKS
  auto makeDangling = []() {
    MaybeOwningPtr<int> owner{std::make_shared<int>(1)};
    return std::optional{owner.nonOwningCopy()};
  };
  EXPECT_DEATH_IF_SUPPORTED(makeDangling().reset(), "outlived the owner");
  EXPECT_DEATH_IF_SUPPORTED(
      {
        auto dangling = makeDangling();
        MaybeOwningPtr<int> other{std::make_shared<int>(2)};
        dangling.value() = other;
      },
      "outlived the owner");
  // A non-owning pointer that is destroyed before its owner is fine.
  MaybeOwningPtr<int> owner{std::make_shared<int>(1)};
  { auto nonOwning = owner.nonOwningCopy(); }
#else
  GTEST_SKIP() << "The lifetime checks are disabled in this build";
#endif
}

}  // namespace
