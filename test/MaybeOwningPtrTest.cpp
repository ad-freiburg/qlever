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

#include "util/MaybeOwningPtr.h"

namespace {

using ad_utility::MaybeOwningPtr;

// An owning pointer keeps its object alive, a non-owning copy doesn't.
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

// A non-owning pointer to an object whose lifetime is managed elsewhere.
TEST(MaybeOwningPtr, uncheckedNonOwning) {
  static int value = 3;
  auto ptr = MaybeOwningPtr<int>::makeUncheckedNonOwning(&value);
  EXPECT_FALSE(ptr.isOwning());
  EXPECT_EQ(ptr.get(), &value);
  auto copy = ptr.nonOwningCopy();
  EXPECT_EQ(copy.get(), &value);
}

// The pointer must not be null.
TEST(MaybeOwningPtr, nullptrIsRejected) {
  EXPECT_ANY_THROW(MaybeOwningPtr<int>{std::shared_ptr<int>{}});
  EXPECT_ANY_THROW(MaybeOwningPtr<int>::makeUncheckedNonOwning(nullptr));
}

// Destroying or overwriting a non-owning pointer that outlived its owner
// terminates the program when the lifetime checks are enabled.
TEST(MaybeOwningPtr, lifetimeCheck) {
#ifdef QL_ENABLE_LIFETIME_CHECKS
  auto makeDangling = []() {
    MaybeOwningPtr<int> owner{std::make_shared<int>(1)};
    return std::optional{owner.nonOwningCopy()};
  };
  EXPECT_DEATH(makeDangling().reset(), "outlived the owner");
  EXPECT_DEATH(
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
