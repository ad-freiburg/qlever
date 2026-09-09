// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <string>
#include <type_traits>
#include <vector>

#include "util/CopyOnWritePtr.h"

using ad_utility::CopyOnWritePtr;

// Test that the default constructor creates a value-initialized `T`.
TEST(CopyOnWritePtr, defaultConstructorValueInitializes) {
  CopyOnWritePtr<int> ptr;
  EXPECT_EQ(*ptr, 0);
  EXPECT_FALSE(ptr.isShared());

  CopyOnWritePtr<std::vector<int>> vec;
  EXPECT_TRUE(vec->empty());
}

// Test the constructor from a value and the three read accessors.
TEST(CopyOnWritePtr, constructFromValueAndRead) {
  CopyOnWritePtr<std::string> ptr{"hello"};
  EXPECT_EQ(*ptr, "hello");
  EXPECT_EQ(ptr.read(), "hello");
  EXPECT_EQ(ptr->size(), 5u);
  EXPECT_FALSE(ptr.isShared());
}

// Test that `write()` mutates in place as long as nobody shares the value.
TEST(CopyOnWritePtr, writeWithoutCopyDoesNotClone) {
  CopyOnWritePtr<std::string> ptr{"hello"};
  const std::string* addressBefore = &*ptr;
  ptr.write() += " world";
  EXPECT_EQ(*ptr, "hello world");
  EXPECT_EQ(&*ptr, addressBefore);
}

// Test that a copy shares the value until one of the two writes, and that a
// write then leaves the other one untouched.
TEST(CopyOnWritePtr, copiesShareUntilWrite) {
  CopyOnWritePtr<std::string> original{"hello"};
  CopyOnWritePtr<std::string> copy = original;
  EXPECT_EQ(&*original, &*copy);
  EXPECT_TRUE(original.isShared());
  EXPECT_TRUE(copy.isShared());

  // The write through `copy` clones, so `original` keeps the old value and
  // neither of the two is shared any more.
  copy.write() += " world";
  EXPECT_EQ(*copy, "hello world");
  EXPECT_EQ(*original, "hello");
  EXPECT_NE(&*original, &*copy);
  EXPECT_FALSE(original.isShared());
  EXPECT_FALSE(copy.isShared());

  // A further write through `copy` therefore mutates in place.
  const std::string* addressBefore = &*copy;
  copy.write() += "!";
  EXPECT_EQ(*copy, "hello world!");
  EXPECT_EQ(&*copy, addressBefore);
  EXPECT_EQ(*original, "hello");
}

// Test the intended usage: snapshots are copies, and each of them keeps the
// value from the time it was taken while the original is written to.
TEST(CopyOnWritePtr, writeThroughOriginalKeepsSnapshotsIntact) {
  CopyOnWritePtr<std::vector<int>> original{std::vector<int>{1, 2, 3}};
  CopyOnWritePtr<std::vector<int>> snapshot1 = original;
  original.write().push_back(4);
  CopyOnWritePtr<std::vector<int>> snapshot2 = original;
  original.write().push_back(5);

  EXPECT_THAT(*snapshot1, ::testing::ElementsAre(1, 2, 3));
  EXPECT_THAT(*snapshot2, ::testing::ElementsAre(1, 2, 3, 4));
  EXPECT_THAT(*original, ::testing::ElementsAre(1, 2, 3, 4, 5));

  // Each write cloned, so none of the three shares its value any more.
  EXPECT_FALSE(snapshot1.isShared());
  EXPECT_FALSE(snapshot2.isShared());
  EXPECT_FALSE(original.isShared());
}

// Test that with three pointers to one value, a write through one of them
// clones only that one and the other two keep sharing.
TEST(CopyOnWritePtr, sharingIsTransitive) {
  CopyOnWritePtr<int> a{1};
  CopyOnWritePtr<int> b = a;
  CopyOnWritePtr<int> c = b;
  EXPECT_EQ(&*a, &*c);

  b.write() = 2;
  EXPECT_EQ(*a, 1);
  EXPECT_EQ(*b, 2);
  EXPECT_EQ(*c, 1);
  EXPECT_TRUE(a.isShared());
  EXPECT_FALSE(b.isShared());
  EXPECT_TRUE(c.isShared());
}

// Test that copy assignment makes the target share the value of the source.
TEST(CopyOnWritePtr, copyAssignmentSharesAgain) {
  CopyOnWritePtr<int> a{1};
  CopyOnWritePtr<int> b{2};
  EXPECT_NE(&*a, &*b);

  b = a;
  EXPECT_EQ(&*a, &*b);
  EXPECT_EQ(*b, 1);
  EXPECT_TRUE(a.isShared());
}

// Test that moving transfers the pointer without cloning and without
// affecting the sharing with other copies.
TEST(CopyOnWritePtr, moveTransfersWithoutCloning) {
  CopyOnWritePtr<std::string> a{"hello"};
  CopyOnWritePtr<std::string> copy = a;
  const std::string* address = &*a;

  CopyOnWritePtr<std::string> b = std::move(a);
  EXPECT_EQ(&*b, address);
  EXPECT_TRUE(b.isShared());
  EXPECT_TRUE(copy.isShared());

  b.write() += " world";
  EXPECT_EQ(*b, "hello world");
  EXPECT_EQ(*copy, "hello");
}

// Check at compile time that `write()` is the only way to get a mutable
// reference, so that an accidental mutation of a shared value cannot compile.
TEST(CopyOnWritePtr, onlyWriteGivesMutableAccess) {
  static_assert(
      std::is_same_v<decltype(*std::declval<const CopyOnWritePtr<int>&>()),
                     const int&>);
  static_assert(
      std::is_same_v<decltype(std::declval<CopyOnWritePtr<int>&>().read()),
                     const int&>);
  static_assert(
      std::is_same_v<decltype(std::declval<CopyOnWritePtr<int>&>().write()),
                     int&>);
  static_assert(!std::is_invocable_v<decltype(&CopyOnWritePtr<int>::write),
                                     const CopyOnWritePtr<int>&>);
}
