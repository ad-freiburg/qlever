//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <boost/unordered_set.hpp>
#include <memory>
#include <string_view>
#include <type_traits>
#include <vector>

#include "backports/memory.h"

// _____________________________________________________________________________
TEST(MakeUniqueForOverwrite, arrayOfUnknownBound) {
  auto ptr = ql::make_unique_for_overwrite<char[]>(16);
  static_assert(std::is_same_v<decltype(ptr), std::unique_ptr<char[]>>);
  ASSERT_NE(ptr.get(), nullptr);
  // The contents are indeterminate, but the memory has to be writable and
  // readable back.
  for (size_t i = 0; i < 16; ++i) {
    ptr[i] = static_cast<char>(i);
  }
  for (size_t i = 0; i < 16; ++i) {
    EXPECT_EQ(ptr[i], static_cast<char>(i));
  }
}

// _____________________________________________________________________________
TEST(MakeUniqueForOverwrite, sizeZero) {
  auto ptr = ql::make_unique_for_overwrite<char[]>(0);
  EXPECT_NE(ptr.get(), nullptr);
}

// _____________________________________________________________________________
TEST(MakeUniqueForOverwrite, singleObject) {
  auto ptr = ql::make_unique_for_overwrite<int>();
  static_assert(std::is_same_v<decltype(ptr), std::unique_ptr<int>>);
  ASSERT_NE(ptr.get(), nullptr);
  *ptr = 42;
  EXPECT_EQ(*ptr, 42);
}

// _____________________________________________________________________________
TEST(MakeUniqueForOverwrite, nonTrivialTypeIsDefaultConstructed) {
  struct S {
    int value_ = 42;
  };
  auto single = ql::make_unique_for_overwrite<S>();
  EXPECT_EQ(single->value_, 42);
  auto array = ql::make_unique_for_overwrite<S[]>(3);
  EXPECT_EQ(array[0].value_, 42);
  EXPECT_EQ(array[2].value_, 42);
}

// Test that `ql::to_address` returns a raw pointer unchanged.
TEST(ToAddress, rawPointer) {
  int value = 42;
  EXPECT_EQ(ql::to_address(&value), &value);
  const int* null = nullptr;
  EXPECT_EQ(ql::to_address(null), nullptr);
}

// Test `ql::to_address` for the iterators of a vector, a string view and a
// node-based set.
TEST(ToAddress, iterators) {
  std::vector<int> vector{1, 2, 3};
  EXPECT_EQ(ql::to_address(vector.begin() + 1), &vector[1]);
  static_assert(std::is_same_v<decltype(ql::to_address(vector.begin())), int*>);

  std::string_view view = "abc";
  EXPECT_EQ(ql::to_address(view.begin()), view.data());
  static_assert(
      std::is_same_v<decltype(ql::to_address(view.begin())), const char*>);

  // The iterator of a node-based set (as used by `LocalVocab`).
  boost::unordered_set<int> set{42};
  EXPECT_EQ(ql::to_address(set.begin()), &(*set.begin()));
  static_assert(
      std::is_same_v<decltype(ql::to_address(set.begin())), const int*>);
}

// Test `ql::to_address` for a smart pointer.
TEST(ToAddress, smartPointer) {
  auto pointer = std::make_unique<int>(42);
  EXPECT_EQ(ql::to_address(pointer), pointer.get());
}
