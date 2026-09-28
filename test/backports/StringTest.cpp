// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>

#include "backports/string.h"

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteExactSize) {
  std::string s = "initial";
  const std::string text = "Hello, World!";
  ql::resize_and_overwrite(s, text.size(), [&](char* buf, size_t count) {
    EXPECT_EQ(count, text.size());
    std::memcpy(buf, text.data(), text.size());
    return text.size();
  });
  EXPECT_EQ(s, text);
  EXPECT_EQ(s.size(), text.size());
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteSmallerSize) {
  // Start with enough capacity so that truncation must happen in place.
  std::string s(10, 'x');
  const char* dataBefore = s.data();
  const std::string full = "abcdefghij";
  ql::resize_and_overwrite(s, full.size(), [&](char* buf, size_t count) {
    EXPECT_EQ(count, full.size());
    std::memcpy(buf, full.data(), full.size());
    return 4;
  });
  EXPECT_EQ(s, "abcd");
  EXPECT_EQ(s.size(), 4u);
  EXPECT_EQ(s.data(), dataBefore);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteZeroSize) {
  std::string s = "not empty";
  ql::resize_and_overwrite(s, 20, [](char*, size_t) { return 0u; });
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteZeroCapacity) {
  std::string s;
  ql::resize_and_overwrite(s, 0, [](char*, size_t count) {
    EXPECT_EQ(count, 0u);
    return 0u;
  });
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

// _____________________________________________________________________________
// Negative test: an operation returning more than the granted size violates
// the contract on both the fallback and the C++23 branch.
TEST(StringTest, ResizeAndOverwriteOversizedResultThrows) {
  std::string s;
  ASSERT_THROW(ql::resize_and_overwrite(s, 4, [](char*, size_t) { return 5u; }),
               ad_utility::Exception);
}

// _____________________________________________________________________________
// A move-only operation passed as an rvalue must work: the backport moves it
// into the C++23 branch lambda instead of capturing a reference to it.
TEST(StringTest, ResizeAndOverwriteMoveOnlyOperation) {
  std::string s;
  const std::string text = "move-only";
  auto op = [payload = std::make_unique<std::string>(text), &text](
                char* buf, size_t count) {
    EXPECT_EQ(count, text.size());
    std::memcpy(buf, payload->data(), payload->size());
    return payload->size();
  };
  ql::resize_and_overwrite(s, text.size(), std::move(op));
  EXPECT_EQ(s, text);
}
