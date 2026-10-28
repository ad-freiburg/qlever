// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#if __has_include(<version>)
#include <version>
#endif

#include "backports/string.h"
#include "util/Exception.h"

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
  std::string s(10, 'x');
  const std::string full = "abcdefghij";
  ql::resize_and_overwrite(s, full.size(), [&](char* buf, size_t count) {
    EXPECT_EQ(count, full.size());
    std::memcpy(buf, full.data(), full.size());
    return size_t{4};
  });
  EXPECT_EQ(s, "abcd");
  EXPECT_EQ(s.size(), 4u);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteZeroSize) {
  std::string s = "not empty";
  ql::resize_and_overwrite(s, 20, [](char*, size_t) { return 0u; });
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteEmptyStringZeroCount) {
  std::string s;
  ql::resize_and_overwrite(s, 0, [](char*, size_t count) {
    EXPECT_EQ(count, 0u);
    return 0u;
  });
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

// _____________________________________________________________________________
// An oversize result violates the contract on both branches: the call
// throws, and the string is left at size `count` with the written contents
// (documented in `backports/string.h`). Start from a non-empty string so that
// the post-throw state is observable.
TEST(StringTest, ResizeAndOverwriteOversizedResultThrows) {
  std::string s = "initial";
  try {
    ql::resize_and_overwrite(s, 4, [](char* buf, size_t) {
      std::memcpy(buf, "abcd", 4);
      return size_t{5};
    });
    FAIL() << "expected ad_utility::Exception";
  } catch (const ad_utility::Exception& e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr("newSize <= count"));
  }
  EXPECT_EQ(s, "abcd");
  EXPECT_EQ(s.size(), 4u);
}

// _____________________________________________________________________________
// The backport is generic in the character type, like the standard member.
TEST(StringTest, ResizeAndOverwriteWideString) {
  std::wstring s = L"initial";
  const std::wstring text = L"wide";
  ql::resize_and_overwrite(s, text.size(), [&](wchar_t* buf, size_t count) {
    EXPECT_EQ(count, text.size());
    std::memcpy(buf, text.data(), text.size() * sizeof(wchar_t));
    return text.size();
  });
  EXPECT_EQ(s, text);
}

// _____________________________________________________________________________
// Move-only rvalue operations work via the moved-into lambda.
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
