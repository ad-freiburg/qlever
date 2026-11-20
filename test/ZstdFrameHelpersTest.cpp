// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "util/compressionUsingZstd/ZstdExpected.h"
#include "util/compressionUsingZstd/ZstdLittleEndian.h"

using namespace ad_utility::zstd;

// An `Expected` holds either a value or an error, and accessing the wrong one
// is a bug that is detected.
TEST(ZstdExpected, valueOrError) {
  Expected<std::string> value{std::string{"hello"}};
  ASSERT_TRUE(value.hasValue());
  EXPECT_EQ(value.value(), "hello");
  EXPECT_EQ(value->size(), 5u);
  EXPECT_ANY_THROW(value.error());
  EXPECT_EQ(std::move(value).value(), "hello");

  Expected<std::string> error{ZstdError{"something is wrong"}};
  ASSERT_FALSE(error.hasValue());
  EXPECT_EQ(error.error().message_, "something is wrong");
  EXPECT_ANY_THROW(error.value());
  const auto& constError = error;
  EXPECT_ANY_THROW(constError.value());
  EXPECT_EQ(std::move(error).error().message_, "something is wrong");

  Expected<void> success;
  EXPECT_TRUE(success.hasValue());
  EXPECT_ANY_THROW(success.error());
  Expected<void> failure{ZstdError{"failed"}};
  ASSERT_FALSE(failure.hasValue());
  EXPECT_EQ(failure.error().message_, "failed");
  EXPECT_EQ(std::move(failure).error().message_, "failed");
}

// Integers are written and read in little-endian order, independently of the
// byte order of the platform.
TEST(ZstdLittleEndian, readAndAppend) {
  using namespace ad_utility::zstd::detail;
  std::vector<char> bytes;
  appendLittleEndian<3>(bytes, 0x123456);
  EXPECT_EQ(bytes, (std::vector<char>{0x56, 0x34, 0x12}));
  EXPECT_EQ(readLittleEndian<3>(bytes), 0x123456u);
  EXPECT_EQ(readLittleEndian<1>(bytes), 0x56u);
  // Bytes >= 128 are not sign-extended.
  bytes.clear();
  appendLittleEndian<8>(bytes, 0xFFEEDDCCBBAA9988);
  EXPECT_EQ(readLittleEndian<8>(bytes), 0xFFEEDDCCBBAA9988u);
  EXPECT_EQ(readLittleEndian<2>(bytes), 0x9988u);
  // Too few bytes to read, or a value that does not fit, are bugs.
  EXPECT_ANY_THROW(readLittleEndian<4>(ql::span<const char>{bytes}.first(3)));
  EXPECT_ANY_THROW(appendLittleEndian<1>(bytes, 256));
}
