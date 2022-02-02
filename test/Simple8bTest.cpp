// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include "../src/util/Simple8bCode.h"

using std::string;

namespace ad_utility {
// _____________________________________________________________________________
TEST(Simple8bTest, testEncode) {
  unsigned int* plain = new unsigned int[5];
  plain[0] = 1;
  plain[1] = 2;
  plain[2] = 3;
  plain[3] = 0;
  plain[4] = 1;
  uint64_t* encoded = new uint64_t[1000];
  Simple8bCode::encode(plain, 5, encoded);
  ASSERT_EQ(static_cast<uint64_t>(0x0000000000001393), encoded[0]);
  delete[] plain;

  plain = new unsigned int[100];
  for (size_t i = 0; i < 100; ++i) {
    plain[i] = 1;
  }
  Simple8bCode::encode(plain, 100, encoded);
  ASSERT_EQ(static_cast<uint64_t>(0xFFFFFFFFFFFFFFF2), encoded[0]);
  delete[] plain;

  plain = new unsigned int[1000];
  for (size_t i = 0; i < 1000; ++i) {
    plain[i] = 1;
  }
  Simple8bCode::encode(plain, 1000, encoded);
  ASSERT_EQ(static_cast<uint64_t>(0xFFFFFFFFFFFFFFF2), encoded[0]);
  ASSERT_EQ(size_t(2), encoded[0] % 16);
  ASSERT_EQ(static_cast<uint64_t>(0xFFFFFFFFFFFFFFF2), encoded[1]);
  ASSERT_EQ(size_t(2), encoded[1] % 16);
  ASSERT_EQ(static_cast<uint64_t>(0xFFFFFFFFFFFFFFF2), encoded[2]);
  ASSERT_EQ(static_cast<uint64_t>(0xFFFFFFFFFFFFFFF2), encoded[3]);

  for (size_t i = 0; i < 1000; ++i) {
    plain[i] = i;
  }
  Simple8bCode::encode(plain, 35, encoded);
  ASSERT_EQ(size_t(5), encoded[0] % 16);
  ASSERT_EQ(size_t(6), encoded[1] % 16);
  ASSERT_EQ(size_t(7), encoded[2] % 16);

  delete[] plain;
  delete[] encoded;
}
// _____________________________________________________________________________
TEST(Simple8bTest, testEncodeDecode32Bit) {
  // plain[i] = 0
  uint32_t* plain = new uint32_t[10000];
  for (size_t i = 0; i < 10000; ++i) {
    plain[i] = 0;
  }
  uint64_t* encoded = new uint64_t[10000];
  size_t encodedSize = Simple8bCode::encode(plain, 10000, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 10000);
  for (size_t i = 0; i < 10; ++i) {
    ASSERT_EQ(size_t(0), encoded[i] % 16);
  }
  uint32_t* decoded = new uint32_t[10000 + 239];
  Simple8bCode::decode(encoded, 10000, decoded);
  for (size_t i = 0; i < 10000; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;

  // plain[i] = 1
  plain = new uint32_t[10000];
  for (size_t i = 0; i < 10000; ++i) {
    plain[i] = 1;
  }
  encoded = new uint64_t[10000];
  encodedSize = Simple8bCode::encode(plain, 10000, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 10000);
  decoded = new uint32_t[10000 + 239];
  Simple8bCode::decode(encoded, 10000, decoded);
  for (size_t i = 0; i < 5000; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  for (size_t i = 500; i < 10000; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;

  // plain[i] = i
  plain = new uint32_t[10000];
  for (size_t i = 0; i < 10000; ++i) {
    plain[i] = i;
  }
  encoded = new uint64_t[10000];
  encodedSize = Simple8bCode::encode(plain, 10000, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 10000);
  decoded = new uint32_t[10000 + 239];
  Simple8bCode::decode(encoded, 20, decoded);
  for (size_t i = 0; i < 20; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;

  // plain[i] = i % 10
  plain = new uint32_t[10000];
  for (size_t i = 0; i < 10000; ++i) {
    plain[i] = i % 10;
  }
  encoded = new uint64_t[10000];
  encodedSize = Simple8bCode::encode(plain, 10000, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 10000);
  decoded = new uint32_t[10000 + 239];
  Simple8bCode::decode(encoded, 10000, decoded);
  for (size_t i = 0; i < 10000; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;

  // plain[i] = 1000 * i
  plain = new uint32_t[1000];
  for (size_t i = 0; i < 1000; ++i) {
    plain[i] = i * 1000;
  }
  encoded = new uint64_t[1000];
  encodedSize = Simple8bCode::encode(plain, 1000, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 1000);
  decoded = new uint32_t[1000 + 239];
  Simple8bCode::decode(encoded, 1000, decoded);
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;

  // plain[i] = i for i < 400 && i >= 800
  // plain[i] = 1 for 400 <= i < 800
  plain = new uint32_t[1000];
  for (size_t i = 0; i < 1000; ++i) {
    if (i < 400 || i >= 800) {
      plain[i] = i;
    } else {
      plain[i] = 1;
    }
  }
  encoded = new uint64_t[1000];
  encodedSize = Simple8bCode::encode(plain, 1000, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 1000);
  decoded = new uint32_t[1000 + 239];
  Simple8bCode::decode(encoded, 1000, decoded);
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;
}
// _____________________________________________________________________________
TEST(Simple8bTest, testEncodeDecode64Bit) {
  // plain[i] = i * 1000000000
  uint64_t* plain = new uint64_t[1000];
  for (size_t i = 0; i < 1000; ++i) {
    plain[i] = 1000000000 * i;
  }
  uint64_t* encoded = new uint64_t[1000];
  size_t encodedSize = Simple8bCode::encode(plain, 1000, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 1000);
  uint64_t* decoded = new uint64_t[1000 + 239];
  Simple8bCode::decode(encoded, 1000, decoded);
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;
}
// _____________________________________________________________________________
TEST(Simple8bTest, testEncodeDecode242times0) {
  // plain[i] = i * 1000000000
  uint64_t* plain = new uint64_t[242];
  for (size_t i = 0; i < 240; ++i) {
    plain[i] = 0;
  }
  plain[240] = 0;
  plain[241] = 0;
  uint64_t* encoded = new uint64_t[242];
  size_t encodedSize = Simple8bCode::encode(plain, 242, encoded);
  ASSERT_TRUE(encodedSize < 2 * sizeof(uint64_t) * 242);
  uint64_t* decoded = new uint64_t[242 + 239];
  Simple8bCode::decode(encoded, 242, decoded);
  for (size_t i = 0; i < 242; ++i) {
    ASSERT_EQ(plain[i], decoded[i]);
  }
  delete[] plain;
  delete[] encoded;
  delete[] decoded;
}
}  // namespace ad_utility
