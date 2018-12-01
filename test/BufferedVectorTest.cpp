// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>
#include <vector>
#include "../src/util/BufferedVector.h"

using ad_utility::BufferedVector;

// ______________________________________________________
TEST(BufferedVectorTest, Constructor) {
  BufferedVector<int> b(15, "_testBuf.dat");
  ASSERT_EQ(b.threshold(), 15u);
  ASSERT_TRUE(b.isInternal());
  ASSERT_EQ(b.size(), 0u);
}

// ______________________________________________________
TEST(BufferedVectorTest, PushBackSmall) {
  BufferedVector<int> b(15, "_testBuf.dat");
  for (int i = 0; i < 13; ++i) {
    b.push_back(i);
  }
  ASSERT_EQ(b.threshold(), 15u);
  ASSERT_TRUE(b.isInternal());
  ASSERT_EQ(b.size(), 13u);

  int idx = 0;
  for (const auto& el : b) {
    ASSERT_EQ(el, idx);
    ++idx;
  }
  for (int i = 0; i < 13; ++i) {
    ASSERT_EQ(b[i], i);
  }

  idx = 0;
  for (auto& el : b) {
    ASSERT_EQ(el++, idx);
    ASSERT_EQ(idx + 1, b[idx]);
    ++idx;
  }
}

// ______________________________________________________
TEST(BufferedVectorTest, PushBackBig) {
  BufferedVector<int> b(15, "_testBuf.dat");
  for (int i = 0; i < 25; ++i) {
    b.push_back(i);
  }
  ASSERT_EQ(b.threshold(), 15u);
  ASSERT_FALSE(b.isInternal());
  ASSERT_EQ(b.size(), 25u);

  int idx = 0;
  for (const auto& el : b) {
    ASSERT_EQ(el, idx);
    ++idx;
  }
  for (int i = 0; i < 25; ++i) {
    ASSERT_EQ(b[i], i);
  }

  idx = 0;
  for (auto& el : b) {
    ASSERT_EQ(el++, idx);
    ASSERT_EQ(idx + 1, b[idx]);
    ++idx;
  }
}

// ______________________________________________________
TEST(BufferedVectorTest, Clear) {
  BufferedVector<int> b(15, "_testBuf.dat");
  for (int i = 0; i < 25; ++i) {
    b.push_back(i);
  }
  ASSERT_EQ(b.threshold(), 15u);
  ASSERT_FALSE(b.isInternal());
  ASSERT_EQ(b.size(), 25u);

  b.clear();
  ASSERT_EQ(b.threshold(), 15u);
  ASSERT_TRUE(b.isInternal());
  ASSERT_EQ(b.size(), 0u);

  // add Different elements than before to see if this really works

  for (int i = 0; i < 30; ++i) {
    b.push_back(i + 42);
  }
  ASSERT_EQ(b.threshold(), 15u);
  ASSERT_FALSE(b.isInternal());
  ASSERT_EQ(b.size(), 30u);

  int idx = 0;
  for (const auto& el : b) {
    ASSERT_EQ(el, idx + 42);
    ++idx;
  }
  for (int i = 0; i < 30; ++i) {
    ASSERT_EQ(b[i], i + 42);
  }

  idx = 0;
  for (auto& el : b) {
    ASSERT_EQ(el++, idx + 42);
    ASSERT_EQ(idx + 1 + 42, b[idx]);
    ++idx;
  }
}
