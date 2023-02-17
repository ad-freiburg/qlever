// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/BufferedVector.h"

using ad_utility::BufferedVector;

// ___________________________________________________________________________
TEST(BufferedVector, constructor) {
  BufferedVector<int> b(15, "_testBufConstructor.dat");
  ASSERT_EQ(b.threshold(), 15u);
  ASSERT_TRUE(b.isInternal());
  ASSERT_EQ(b.size(), 0u);
}

// ___________________________________________________________________________
TEST(BufferedVector, pushBackSmall) {
  BufferedVector<int> b(15, "_testBufPushBackSmall.dat");
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

// ___________________________________________________________________________
TEST(BufferedVector, pushBackBig) {
  BufferedVector<int> b(15, "_testBufPushBackBig.dat");
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

// ___________________________________________________________________________
TEST(BufferedVector, emplace_back) {
  // The main logic is already tested in `push_back`, so we only test the
  // difference
  {
    BufferedVector<int> b(15, "_testBufEmplaceBack.dat");
    b.emplace_back(3);
    b.emplace_back(-14);
    ASSERT_EQ(b.size(), 2);
    ASSERT_EQ(b[0], 3);
    ASSERT_EQ(b[1], -14);
  }
  {
    // A simple class that has a constructor with multiple arguments.
    struct S {
      size_t value_ = 42;
      S() = default;
      S(size_t a, size_t b) : value_{a + b} {}
      explicit S(std::string_view s) : value_{s.size()} {}
    };
    BufferedVector<S> b(15, "_testBufEmplaceBackStruct.dat");
    b.emplace_back(14u, 7u);
    b.emplace_back("hello");
    b.emplace_back();
    ASSERT_EQ(b.size(), 3);
    ASSERT_EQ(b[0].value_, 21u);
    ASSERT_EQ(b[1].value_, 5);
    ASSERT_EQ(b[2].value_, 42);
  }
}

// ___________________________________________________________________________
TEST(BufferedVector, clear) {
  BufferedVector<int> b(15, "_testBufClear.dat");
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

// ___________________________________________________________________________
TEST(BufferedVector, resize) {
  BufferedVector<int> b{5, "_testBufResize.dat"};
  b.push_back(0);
  b.push_back(1);
  b.resize(4);
  ASSERT_EQ(4, b.size());
  ASSERT_TRUE(b.isInternal());
  ASSERT_EQ(0, b[0]);
  ASSERT_EQ(1, b[1]);
  b.resize(5);
  ASSERT_FALSE(b.isInternal());
  ASSERT_EQ(5, b.size());
  ASSERT_EQ(0, b[0]);
  ASSERT_EQ(1, b[1]);
  b[2] = 2;
  b[3] = 3;
  b[4] = 4;
  b.resize(3000);
  ASSERT_FALSE(b.isInternal());
  ASSERT_EQ(3000, b.size());
  for (size_t i = 0; i < 5; ++i) {
    ASSERT_EQ(i, b[i]);
  }
  b[5] = 5;
  b[6] = 6;
  b.resize(7);
  ASSERT_FALSE(b.isInternal());
  ASSERT_EQ(7, b.size());
  for (size_t i = 0; i < 7; ++i) {
    ASSERT_EQ(i, b[i]);
  }
  // Resizing back from the external to the internal vector
  b.resize(4);
  ASSERT_TRUE(b.isInternal());
  ASSERT_EQ(4, b.size());
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_EQ(i, b[i]);
  }
}

// ___________________________________________________________________________
TEST(BufferedVector, insert) {
  std::vector<int> i{12, 10, 8};
  BufferedVector<int> b{5, "_testBufInsert.dat"};
  b.insert(b.begin(), i.begin(), i.end());
  ASSERT_THAT(b, ::testing::ElementsAreArray(i));

  {
    // Insert at beginning.
    std::vector<int> i2{3, 0, 1};
    b.insert(b.begin(), i2.begin(), i2.end());
    i.insert(i.begin(), i2.begin(), i2.end());
    ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  }
  {
    // Insert at end.
    std::vector<int> i2{-17, 12};
    b.insert(b.end(), i2.begin(), i2.end());
    i.insert(i.end(), i2.begin(), i2.end());
    ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  }
  {
    // Insert in the middle.
    std::vector<int> i2{83};
    b.insert(b.end(), i2.begin(), i2.end());
    i.insert(i.end(), i2.begin(), i2.end());
    ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  }

  // Corner cases
  ASSERT_ANY_THROW(b.insert(b.begin() - 1, i.begin(), i.end()));
  ASSERT_ANY_THROW(b.insert(b.end() + 3, i.begin(), i.end()));
  ASSERT_ANY_THROW(b.insert(b.begin(), b.begin(), b.end()));
  ASSERT_ANY_THROW(b.insert(b.begin(), i.end(), i.begin()));
}

// ___________________________________________________________________________
TEST(BufferedVector, erase) {
  std::vector<int> i{12, 10, 8, 6, 4, 2};
  BufferedVector<int> b{5, "_testBufErase.dat"};
  b.insert(b.begin(), i.begin(), i.end());
  {
    // Erase at the beginning.
    b.erase(b.begin(), b.begin() + 2);
    i.erase(i.begin(), i.begin() + 2);
    ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  }
  {
    // Erase at the end.
    b.erase(b.end() - 1, b.end());
    i.erase(i.end() - 1, i.end());
    ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  }
  {
    // Erase in the middle.
    b.erase(b.begin() + 1, b.begin() + 2);
    i.erase(i.begin() + 1, i.begin() + 2);
    ASSERT_THAT(b, ::testing::ElementsAreArray(i));
    ASSERT_THAT(b, ::testing::ElementsAre(8, 4));
  }

  // Corner cases
  ASSERT_ANY_THROW(b.erase(b.begin() - 1, b.begin()));
  ASSERT_ANY_THROW(b.erase(b.begin(), b.end() + 1));
  ASSERT_ANY_THROW(b.erase(b.end(), b.begin()));
}

// ___________________________________________________________________________
TEST(BufferedVector, reserveAndShrink) {
  // `reserve` and `shrink_to_fit` currently do nothing, and even if they did,
  // they shouldn't change the contained elements.
  std::vector<int> i{12, 10, 8, 6, 4, 2};
  BufferedVector<int> b{5, "_testBufReserve.dat"};
  b.insert(b.end(), i.begin(), i.end());
  b.reserve(27000);
  ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  ASSERT_EQ(b.size(), 6);
  b.reserve(0);
  ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  ASSERT_EQ(b.size(), 6);
  b.shrink_to_fit();
  ASSERT_THAT(b, ::testing::ElementsAreArray(i));
  ASSERT_EQ(b.size(), 6);
}

// ___________________________________________________________________________
TEST(BufferedVector, moveConstructorAndAssignment) {
  // `reserve` and `shrink_to_fit` currently do nothing, and even if they did,
  // they shouldn't change the contained elements.
  BufferedVector<int> b{4, "_testBufMove.dat"};
  std::vector<int> i{12, 10, 8};
  b.insert(b.begin(), i.begin(), i.end());

  auto b2{std::move(b)};
  ASSERT_TRUE(b2.isInternal());
  ASSERT_THAT(b2, ::testing::ElementsAreArray(i));
  b = std::move(b2);
  ASSERT_TRUE(b.isInternal());
  ASSERT_THAT(b, ::testing::ElementsAreArray(i));

  // Same for external stuff.
  i.push_back(12);
  b.push_back(12);
  i.push_back(-13);
  b.push_back(-13);
  ASSERT_FALSE(b.isInternal());

  auto b3{std::move(b)};
  ASSERT_FALSE(b3.isInternal());
  ASSERT_THAT(b3, ::testing::ElementsAreArray(i));
  b2 = std::move(b3);
  ASSERT_FALSE(b2.isInternal());
  ASSERT_THAT(b2, ::testing::ElementsAreArray(i));
}
