// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>
//
#include <gtest/gtest.h>
#include <unistd.h>

#include <stdexcept>
#include <vector>

#include "../src/util/MmapVector.h"

using ad_utility::MmapVector;
using ad_utility::MmapVectorView;

// we use 5000 ints to test so that we need more than one page.

// ___________________________________________________________________
TEST(MmapVectorTest, DefaultConstructor) {
  MmapVector<int> v;
  ASSERT_EQ(size_t(0), v.size());
  ASSERT_EQ(nullptr, v.begin());
  ASSERT_EQ(nullptr, v.end());
  ASSERT_THROW(v[0], ad_utility::UninitializedArrayException);
  ASSERT_THROW(v.at(0), ad_utility::UninitializedArrayException);
  ASSERT_THROW(v[0] = 2, ad_utility::UninitializedArrayException);
  ASSERT_THROW(v.at(0) = 2, ad_utility::UninitializedArrayException);
}

// ___________________________________________________________________
TEST(MmapVectorTest, NewEmptyFileConstructor) {
  MmapVector<int> v("_test.mmap", ad_utility::CreateTag());
  ASSERT_EQ(size_t(0), v.size());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin(), v.end());
}

// ___________________________________________________________________
TEST(MmapVectorTest, NewFileSizeConstructor) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  MmapVector<int> v(s, "_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_LE(s, v.capacity());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());
}

// ___________________________________________________________________
TEST(MmapVectorTest, AccessOperator) {
  {
    MmapVector<int> v(5000, "_test.mmap");
    for (size_t i = 0; i < 5000; i++) {
      v[i] = 5000 - i;
    }
    for (int i = 0; i < 5000; i++) {
      ASSERT_EQ(v[i], 5000 - i);
      const auto x = v[i];
      ASSERT_EQ(x, 5000 - i);
    }
  }
  size_t s = 5000;
  MmapVectorView<int> v("_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());

  for (int i = 0; i < 5000; i++) {
    ASSERT_EQ(v[i], 5000 - i);
  }
}

// ___________________________________________________________________
TEST(MmapVectorTest, At) {
  {
    MmapVector<int> v(5000, "_test.mmap");
    for (int i = 0; i < 5000; i++) {
      v.at(i) = 5000 - i;
    }
    for (int i = 0; i < 5000; i++) {
      ASSERT_EQ(v.at(i), 5000 - i);
      const auto x = v.at(i);
      ASSERT_EQ(x, 5000 - i);
    }

    ASSERT_THROW(v.at(5000), std::out_of_range);
  }

  MmapVectorView<int> v("_test.mmap");
  size_t s = 5000;
  ASSERT_EQ(s, v.size());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());
  for (int i = 0; i < 5000; i++) {
    ASSERT_EQ(v.at(i), 5000 - i);
  }
  ASSERT_THROW(v.at(5000), std::out_of_range);
}

// ___________________________________________________________________
TEST(MmapVectorTest, DefaultValueConstructor) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  MmapVector<int> v(s, 42, "_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_LE(s, v.capacity());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v[i], 42);
    ASSERT_EQ(v.at(i), 42);
  }
}

// ___________________________________________________________________
TEST(MmapVectorTest, IteratorConstructor) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  std::vector<int> tmpVec(5000);
  for (int i = 0; i < 5000; ++i) {
    tmpVec[i] = 5000 - i;
  }
  MmapVector<int> v(tmpVec.begin(), tmpVec.end(), "_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_LE(s, v.capacity());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v[i], tmpVec[i]);
    ASSERT_EQ(v.at(i), tmpVec[i]);
  }
}

// ___________________________________________________________________
TEST(MmapVectorTest, PushBackRvalue) {
  {
    MmapVector<int> v(0, "_test.mmap");
    ASSERT_EQ(size_t(0), v.size());
    for (int i = 0; i < 5000; i++) {
      v.push_back(5000 - i);
    }
    size_t s = 5000;
    ASSERT_EQ(s, v.size());
    ASSERT_LE(s, v.capacity());
    for (int i = 0; i < 5000; i++) {
      ASSERT_EQ(v.at(i), 5000 - i);
    }
  }
}

// ___________________________________________________________________
TEST(MmapVectorTest, PushBackLvalue) {
  {
    MmapVector<int> v(0, "_test.mmap");
    ASSERT_EQ(size_t(0), v.size());
    for (int i = 0; i < 5000; i++) {
      int tmp = 5000 - i;
      v.push_back(tmp);
    }
    size_t s = 5000;
    ASSERT_EQ(s, v.size());
    ASSERT_LE(s, v.capacity());
    for (int i = 0; i < 5000; i++) {
      ASSERT_EQ(v.at(i), 5000 - i);
    }
  }
}

// ____________________________________________________________________
TEST(MmapVectorTest, constIterators) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  std::vector<int> tmpVec(5000);
  for (int i = 0; i < 5000; ++i) {
    tmpVec[i] = 5000 - i;
  }
  {
    MmapVector<int> v(tmpVec.begin(), tmpVec.end(), "_test.mmap");
    ASSERT_EQ(s, v.size());
    ASSERT_LE(s, v.capacity());
    ASSERT_NE(nullptr, v.begin());
    ASSERT_EQ(v.begin() + s, v.end());

    {
      int idx = 0;
      auto it = v.cbegin();
      while (it != v.cend()) {
        ASSERT_EQ(*it, tmpVec[idx]);
        ++idx;
        ++it;
      }

      idx = 0;
      for (const auto& x : v) {
        ASSERT_EQ(x, tmpVec[idx]);
        ++idx;
      }
    }
  }
  MmapVectorView<int> v("_test.mmap");
  int idx = 0;
  auto it = v.cbegin();
  while (it != v.cend()) {
    ASSERT_EQ(*it, tmpVec[idx]);
    ++idx;
    ++it;
  }

  idx = 0;
  for (const auto& x : v) {
    ASSERT_EQ(x, tmpVec[idx]);
    ++idx;
  }
}

// ____________________________________________________________________
TEST(MmapVectorTest, nonConstIterators) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  // initialize all elements to 42
  MmapVector<int> v(s, 42, "_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_LE(s, v.capacity());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());

  for (auto& x : v) {
    ++x;
  }

  for (size_t i = 0; i < s; ++i) {
    ASSERT_EQ(43, v[i]);
  }
}

// ____________________________________________________________________
TEST(MmapVectorTest, Close) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  // initialize all elements to 42
  MmapVector<int> v(s, 42, "_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_LE(s, v.capacity());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());
  v.close();

  ASSERT_EQ(size_t(0), v.size());
  ASSERT_EQ(nullptr, v.begin());

  MmapVectorView<int> v2("_test.mmap");
  ASSERT_EQ(s, v2.size());
  ASSERT_NE(nullptr, v2.begin());
  ASSERT_EQ(v2.begin() + s, v2.end());
  v2.close();

  ASSERT_EQ(size_t(0), v2.size());
  ASSERT_EQ(nullptr, v2.begin());
}

// ____________________________________________________________________
TEST(MmapVectorTest, Reuse) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  // initialize all elements to 42
  {
    MmapVector<int> v(s, "_test.mmap");
    ASSERT_EQ(s, v.size());
    ASSERT_LE(s, v.capacity());
    ASSERT_NE(nullptr, v.begin());
    ASSERT_EQ(v.begin() + s, v.end());
    for (int i = 0; i < 5000; ++i) {
      v[i] = i;
    }
  }
  // v is now destroyed
  MmapVector<int> v2("_test.mmap", ad_utility::ReuseTag());
  ASSERT_EQ(s, v2.size());
  ASSERT_LE(s, v2.capacity());
  ASSERT_NE(nullptr, v2.begin());
  ASSERT_EQ(v2.begin() + s, v2.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v2[i], i);
  }

  // v is now destroyed
  MmapVectorView<int> v3("_test.mmap");
  ASSERT_EQ(s, v3.size());
  ASSERT_NE(nullptr, v3.begin());
  ASSERT_EQ(v3.begin() + s, v3.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v3[i], i);
  }
}

// ____________________________________________________________________
TEST(MmapVectorTest, MoveConstructor) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings

  size_t s = 5000;
  {
    // initialize all elements to 42
    MmapVector<int> v(s, "_test.mmap");
    ASSERT_EQ(s, v.size());
    ASSERT_LE(s, v.capacity());
    ASSERT_NE(nullptr, v.begin());
    ASSERT_EQ(v.begin() + s, v.end());
    for (int i = 0; i < 5000; ++i) {
      v[i] = i;
    }

    MmapVector<int> v2(std::move(v));
    ASSERT_EQ(nullptr, v.begin());

    ASSERT_EQ(s, v2.size());
    ASSERT_LE(s, v2.capacity());
    ASSERT_NE(nullptr, v2.begin());
    ASSERT_EQ(v2.begin() + s, v2.end());
    for (int i = 0; i < 5000; ++i) {
      ASSERT_EQ(v2[i], i);
    }
  }
  MmapVectorView<int> v("_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v[i], i);
  }

  MmapVectorView<int> v2(std::move(v));
  ASSERT_EQ(nullptr, v.begin());

  ASSERT_EQ(s, v2.size());
  ASSERT_NE(nullptr, v2.begin());
  ASSERT_EQ(v2.begin() + s, v2.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v2[i], i);
  }
}

// ____________________________________________________________________
TEST(MmapVectorTest, MoveAssignment) {
  // make sure that we get a unsignned 5000 to prevent compiler warnings
  size_t s = 5000;
  {
    // initialize all elements to 42
    MmapVector<int> v(s, "_test.mmap");
    ASSERT_EQ(s, v.size());
    ASSERT_LE(s, v.capacity());
    ASSERT_NE(nullptr, v.begin());
    ASSERT_EQ(v.begin() + s, v.end());
    for (int i = 0; i < 5000; ++i) {
      v[i] = i;
    }

    MmapVector<int> v2 = std::move(v);
    ASSERT_EQ(nullptr, v.begin());

    ASSERT_EQ(s, v2.size());
    ASSERT_LE(s, v2.capacity());
    ASSERT_NE(nullptr, v2.begin());
    ASSERT_EQ(v2.begin() + s, v2.end());
    for (int i = 0; i < 5000; ++i) {
      ASSERT_EQ(v2[i], i);
    }
  }
  MmapVectorView<int> v("_test.mmap");
  ASSERT_EQ(s, v.size());
  ASSERT_NE(nullptr, v.begin());
  ASSERT_EQ(v.begin() + s, v.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v[i], i);
  }

  MmapVectorView<int> v2 = std::move(v);
  ASSERT_EQ(nullptr, v.begin());

  ASSERT_EQ(s, v2.size());
  ASSERT_NE(nullptr, v2.begin());
  ASSERT_EQ(v2.begin() + s, v2.end());
  for (int i = 0; i < 5000; ++i) {
    ASSERT_EQ(v2[i], i);
  }
}
