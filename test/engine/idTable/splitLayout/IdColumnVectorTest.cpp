// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <vector>

#include "IdColumnTestHelpers.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "util/MemorySize/MemorySize.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::sampleIds;
using testHelpers::testAllocator;
using testHelpers::TestAllocator;

// Test the constructors and the functions that change the size.
TEST(IdColumnVectorTest, constructionAndGrowth) {
  // An empty vector grows by `push_back` and `emplace_back`.
  auto ids = sampleIds();
  IdColumnVector vec{testAllocator()};
  EXPECT_TRUE(vec.empty());
  for (const Id id : ids) {
    vec.push_back(id);
  }
  EXPECT_EQ(vec.size(), ids.size());
  for (size_t i = 0; i < vec.size(); ++i) {
    EXPECT_EQ(vec[i], ids.at(i));
  }
  vec.emplace_back();
  EXPECT_EQ(vec.size(), ids.size() + 1);

  // `resize` keeps the leading elements, `clear` empties the vector,
  // `reserve` and `shrink_to_fit` do not change the contents.
  vec.resize(2);
  EXPECT_EQ(vec.size(), 2u);
  EXPECT_EQ(vec.at(0), ids.at(0));
  vec.reserve(100);
  EXPECT_EQ(vec.size(), 2u);
  vec.clear();
  EXPECT_TRUE(vec.empty());
  vec.shrink_to_fit();
  EXPECT_TRUE(vec.empty());

  // The sized constructor, the range constructor, and `get_allocator`.
  const IdColumnVector sized{5, testAllocator()};
  EXPECT_EQ(sized.size(), 5u);
  const IdColumnVector fromRange{ids.begin(), ids.end(), testAllocator()};
  ASSERT_EQ(fromRange.size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(fromRange[i], ids.at(i));
  }
  const auto allocator = testAllocator();
  const IdColumnVector withAllocator{allocator};
  EXPECT_EQ(withAllocator.get_allocator(), allocator);
}

// Test that `at()` is bounds-checked, also for writing.
TEST(IdColumnVectorTest, atIsBoundsChecked) {
  IdColumnVector vec{testAllocator()};
  vec.push_back(Id::makeFromInt(1));
  EXPECT_EQ(vec.at(0), Id::makeFromInt(1));
  EXPECT_THROW((void)vec.at(1), ad_utility::Exception);
  EXPECT_THROW(vec.at(1) = Id::makeFromInt(2), ad_utility::Exception);
}

// Test `erase` and `insert` with const iterators and with mutable iterators.
TEST(IdColumnVectorTest, eraseAndInsert) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  // Erase a range and insert two ids at its place, via const iterators.
  auto view = vec.asConstView();
  vec.erase(view.begin() + 1, view.begin() + 3);
  EXPECT_EQ(vec.size(), ids.size() - 2);
  EXPECT_EQ(vec[0], ids.at(0));
  EXPECT_EQ(vec[1], ids.at(3));
  std::vector toInsert{Id::makeFromInt(1), Id::makeFromInt(2)};
  auto viewAfterErase = vec.asConstView();
  vec.insert(viewAfterErase.begin() + 1, toInsert.begin(), toInsert.end());
  EXPECT_EQ(vec.size(), ids.size());
  EXPECT_EQ(vec[1], toInsert.at(0));
  EXPECT_EQ(vec[2], toInsert.at(1));
  EXPECT_EQ(vec[3], ids.at(3));

  // Erase a single element and insert one id at the front, via mutable
  // iterators.
  auto mutableView = vec.asView();
  vec.erase(mutableView.begin());
  EXPECT_EQ(vec.size(), ids.size() - 1);
  EXPECT_EQ(vec[0], toInsert.at(0));
  std::vector toInsertFront{Id::makeFromInt(3)};
  auto mutableViewAfterErase = vec.asView();
  vec.insert(mutableViewAfterErase.begin(), toInsertFront.begin(),
             toInsertFront.end());
  EXPECT_EQ(vec[0], toInsertFront.at(0));
  EXPECT_EQ(vec[1], toInsert.at(0));
}

// Test `asView`, `asConstView`, and the implicit conversions to the views.
TEST(IdColumnVectorTest, asViewAndImplicitConversion) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  // The explicit views have the size of the vector.
  EXPECT_EQ(vec.asView().size(), vec.size());
  EXPECT_EQ(vec.asConstView().size(), vec.size());

  // The vector converts implicitly to both views.
  auto takesView = [](const IdColumnRef& v) { return v.size(); };
  auto takesConstView = [](const ConstIdColumnRef& v) { return v.size(); };
  EXPECT_EQ(takesView(vec), vec.size());
  EXPECT_EQ(takesConstView(vec), vec.size());
}

// Test that a failed `insert` leaves the vector unchanged, also when only
// the second of the two underlying arrays fails to grow.
TEST(IdColumnVectorTest, insertRollsBackOnDatatypesAllocationFailure) {
  // A memory limit that suffices for the payloads, but not for the
  // datatypes.
  constexpr size_t numElements = 1000;
  TestAllocator allocator{ad_utility::testing::makeAllocator(
      ad_utility::MemorySize::bytes(numElements * sizeof(uint64_t) + 500))};
  IdColumnVector vec{allocator};

  // The insert throws and the vector is still empty.
  std::vector toInsert(numElements, Id::makeFromInt(42));
  EXPECT_THROW(
      vec.insert(vec.asView().begin(), toInsert.begin(), toInsert.end()),
      ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_TRUE(vec.empty());
}
