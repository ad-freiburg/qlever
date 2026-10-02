// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
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
#include "util/MemoryLimitTracker.h"
#include "util/MemorySize/MemorySize.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::sampleIds;
using testHelpers::testAllocator;
using testHelpers::TestAllocator;

// _____________________________________________________________________________
TEST(IdColumnVectorTest, growthAndAllocatorPropagation) {
  IdColumnVector vec{testAllocator()};
  EXPECT_TRUE(vec.empty());

  for (Id id : sampleIds()) {
    vec.push_back(id);
  }
  EXPECT_EQ(vec.size(), sampleIds().size());
  for (size_t i = 0; i < vec.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(vec[i]), sampleIds().at(i));
  }

  vec.emplace_back();
  EXPECT_EQ(vec.size(), sampleIds().size() + 1);

  vec.resize(2);
  EXPECT_EQ(vec.size(), 2u);
  EXPECT_EQ(static_cast<Id>(vec.at(0)), sampleIds().at(0));

  vec.reserve(100);
  vec.clear();
  EXPECT_TRUE(vec.empty());
  vec.shrink_to_fit();

  IdColumnVector sized{5, testAllocator()};
  EXPECT_EQ(sized.size(), 5u);

  auto allocator = testAllocator();
  IdColumnVector withAllocator{allocator};
  EXPECT_EQ(withAllocator.get_allocator(), allocator);
}

// _____________________________________________________________________________
TEST(IdColumnVectorTest, rangeConstructorFromIds) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  ASSERT_EQ(vec.size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(vec[i]), ids.at(i));
  }
}

// _____________________________________________________________________________
TEST(IdColumnVectorTest, atIsBoundsCheckedUnlikeOperatorBrackets) {
  IdColumnVector vec{testAllocator()};
  vec.push_back(Id::makeFromInt(1));
  EXPECT_EQ(static_cast<Id>(vec.at(0)), Id::makeFromInt(1));
  EXPECT_THROW((void)vec.at(1), ad_utility::Exception);
  EXPECT_THROW(vec.at(1) = Id::makeFromInt(2), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(IdColumnVectorTest, eraseAndInsert) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  auto view = vec.asConstView();
  vec.erase(view.begin() + 1, view.begin() + 3);
  EXPECT_EQ(vec.size(), ids.size() - 2);
  EXPECT_EQ(static_cast<Id>(vec[0]), ids.at(0));
  EXPECT_EQ(static_cast<Id>(vec[1]), ids.at(3));

  std::vector toInsert{Id::makeFromInt(1), Id::makeFromInt(2)};
  auto viewAfterErase = vec.asConstView();
  vec.insert(viewAfterErase.begin() + 1, toInsert.begin(), toInsert.end());
  EXPECT_EQ(static_cast<Id>(vec[1]), toInsert.at(0));
  EXPECT_EQ(static_cast<Id>(vec[2]), toInsert.at(1));
}

// _____________________________________________________________________________
TEST(IdColumnVectorTest, eraseAndInsertWithMutableIterator) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  auto view = vec.asView();
  vec.erase(view.begin());
  EXPECT_EQ(vec.size(), ids.size() - 1);
  EXPECT_EQ(static_cast<Id>(vec[0]), ids.at(1));

  std::vector toInsert{Id::makeFromInt(1)};
  auto viewAfterErase = vec.asView();
  vec.insert(viewAfterErase.begin(), toInsert.begin(), toInsert.end());
  EXPECT_EQ(static_cast<Id>(vec[0]), toInsert.at(0));
  EXPECT_EQ(static_cast<Id>(vec[1]), ids.at(1));
}

// _____________________________________________________________________________
TEST(IdColumnVectorTest, asViewAndImplicitConversion) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  const auto view = vec.asView();
  EXPECT_EQ(view.size(), vec.size());
  const auto constView = vec.asConstView();
  EXPECT_EQ(constView.size(), vec.size());

  auto takesView = [](const columnBasedIdTable::splitLayout::IdColumnRef& v) {
    return v.size();
  };
  auto takesConstView =
      [](const columnBasedIdTable::splitLayout::ConstIdColumnRef& v) {
        return v.size();
      };
  EXPECT_EQ(takesView(vec), vec.size());
  EXPECT_EQ(takesConstView(vec), vec.size());
}

// _____________________________________________________________________________
TEST(IdColumnVectorTest, insertRollsBackOnDatatypesAllocationFailure) {
  constexpr size_t numElements = 1000;
  TestAllocator allocator{ad_utility::testing::makeAllocator(
      ad_utility::MemorySize::bytes(numElements * sizeof(uint64_t) + 500))};
  IdColumnVector vec{allocator};

  std::vector toInsert(numElements, Id::makeFromInt(42));
  EXPECT_THROW(
      vec.insert(vec.asView().begin(), toInsert.begin(), toInsert.end()),
      ad_utility::detail::AllocationExceedsLimitException);

  // The vector must be left exactly as it was before the failed insert, not
  // partially grown.
  EXPECT_TRUE(vec.empty());
}
