// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "IdColumnTestHelpers.h"
#include "engine/idTable/splitLayout/IdColumn.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "engine/idTable/splitLayout/IdRef.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::sampleIds;
using testHelpers::TestAllocator;

// _____________________________________________________________________________
TEST(IdColumnTest, viewConstructionAccessAndDefaultState) {
  columnBasedIdTable::splitLayout::ConstIdColumnRef defaultView;
  EXPECT_EQ(defaultView.size(), 0u);
  EXPECT_TRUE(defaultView.empty());
  std::array<columnBasedIdTable::splitLayout::ConstIdColumnRef, 3> arrayOfViews;
  EXPECT_TRUE(arrayOfViews[1].empty());

  auto ids = sampleIds();
  std::vector<uint64_t> payloads;
  std::vector<uint8_t> datatypes;
  for (Id id : ids) {
    auto [datatype_, payload_] = getBitsCompat(id);
    payloads.push_back(payload_);
    datatypes.push_back(datatype_);
  }

  columnBasedIdTable::splitLayout::IdColumnRef view{
      payloads.data(), datatypes.data(), payloads.size()};
  ASSERT_EQ(view.size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(view[i]), ids.at(i));
    EXPECT_EQ(static_cast<Id>(view.at(i)), ids.at(i));
  }
  EXPECT_EQ(static_cast<Id>(view.front()), ids.front());
  EXPECT_EQ(static_cast<Id>(view.back()), ids.back());

  // Implicit conversion to the const view.
  columnBasedIdTable::splitLayout::ConstIdColumnRef constView = view;
  EXPECT_EQ(constView.size(), view.size());

  // Raw access to the two underlying arrays.
  ASSERT_EQ(view.rawPayloads().size(), ids.size());
  ASSERT_EQ(view.rawDatatypes().size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(view.rawPayloads()[i], getBitsCompat(ids.at(i)).payload_);
    EXPECT_EQ(view.rawDatatypes()[i], getBitsCompat(ids.at(i)).datatype_);
  }

  // Mutation through the mutable view is visible in the backing arrays.
  view[0] = Id::makeFromInt(-1);
  EXPECT_EQ(payloads.at(0), getBitsCompat(Id::makeFromInt(-1)).payload_);
}

// _____________________________________________________________________________
TEST(IdColumnTest, constructIdColumnView) {
  constexpr columnBasedIdTable::splitLayout::ConstIdColumnRef defaultView;
  EXPECT_TRUE(defaultView.empty());
  EXPECT_EQ(defaultView.size(), 0u);

  auto [datatype_, payload_] = getBitsCompat(Id::makeFromInt(42));
  const columnBasedIdTable::splitLayout::IdColumnRef mutableView{&payload_,
                                                                 &datatype_, 1};
  EXPECT_FALSE(mutableView.empty());
  EXPECT_EQ(mutableView.size(), 1u);
  EXPECT_EQ(static_cast<Id>(mutableView[0]), Id::makeFromInt(42));
  static_assert(std::is_same_v<decltype(mutableView[0]), IdRef>);

  const columnBasedIdTable::splitLayout::ConstIdColumnRef constConvertedView{
      mutableView};
  EXPECT_FALSE(constConvertedView.empty());
  EXPECT_EQ(constConvertedView.size(), 1u);
  EXPECT_EQ(static_cast<Id>(constConvertedView[0]), Id::makeFromInt(42));
  static_assert(std::is_same_v<decltype(constConvertedView[0]), ConstIdRef>);
}

// _____________________________________________________________________________
TEST(IdColumnTest, idColumnViewValueOperators) {
  constexpr columnBasedIdTable::splitLayout::ConstIdColumnRef defaultView;
  EXPECT_TRUE(defaultView.empty());
  EXPECT_EQ(defaultView.size(), 0u);

  // `operator[]`/`at()`/`front()`/`back()` are all bounds-checked (via
  // `AD_CONTRACT_CHECK` in `operator[]`, which `at()`/`front()`/`back()` are
  // implemented in terms of), so all four throw on an empty view.
  ASSERT_THROW((void)defaultView.at(0u), ad_utility::Exception);
  ASSERT_THROW((void)defaultView[0u], ad_utility::Exception);
  ASSERT_THROW((void)defaultView.front(), ad_utility::Exception);
  ASSERT_THROW((void)defaultView.back(), ad_utility::Exception);

  auto [datatype_, payload_] = getBitsCompat(Id::makeFromInt(42));
  const columnBasedIdTable::splitLayout::IdColumnRef mutableView{&payload_,
                                                                 &datatype_, 1};
  EXPECT_FALSE(mutableView.empty());
  EXPECT_EQ(mutableView.size(), 1u);
  EXPECT_EQ(static_cast<Id>(mutableView[0]), Id::makeFromInt(42));
  EXPECT_EQ(static_cast<Id>(mutableView[0]), mutableView.at(0u));
  EXPECT_EQ(static_cast<Id>(mutableView[0]), mutableView.front());
  EXPECT_EQ(static_cast<Id>(mutableView[0]), mutableView.back());

  mutableView.at(0) = Id::makeFromInt(12);
  EXPECT_EQ(static_cast<Id>(mutableView[0]), Id::makeFromInt(12));
  EXPECT_EQ(static_cast<Id>(mutableView[0]), mutableView.at(0u));

  auto [datatype_2, payload_2] = getBitsCompat(Id::makeFromInt(13));
  std::vector payloads = {payload_, payload_2};
  std::vector datatypes = {datatype_, datatype_2};
  const columnBasedIdTable::splitLayout::IdColumnRef mutableView2{
      payloads.data(), datatypes.data(), payloads.size()};

  EXPECT_FALSE(mutableView2.empty());
  EXPECT_EQ(mutableView2.size(), 2u);
  EXPECT_EQ(static_cast<Id>(mutableView2[0]), Id::makeFromInt(12));
  EXPECT_EQ(static_cast<Id>(mutableView2[1]), Id::makeFromInt(13));
  EXPECT_EQ(static_cast<Id>(mutableView2.at(1u)), Id::makeFromInt(13));
  EXPECT_EQ(static_cast<Id>(mutableView2[0]), mutableView2.front());
  EXPECT_EQ(static_cast<Id>(mutableView2[1]), mutableView2.back());
  EXPECT_EQ(mutableView2.front(), Id::makeFromInt(12));
  EXPECT_EQ(mutableView2.back(), Id::makeFromInt(13));
}

// _____________________________________________________________________________
TEST(IdColumnTest, subspanFirstLast) {
  auto ids = sampleIds();
  std::vector<uint64_t> payloads;
  std::vector<uint8_t> datatypes;
  for (const Id id : ids) {
    auto [datatype_, payload_] = getBitsCompat(id);
    payloads.push_back(payload_);
    datatypes.push_back(datatype_);
  }
  columnBasedIdTable::splitLayout::ConstIdColumnRef view{
      payloads.data(), datatypes.data(), payloads.size()};

  // 1. `subspan(offset, count)`: a slice `[offset, offset + count)`.
  auto middle = view.subspan(2, 3);
  ASSERT_EQ(middle.size(), 3u);
  for (size_t i = 0; i < middle.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(middle[i]), ids.at(2 + i));
  }

  // 2. `count` defaults to `npos`, meaning "until the end of the view" --
  //    no need to pass `ids.size() - offset` yourself.
  auto toEnd = view.subspan(2);
  EXPECT_EQ(toEnd.size(), ids.size() - 2);
  EXPECT_EQ(static_cast<Id>(toEnd[0]), ids.at(2));
  EXPECT_EQ(static_cast<Id>(toEnd.back()), ids.back());

  // 3. `offset == size()` is a legal edge case: the empty subrange right at
  //    the end (like `.substr(str.size())` on a `std::string`).
  auto atTheEnd = view.subspan(view.size(), 0);
  EXPECT_TRUE(atTheEnd.empty());

  // 4. `first(n)`/`last(n)` are defined in terms of `subspan`, so they
  //    should agree with the equivalent `subspan` call exactly.
  EXPECT_EQ(view.first(3).size(), 3u);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(static_cast<Id>(view.first(3)[i]), ids.at(i));
  }
  EXPECT_EQ(view.last(3).size(), 3u);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(static_cast<Id>(view.last(3)[i]), ids.at(ids.size() - 3 + i));
  }

  // 5. All three are bounds-checked via `AD_CONTRACT_CHECK`.
  EXPECT_THROW((void)view.subspan(view.size() + 1), ad_utility::Exception);
  EXPECT_THROW((void)view.subspan(0, view.size() + 1), ad_utility::Exception);
  EXPECT_THROW((void)view.last(view.size() + 1), ad_utility::Exception);
}
