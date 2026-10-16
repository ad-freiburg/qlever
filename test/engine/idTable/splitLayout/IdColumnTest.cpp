// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <type_traits>
#include <vector>

#include "IdColumnTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/splitLayout/IdColumn.h"
#include "engine/idTable/splitLayout/IdRef.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::sampleIds;

namespace {
// Split the `ids` into the two arrays of the split layout.
std::pair<std::vector<uint64_t>, std::vector<uint8_t>> splitIds(
    const std::vector<Id>& ids) {
  std::vector<uint64_t> payloads;
  std::vector<uint8_t> datatypes;
  for (const Id id : ids) {
    auto [datatype, payload] = getBitsCompat(id);
    payloads.push_back(payload);
    datatypes.push_back(datatype);
  }
  return {std::move(payloads), std::move(datatypes)};
}
}  // namespace

// Test the default view, the element access of a view, the implicit
// conversion to the const view, and writing through the mutable view.
TEST(IdColumnTest, elementAccess) {
  // A default-constructed view is empty, also inside an array of views, and
  // `at()`, `front()` and `back()` throw on it (`operator[]` is only checked
  // in builds with expensive checks).
  constexpr ConstIdColumnRef defaultView;
  EXPECT_TRUE(defaultView.empty());
  EXPECT_EQ(defaultView.size(), 0u);
  std::array<ConstIdColumnRef, 3> arrayOfViews;
  EXPECT_TRUE(arrayOfViews[1].empty());
  EXPECT_THROW((void)defaultView.at(0u), ad_utility::Exception);
  EXPECT_THROW((void)defaultView.front(), ad_utility::Exception);
  EXPECT_THROW((void)defaultView.back(), ad_utility::Exception);

  // A mutable view of the sample ids yields each element via `operator[]`,
  // `at()`, `front()` and `back()`.
  auto ids = sampleIds();
  auto [payloads, datatypes] = splitIds(ids);
  const IdColumnRef view{payloads.data(), datatypes.data(), payloads.size()};
  // `operator[]` returns the restricted reference (what `auto` deduces).
  static_assert(std::is_same_v<decltype(view[0]), IdRef::Base>);
  ASSERT_EQ(view.size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(view[i], ids.at(i));
    EXPECT_EQ(view.at(i), ids.at(i));
  }
  EXPECT_EQ(view.front(), ids.front());
  EXPECT_EQ(view.back(), ids.back());

  // The mutable view converts implicitly to the const view, which yields
  // `ConstIdRef`s and sees the same elements.
  const ConstIdColumnRef constView = view;
  static_assert(std::is_same_v<decltype(constView[0]), ConstIdRef::Base>);
  EXPECT_EQ(constView.size(), view.size());
  EXPECT_EQ(constView.front(), ids.front());
  EXPECT_EQ(constView.back(), ids.back());

  // The raw arrays are the two input arrays.
  ASSERT_EQ(view.rawPayloads().size(), ids.size());
  ASSERT_EQ(view.rawDatatypes().size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(view.rawPayloads()[i], getBitsCompat(ids.at(i)).payload_);
    EXPECT_EQ(view.rawDatatypes()[i], getBitsCompat(ids.at(i)).datatype_);
  }

  // Writing through `operator[]` or `at()` of the mutable view changes the
  // backing arrays.
  view[0] = Id::makeFromInt(-1);
  EXPECT_EQ(payloads.at(0), getBitsCompat(Id::makeFromInt(-1)).payload_);
  view.at(1) = Id::makeFromInt(12);
  EXPECT_EQ(view[1], Id::makeFromInt(12));
  EXPECT_EQ(constView[1], Id::makeFromInt(12));
}

// Test `subspan`, `first` and `last`.
TEST(IdColumnTest, subspanFirstLast) {
  auto ids = sampleIds();
  auto [payloads, datatypes] = splitIds(ids);
  const ConstIdColumnRef view{payloads.data(), datatypes.data(),
                              payloads.size()};

  // `subspan(offset, count)` is the slice `[offset, offset + count)`.
  auto middle = view.subspan(2, 3);
  ASSERT_EQ(middle.size(), 3u);
  for (size_t i = 0; i < middle.size(); ++i) {
    EXPECT_EQ(middle[i], ids.at(2 + i));
  }

  // Without `count`, the subspan reaches until the end of the view.
  auto toEnd = view.subspan(2);
  EXPECT_EQ(toEnd.size(), ids.size() - 2);
  EXPECT_EQ(toEnd[0], ids.at(2));
  EXPECT_EQ(toEnd.back(), ids.back());

  // `offset == size()` gives the empty subspan at the end.
  EXPECT_TRUE(view.subspan(view.size(), 0).empty());
  EXPECT_TRUE(view.subspan(view.size()).empty());

  // `first(n)` and `last(n)` are the first and the last `n` elements.
  ASSERT_EQ(view.first(3).size(), 3u);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(view.first(3)[i], ids.at(i));
  }
  ASSERT_EQ(view.last(3).size(), 3u);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(view.last(3)[i], ids.at(ids.size() - 3 + i));
  }

  // All three throw when the requested range exceeds the view.
  EXPECT_THROW((void)view.subspan(view.size() + 1), ad_utility::Exception);
  EXPECT_THROW((void)view.subspan(0, view.size() + 1), ad_utility::Exception);
  EXPECT_THROW((void)view.first(view.size() + 1), ad_utility::Exception);
  EXPECT_THROW((void)view.last(view.size() + 1), ad_utility::Exception);
}

// Test that `operator[]`, `at`, `front` and `back` return the restricted
// reference: Writing is possible via an rvalue or via an explicitly requested
// `IdRef`, but not via a variable that was created by `auto`.
TEST(IdColumnTest, restrictedElementAccess) {
  auto ids = sampleIds();
  auto [payloads, datatypes] = splitIds(ids);
  const IdColumnRef view{payloads.data(), datatypes.data(), payloads.size()};
  using Restricted = IdRef::Base;
  static_assert(std::is_same_v<decltype(view.at(0)), Restricted>);
  static_assert(std::is_same_v<decltype(view.front()), Restricted>);
  static_assert(std::is_same_v<decltype(view.back()), Restricted>);
  static_assert(std::is_assignable_v<decltype(view[0]), Id>);
  static_assert(!std::is_assignable_v<decltype(view[0])&, Id>);

  // Assigning to the temporary writes to the column.
  const Id first = Id::makeFromInt(42);
  view[1] = first;
  EXPECT_EQ(view[1], first);
  EXPECT_EQ(payloads.at(1), getBitsCompat(first).payload_);
  EXPECT_EQ(datatypes.at(1), getBitsCompat(first).datatype_);

  // The same for copying the value of another element.
  view[2] = view[1];
  EXPECT_EQ(view[2], first);

  // A variable has to request the writable reference explicitly.
  IdRef ref = view[3];
  const Id second = Id::makeFromInt(43);
  ref = second;
  EXPECT_EQ(view[3], second);

  // `auto` copies the (read-only) reference, which still reads the column.
  auto autoRef = view[3];
  view[3] = first;
  EXPECT_EQ(autoRef, first);
}

// Test that STL algorithms work on a column, whose iterators yield the
// restricted reference.
TEST(IdColumnTest, algorithmsOnColumns) {
  auto ids = sampleIds();
  auto [payloads, datatypes] = splitIds(ids);
  const IdColumnRef view{payloads.data(), datatypes.data(), payloads.size()};

  // Writing via a lambda that takes the explicit `IdRef`. Note that
  // `ql::ranges::for_each` would not accept this lambda: It requires that the
  // function can also be called with an `Id&` (`std::iter_value_t&`), which
  // cannot be converted to an `IdRef`. `std::for_each` and range-based `for`
  // loops do not have this requirement.
  const Id one = Id::makeFromInt(1);
  std::for_each(view.begin(), view.end(), [one](IdRef id) { id = one; });
  for (size_t i = 0; i < view.size(); ++i) {
    EXPECT_EQ(view[i], one);
  }
  const Id two = Id::makeFromInt(2);
  for (IdRef id : view) {
    id = two;
  }
  EXPECT_EQ(view.front(), two);
  EXPECT_EQ(view.back(), two);

  // Sorting a column moves the elements via the references.
  for (size_t i = 0; i < view.size(); ++i) {
    view[i] = Id::makeFromInt(static_cast<int64_t>(view.size() - i));
  }
  ql::ranges::sort(view);
  for (size_t i = 0; i < view.size(); ++i) {
    EXPECT_EQ(view[i], Id::makeFromInt(static_cast<int64_t>(i + 1)));
  }
  std::reverse(view.begin(), view.end());
  EXPECT_EQ(view.front(), Id::makeFromInt(static_cast<int64_t>(view.size())));
  EXPECT_EQ(view.back(), Id::makeFromInt(1));
}
