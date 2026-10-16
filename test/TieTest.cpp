// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <tuple>
#include <type_traits>
#include <utility>

#include "./engine/idTable/splitLayout/IdColumnTestHelpers.h"
#include "./util/AllocatorTestHelpers.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "util/ConstexprUtils.h"
#include "util/Tie.h"

using ad_utility::tieElement;
using ad_utility::TieElement;
using ad_utility::tieElementsByIndexSequence;
using columnBasedIdTable::splitLayout::ConstIdRef;
using columnBasedIdTable::splitLayout::IdColumnVector;
using columnBasedIdTable::splitLayout::IdRef;
using columnBasedIdTable::splitLayout::testHelpers::testAllocator;
using columnBasedIdTable::splitLayout::testHelpers::TestAllocator;
namespace {
Id intId(int64_t value) { return Id::makeFromInt(value); }

// Test the types of the tied elements: an lvalue reference stays a reference, a
// proxy that is returned by value is stored by value.
TEST(Tie, tieElement) {
  static_assert(std::is_same_v<TieElement<Id&>, Id&>);
  static_assert(std::is_same_v<TieElement<const Id&>, const Id&>);
  static_assert(std::is_same_v<TieElement<IdRef>, IdRef>);
  static_assert(std::is_same_v<TieElement<const ConstIdRef>, ConstIdRef>);
  static_assert(std::is_same_v<TieElement<ConstIdRef&&>, ConstIdRef>);

  // The proxies of the split layout are marked as such, other types are not.
  static_assert(qlever::isProxy<IdRef>);
  static_assert(qlever::isProxy<ConstIdRef>);
  static_assert(!qlever::isProxy<Id>);
  static_assert(!qlever::isProxy<int>);
}

// The elements of a row of an `IdTable` are lvalue references, so the tuple
// behaves like `std::tie`.
TEST(Tie, lvalueReferences) {
  IdTable table{3, ad_utility::testing::makeAllocator()};
  table.push_back({intId(1), intId(2), intId(3)});
  table.push_back({intId(4), intId(5), intId(6)});

  IdTable::row_reference row = table[0];
  auto tied = tieElementsByIndexSequence(row, std::index_sequence<0, 2>{});
  static_assert(std::is_same_v<decltype(tied), std::tuple<Id&, Id&>>);

  // The tuple refers to the elements of the table ...
  EXPECT_EQ(&std::get<0>(tied), &table(0, 0));
  EXPECT_EQ(&std::get<1>(tied), &table(0, 2));
  EXPECT_EQ(tied, std::tie(table(0, 0), table(0, 2)));

  // ... and writing through it changes the table.
  std::get<1>(tied) = intId(30);
  EXPECT_EQ(table(0, 2), intId(30));

  // The elements of a `const` row are `const` references.
  const IdTable& constTable = table;
  IdTable::const_row_reference constRow = constTable[1];
  auto constTied =
      tieElementsByIndexSequence(constRow, std::index_sequence<1, 2>{});
  static_assert(
      std::is_same_v<decltype(constTied), std::tuple<const Id&, const Id&>>);
  EXPECT_EQ(std::get<0>(constTied), intId(5));
  EXPECT_EQ(std::get<1>(constTied), intId(6));

  // Tied rows are compared lexicographically.
  IdTable::row_reference secondRow = table[1];
  EXPECT_TRUE(
      tieElementsByIndexSequence(row, std::index_sequence<0, 1>{}) <
      tieElementsByIndexSequence(secondRow, std::index_sequence<0, 1>{}));
  EXPECT_FALSE(
      tieElementsByIndexSequence(secondRow, std::index_sequence<0, 1>{}) <
      tieElementsByIndexSequence(row, std::index_sequence<0, 1>{}));

  // An `ad_utility::ValueSequence` works as well as a `std::index_sequence`.
  auto viaValueSequence = tieElementsByIndexSequence(
      row, ad_utility::ValueSequence<size_t, 2, 1>{});
  EXPECT_EQ(&std::get<0>(viaValueSequence), &table(0, 2));
  EXPECT_EQ(&std::get<1>(viaValueSequence), &table(0, 1));
}

// A column of the split layout returns a proxy by value from `operator[]`: the
// proxies are stored in the tuple by value, but still refer to the elements of
// the column.
TEST(Tie, proxies) {
  IdColumnVector column{testAllocator()};
  column.push_back(intId(10));
  column.push_back(Id::makeFromBool(true));
  column.push_back(intId(30));

  auto tied = tieElementsByIndexSequence(column, std::index_sequence<0, 2>{});
  static_assert(std::is_same_v<decltype(tied), std::tuple<IdRef, IdRef>>);
  EXPECT_EQ(static_cast<Id>(std::get<0>(tied)), intId(10));
  EXPECT_EQ(static_cast<Id>(std::get<1>(tied)), intId(30));

  // Writing through the proxy in the tuple changes the column ...
  std::get<1>(tied) = intId(31);
  EXPECT_EQ(static_cast<Id>(column[2]), intId(31));

  // ... and a change of the column is seen through the proxy in the tuple.
  column[0] = intId(11);
  EXPECT_EQ(static_cast<Id>(std::get<0>(tied)), intId(11));

  // Tied elements are compared lexicographically.
  IdColumnVector other{testAllocator()};
  other.push_back(intId(11));
  other.push_back(intId(0));
  other.push_back(intId(32));
  auto otherTied =
      tieElementsByIndexSequence(other, std::index_sequence<0, 2>{});
  EXPECT_TRUE(tied < otherTied);
  EXPECT_FALSE(otherTied < tied);

  // The elements of a `const` column are `ConstIdRef`s.
  const auto& constColumn = column;
  auto constTied =
      tieElementsByIndexSequence(constColumn, std::index_sequence<1, 2>{});
  static_assert(
      std::is_same_v<decltype(constTied), std::tuple<ConstIdRef, ConstIdRef>>);
  EXPECT_EQ(static_cast<Id>(std::get<0>(constTied)), Id::makeFromBool(true));
  EXPECT_EQ(static_cast<Id>(std::get<1>(constTied)), intId(31));

  // An `ad_utility::ValueSequence` works as well as a `std::index_sequence`.
  auto viaValueSequence = tieElementsByIndexSequence(
      column, ad_utility::ValueSequence<size_t, 2, 0>{});
  EXPECT_EQ(static_cast<Id>(std::get<0>(viaValueSequence)), intId(31));
  EXPECT_EQ(static_cast<Id>(std::get<1>(viaValueSequence)), intId(11));
}

// `ad_utility::tie` with lvalues behaves exactly like `std::tie`.
TEST(Tie, tieLvalueReferences) {
  Id a = intId(1);
  Id b = intId(2);
  const Id c = intId(3);

  auto tied = ad_utility::tie(a, b);
  static_assert(std::is_same_v<decltype(tied), decltype(std::tie(a, b))>);
  static_assert(std::is_same_v<decltype(tied), std::tuple<Id&, Id&>>);

  // The tuple refers to the variables ...
  EXPECT_EQ(&std::get<0>(tied), &a);
  EXPECT_EQ(&std::get<1>(tied), &b);
  EXPECT_EQ(tied, std::tie(a, b));

  // ... and writing through it changes them.
  std::get<1>(tied) = intId(20);
  EXPECT_EQ(b, intId(20));

  // A `const` lvalue stays a `const` reference.
  auto constTied = ad_utility::tie(c, a);
  static_assert(
      std::is_same_v<decltype(constTied), std::tuple<const Id&, Id&>>);
  EXPECT_EQ(&std::get<0>(constTied), &c);

  // The elements of a table can be compared lexicographically, like with
  // `std::tie`.
  IdTable table{3, ad_utility::testing::makeAllocator()};
  table.push_back({intId(1), intId(2), intId(3)});
  table.push_back({intId(1), intId(2), intId(4)});
  table.push_back({intId(2), intId(0), intId(0)});
  auto row = [&table](size_t i) {
    return ad_utility::tie(table(i, 0), table(i, 1), table(i, 2));
  };
  EXPECT_TRUE(row(0) < row(1));
  EXPECT_TRUE(row(1) < row(2));
  EXPECT_TRUE(row(0) <= row(0));
  EXPECT_FALSE(row(2) <= row(1));
}

// `ad_utility::tie` also accepts proxies that are returned by value, which
// `std::tie` cannot bind. The proxies are stored by value, but still refer to
// the elements.
TEST(Tie, tieProxies) {
  IdColumnVector<TestAllocator> column{testAllocator()};
  column.push_back(intId(10));
  column.push_back(intId(20));
  column.push_back(intId(30));

  auto tied = ad_utility::tie(column[0], column[2]);
  static_assert(std::is_same_v<decltype(tied), std::tuple<IdRef, IdRef>>);
  EXPECT_EQ(static_cast<Id>(std::get<0>(tied)), intId(10));
  EXPECT_EQ(static_cast<Id>(std::get<1>(tied)), intId(30));

  // Writing through the proxy in the tuple changes the column ...
  std::get<1>(tied) = intId(31);
  EXPECT_EQ(static_cast<Id>(column[2]), intId(31));

  // ... and a change of the column is seen through the proxy in the tuple.
  column[0] = intId(11);
  EXPECT_EQ(static_cast<Id>(std::get<0>(tied)), intId(11));

  // Proxies and lvalues can be mixed.
  Id a = intId(5);
  auto mixed = ad_utility::tie(a, column[1]);
  static_assert(std::is_same_v<decltype(mixed), std::tuple<Id&, IdRef>>);
  std::get<0>(mixed) = intId(6);
  std::get<1>(mixed) = intId(7);
  EXPECT_EQ(a, intId(6));
  EXPECT_EQ(static_cast<Id>(column[1]), intId(7));

  // The elements of a `const` column are `ConstIdRef`s.
  const auto& constColumn = column;
  auto constTied = ad_utility::tie(constColumn[0], constColumn[1]);
  static_assert(
      std::is_same_v<decltype(constTied), std::tuple<ConstIdRef, ConstIdRef>>);
  EXPECT_EQ(static_cast<Id>(std::get<0>(constTied)), intId(11));
  EXPECT_EQ(static_cast<Id>(std::get<1>(constTied)), intId(7));

  // Tied proxies are compared lexicographically, like with `std::tie`.
  IdColumnVector<TestAllocator> other{testAllocator()};
  other.push_back(intId(11));
  other.push_back(intId(8));
  EXPECT_TRUE(ad_utility::tie(column[0], column[1]) <
              ad_utility::tie(other[0], other[1]));
  EXPECT_FALSE(ad_utility::tie(other[0], other[1]) <
               ad_utility::tie(column[0], column[1]));
}

// Like `std::tie`, `ad_utility::tie` and `tieElementsByIndexSequence` can be
// used in constant expressions, and `ad_utility::tie` also works without or
// with a single argument.
TEST(Tie, constexprAndEdgeCases) {
  static constexpr int x = 1;
  static constexpr int y = 2;
  constexpr auto tied = ad_utility::tie(x, y);
  static_assert(std::get<0>(tied) == 1 && std::get<1>(tied) == 2);
  static_assert(&std::get<0>(tied) == &x);

  static constexpr std::array<int, 3> row{1, 2, 3};
  constexpr auto viaHelper =
      tieElementsByIndexSequence(row, std::index_sequence<0, 2>{});
  static_assert(std::get<0>(viaHelper) == 1 && std::get<1>(viaHelper) == 3);

  Id a = intId(1);
  static_assert(std::is_same_v<decltype(ad_utility::tie()), std::tuple<>>);
  static_assert(std::is_same_v<decltype(ad_utility::tie(a)), std::tuple<Id&>>);
  EXPECT_EQ(&std::get<0>(ad_utility::tie(a)), &a);
}

// Call `tieElement<T>()` and check that it yields a `type_identity` of
// `Expected`. `tieElement` is only needed at compile time, and a call that the
// compiler can evaluate (like `auto x = tieElement<T>();`) is never executed at
// runtime, so it would not show up in coverage reports. Calling it through a
// volatile function pointer makes the compiler really emit and run the call.
template <typename T, typename Expected>
void expectTieElement() {
  auto (*volatile function)() = &tieElement<T>;
  auto identity = function();
  static_assert(
      std::is_same_v<decltype(identity), ql::type_identity<Expected>>);
  (void)identity;
}
}  // namespace

// `tieElement<T>()` returns a `type_identity` of the type that is stored in the
// tuple. An lvalue reference stays a reference (also for a proxy), ...
TEST(Tie, tieElementOfLvalues) {
  expectTieElement<Id&, Id&>();
  expectTieElement<const Id&, const Id&>();

  expectTieElement<IdRef&, IdRef&>();
  expectTieElement<const IdRef&, const IdRef&>();

  expectTieElement<ConstIdRef&, ConstIdRef&>();
  expectTieElement<const ConstIdRef&, const ConstIdRef&>();
}

// ... while a proxy that is returned by value is stored by value, without
// `const` or reference qualifiers.
TEST(Tie, tieElementOfProxiesReturnedByValue) {
  expectTieElement<IdRef, IdRef>();
  expectTieElement<IdRef&&, IdRef>();
  expectTieElement<const IdRef, IdRef>();

  expectTieElement<ConstIdRef, ConstIdRef>();
  expectTieElement<ConstIdRef&&, ConstIdRef>();
  expectTieElement<const ConstIdRef, ConstIdRef>();
}
