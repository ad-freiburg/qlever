// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <tuple>
#include <type_traits>
#include <utility>

#include "./engine/idTable/splitLayout/IdColumnTestHelpers.h"
#include "./util/AllocatorTestHelpers.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "util/ConstexprUtils.h"
#include "util/TieHelper.h"

using ad_utility::TieElement;
using ad_utility::tieHelper;
using columnBasedIdTable::splitLayout::ConstIdRef;
using columnBasedIdTable::splitLayout::IdColumnVector;
using columnBasedIdTable::splitLayout::IdRef;
using columnBasedIdTable::splitLayout::testHelpers::testAllocator;
using columnBasedIdTable::splitLayout::testHelpers::TestAllocator;

namespace {
Id intId(int64_t value) { return Id::makeFromInt(value); }
}  // namespace

// Test the types of the tied elements: an lvalue reference stays a reference, a
// proxy that is returned by value is stored by value.
TEST(TieHelper, tieElement) {
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
TEST(TieHelper, lvalueReferences) {
  IdTable table{3, ad_utility::testing::makeAllocator()};
  table.push_back({intId(1), intId(2), intId(3)});
  table.push_back({intId(4), intId(5), intId(6)});

  IdTable::row_reference row = table[0];
  auto tied = tieHelper(row, std::index_sequence<0, 2>{});
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
  auto constTied = tieHelper(constRow, std::index_sequence<1, 2>{});
  static_assert(
      std::is_same_v<decltype(constTied), std::tuple<const Id&, const Id&>>);
  EXPECT_EQ(std::get<0>(constTied), intId(5));
  EXPECT_EQ(std::get<1>(constTied), intId(6));

  // Tied rows are compared lexicographically.
  IdTable::row_reference secondRow = table[1];
  EXPECT_TRUE(tieHelper(row, std::index_sequence<0, 1>{}) <
              tieHelper(secondRow, std::index_sequence<0, 1>{}));
  EXPECT_FALSE(tieHelper(secondRow, std::index_sequence<0, 1>{}) <
               tieHelper(row, std::index_sequence<0, 1>{}));

  // An `ad_utility::ValueSequence` works as well as a `std::index_sequence`.
  auto viaValueSequence =
      tieHelper(row, ad_utility::ValueSequence<size_t, 2, 1>{});
  EXPECT_EQ(&std::get<0>(viaValueSequence), &table(0, 2));
  EXPECT_EQ(&std::get<1>(viaValueSequence), &table(0, 1));
}

// A column of the split layout returns a proxy by value from `operator[]`: the
// proxies are stored in the tuple by value, but still refer to the elements of
// the column.
TEST(TieHelper, proxies) {
  IdColumnVector column{testAllocator()};
  column.push_back(intId(10));
  column.push_back(Id::makeFromBool(true));
  column.push_back(intId(30));

  auto tied = tieHelper(column, std::index_sequence<0, 2>{});
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
  auto otherTied = tieHelper(other, std::index_sequence<0, 2>{});
  EXPECT_TRUE(tied < otherTied);
  EXPECT_FALSE(otherTied < tied);

  // The elements of a `const` column are `ConstIdRef`s.
  const auto& constColumn = column;
  auto constTied = tieHelper(constColumn, std::index_sequence<1, 2>{});
  static_assert(
      std::is_same_v<decltype(constTied), std::tuple<ConstIdRef, ConstIdRef>>);
  EXPECT_EQ(static_cast<Id>(std::get<0>(constTied)), Id::makeFromBool(true));
  EXPECT_EQ(static_cast<Id>(std::get<1>(constTied)), intId(31));

  // An `ad_utility::ValueSequence` works as well as a `std::index_sequence`.
  auto viaValueSequence =
      tieHelper(column, ad_utility::ValueSequence<size_t, 2, 0>{});
  EXPECT_EQ(static_cast<Id>(std::get<0>(viaValueSequence)), intId(31));
  EXPECT_EQ(static_cast<Id>(std::get<1>(viaValueSequence)), intId(11));
}
