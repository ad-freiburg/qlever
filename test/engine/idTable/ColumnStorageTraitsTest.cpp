// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/idTable/ColumnStorageTraits.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "global/Id.h"
#include "rdfTypes/GeoPoint.h"
#include "util/memorySize/MemorySize.h"

namespace {
using columnBasedIdTable::ColumnStorageTraits;
namespace splitLayout = columnBasedIdTable::splitLayout;
using ad_utility::MemorySize;

// The two kinds of columns: a `std::vector<Id>` (which `IdTable` uses without
// the flag `USE_SPLIT_LAYOUT`) and the split columns with separate arrays for
// the payloads and the datatypes. Both are used with the `Id` of the current
// build, the split columns also work with the legacy `ValueId`.
struct VectorColumns {
  using Storage = std::vector<Id, detail::DefaultAllocator>;
  // The bytes of one entry that the allocator of the table is charged with.
  static constexpr size_t bytesPerEntry = sizeof(Id);
};
struct SplitColumns {
  using Storage = splitLayout::IdColumnVector<detail::DefaultAllocator>;
  static constexpr size_t bytesPerEntry =
      sizeof(Id::PayloadType) + sizeof(Id::DatatypeType);
};

template <typename Columns>
using TableOf = columnBasedIdTable::IdTable<Id, 0, typename Columns::Storage>;
template <typename Columns>
using TraitsOf = ColumnStorageTraits<typename Columns::Storage, Id>;

// True iff a `C` has a member function `data()`, that is direct access to its
// memory as one contiguous array.
template <typename C, typename = void>
struct HasData : std::false_type {};
template <typename C>
struct HasData<C, std::void_t<decltype(std::declval<C&>().data())>>
    : std::true_type {};

// `Id`s of all datatypes that can be stored without a vocabulary, with
// payloads at the edges of their range.
std::vector<Id> idsAtTheEdges() {
  using I = Id::IntegerType;
  return {Id::makeUndefined(),
          Id::makeFromBool(false),
          Id::makeFromBool(true),
          Id::makeFromInt(0),
          Id::makeFromInt(-1),
          Id::makeFromInt(I::min()),
          Id::makeFromInt(I::max()),
          Id::makeFromDouble(-0.0),
          Id::makeFromDouble(std::numeric_limits<double>::infinity()),
          Id::makeFromDouble(-std::numeric_limits<double>::infinity()),
          Id::makeFromDouble(std::numeric_limits<double>::quiet_NaN()),
          Id::makeFromDouble(std::numeric_limits<double>::denorm_min()),
          Id::makeFromVocabIndex(VocabIndex::make(0)),
          Id::makeFromVocabIndex(VocabIndex::make(Id::maxIndex)),
          Id::makeFromTextRecordIndex(TextRecordIndex::make(Id::maxIndex)),
          Id::makeFromWordVocabIndex(WordVocabIndex::make(Id::maxIndex)),
          Id::makeFromBlankNodeIndex(BlankNodeIndex::make(Id::maxIndex)),
          Id::makeFromGeoPoint(GeoPoint{-90.0, -180.0}),
          Id::makeFromGeoPoint(GeoPoint{90.0, 180.0}),
          Id::fromBits(Datatype::EncodedVal, 12345)};
}

// The content of a `table` as plain `Id`s, row by row, to compare tables with
// different kinds of columns.
template <typename Table>
std::vector<std::vector<Id>> contentOf(const Table& table) {
  std::vector<std::vector<Id>> result;
  for (size_t row = 0; row < table.numRows(); ++row) {
    std::vector<Id>& r = result.emplace_back();
    for (size_t col = 0; col < table.numColumns(); ++col) {
      r.push_back(static_cast<Id>(table(row, col)));
    }
  }
  return result;
}

Id I(int64_t i) { return Id::makeFromInt(i); }

template <typename Columns>
class ColumnStorageTraitsTest : public ::testing::Test {};
using AllColumns = ::testing::Types<VectorColumns, SplitColumns>;
TYPED_TEST_SUITE(ColumnStorageTraitsTest, AllColumns);
}  // namespace

// Test the contract of the types that the traits hand out: an element can be
// written through a `Ref` but not through a `ConstRef`, both can be read as an
// `Id`, a column can be viewed as a `ConstColumn` but a const column cannot be
// viewed as a mutable `Column`.
TYPED_TEST(ColumnStorageTraitsTest, contractOfTheTypes) {
  using Storage = typename TypeParam::Storage;
  using Traits = TraitsOf<TypeParam>;
  static_assert(std::is_assignable_v<typename Traits::Ref, Id>);
  static_assert(!std::is_assignable_v<typename Traits::ConstRef, Id>);
  static_assert(std::is_convertible_v<typename Traits::Ref, Id>);
  static_assert(std::is_convertible_v<typename Traits::ConstRef, Id>);
  static_assert(std::is_convertible_v<Storage&, typename Traits::Column>);
  static_assert(
      std::is_convertible_v<const Storage&, typename Traits::ConstColumn>);
  static_assert(
      !std::is_convertible_v<const Storage&, typename Traits::Column>);
  static_assert(std::is_convertible_v<typename Traits::Column,
                                      typename Traits::ConstColumn>);
  static_assert(!std::is_convertible_v<typename Traits::ConstColumn,
                                       typename Traits::Column>);
}

// Test that only the columns of a `std::vector` give access to their memory as
// one array of `Id`s. A split column has no such memory, so code that relies
// on it (like a `memcpy` of a column) must not compile for it.
TEST(ColumnStorageTraits, onlyVectorColumnsHaveContiguousIds) {
  static_assert(HasData<TraitsOf<VectorColumns>::Column>::value);
  static_assert(HasData<TraitsOf<VectorColumns>::ConstColumn>::value);
  static_assert(!HasData<TraitsOf<SplitColumns>::Column>::value);
  static_assert(!HasData<TraitsOf<SplitColumns>::ConstColumn>::value);
}

// Test that an `IdTable` hands out exactly the types of the traits of its
// columns, for the element access and for the columns.
TYPED_TEST(ColumnStorageTraitsTest, idTableHandsOutTheTypesOfTheTraits) {
  using Traits = TraitsOf<TypeParam>;
  using Table = TableOf<TypeParam>;
  static_assert(std::is_same_v<decltype(std::declval<Table&>()(0, 0)),
                               typename Traits::Ref>);
  static_assert(std::is_same_v<decltype(std::declval<const Table&>()(0, 0)),
                               typename Traits::ConstRef>);
  static_assert(std::is_same_v<decltype(std::declval<Table&>().at(0, 0)),
                               typename Traits::Ref>);
  static_assert(std::is_same_v<decltype(std::declval<Table&>().getColumn(0)),
                               typename Traits::Column>);
  static_assert(
      std::is_same_v<decltype(std::declval<const Table&>().getColumn(0)),
                     typename Traits::ConstColumn>);
}

// Test that every `Id` keeps all its bits when it is stored in a column and
// read again, also at the edges of the ranges of the datatypes.
TYPED_TEST(ColumnStorageTraitsTest, idsKeepAllTheirBits) {
  const auto ids = idsAtTheEdges();
  TableOf<TypeParam> table{1, ad_utility::testing::makeAllocator()};
  for (Id id : ids) {
    table.push_back({id});
  }
  ASSERT_EQ(table.numRows(), ids.size());
  const auto& constTable = table;
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(constTable(i, 0)).getBits(), ids[i].getBits())
        << i;
    EXPECT_EQ(static_cast<Id>(constTable.getColumn(0)[i]).getBits(),
              ids[i].getBits())
        << i;
  }
}

// Test that the element access via `operator()`, via `at` and via a column
// refer to the same storage: a write via one of them is seen by the others.
TYPED_TEST(ColumnStorageTraitsTest, allAccessesReferToTheSameStorage) {
  TableOf<TypeParam> table{2, ad_utility::testing::makeAllocator()};
  table.resize(3);
  table(0, 0) = I(10);
  table.at(1, 0) = I(11);
  table.getColumn(0)[2] = I(12);
  table.getColumns()[1][0] = I(20);

  const auto& constTable = table;
  for (size_t row = 0; row < 3; ++row) {
    Id expected = I(10 + static_cast<int64_t>(row));
    EXPECT_EQ(static_cast<Id>(constTable(row, 0)), expected);
    EXPECT_EQ(static_cast<Id>(constTable.at(row, 0)), expected);
    EXPECT_EQ(static_cast<Id>(constTable.getColumn(0)[row]), expected);
  }
  EXPECT_EQ(static_cast<Id>(constTable(0, 1)), I(20));
}

// Test that a value that was read from the table is a copy: it does not
// change when the table is written afterwards.
TYPED_TEST(ColumnStorageTraitsTest, readValuesAreCopies) {
  TableOf<TypeParam> table{1, ad_utility::testing::makeAllocator()};
  table.push_back({I(1)});
  Id copy = table(0, 0);
  table(0, 0) = I(2);
  EXPECT_EQ(copy, I(1));
  EXPECT_EQ(static_cast<Id>(table(0, 0)), I(2));
}

// Test that the views of a table refer to its storage: they see the right rows
// and columns, and also writes to the table after they were created.
TYPED_TEST(ColumnStorageTraitsTest, viewsReferToTheTable) {
  TableOf<TypeParam> table{2, ad_utility::testing::makeAllocator()};
  for (int64_t i = 0; i < 4; ++i) {
    table.push_back({I(i), I(100 + i)});
  }
  auto rows = table.subView(1, 2);
  auto all = table.template asStaticView<0>();
  std::vector<ColumnIndex> swapped{1, 0};
  auto columns = table.asColumnSubsetView(swapped);

  ASSERT_EQ(rows.numRows(), 2u);
  EXPECT_EQ(static_cast<Id>(rows(0, 0)), I(1));
  EXPECT_EQ(static_cast<Id>(rows(1, 1)), I(102));
  EXPECT_EQ(static_cast<Id>(all.at(3, 1)), I(103));
  EXPECT_EQ(static_cast<Id>(columns(0, 0)), I(100));
  EXPECT_EQ(static_cast<Id>(columns(0, 1)), I(0));

  // The views do not copy, so they see a later write to the table.
  table(2, 0) = I(42);
  EXPECT_EQ(static_cast<Id>(rows(1, 0)), I(42));
  EXPECT_EQ(static_cast<Id>(all(2, 0)), I(42));
  EXPECT_EQ(static_cast<Id>(columns(2, 1)), I(42));
}

// Test that a table with split columns behaves exactly like a table with
// `std::vector` columns: the same operations give the same content.
TEST(ColumnStorageTraits, splitColumnsBehaveLikeVectorColumns) {
  auto run = [](auto columnsTag) {
    using Columns = decltype(columnsTag);
    auto alloc = ad_utility::testing::makeAllocator();
    TableOf<Columns> table{3, alloc};
    for (int64_t i = 0; i < 5; ++i) {
      table.push_back({I(5 - i), I(10 * i), I(-i)});
    }
    table.emplace_back();
    table(5, 0) = I(0);
    table(5, 1) = Id::makeUndefined();
    table(5, 2) = Id::makeFromDouble(1.5);

    // Rows 1 and 2 of a second table, with its columns permuted, and a column
    // that does not exist there filled with a default value.
    TableOf<Columns> other{2, alloc};
    for (int64_t i = 0; i < 3; ++i) {
      other.push_back({I(1000 + i), I(2000 + i)});
    }
    table.insertAtEnd(other, 1, 3, std::vector<ColumnIndex>{1, 7, 0},
                      Id::makeFromBool(true));

    table.erase(table.begin() + 1);
    ql::ranges::sort(table, std::less<>{},
                     [](const auto& row) { return static_cast<Id>(row[0]); });

    // A clone is independent of the original.
    auto clone = table.clone();
    clone(0, 0) = I(-99);

    std::vector<ColumnIndex> subset{2, 0};
    table.setColumnSubset(subset);
    return std::pair{contentOf(table), contentOf(clone)};
  };

  auto [vectorTable, vectorClone] = run(VectorColumns{});
  auto [splitTable, splitClone] = run(SplitColumns{});
  EXPECT_EQ(splitTable, vectorTable);
  EXPECT_EQ(splitClone, vectorClone);
  // The operations really did something (and not just nothing in both cases).
  EXPECT_EQ(vectorTable.size(), 7u);
  EXPECT_EQ(vectorTable.at(0).size(), 2u);
}

// Test that the allocator of a table is charged with the bytes of the entries
// of its columns: 9 bytes per entry for split columns (payload and datatype),
// `sizeof(Id)` for a `std::vector`.
TYPED_TEST(ColumnStorageTraitsTest, memoryPerEntry) {
  auto alloc = ad_utility::testing::makeAllocator(MemorySize::megabytes(1));
  auto before = alloc.amountMemoryLeft();
  TableOf<TypeParam> table{2, alloc};
  table.resize(1000);
  auto used = before - alloc.amountMemoryLeft();
  EXPECT_EQ(used.getBytes(), 2 * 1000 * TypeParam::bytesPerEntry);
}

// Test that `BYTES_PER_ID_COLUMN_ENTRY`, which all the memory estimates use,
// is the memory that an entry of the columns of `IdTable` really takes. This
// fails if the columns of `IdTable` change (for example to split columns with
// the flag `USE_SPLIT_LAYOUT`) without the constant.
TEST(ColumnStorageTraits, bytesPerIdColumnEntryMatchTheColumnsOfIdTable) {
  auto alloc = ad_utility::testing::makeAllocator(MemorySize::megabytes(1));
  auto before = alloc.amountMemoryLeft();
  IdTable table{2, alloc};
  table.resize(1000);
  auto used = before - alloc.amountMemoryLeft();
  EXPECT_EQ(used.getBytes(), 2 * 1000 * BYTES_PER_ID_COLUMN_ENTRY);
}
