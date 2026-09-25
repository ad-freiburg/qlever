// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for the new, isolated split-column machinery (`IdRef`, the column
// view/iterator, `IdColumnVector`, `ColumnStorageTraits`, byte-level
// (de)serialization), which a later commit wires in as the global
// `IdColumn`/`ConstIdColumn`. Until then those aliases still mean
// `ql::span<[const] Id>`, a distinct type from `columnBasedIdTable::IdColumn`
// tested here, so this file spells the latter out fully qualified instead of
// relying on `using namespace columnBasedIdTable` below (which would
// otherwise make an unqualified `IdColumn` ambiguous).

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/ColumnStorageTraits.h"
#include "engine/idTable/IdColumn.h"
#include "engine/idTable/IdColumnByteIO.h"
#include "engine/idTable/IdColumnVector.h"
#include "engine/idTable/IdRef.h"
#include "util/AllocatorWithLimit.h"
#include "util/UninitializedAllocator.h"

using namespace columnBasedIdTable;

namespace {
using TestAllocator =
    ad_utility::default_init_allocator<Id, ad_utility::AllocatorWithLimit<Id>>;
TestAllocator testAllocator() {
  return TestAllocator{ad_utility::testing::makeAllocator()};
}

// A representative sample of `Id`s, covering all the datatypes for which the
// bit representation is not just a pointer.
std::vector<Id> sampleIds() {
  return {Id::makeUndefined(),
         Id::makeFromBool(true),
         Id::makeFromBool(false),
         Id::makeFromInt(42),
         Id::makeFromInt(-42),
         Id::makeFromDouble(13.37),
         Id::makeFromVocabIndex(VocabIndex::make(123)),
         Id::makeFromBlankNodeIndex(BlankNodeIndex::make(7))};
}
}  // namespace

// _____________________________________________________________________________
TEST(IdColumnTest, idRefRoundtripsAndMirrorsApi) {
  for (Id id : sampleIds()) {
    auto bits = getBitsCompat(id);
    uint64_t payload = bits.payload_;
    uint8_t datatype = bits.datatype_;

    ConstIdRef constRef{&payload, &datatype};
    EXPECT_EQ(static_cast<Id>(constRef), id);
    EXPECT_EQ(constRef.getDatatype(), id.getDatatype());
    EXPECT_EQ(constRef.getBits(), getBitsCompat(id));
    EXPECT_EQ(constRef.isUndefined(), id.isUndefined());
    EXPECT_EQ(constRef == id, true);
    EXPECT_EQ(id == constRef, true);

    IdRef mutableRef{&payload, &datatype};
    EXPECT_EQ(static_cast<Id>(mutableRef), id);
    auto other = Id::makeFromInt(999);
    mutableRef = other;
    EXPECT_EQ(static_cast<Id>(mutableRef), other);
    EXPECT_EQ(payload, getBitsCompat(other).payload_);
    EXPECT_EQ(datatype, getBitsCompat(other).datatype_);
  }
}

// _____________________________________________________________________________
TEST(IdColumnTest, viewConstructionAccessAndDefaultState) {
  // A default-constructed view is empty, and storable in an array together
  // with views into other columns (as `IdTable`'s `getColumns()` needs).
  columnBasedIdTable::ConstIdColumn defaultView;
  EXPECT_EQ(defaultView.size(), 0u);
  EXPECT_TRUE(defaultView.empty());
  std::array<columnBasedIdTable::ConstIdColumn, 3> arrayOfViews;
  EXPECT_TRUE(arrayOfViews[1].empty());

  auto ids = sampleIds();
  std::vector<uint64_t> payloads;
  std::vector<uint8_t> datatypes;
  for (Id id : ids) {
    auto bits = getBitsCompat(id);
    payloads.push_back(bits.payload_);
    datatypes.push_back(bits.datatype_);
  }

  columnBasedIdTable::IdColumn view{payloads.data(), datatypes.data(),
                                    payloads.size()};
  ASSERT_EQ(view.size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(view[i]), ids.at(i));
    EXPECT_EQ(static_cast<Id>(view.at(i)), ids.at(i));
  }
  EXPECT_EQ(static_cast<Id>(view.front()), ids.front());
  EXPECT_EQ(static_cast<Id>(view.back()), ids.back());

  // Implicit conversion to the const view.
  columnBasedIdTable::ConstIdColumn constView = view;
  EXPECT_EQ(constView.size(), view.size());

  // `subspan`/`first`/`last`.
  auto sub = view.subspan(2, 3);
  ASSERT_EQ(sub.size(), 3u);
  for (size_t i = 0; i < sub.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(sub[i]), ids.at(2 + i));
  }
  EXPECT_EQ(static_cast<Id>(view.first(1)[0]), ids.front());
  EXPECT_EQ(static_cast<Id>(view.last(1)[0]), ids.back());

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
TEST(IdColumnTest, iterSwapSwapsTheReferencedValues) {
  auto ids = sampleIds();
  IdColumnVector<TestAllocator> vec{ids.begin(), ids.end(), testAllocator()};
  auto view = vec.asView();
  ql::ranges::iter_swap(view.begin() + 1, view.begin() + 2);
  EXPECT_EQ(static_cast<Id>(vec[1]), ids.at(2));
  EXPECT_EQ(static_cast<Id>(vec[2]), ids.at(1));
}

// _____________________________________________________________________________
TEST(IdColumnTest, iteratorSupportsRangeBasedForAndSort) {
  auto ids = sampleIds();
  IdColumnVector<TestAllocator> vec{ids.begin(), ids.end(), testAllocator()};

  std::vector<Id> viaIteration;
  for (Id id : vec.asConstView()) {
    viaIteration.push_back(id);
  }
  EXPECT_THAT(viaIteration, ::testing::ElementsAreArray(ids));

  // Random access and `std::distance`.
  auto view = vec.asConstView();
  EXPECT_EQ(view.end() - view.begin(), static_cast<ptrdiff_t>(ids.size()));
  EXPECT_EQ(static_cast<Id>(*(view.begin() + 2)), ids.at(2));

  // Sorting through the mutable view/iterator swaps the referenced values.
  auto mutableView = vec.asView();
  ql::ranges::sort(mutableView, {}, [](Id id) { return getBitsCompat(id); });
  std::vector<Id> sortedIds = ids;
  ql::ranges::sort(sortedIds, {}, [](Id id) { return getBitsCompat(id); });
  std::vector<Id> afterSort(mutableView.begin(), mutableView.end());
  EXPECT_THAT(afterSort, ::testing::ElementsAreArray(sortedIds));
}

// _____________________________________________________________________________
TEST(IdColumnTest, idColumnVectorGrowthAndAllocatorPropagation) {
  IdColumnVector<TestAllocator> vec{testAllocator()};
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

  // Constructing with an explicit size default-initializes that many `Id`s.
  IdColumnVector<TestAllocator> sized{5, testAllocator()};
  EXPECT_EQ(sized.size(), 5u);

  // The allocator that was passed in is preserved.
  auto allocator = testAllocator();
  IdColumnVector<TestAllocator> withAllocator{allocator};
  EXPECT_EQ(withAllocator.get_allocator(), allocator);
}

// _____________________________________________________________________________
TEST(IdColumnTest, idColumnVectorEraseAndInsert) {
  auto ids = sampleIds();
  IdColumnVector<TestAllocator> vec{ids.begin(), ids.end(), testAllocator()};

  auto view = vec.asConstView();
  vec.erase(view.begin() + 1, view.begin() + 3);
  EXPECT_EQ(vec.size(), ids.size() - 2);
  EXPECT_EQ(static_cast<Id>(vec[0]), ids.at(0));
  EXPECT_EQ(static_cast<Id>(vec[1]), ids.at(3));

  std::vector<Id> toInsert{Id::makeFromInt(1), Id::makeFromInt(2)};
  auto viewAfterErase = vec.asConstView();
  vec.insert(viewAfterErase.begin() + 1, toInsert.begin(), toInsert.end());
  EXPECT_EQ(static_cast<Id>(vec[1]), toInsert.at(0));
  EXPECT_EQ(static_cast<Id>(vec[2]), toInsert.at(1));
}

// _____________________________________________________________________________
TEST(IdColumnTest, columnStorageTraitsResolvesGenericAndIdCase) {
  static_assert(std::is_same_v<
                ColumnStorageTraits<std::vector<int>, int>::Ref, int&>);
  static_assert(
      std::is_same_v<ColumnStorageTraits<std::vector<int>, int>::Column,
                    ql::span<int>>);

  static_assert(
      std::is_same_v<
          ColumnStorageTraits<IdColumnVector<TestAllocator>, Id>::Ref, IdRef>);
  static_assert(
      std::is_same_v<
          ColumnStorageTraits<IdColumnVector<TestAllocator>, Id>::ConstRef,
          ConstIdRef>);
  static_assert(
      std::is_same_v<
          ColumnStorageTraits<IdColumnVector<TestAllocator>, Id>::Column,
          columnBasedIdTable::IdColumn>);
  static_assert(
      std::is_same_v<
          ColumnStorageTraits<IdColumnVector<TestAllocator>, Id>::ConstColumn,
          columnBasedIdTable::ConstIdColumn>);
}

// _____________________________________________________________________________
TEST(IdColumnTest, packAndUnpackBytesRoundtrip) {
  auto ids = sampleIds();
  IdColumnVector<TestAllocator> vec{ids.begin(), ids.end(), testAllocator()};

  auto bytes = packIdColumnToBytes(vec.asConstView());
  // This is exactly the class of bug that broke 37 tests in a previous
  // attempt at this refactor: the packed buffer's size must be exactly
  // `size * BYTES_PER_ID_COLUMN_ENTRY`, not e.g. `size * sizeof(Id)`.
  ASSERT_EQ(bytes.size(), ids.size() * BYTES_PER_ID_COLUMN_ENTRY);

  IdColumnVector<TestAllocator> roundtripped{ids.size(), testAllocator()};
  unpackBytesToIdColumn(ql::span<const char>{bytes}, roundtripped.asView());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(roundtripped[i]), ids.at(i));
  }
}
