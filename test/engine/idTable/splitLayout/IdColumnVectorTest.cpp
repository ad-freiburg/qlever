// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <iterator>
#include <type_traits>
#include <utility>
#include <vector>

#include "IdColumnTestHelpers.h"
#include "backports/algorithm.h"
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

// Test that `operator[]` and `at` return the restricted reference, like the
// column views: Writing is possible via an rvalue or via an explicitly
// requested `IdRef`, but not via a variable that was created by `auto`.
TEST(IdColumnVectorTest, restrictedElementAccess) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const auto& constVec = vec;
  static_assert(std::is_same_v<decltype(vec[0]), IdRef::Base>);
  static_assert(std::is_same_v<decltype(vec.at(0)), IdRef::Base>);
  static_assert(std::is_same_v<decltype(constVec[0]), ConstIdRef::Base>);
  static_assert(std::is_same_v<decltype(constVec.at(0)), ConstIdRef::Base>);
  static_assert(std::is_assignable_v<decltype(vec[0]), Id>);
  static_assert(!std::is_assignable_v<decltype(vec[0])&, Id>);
  static_assert(!std::is_assignable_v<decltype(vec.at(0))&, Id>);

  // Assigning to the temporary writes to the vector.
  const Id first = Id::makeFromInt(42);
  vec[1] = first;
  EXPECT_EQ(vec[1], first);
  vec.at(2) = first;
  EXPECT_EQ(constVec[2], first);

  // A variable has to request the writable reference explicitly.
  IdRef ref = vec[3];
  const Id second = Id::makeFromInt(43);
  ref = second;
  EXPECT_EQ(vec[3], second);

  // `auto` copies the (read-only) reference, which still reads the vector.
  auto autoRef = vec[3];
  vec[3] = first;
  EXPECT_EQ(autoRef, first);
}

// The following tests are the generic version of `restrictedElementAccess`.
// Generic code does not name `IdRef`, but takes the types from the iterator and
// the range of a column:
// - `IteratorReference` is `std::iterator_traits<It>::reference`. It is the
//   reference that a variable can request for writing: `IdRef` for a split
//   column, `Id&` for a column of plain `Id`s.
// - `ql::ranges::range_reference_t` is what dereferencing yields. For a split
//   column this is the restricted reference, which only allows writing via a
//   temporary.
namespace {
template <typename Column>
using IteratorReference =
    typename std::iterator_traits<ql::ranges::iterator_t<Column>>::reference;

// Whether the three generic types of a `Column` are the given ones.
template <typename Column, typename Reference, typename Restricted>
inline constexpr bool hasReferenceTypes =
    std::is_same_v<IteratorReference<Column>, Reference> &&
    std::is_same_v<ql::ranges::range_reference_t<Column>, Restricted> &&
    std::is_same_v<ql::ranges::range_value_t<Column>, Id>;

// Write to the first element of a writable `column` and read it again. It uses
// the generic types only, so it works for every column, whatever its reference
// is.
template <typename Column>
void expectGenericWriteAndRead(Column& column) {
  using Reference = IteratorReference<Column>;
  using Value = ql::ranges::range_value_t<Column>;
  const Value first = Id::makeFromInt(11);
  const Value second = Id::makeFromInt(12);

  // Request the writable reference explicitly, like `IdRef ref = vec[3]`.
  Reference ref = *ql::ranges::begin(column);
  ref = first;
  EXPECT_EQ(*ql::ranges::begin(column), first);
  // Write to the temporary that dereferencing yields.
  *ql::ranges::begin(column) = second;
  EXPECT_EQ(ref, second);
  // The reference converts to the plain value.
  Value copy = ref;
  EXPECT_EQ(copy, second);
}

// Read the first element of a `column` that is `expected`, via the generic
// types. A const column only has read-only references.
template <typename Column>
void expectGenericRead(const Column& column, const Id expected) {
  using Reference = IteratorReference<const Column>;
  static_assert(!std::is_assignable_v<Reference&, Id>);
  Reference ref = *ql::ranges::begin(column);
  EXPECT_EQ(ref, expected);
  ql::ranges::range_value_t<const Column> value = ref;
  EXPECT_EQ(value, expected);
}
}  // namespace

// Test that the generic types of a column resolve to the proxies of the split
// layout for the vector and its views, and to true references for a column of
// plain `Id`s.
TEST(IdColumnVectorTest, genericReferenceTypes) {
  using Vec = IdColumnVector<TestAllocator>;
  static_assert(hasReferenceTypes<Vec, IdRef, IdRef::Base>);
  static_assert(hasReferenceTypes<IdColumnRef, IdRef, IdRef::Base>);
  static_assert(hasReferenceTypes<const Vec, ConstIdRef, ConstIdRef::Base>);
  static_assert(
      hasReferenceTypes<ConstIdColumnRef, ConstIdRef, ConstIdRef::Base>);

  // A column of plain `Id`s.
  static_assert(hasReferenceTypes<std::vector<Id>, Id&, Id&>);
  static_assert(hasReferenceTypes<const std::vector<Id>, const Id&, const Id&>);
}

// Test that generic code can write via the reference from the iterator traits,
// for the split columns and for plain `Id`s (a variable of the restricted
// reference cannot be assigned to).
TEST(IdColumnVectorTest, genericWritableReference) {
  using Vec = IdColumnVector<TestAllocator>;
  using Restricted = ql::ranges::range_reference_t<Vec>;
  static_assert(std::is_assignable_v<Restricted, Id>);
  static_assert(!std::is_assignable_v<Restricted&, Id>);
  static_assert(std::is_assignable_v<IteratorReference<Vec>&, Id>);

  auto ids = sampleIds();
  Vec vec{ids.begin(), ids.end(), testAllocator()};
  expectGenericWriteAndRead(vec);
  auto view = vec.asView();
  expectGenericWriteAndRead(view);
  expectGenericWriteAndRead(ids);
}

// Test that a const column and a const view only have read-only references,
// which convert to the plain `Id`.
TEST(IdColumnVectorTest, genericConstReference) {
  using Vec = IdColumnVector<TestAllocator>;
  static_assert(
      !std::is_assignable_v<ql::ranges::range_reference_t<const Vec>, Id>);

  const Id value = Id::makeFromInt(42);
  const std::vector<Id> ids{value};
  Vec vec{ids.begin(), ids.end(), testAllocator()};
  expectGenericRead(std::as_const(vec), value);
  expectGenericRead(vec.asConstView(), value);
  expectGenericRead(ids, value);
}
