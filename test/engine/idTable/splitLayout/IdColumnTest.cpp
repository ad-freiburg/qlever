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
#include <functional>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "IdColumnTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/splitLayout/IdColumn.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "util/Forward.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::sampleIds;

namespace {
// Split the `ids` into the two arrays of the split layout.
std::pair<std::vector<uint64_t>, std::vector<uint8_t>> splitIds(
    const std::vector<Id>& ids) {
  std::vector<uint64_t> payloads;
  std::vector<uint8_t> datatypes;
  for (const Id id : ids) {
    payloads.push_back(id.getPayloadBits());
    datatypes.push_back(id.getDatatypeBits());
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
    EXPECT_EQ(view.rawPayloads()[i], ids.at(i).getPayloadBits());
    EXPECT_EQ(view.rawDatatypes()[i], ids.at(i).getDatatypeBits());
  }

  // Writing through `operator[]` or `at()` of the mutable view changes the
  // backing arrays.
  view[0] = Id::makeFromInt(-1);
  EXPECT_EQ(payloads.at(0), Id::makeFromInt(-1).getPayloadBits());
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
  EXPECT_EQ(payloads.at(1), first.getPayloadBits());
  EXPECT_EQ(datatypes.at(1), first.getDatatypeBits());

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
  // TODO <pas-kes>: fix this test once the new SplitLayoutId is available.
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

// -----------------------------------------------------------------------------
// `ql::ranges::for_each` with a function that takes an `IdRef`.
//
// `ql::ranges::for_each(column, f)` (like all the range algorithms of the
// standard library) only accepts `f` if `std::indirectly_unary_invocable`
// holds. Among other things, `f` has to be callable with
//   1. what `*it` yields, and
//   2. an lvalue of the value type of the column (`std::iter_value_t<It>&`),
//      which is an `Id&` for the legacy as well as for the split layout.
// In the legacy layout a column is a `ql::span<Id>` and `IdRef` is just `Id&`
// (see `IdTable.h`), so both hold. In the split layout, `*it` yields a proxy,
// and requirement 2 fails for a parameter `IdRef`, `ConstIdRef` or
// `IdRef::Base`: An `Id&` refers to a packed `Id`, from which no pair of
// pointers into the two separate arrays of a split column can be built.
// `std::for_each` and range-based `for` loops do not have requirement 2, and a
// parameter of type `Id` or `auto&&` is fine, too.
// -----------------------------------------------------------------------------
namespace {
// Is `ql::ranges::for_each(std::declval<Range>(), std::declval<F>())` valid?
template <typename Range, typename F, typename = void>
struct CanRangesForEach : std::false_type {};
template <typename Range, typename F>
struct CanRangesForEach<Range, F,
                        std::void_t<decltype(ql::ranges::for_each(
                            std::declval<Range>(), std::declval<F>()))>>
    : std::true_type {};

// Call `ql::ranges::for_each(column, f)` and return `true` if that is possible.
// Otherwise do nothing and return `false`. This is what lets a test compile
// both before and after the problem is solved.
template <typename Column, typename F>
bool rangesForEachIfAccepted(Column& column, F f) {
  if constexpr (CanRangesForEach<Column&, F>::value) {
    ql::ranges::for_each(column, std::move(f));
    return true;
  } else {
    return false;
  }
}

// Algorithms that work for a legacy column (`ql::span<Id>`) as well as for a
// split column. `WriteRef` is the type that a function takes to write to an
// element: `Id&` for the legacy column, `IdRef` for the split column.
template <typename WriteRef, typename Column>
void expectCommonAlgorithmsWork(Column column) {
  const auto expectAll = [&column](const Id expected) {
    for (size_t i = 0; i < column.size(); ++i) {
      EXPECT_EQ(column[i], expected) << "at index " << i;
    }
  };
  const Id one = Id::makeFromInt(1);
  const Id two = Id::makeFromInt(2);
  const Id three = Id::makeFromInt(3);

  // `std::for_each` accepts a function with the parameter `WriteRef`.
  std::for_each(column.begin(), column.end(), [one](WriteRef id) { id = one; });
  expectAll(one);

  // So does a range-based `for` loop.
  for (WriteRef id : column) {
    id = two;
  }
  expectAll(two);

  // `ql::ranges::for_each` accepts a generic parameter. `AD_FWD` is needed for
  // the split column, because its elements are only writable as rvalues (for
  // the legacy column, `auto&&` is just `Id&`).
  ql::ranges::for_each(column,
                       [three](auto&& id) -> void { AD_FWD(id) = three; });
  expectAll(three);

  // It also accepts a function that takes an `Id` by value (read-only).
  size_t numThrees = 0;
  ql::ranges::for_each(column, [&numThrees, three](const Id id) {
    numThrees += (id == three) ? 1 : 0;
  });
  EXPECT_EQ(numThrees, column.size());

  // Sorting and reversing move the elements around.
  const auto n = static_cast<int64_t>(column.size());
  for (int64_t i = 0; i < n; ++i) {
    column[i] = Id::makeFromInt(n - i);
  }
  ql::ranges::sort(column);
  for (int64_t i = 0; i < n; ++i) {
    EXPECT_EQ(column[i], Id::makeFromInt(i + 1));
  }
  std::reverse(column.begin(), column.end());
  for (int64_t i = 0; i < n; ++i) {
    EXPECT_EQ(column[i], Id::makeFromInt(n - i));
  }
}

// Guard for the future switch of the value type of the columns: Functions that
// take a plain `Id` have to keep working with the elements of a column. The
// algorithms call them with `*it` and with `std::iter_value_t&` (e.g. the
// temporary that `sort` keeps for the pivot). That only works as long as the
// value type of the column is `Id`, or converts to it. It would break if the
// value type were a different type than `Id`.
template <typename Column>
void expectFunctionsTakingIdWork(Column column) {
  const auto n = static_cast<int64_t>(column.size());
  for (int64_t i = 0; i < n; ++i) {
    column[i] = Id::makeFromInt(n - i);
  }
  const auto less = [](const Id a, const Id b) { return a < b; };

  // A comparator that takes two `Id`s.
  ql::ranges::sort(column, less);
  for (int64_t i = 0; i < n; ++i) {
    EXPECT_EQ(column[i], Id::makeFromInt(i + 1));
  }

  // A projection that takes an `Id`.
  ql::ranges::sort(column, std::greater<>{},
                   [](const Id id) { return id.getInt(); });
  for (int64_t i = 0; i < n; ++i) {
    EXPECT_EQ(column[i], Id::makeFromInt(n - i));
  }

  // Binary search for an `Id`, with a comparator that takes `Id`s and with the
  // default comparison.
  ql::ranges::sort(column, less);
  const Id three = Id::makeFromInt(3);
  EXPECT_EQ(ql::ranges::lower_bound(column, three, less) - column.begin(), 2);
  EXPECT_EQ(ql::ranges::lower_bound(column, three) - column.begin(), 2);

  // The type of a copy of the elements is `Id`, so it can be used as one.
  std::vector copy(column.begin(), column.end());
  static_assert(std::is_same_v<typename decltype(copy)::value_type, Id>);
  const Id first = copy.front();
  EXPECT_EQ(first, Id::makeFromInt(1));
}
}  // namespace

// For the legacy layout, `ql::ranges::for_each` with a parameter `IdRef` (which
// is `Id&` there) and `ConstIdRef` (`const Id&`) works already.
TEST(IdColumnRangesForEachTest, legacyColumnAcceptsIdRefAndConstIdRef) {
  auto ids = sampleIds();
  const ql::span<Id> column{ids.data(), ids.size()};

  const auto write = [](Id& id) { id = Id::makeFromInt(1); };
  static_assert(CanRangesForEach<const ql::span<Id>&, decltype(write)>::value);
  ql::ranges::for_each(column, write);
  for (const Id id : ids) {
    EXPECT_EQ(id, Id::makeFromInt(1));
  }

  size_t numInts = 0;
  const auto read = [&numInts](const Id& id) {
    numInts += (id == Id::makeFromInt(1)) ? 1 : 0;
  };
  static_assert(CanRangesForEach<const ql::span<Id>&, decltype(read)>::value);
  ql::ranges::for_each(column, read);
  EXPECT_EQ(numInts, ids.size());
}

// Everything except `ql::ranges::for_each` with a parameter `IdRef` etc. works
// the same for the legacy and for the split column.
TEST(IdColumnRangesForEachTest, otherAlgorithmsWorkForBothLayouts) {
  auto ids = sampleIds();
  expectCommonAlgorithmsWork<Id&>(ql::span<Id>{ids.data(), ids.size()});

  auto [payloads, datatypes] = splitIds(ids);
  expectCommonAlgorithmsWork<IdRef>(
      IdColumnRef{payloads.data(), datatypes.data(), payloads.size()});
}

// Functions that take a plain `Id` work with the elements of a legacy column
// and of a split column. This guards the switch of the value type of the split
// columns, see `expectFunctionsTakingIdWork`.
TEST(IdColumnRangesForEachTest, functionsTakingIdWorkForBothLayouts) {
  auto ids = sampleIds();
  expectFunctionsTakingIdWork(ql::span<Id>{ids.data(), ids.size()});

  auto [payloads, datatypes] = splitIds(ids);
  expectFunctionsTakingIdWork(
      IdColumnRef{payloads.data(), datatypes.data(), payloads.size()});
}

// A function with a parameter `IdRef`, `ConstIdRef` or `IdRef::Base` can be
// called with what the iterator of a split column yields. That is not the
// problem. This has to stay true when the problem is solved.
TEST(IdColumnRangesForEachTest,
     splitColumnReferenceIsAcceptedByIdRefFunctions) {
  using Reference = decltype(*std::declval<IdColumnRef>().begin());
  using ConstReference = decltype(*std::declval<ConstIdColumnRef>().begin());
  const auto write = [](IdRef) {};
  const auto read = [](ConstIdRef) {};
  const auto restricted = [](IdRef::Base) {};
  static_assert(std::is_invocable_v<decltype(write), Reference>);
  static_assert(std::is_invocable_v<decltype(read), ConstReference>);
  static_assert(std::is_invocable_v<decltype(restricted), Reference>);
  // `std::for_each` (no further requirement) accepts them.
  auto ids = sampleIds();
  auto [payloads, datatypes] = splitIds(ids);
  const IdColumnRef column{payloads.data(), datatypes.data(), payloads.size()};
  std::for_each(column.begin(), column.end(), write);
  std::for_each(column.begin(), column.end(), restricted);
  const ConstIdColumnRef constColumn = column;
  std::for_each(constColumn.begin(), constColumn.end(), read);
}

// THE PROBLEM: `ql::ranges::for_each` on a split column with a function that
// takes an `IdRef`, a `ConstIdRef` or an `IdRef::Base`.
//
// This test compiles and passes before and after the problem is solved:
//  - As long as the problem exists, `ql::ranges::for_each` rejects the
//    functions, and the test is reported as SKIPPED (with the reason).
//  - As soon as it is solved, all three calls are made for real, and the test
//    checks their effect, so it is reported as PASSED. Then the `GTEST_SKIP`
//    branch and `rangesForEachIfAccepted` can be removed.
TEST(IdColumnRangesForEachTest, splitColumnWithIdRefFunctions) {
  auto ids = sampleIds();
  auto [payloads, datatypes] = splitIds(ids);
  const IdColumnRef column{payloads.data(), datatypes.data(), payloads.size()};
  std::vector<std::string> rejected;

  // Write via `IdRef`.
  const Id one = Id::makeFromInt(1);
  if (rangesForEachIfAccepted(column, [one](IdRef id) { id = one; })) {
    for (size_t i = 0; i < column.size(); ++i) {
      EXPECT_EQ(column[i], one);
    }
  } else {
    rejected.push_back("[](IdRef id)");
  }

  // Read via `ConstIdRef` (needs the const view of the column).
  size_t numOnes = 0;
  ConstIdColumnRef constColumn = column;
  if (rangesForEachIfAccepted(constColumn, [&numOnes, one](ConstIdRef id) {
        numOnes += (id == one) ? 1 : 0;
      })) {
    EXPECT_EQ(numOnes, column.size());
  } else {
    rejected.push_back("[](ConstIdRef id)");
  }

  // Write via the restricted reference. Note that the assignment needs an
  // rvalue.
  const Id two = Id::makeFromInt(2);
  if (rangesForEachIfAccepted(column,
                              [two](IdRef::Base id) { std::move(id) = two; })) {
    for (size_t i = 0; i < column.size(); ++i) {
      EXPECT_EQ(column[i], two);
    }
  } else {
    rejected.push_back("[](IdRef::Base id)");
  }

  if (!rejected.empty()) {
    std::string functions;
    for (const auto& function : rejected) {
      functions += (functions.empty() ? "" : ", ") + function;
    }
    GTEST_SKIP()
        << "Problem not solved yet: ql::ranges::for_each rejects " << functions
        << " for a split column, because it requires that the function can "
           "also be called with an `Id&` (std::iter_value_t&), and `Id&` is "
           "convertible to IdRef: "
        << std::is_convertible_v<Id&, IdRef> << ", to ConstIdRef: "
        << std::is_convertible_v<Id&, ConstIdRef> << ", to IdRef::Base: "
        << std::is_convertible_v<Id&, IdRef::Base> << ".";
  }
}
