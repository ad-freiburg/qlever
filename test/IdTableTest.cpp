// Copyright 2018 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors : 2018      Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//           2022-     Johannes Kalmbach(kalmbach@cs.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/BufferedVector.h"

using namespace ad_utility::testing;
namespace {
auto V = ad_utility::testing::VocabId;
}

// This unit tests is part of the documentation of the `IdTable` class. It
// demonstrates the correct usage of the proxy references that
// are returned by the `IdTable` when calling `operator[]` or when dereferencing
// an iterator.
TEST(IdTable, DocumentationOfIteratorUsage) {
  IdTable t{2, makeAllocator()};
  t.push_back({V(42), V(43)});

  // The following read-only calls use the proxy object and do not copy
  // the rows (The right hand side of the `ASSERT_EQ` calls has the type
  // `IdTable::row_reference_restricted`. The table is not changed, as there is
  // no write access.
  ASSERT_EQ(V(42), t[0][0]);
  ASSERT_EQ(V(42), t.at(0)[0]);
  ASSERT_EQ(V(42), t(0, 0));
  ASSERT_EQ(V(42), (*t.begin())[0]);

  // Writing to the table directly via a temporary proxy reference is ok, as
  // the syntax of all the following calls indicates that the table should
  // be changed.
  t[0][0] = V(5);
  ASSERT_EQ(V(5), t(0, 0));
  t(0, 0) = V(6);
  ASSERT_EQ(V(6), t(0, 0));
  (*t.begin())[0] = V(4);
  ASSERT_EQ(V(4), t(0, 0));

  // The following examples also mutate the `IdTable` which is again expected,
  // because we explicitly bind to a reference:
  {
    IdTable::row_reference ref = t[0];
    ref[0] = V(12);
    ASSERT_EQ(V(12), t(0, 0));
  }

  // This is the interface that all generic algorithms that work on iterators
  // use. The type of `ref` is also `IdTable::row_reference`.
  {
    std::iterator_traits<IdTable::iterator>::reference ref = *(t.begin());
    ref[0] = V(13);
    ASSERT_EQ(V(13), t(0, 0));
  }

  // The following calls do not change the table, but are also not expected to
  // do so because we explicitly bind to a `value_type`.
  {
    IdTable::row_type row = t[0];  // Explitly copy/materialize the full row.
    row[0] = V(50);
    // We have changed the copied row, but not the table.
    ASSERT_EQ(V(50), row[0]);
    ASSERT_EQ(V(13), t[0][0]);
  }
  {
    // Exactly the same example, but with `iterator`s and `iterator_traits`.
    std::iterator_traits<IdTable::iterator>::value_type row =
        *t.begin();  // Explitly copy/materialize the full row.
    row[0] = V(51);
    // We have changed the copied row, but not the table.
    ASSERT_EQ(V(51), row[0]);
    ASSERT_EQ(V(13), t[0][0]);
  }

  // The following examples show the cases where a syntax would lead to
  // unexpected behavior and is therefore disabled.

  {
    auto rowProxy = t[0];
    // x actually is a `row_reference_restricted`. This is unexpected because
    // `auto` typically yields a value type. Reading from the proxy is fine, as
    // read access never does any harm.
    Id id = rowProxy[0];
    ASSERT_EQ(V(13), id);
    // The following syntax would change the table unexpectedly and therefore
    // doesn't compile
    // rowProxy[0] = V(32);  // Would change `t`, but the variable was created
    // via auto!
    // The technical reason is that the `operator[]` returns a `const Id&` even
    // though the `rowProxy` object is not const:
#if false
//#ifdef __GLIBCXX__
    static_assert(std::is_same_v<const Id&, decltype(rowProxy[0])>);
#endif
  }
  {
    // Exactly the same example, but with an iterator.
    auto rowProxy = *t.begin();
    // `rowProxy` actually is a `row_reference_restricted`. This is unexpected
    // because `auto` typically yields a value type. Reading from the proxy is
    // fine, as read access never does any harm.
    Id id = rowProxy[0];
    ASSERT_EQ(V(13), id);
    // The following syntax would change the table unexpectedly and therefore
    // doesn't compile
    // rowProxy[0] = V(32);  // Would change `t`, but the variable was created
    // via auto!
    // The technical reason is that the `operator[]` returns a `const Id&` even
    // though the `rowProxy` object is not const:
#if false
//#ifdef __GLIBCXX__
    static_assert(std::is_same_v<const Id&, decltype(rowProxy[0])>);
#endif
  }

  // The following example demonstrates the remaining loophole how a
  // `row_reference_restricted` variable can still be used to modify the table,
  // but it has a rather "creative" syntax that shouldn't pass any code review.
  {
    auto rowProxy = *t.begin();
    // The write access to an rvalue of type `row_reference_restricted` is
    // allowed. This is necessary to make the above examples like `*t.begin() =
    // V(12)` work. However this syntax (explicitly moving, and then directly
    // writing to the moved object) is not something that should ever be
    // written.
    std::move(rowProxy)[0] = V(4321);
    ASSERT_EQ(V(4321), t(0, 0));
  }
}

// The following test demonstrates the iterator functionality of a single
// row.
TEST(IdTable, rowIterators) {
  using IntTable = columnBasedIdTable::IdTable<int, 0>;
  auto testRow = [](auto row) {
    row[0] = 0;
    row[1] = 1;
    row[2] = 2;
    ASSERT_TRUE(std::is_sorted(row.begin(), row.end()));
    ASSERT_TRUE(std::is_sorted(row.cbegin(), row.cend()));
    ASSERT_TRUE(
        std::is_sorted(std::as_const(row).begin(), std::as_const(row).end()));

    row[0] = 3;
    ASSERT_FALSE(std::is_sorted(row.begin(), row.end()));
    ASSERT_FALSE(std::is_sorted(row.cbegin(), row.cend()));
    ASSERT_FALSE(
        std::is_sorted(std::as_const(row).begin(), std::as_const(row).end()));
    row[0] = 0;
    row[2] = -1;
    ASSERT_FALSE(std::is_sorted(row.begin(), row.end()));
    ASSERT_FALSE(std::is_sorted(row.cbegin(), row.cend()));
    ASSERT_FALSE(
        std::is_sorted(std::as_const(row).begin(), std::as_const(row).end()));

    std::ranges::sort(row.begin(), row.end());
    ASSERT_EQ(-1, row[0]);
    ASSERT_EQ(0, row[1]);
    ASSERT_EQ(1, row[2]);
  };
  using IntTable = columnBasedIdTable::IdTable<int, 0>;
  testRow(IntTable::row_type{3});

  IntTable table{3};
  table.emplace_back();
  testRow(IntTable::row_reference{table[0]});
  // This shouldn't work
  {
    auto row = table[0];
    table[0][0] = 0;
    table[0][1] = 1;
    table[0][2] = 2;
    ASSERT_TRUE(std::is_sorted(row.begin(), row.end()));
    ASSERT_TRUE(std::is_sorted(row.cbegin(), row.cend()));
    ASSERT_TRUE(
        std::is_sorted(std::as_const(row).begin(), std::as_const(row).end()));

    table[0][0] = 3;
    ASSERT_FALSE(std::is_sorted(row.begin(), row.end()));
    ASSERT_FALSE(std::is_sorted(row.cbegin(), row.cend()));
    ASSERT_FALSE(
        std::is_sorted(std::as_const(row).begin(), std::as_const(row).end()));
    table[0][0] = 0;
    table[0][2] = -1;
    ASSERT_FALSE(std::is_sorted(row.begin(), row.end()));
    ASSERT_FALSE(std::is_sorted(row.cbegin(), row.cend()));
    ASSERT_FALSE(
        std::is_sorted(std::as_const(row).begin(), std::as_const(row).end()));

    // Sorting the proxy type `row_reference_restricted` can
    // only be performed via `std::sort` as follows:
    std::sort(std::move(row).begin(), std::move(row).end());
    // The following calls all would not compile:
    // std::sort(row.begin(), row.end());
    // std::ranges::sort(row);
    // std::ranges::sort(std::move(row));
    ASSERT_EQ(-1, row[0]);
    ASSERT_EQ(0, row[1]);
    ASSERT_EQ(1, row[2]);
  }
}

// Run a test case for the following different instantiations of the `IdTable`
// template:
// - The default `IdTable` (stores `Id`s in a `vector<Id, AllocatorWithLimit>`.
// - An `IdTable` that stores plain `int`s in a plain `std::vector`.
// - An `IdTable` that stores `Id`s in a `BufferedVector`.
// Arguments:
// `NumIdTables` - The number of distinct `IdTable` objects that are used inside
//                 the test case
// `test case` -   A lambda that is templated on a specific `IdTable`
// instantiation, and takes one or two arguments. The first argument is a
// function that converts a `size_t` to the `value_type` of the `IdTable`, and
// the second one (if present) is a `std::vector` with `NumIdTables` entries
// that represent the additional arguments that are needed to instantiate an
// `IdTable` (e.g. an allocator or a `BufferedVector`).
template <size_t NumIdTables>
void runTestForDifferentTypes(auto testCase, std::string testCaseName) {
  using Buffer = ad_utility::BufferedVector<Id>;
  using BufferedTable = columnBasedIdTable::IdTable<Id, 0, Buffer>;
  using IntTable = columnBasedIdTable::IdTable<int, 0>;
  // Prepare the vectors of `allocators` and distinct `BufferedVector`s needed
  // for the respective `IdTable` types.
  std::vector<std::decay_t<decltype(makeAllocator())>> allocators;
  std::vector<std::vector<Buffer>> buffers;
  for (size_t i = 0; i < NumIdTables; ++i) {
    buffers.emplace_back();
    // This makes enough room for IdTables of 20 columns.
    for (size_t j = 0; j < 20; ++j) {
      buffers.back().emplace_back(3, testCaseName + std::to_string(i) + "-" +
                                         std::to_string(j) + ".dat");
    }
    allocators.emplace_back(makeAllocator());
  }
  testCase.template operator()<IdTable>(V, std::move(allocators));
  testCase.template operator()<BufferedTable>(V, std::move(buffers));
  auto makeInt = [](auto el) { return static_cast<int>(el); };
  testCase.template operator()<IntTable>(makeInt);
}

// This helper function has to be used inside the `testCase` lambdas for the
// `runTestForDifferentTypes` function above whenever a copy of an `IdTable` has
// to be made. It is necessary because for some `IdTable` instantiations
// (for example when the data is stored in a `BufferedVector`) the `clone`
// member function needs additional arguments. Currently, the only additional
// argument is the filenmae for the copy for `IdTables` that store their data in
// a `BufferedVector`. For an example usage see the test cases below.
auto clone(const auto& table, auto... args) {
  if constexpr (requires { table.clone(); }) {
    return table.clone();
  } else {
    return table.clone(std::move(args)...);
  }
}

TEST(IdTable, push_back_and_assign) {
  // A lambda that is used as the `testCase` argument to the
  // `runTestForDifferentTypes` function (see above for details).
  auto runTestForIdTable = []<typename Table>(auto make,
                                              auto... additionalArgs) {
    constexpr size_t NUM_ROWS = 30;
    constexpr size_t NUM_COLS = 4;

    Table t1{NUM_COLS, std::move(additionalArgs.at(0))...};
    // Fill the rows with numbers counting up from 1
    for (size_t i = 0; i < NUM_ROWS; i++) {
      t1.push_back({make(i * NUM_COLS + 1), make(i * NUM_COLS + 2),
                    make(i * NUM_COLS + 3), make(i * NUM_COLS + 4)});
    }

    IdTable t2{NUM_COLS, makeAllocator()};
    // Test the push_back function for spans
    for (size_t i = 0; i < NUM_ROWS; i++) {
      std::vector<ValueId> row;
      row.push_back(Id::makeFromInt(i * NUM_COLS + 1));
      row.push_back(Id::makeFromInt(i * NUM_COLS + 2));
      row.push_back(Id::makeFromInt(i * NUM_COLS + 3));
      row.push_back(Id::makeFromInt(i * NUM_COLS + 4));
      t2.push_back(row);
    }

    ASSERT_EQ(NUM_ROWS, t1.size());
    ASSERT_EQ(NUM_ROWS, t1.numRows());
    ASSERT_EQ(NUM_COLS, t1.numColumns());
    ASSERT_EQ(NUM_ROWS, t2.size());
    ASSERT_EQ(NUM_ROWS, t2.numRows());
    ASSERT_EQ(NUM_COLS, t2.numColumns());
    // Check the entries
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      ASSERT_EQ(make(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
      ASSERT_EQ(Id::makeFromInt(i + 1), t2(i / NUM_COLS, i % NUM_COLS));
    }

    // Assign new values to the entries
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      t1(i / NUM_COLS, i % NUM_COLS) = make((NUM_ROWS * NUM_COLS) - i);
    }

    // Test for the new entries
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      ASSERT_EQ(t1(i / NUM_COLS, i % NUM_COLS),
                make((NUM_ROWS * NUM_COLS) - i));
    }
  };
  runTestForDifferentTypes<1>(runTestForIdTable, "idTableTest.pushBackAssign");
}

// __________________________________________________________________
TEST(IdTable, at) {
  // A lambda that is used as the `testCase` argument to the
  // `runTestForDifferentTypes` function (see above for details).
  auto runTestForIdTable = []<typename Table>(auto make,
                                              auto... additionalArgs) {
    constexpr size_t NUM_ROWS = 30;
    constexpr size_t NUM_COLS = 4;

    Table t1{NUM_COLS, std::move(additionalArgs.at(0))...};
    t1.resize(1);
    t1.at(0, 0) = make(42);
    ASSERT_EQ(t1.at(0, 0), make(42));
    ASSERT_EQ(std::as_const(t1).at(0, 0), make(42));
    // Valid row but invalid column
    ASSERT_ANY_THROW(t1.at(0, NUM_COLS));
    ASSERT_ANY_THROW(std::as_const(t1).at(0, NUM_COLS));

    // Valid column but invalid row
    ASSERT_ANY_THROW(t1.at(NUM_ROWS, 0));
    ASSERT_ANY_THROW(std::as_const(t1).at(NUM_ROWS, 0));
  };
  runTestForDifferentTypes<1>(runTestForIdTable, "idTableTest.at");
}

TEST(IdTable, insertAtEnd) {
  // A lambda that is used as the `testCase` argument to the
  // `runTestForDifferentTypes` function (see above for details).
  auto runTestForIdTable = []<typename Table>(auto make,
                                              auto... additionalArgs) {
    Table t1{4, std::move(additionalArgs.at(0))...};
    t1.push_back({make(7), make(2), make(4), make(1)});
    t1.push_back({make(0), make(22), make(1), make(4)});

    Table init{4, std::move(additionalArgs.at(1))...};
    init.push_back({make(1), make(0), make(6), make(3)});
    init.push_back({make(3), make(1), make(8), make(2)});
    init.push_back({make(0), make(6), make(8), make(5)});
    init.push_back({make(9), make(2), make(6), make(8)});

    Table t2 = clone(init, std::move(additionalArgs.at(2))...);
    // Test inserting at the end
    t2.insertAtEnd(t1.begin(), t1.end());
    for (size_t i = 0; i < init.size(); i++) {
      ASSERT_EQ(init[i], t2[i]) << i;
    }
    for (size_t i = 0; i < t1.size(); i++) {
      ASSERT_EQ(t1[i], t2[i + init.size()]);
    }
  };
  runTestForDifferentTypes<3>(runTestForIdTable, "idTableTest.insertAtEnd");
}

TEST(IdTable, reserve_and_resize) {
  // A lambda that is used as the `testCase` argument to the
  // `runTestForDifferentTypes` function (see above for details).
  auto runTestForIdTable = []<typename Table>(auto make,
                                              auto... additionalArgs) {
    constexpr size_t NUM_ROWS = 34;
    constexpr size_t NUM_COLS = 20;

    // Test a reserve call before insertions
    Table t1(NUM_COLS, std::move(additionalArgs.at(0))...);
    t1.reserve(NUM_ROWS);

    // Fill the rows with numbers counting up from 1
    for (size_t i = 0; i < NUM_ROWS; i++) {
      t1.emplace_back();
      for (size_t j = 0; j < NUM_COLS; j++) {
        t1(i, j) = make(i * NUM_COLS + 1 + j);
      }
    }

    ASSERT_EQ(NUM_ROWS, t1.size());
    ASSERT_EQ(NUM_ROWS, t1.numRows());
    ASSERT_EQ(NUM_COLS, t1.numColumns());
    // check the entries
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      ASSERT_EQ(make(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
    }

    // Test a resize call instead of insertions
    Table t2(NUM_COLS, std::move(additionalArgs.at(1))...);
    t2.resize(NUM_ROWS);

    // Fill the rows with numbers counting up from 1
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      t2(i / NUM_COLS, i % NUM_COLS) = make(i + 1);
    }

    ASSERT_EQ(NUM_ROWS, t2.size());
    ASSERT_EQ(NUM_ROWS, t2.numRows());
    ASSERT_EQ(NUM_COLS, t2.numColumns());
    // check the entries
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      ASSERT_EQ(make(i + 1), t2(i / NUM_COLS, i % NUM_COLS));
    }
  };
  runTestForDifferentTypes<2>(runTestForIdTable,
                              "idTableTest.reserveAndResize");
}

TEST(IdTable, copyAndMove) {
  // A lambda that is used as the `testCase` argument to the
  // `runTestForDifferentTypes` function (see above for details).
  auto runTestForIdTable = []<typename Table>(auto make,
                                              auto... additionalArgs) {
    constexpr size_t NUM_ROWS = 100;
    constexpr size_t NUM_COLS = 4;

    Table t1(NUM_COLS, std::move(additionalArgs.at(0))...);
    // Fill the rows with numbers counting up from 1
    for (size_t i = 0; i < NUM_ROWS; i++) {
      t1.push_back({make(i * NUM_COLS + 1), make(i * NUM_COLS + 2),
                    make(i * NUM_COLS + 3), make(i * NUM_COLS + 4)});
    }

    // Test all copy and move constructors and assignment operators
    Table t2 = clone(t1, std::move(additionalArgs.at(1))...);
    Table t3{NUM_COLS, std::move(additionalArgs.at(2))...};
    t3 = clone(t1, std::move(additionalArgs.at(3))...);
    Table tmp = clone(t1, std::move(additionalArgs.at(4))...);
    Table t4(std::move(t1));
    Table t5{NUM_COLS, std::move(additionalArgs.at(5))...};
    t5 = std::move(tmp);

    // t1 and tmp have been moved from
    ASSERT_EQ(0, t1.numRows());
    ASSERT_EQ(0, tmp.numRows());

    ASSERT_EQ(NUM_ROWS, t2.size());
    ASSERT_EQ(NUM_ROWS, t2.numRows());
    ASSERT_EQ(NUM_COLS, t2.numColumns());

    ASSERT_EQ(NUM_ROWS, t3.size());
    ASSERT_EQ(NUM_ROWS, t3.numRows());
    ASSERT_EQ(NUM_COLS, t3.numColumns());

    ASSERT_EQ(NUM_ROWS, t4.size());
    ASSERT_EQ(NUM_ROWS, t4.numRows());
    ASSERT_EQ(NUM_COLS, t4.numColumns());

    ASSERT_EQ(NUM_ROWS, t5.size());
    ASSERT_EQ(NUM_ROWS, t5.numRows());
    ASSERT_EQ(NUM_COLS, t5.numColumns());

    // check the entries
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      ASSERT_EQ(make(i + 1), t2(i / NUM_COLS, i % NUM_COLS));
      ASSERT_EQ(make(i + 1), t3(i / NUM_COLS, i % NUM_COLS));
      ASSERT_EQ(make(i + 1), t4(i / NUM_COLS, i % NUM_COLS));
      ASSERT_EQ(make(i + 1), t5(i / NUM_COLS, i % NUM_COLS));
    }
  };

  runTestForDifferentTypes<6>(runTestForIdTable, "idTableTest.copyAndMove");
}

TEST(IdTable, erase) {
  constexpr size_t NUM_ROWS = 12;
  constexpr size_t NUM_COLS = 4;

  IdTable t1(NUM_COLS, makeAllocator());
  // Fill the rows with numbers counting up from 1
  for (size_t j = 0; j < 2 * NUM_ROWS; j++) {
    size_t i = j / 2;
    t1.push_back({V(i * NUM_COLS + 1), V(i * NUM_COLS + 2), V(i * NUM_COLS + 3),
                  V(i * NUM_COLS + 4)});
  }
  // i will underflow and then be larger than 2 * NUM_ROWS
  for (size_t i = 2 * NUM_ROWS - 1; i <= 2 * NUM_ROWS; i -= 2) {
    t1.erase(t1.begin() + i);
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.numRows());
  ASSERT_EQ(NUM_COLS, t1.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(V(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  t1.erase(t1.begin(), t1.end());
  ASSERT_EQ(0u, t1.size());
}

TEST(IdTable, iterating) {
  constexpr size_t NUM_ROWS = 42;
  constexpr size_t NUM_COLS = 17;

  IdTable t1(NUM_COLS, makeAllocator());
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.emplace_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = V(i * NUM_COLS + 1 + j);
    }
  }

  // Test the iterator equality operator
  ASSERT_EQ((t1.end() - 1) + 1, t1.end());
  IdTable::iterator it = t1.begin();
  for (size_t i = 0; i < NUM_ROWS; i++) {
    ++it;
  }
  ASSERT_EQ(t1.end(), it);

  size_t row_index = 0;
  for (auto row : t1) {
    for (size_t i = 0; i < NUM_COLS; i++) {
      ASSERT_EQ(V(row_index * NUM_COLS + i + 1), row[i]);
    }
    row_index++;
  }
}

TEST(IdTable, sortTest) {
  IdTable test(2, makeAllocator());
  test.push_back({V(3), V(1)});
  test.push_back({V(8), V(9)});
  test.push_back({V(1), V(5)});
  test.push_back({V(0), V(4)});
  test.push_back({V(5), V(8)});
  test.push_back({V(6), V(2)});

  IdTable orig = test.clone();

  // First check for the requirements of the iterator:
  // Value Swappable : swap rows 0 and 2
  IdTable::iterator i1 = test.begin();
  IdTable::iterator i2 = i1 + 2;
  std::iter_swap(i1, i2);
  ASSERT_EQ(orig[0], test[2]);
  ASSERT_EQ(orig.at(0), test.at(2));
  ASSERT_EQ(orig[2], test[0]);
  ASSERT_EQ(orig.at(2), test.at(0));

  // The value is move Assignable : create a temporary copy of 3 and move it to
  // 1
  i1 = test.begin() + 1;
  i2 = i1 + 3;
  IdTable::row_type tmp(2);
  tmp = *i2;
  *i1 = std::move(tmp);
  ASSERT_EQ(orig[4], test[1]);
  ASSERT_EQ(orig[4], test[4]);

  // The value is move constructible: move construct from a row in the table
  i1 = test.begin() + 4;
  IdTable::row_type tmp2(*i1);
  ASSERT_EQ(*i1, tmp2);

  // Now try the actual sort
  test = orig.clone();
  std::ranges::sort(test, std::less<>{},
                    [](const auto& row) { return row[0]; });

  // The sorted order of the orig tables should be:
  // 3, 2, 0, 4, 5, 1
  ASSERT_EQ(orig[3], test[0]);
  ASSERT_EQ(orig[2], test[1]);
  ASSERT_EQ(orig[0], test[2]);
  ASSERT_EQ(orig[4], test[3]);
  ASSERT_EQ(orig[5], test[4]);
  ASSERT_EQ(orig[1], test[5]);

  // The same tests for the const and mutable overloads of `at()`.
  ASSERT_EQ(orig.at(3), test.at(0));
  ASSERT_EQ(orig.at(2), test.at(1));
  ASSERT_EQ(orig.at(0), test.at(2));
  ASSERT_EQ(orig.at(4), test.at(3));
  ASSERT_EQ(orig.at(5), test.at(4));
  ASSERT_EQ(orig.at(1), test.at(5));

  ASSERT_EQ(std::as_const(orig).at(3), std::as_const(test).at(0));
  ASSERT_EQ(std::as_const(orig).at(2), std::as_const(test).at(1));
  ASSERT_EQ(std::as_const(orig).at(0), std::as_const(test).at(2));
  ASSERT_EQ(std::as_const(orig).at(4), std::as_const(test).at(3));
  ASSERT_EQ(std::as_const(orig).at(5), std::as_const(test).at(4));
  ASSERT_EQ(std::as_const(orig).at(1), std::as_const(test).at(5));
}

// =============================================================================
// IdTableStatic tests
// =============================================================================

TEST(IdTableStaticTest, push_back_and_assign) {
  constexpr size_t NUM_ROWS = 30;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{makeAllocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back({V(i * NUM_COLS + 1), V(i * NUM_COLS + 2), V(i * NUM_COLS + 3),
                  V(i * NUM_COLS + 4)});
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.numRows());
  ASSERT_EQ(NUM_COLS, t1.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(V(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  // assign new values to the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t1(i / NUM_COLS, i % NUM_COLS) = V((NUM_ROWS * NUM_COLS) - i);
  }

  // test for the new entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(V((NUM_ROWS * NUM_COLS) - i), t1(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, insert) {
  IdTableStatic<4> t1{makeAllocator()};
  t1.push_back({V(7), V(2), V(4), V(1)});
  t1.push_back({V(0), V(22), V(1), V(4)});

  IdTableStatic<4> init{makeAllocator()};
  init.push_back({V(1), V(0), V(6), V(3)});
  init.push_back({V(3), V(1), V(8), V(2)});
  init.push_back({V(0), V(6), V(8), V(5)});
  init.push_back({V(9), V(2), V(6), V(8)});

  IdTableStatic<4> t2 = init.clone();

  // Test inserting at the end
  t2.insertAtEnd(t1.begin(), t1.end());
  for (size_t i = 0; i < init.size(); i++) {
    for (size_t j = 0; j < init.numColumns(); j++) {
      EXPECT_EQ(init(i, j), t2(i, j)) << i << ", " << j;
    }
    EXPECT_EQ(init[i], t2[i]) << i << "th row was a mismatch";
  }
  for (size_t i = 0; i < t1.size(); i++) {
    ASSERT_EQ(t1[i], t2[i + init.size()]);
  }
}

TEST(IdTableStaticTest, reserve_and_resize) {
  constexpr size_t NUM_ROWS = 34;
  constexpr size_t NUM_COLS = 20;

  // Test a reserve call before insertions
  IdTableStatic<NUM_COLS> t1{makeAllocator()};
  t1.reserve(NUM_ROWS);

  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.emplace_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = V(i * NUM_COLS + 1 + j);
    }
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.numRows());
  ASSERT_EQ(NUM_COLS, t1.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(V(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  // Test a resize call instead of insertions
  IdTableStatic<NUM_COLS> t2{makeAllocator()};
  t2.resize(NUM_ROWS);

  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t2(i / NUM_COLS, i % NUM_COLS) = V(i + 1);
  }

  ASSERT_EQ(NUM_ROWS, t2.size());
  ASSERT_EQ(NUM_ROWS, t2.numRows());
  ASSERT_EQ(NUM_COLS, t2.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(V(i + 1), t2(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, copyAndMove) {
  constexpr size_t NUM_ROWS = 100;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{makeAllocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back({V(i * NUM_COLS + 1), V(i * NUM_COLS + 2), V(i * NUM_COLS + 3),
                  V(i * NUM_COLS + 4)});
  }

  // Test all copy and move constructors and assignment operators
  IdTableStatic<NUM_COLS> t2 = t1.clone();
  IdTableStatic<NUM_COLS> t3 = t1.clone();
  IdTableStatic<NUM_COLS> tmp = t1.clone();
  IdTableStatic<NUM_COLS> t4(std::move(t1));
  IdTableStatic<NUM_COLS> t5 = std::move(tmp);

  ASSERT_EQ(NUM_ROWS, t2.size());
  ASSERT_EQ(NUM_ROWS, t2.numRows());
  ASSERT_EQ(NUM_COLS, t2.numColumns());

  ASSERT_EQ(NUM_ROWS, t3.size());
  ASSERT_EQ(NUM_ROWS, t3.numRows());
  ASSERT_EQ(NUM_COLS, t3.numColumns());

  ASSERT_EQ(NUM_ROWS, t4.size());
  ASSERT_EQ(NUM_ROWS, t4.numRows());
  ASSERT_EQ(NUM_COLS, t4.numColumns());

  ASSERT_EQ(NUM_ROWS, t5.size());
  ASSERT_EQ(NUM_ROWS, t5.numRows());
  ASSERT_EQ(NUM_COLS, t5.numColumns());

  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(V(i + 1), t2(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(V(i + 1), t3(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(V(i + 1), t4(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(V(i + 1), t5(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableTest, statusAfterMove) {
  {
    IdTableStatic<3> t1{makeAllocator()};
    t1.push_back(std::array{V(1), V(42), V(2304)});

    auto t2 = std::move(t1);
    // `t1` is valid and still has the same number of columns, but they now are
    // empty.
    ASSERT_EQ(3, t1.numColumns());
    ASSERT_EQ(0, t1.numRows());
    ASSERT_NO_THROW(t1.push_back(std::array{V(4), V(16), V(23)}));
    ASSERT_EQ(1, t1.numRows());
    ASSERT_EQ((static_cast<std::array<Id, 3>>(t1[0])),
              (std::array{V(4), V(16), V(23)}));
  }
  {
    using Buffer = ad_utility::BufferedVector<Id>;
    Buffer buffer(0, "IdTableTest.statusAfterMove.dat");
    using BufferedTable = columnBasedIdTable::IdTable<Id, 1, Buffer>;
    BufferedTable table{1, std::array{std::move(buffer)}};
    table.push_back(std::array{V(19)});
    auto t2 = std::move(table);
    // The `table` has been moved from and is invalid, because we don't have a
    // file anymore where we could write the contents. This means that all
    // operations that would have to change the size of the IdTable throw until
    // we have reinstated the column vector by explicitly assigning a newly
    // constructed table. The exceptions that are thrown are from the
    // `BufferedVector` class which throws when it is being accessed after being
    // moved from. In other words, the `IdTable` class needs no special code to
    // handle the case of the columns being stored in a `BufferedVector`.
    AD_EXPECT_THROW_WITH_MESSAGE(
        table.push_back(std::array{V(4)}),
        ::testing::ContainsRegex("Tried to access a DiskBasedArray"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        table.resize(42),
        ::testing::ContainsRegex("Tried to access a DiskBasedArray"));
    table = BufferedTable{
        1, std::array{Buffer{0, "IdTableTest.statusAfterMove2.dat"}}};
    ASSERT_NO_THROW(table.push_back(std::array{V(4)}));
    ASSERT_NO_THROW(table.resize(42));
    ASSERT_EQ(table.size(), 42u);
    ASSERT_EQ(table(0, 0), V(4));
  }
}

TEST(IdTableStaticTest, erase) {
  constexpr size_t NUM_ROWS = 12;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{makeAllocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t j = 0; j < 2 * NUM_ROWS; j++) {
    size_t i = j / 2;
    t1.push_back({V(i * NUM_COLS + 1), V(i * NUM_COLS + 2), V(i * NUM_COLS + 3),
                  V(i * NUM_COLS + 4)});
  }
  // i will underflow and then be larger than 2 * NUM_ROWS
  for (size_t i = 2 * NUM_ROWS - 1; i <= 2 * NUM_ROWS; i -= 2) {
    t1.erase(t1.begin() + i);
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.numRows());
  ASSERT_EQ(NUM_COLS, t1.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(V(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  t1.erase(t1.begin(), t1.end());
  ASSERT_EQ(0u, t1.size());
}

TEST(IdTableStaticTest, iterating) {
  constexpr size_t NUM_ROWS = 42;
  constexpr size_t NUM_COLS = 17;

  IdTableStatic<NUM_COLS> t1{makeAllocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.emplace_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = V(i * NUM_COLS + 1 + j);
    }
  }

  // Test the iterator equality operator
  ASSERT_EQ((t1.end() - 1) + 1, t1.end());
  IdTableStatic<NUM_COLS>::iterator it = t1.begin();
  for (size_t i = 0; i < NUM_ROWS; i++) {
    ++it;
  }
  ASSERT_EQ(t1.end(), it);

  size_t row_index = 0;
  for (const IdTableStatic<NUM_COLS>::row_type& row : t1) {
    for (size_t i = 0; i < NUM_COLS; i++) {
      ASSERT_EQ(V(row_index * NUM_COLS + i + 1), row[i]);
    }
    row_index++;
  }
}

// =============================================================================
// Conversion Tests
// =============================================================================
TEST(IdTable, conversion) {
  IdTable table(3, makeAllocator());
  table.push_back({V(4), V(1), V(0)});
  table.push_back({V(1), V(7), V(8)});
  table.push_back({V(7), V(12), V(2)});
  table.push_back({V(9), V(3), V(4)});

  IdTable initial = table.clone();

  IdTableStatic<3> s = std::move(table).toStatic<3>();
  ASSERT_EQ(4u, s.size());
  ASSERT_EQ(3u, s.numColumns());
  for (size_t i = 0; i < s.size(); i++) {
    for (size_t j = 0; j < s.numColumns(); j++) {
      ASSERT_EQ(initial(i, j), s(i, j));
    }
  }

  table = std::move(s).toDynamic();
  ASSERT_EQ(4u, table.size());
  ASSERT_EQ(3u, table.numColumns());
  for (size_t i = 0; i < table.size(); i++) {
    for (size_t j = 0; j < table.numColumns(); j++) {
      ASSERT_EQ(initial(i, j), table(i, j));
    }
  }

  auto view = table.asStaticView<3>();
  static_assert(std::is_same_v<decltype(view), IdTableView<3>>);
  ASSERT_EQ(4u, view.size());
  ASSERT_EQ(3u, view.numColumns());
  for (size_t i = 0; i < view.size(); i++) {
    for (size_t j = 0; j < view.numColumns(); j++) {
      ASSERT_EQ(initial(i, j), view(i, j));
    }
  }

  // Test with more than 5 columns
  IdTable tableVar(6, makeAllocator());
  tableVar.push_back({V(1), V(2), V(3), V(6), V(5), V(9)});
  tableVar.push_back({V(0), V(4), V(3), V(4), V(5), V(3)});
  tableVar.push_back({V(3), V(2), V(3), V(2), V(5), V(6)});
  tableVar.push_back({V(5), V(5), V(9), V(4), V(7), V(0)});

  IdTable initialVar = tableVar.clone();

  IdTableStatic<6> staticVar = std::move(tableVar).toStatic<6>();
  ASSERT_EQ(initialVar.size(), staticVar.size());
  ASSERT_EQ(initialVar.numColumns(), staticVar.numColumns());
  for (size_t i = 0; i < staticVar.size(); i++) {
    for (size_t j = 0; j < staticVar.numColumns(); j++) {
      ASSERT_EQ(initialVar(i, j), staticVar(i, j));
    }
  }

  IdTable dynamicVar = std::move(staticVar).toDynamic();
  ASSERT_EQ(initialVar.size(), dynamicVar.size());
  ASSERT_EQ(initialVar.numColumns(), dynamicVar.numColumns());
  for (size_t i = 0; i < dynamicVar.size(); i++) {
    for (size_t j = 0; j < dynamicVar.numColumns(); j++) {
      ASSERT_EQ(initialVar(i, j), dynamicVar(i, j));
    }
  }

  auto viewVar = std::move(dynamicVar).asStaticView<6>();
  static_assert(std::is_same_v<decltype(viewVar), IdTableView<6>>);
  ASSERT_EQ(initialVar.size(), viewVar.size());
  ASSERT_EQ(initialVar.numColumns(), viewVar.numColumns());
  for (size_t i = 0; i < viewVar.size(); i++) {
    for (size_t j = 0; j < viewVar.numColumns(); j++) {
      ASSERT_EQ(initialVar(i, j), viewVar(i, j));
    }
  }
}

TEST(IdTable, empty) {
  using IntTable = columnBasedIdTable::IdTable<int, 0>;
  IntTable t{3};
  ASSERT_TRUE(t.empty());
  t.emplace_back();
  ASSERT_FALSE(t.empty());
}

TEST(IdTable, frontAndBack) {
  using IntTable = columnBasedIdTable::IdTable<int, 0>;
  IntTable t{1};
  t.resize(3);
  t(0, 0) = 42;
  t(2, 0) = 43;
  ASSERT_EQ(42, t.front()[0]);
  ASSERT_EQ(42, std::as_const(t).front()[0]);
  ASSERT_EQ(43, t.back()[0]);
  ASSERT_EQ(43, std::as_const(t).back()[0]);
}

TEST(IdTable, setColumnSubset) {
  using IntTable = columnBasedIdTable::IdTable<int, 0>;
  IntTable t{3};  // three columns.
  t.push_back({0, 10, 20});
  t.push_back({1, 11, 21});
  t.push_back({2, 12, 22});
  {
    auto view =
        t.asColumnSubsetView(std::array{ColumnIndex(2), ColumnIndex(0)});
    ASSERT_EQ(2, view.numColumns());
    ASSERT_EQ(3, view.numRows());
    ASSERT_THAT(view.getColumn(0), ::testing::ElementsAre(20, 21, 22));
    ASSERT_THAT(view.getColumn(1), ::testing::ElementsAre(0, 1, 2));
    // Column index too large
    ASSERT_ANY_THROW(t.asColumnSubsetView(std::array{ColumnIndex{3}}));
  }
  t.setColumnSubset(std::array{ColumnIndex(2), ColumnIndex(0)});
  ASSERT_EQ(2, t.numColumns());
  ASSERT_EQ(3, t.numRows());
  ASSERT_THAT(t.getColumn(0), ::testing::ElementsAre(20, 21, 22));
  ASSERT_THAT(t.getColumn(1), ::testing::ElementsAre(0, 1, 2));

  // Empty column subset is not allowed.
  ASSERT_ANY_THROW(t.setColumnSubset(std::vector<ColumnIndex>{}));
  // Duplicate columns are not allowed.
  ASSERT_ANY_THROW(t.setColumnSubset(std::vector<ColumnIndex>{0, 0, 1}));
  // A column index is out of range.
  ASSERT_ANY_THROW(t.setColumnSubset(std::vector<ColumnIndex>{1, 2}));
}

TEST(IdTableStatic, setColumnSubset) {
  using IntTable = columnBasedIdTable::IdTable<int, 3>;
  IntTable t;
  t.push_back({0, 10, 20});
  t.push_back({1, 11, 21});
  t.push_back({2, 12, 22});
  t.setColumnSubset(std::array{ColumnIndex(2), ColumnIndex(0), ColumnIndex(1)});
  ASSERT_EQ(3, t.numColumns());
  ASSERT_EQ(3, t.numRows());
  ASSERT_THAT(t.getColumn(0), ::testing::ElementsAre(20, 21, 22));
  ASSERT_THAT(t.getColumn(1), ::testing::ElementsAre(0, 1, 2));
  ASSERT_THAT(t.getColumn(2), ::testing::ElementsAre(10, 11, 12));

  // Duplicate columns are not allowed.
  ASSERT_ANY_THROW(t.setColumnSubset(std::vector<ColumnIndex>{0, 0, 1}));
  // A column index is out of range.
  ASSERT_ANY_THROW(t.setColumnSubset(std::vector<ColumnIndex>{1, 2, 3}));

  // For static tables, we need a permutation, a real subset is not allowed.
  ASSERT_ANY_THROW(t.setColumnSubset(std::vector<ColumnIndex>{1, 2}));
}

TEST(IdTable, cornerCases) {
  using Dynamic = columnBasedIdTable::IdTable<int, 0>;
  {
    Dynamic dynamic;
    dynamic.setNumColumns(12);
    ASSERT_NO_THROW(dynamic.asStaticView<12>());
    ASSERT_NO_THROW(dynamic.asStaticView<0>());
    ASSERT_ANY_THROW(dynamic.asStaticView<6>());
  }
  {
    Dynamic dynamic;
    // dynamic has 0 rows;
    ASSERT_ANY_THROW(dynamic.at(0));
    ASSERT_ANY_THROW(std::as_const(dynamic).at(0));
  }
  {
    Dynamic dynamic;
    dynamic.setNumColumns(12);
    dynamic.emplace_back();
    dynamic(0, 3) = -24;
    // `setNumColumns` may only be called on an empty table.
    ASSERT_ANY_THROW(dynamic.setNumColumns(3));
    // Wrong number of columns on a non-empty table.
    ASSERT_ANY_THROW(std::move(dynamic).toStatic<3>());
    auto dynamic2 = std::move(dynamic).toStatic<0>();
    ASSERT_EQ(dynamic2.numColumns(), 12u);
    ASSERT_EQ(dynamic2.numRows(), 1u);
    ASSERT_EQ(dynamic2(0, 3), -24);
  }

  using WidthTwo = columnBasedIdTable::IdTable<int, 2>;
  // Wrong number of columns in the constructor.
  ASSERT_ANY_THROW(WidthTwo(3));

  {
    // Test everything that can go wrong when passing in the storage explicitly.
    // This is `vector<vector<int>>` but with a `default_init_allocator`.
    Dynamic::Storage columns;
    columns.resize(2);
    // Wrong number of columns in the constructor
    ASSERT_ANY_THROW(WidthTwo(3, columns));
    // Too few columns.
    columns.resize(1);
    ASSERT_ANY_THROW(WidthTwo(2, columns));
    columns.resize(2);
    columns[0].push_back(42);
    // One of the columns isn't empty
    ASSERT_ANY_THROW(WidthTwo(2, columns));
  }
}

TEST(IdTable, shrinkToFit) {
  // Note: The behavior of the following test case depends on the implementation
  // of `std::vector::reserve` and `std::vector::push_back`. It might be
  // necessary to change them if one of our used standard libraries has a
  // different behavior, but this is unlikely due to ABI stability goals between
  // library versions.
  auto memory = ad_utility::makeAllocationMemoryLeftThreadsafeObject(1_kB);
  IdTable table{2, ad_utility::AllocatorWithLimit<Id>{memory}};
  using namespace ad_utility::memory_literals;
  ASSERT_EQ(memory.ptr().get()->wlock()->amountMemoryLeft(), 1_kB);
  table.reserve(20);
  ASSERT_TRUE(table.empty());
  // 20 rows * 2 columns * 8 bytes per ID were allocated.
  ASSERT_EQ(memory.ptr().get()->wlock()->amountMemoryLeft(), 680_B);
  table.emplace_back();
  table.emplace_back();
  ASSERT_EQ(table.numRows(), 2u);
  ASSERT_EQ(memory.ptr().get()->wlock()->amountMemoryLeft(), 680_B);
  table.shrinkToFit();
  ASSERT_EQ(table.numRows(), 2u);
  // Now only 2 rows * 2 columns * 8 bytes were allocated.
  ASSERT_EQ(memory.ptr().get()->wlock()->amountMemoryLeft(), 968_B);
}

TEST(IdTable, staticAsserts) {
  static_assert(std::is_trivially_copyable_v<IdTableStatic<1>::iterator>);
  static_assert(std::is_trivially_copyable_v<IdTableStatic<1>::const_iterator>);
  static_assert(std::ranges::random_access_range<IdTable>);
  static_assert(std::ranges::random_access_range<IdTableStatic<1>>);
}

// Check that we can completely instantiate `IdTable`s with a different value
// type and a different underlying storage.

template class columnBasedIdTable::IdTable<char, 0>;
static_assert(!std::is_copy_constructible_v<ad_utility::BufferedVector<char>>);
template class columnBasedIdTable::IdTable<char, 0,
                                           ad_utility::BufferedVector<char>>;
