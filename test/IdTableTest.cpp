// Copyright 2018 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors : 2018      Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//           2022-     Johannes Kalmbach(kalmbach@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/BufferedVector.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

// This unit tests is part of the documentation of the `IdTable` class. It
// demonstrates the correct usage of the proxy references that
// are returned by the `IdTable` when calling `operator[]` or when dereferencing
// an iterator.
TEST(IdTableTest, DocumentationOfIteratorUsage) {
  IdTable t{2, allocator()};
  t.push_back({I(42), I(43)});

  // The following read-only calls use the proxy object and do not copy
  // the rows (The right hand side of the `ASSERT_EQ` calls has the type
  // `IdTable::row_reference_restricted`. The table is not changed, as there is
  // no write access.
  ASSERT_EQ(I(42), t[0][0]);
  ASSERT_EQ(I(42), t(0, 0));
  ASSERT_EQ(I(42), (*t.begin())[0]);

  // Writing to the table directly via a temporary proxy reference is ok, as
  // the syntax of all the following calls indicates that the table should
  // be changed.
  t[0][0] = I(5);
  ASSERT_EQ(I(5), t(0, 0));
  t(0, 0) = I(6);
  ASSERT_EQ(I(6), t(0, 0));
  (*t.begin())[0] = I(4);
  ASSERT_EQ(I(4), t(0, 0));

  // The following examples also mutate the `IdTable` which is again expected,
  // because we explicitly bind to a reference:
  {
    IdTable::row_reference ref = t[0];
    ref[0] = I(12);
    ASSERT_EQ(I(12), t(0, 0));
  }

  // This is the interface that all generic algorithms that work on iterators
  // use. The type of `ref` is also `IdTable::row_reference`.
  {
    std::iterator_traits<IdTable::iterator>::reference ref = *(t.begin());
    ref[0] = I(13);
    ASSERT_EQ(I(13), t(0, 0));
  }

  // The following calls do not change the table, but are also not expected to
  // do so because we explicitly bind to a `value_type`.
  {
    IdTable::row_type row = t[0];  // Explitly copy/materialize the full row.
    row[0] = I(50);
    // We have changed the copied row, but not the table.
    ASSERT_EQ(I(50), row[0]);
    ASSERT_EQ(I(13), t[0][0]);
  }
  {
    // Exactly the same example, but with `iterator`s and `iterator_traits`.
    std::iterator_traits<IdTable::iterator>::value_type row =
        *t.begin();  // Explitly copy/materialize the full row.
    row[0] = I(51);
    // We have changed the copied row, but not the table.
    ASSERT_EQ(I(51), row[0]);
    ASSERT_EQ(I(13), t[0][0]);
  }

  // The following examples show the cases where a syntax would lead to
  // unexpected behavior and is therefore disabled.

  {
    auto rowProxy = t[0];
    // x actually is a `row_reference_restricted`. This is unexpected because
    // `auto` typically yields a value type. Reading from the proxy is fine, as
    // read access never does any harm.
    Id id = rowProxy[0];
    ASSERT_EQ(I(13), id);
    // The following syntax would change the table unexpectedly and therefore
    // doesn't compile
    // rowProxy[0] = I(32);  // Would change `t`, but the variable was created
    // via auto!
    // The technical reason is that the `operator[]` returns a `const Id&` even
    // though the `rowProxy` object is not const:
    static_assert(std::is_same_v<const Id&, decltype(rowProxy[0])>);
  }
  {
    // Exactly the same example, but with an iterator.
    auto rowProxy = *t.begin();
    // `rowProxy` actually is a `row_reference_restricted`. This is unexpected
    // because `auto` typically yields a value type. Reading from the proxy is
    // fine, as read access never does any harm.
    Id id = rowProxy[0];
    ASSERT_EQ(I(13), id);
    // The following syntax would change the table unexpectedly and therefore
    // doesn't compile
    // rowProxy[0] = I(32);  // Would change `t`, but the variable was created
    // via auto!
    // The technical reason is that the `operator[]` returns a `const Id&` even
    // though the `rowProxy` object is not const:
    static_assert(std::is_same_v<const Id&, decltype(rowProxy[0])>);
  }

  // The following example demonstrates the remaining loophole how a
  // `row_reference_restricted` variable can still be used to modify the table,
  // but it has a rather "creative" syntax that shouldn't pass any code review.
  {
    auto rowProxy = *t.begin();
    // The write access to an rvalue of type `row_reference_restricted` is
    // allowed. This is necessary to make the above examples like `*t.begin() =
    // I(12)` work. However this syntax (explicitly moving, and then directly
    // writing to the moved object) is not something that should ever be
    // written.
    std::move(rowProxy)[0] = I(4321);
    ASSERT_EQ(I(4321), t(0, 0));
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
  std::vector<std::decay_t<decltype(allocator())>> allocators;
  std::vector<Buffer> buffers;
  for (size_t i = 0; i < NumIdTables; ++i) {
    buffers.emplace_back(3, testCaseName + std::to_string(i) + ".dat");
    allocators.emplace_back(allocator());
  }
  testCase.template operator()<IdTable>(I, std::move(allocators));
  testCase.template operator()<BufferedTable>(I, std::move(buffers));
  auto makeInt = [](auto el) { return static_cast<int>(el); };
  testCase.template operator()<IntTable>(makeInt);
}

// This helper function has to be used inside the `testCase` lambdas for the
// `runTestForDifferenTypes` function above whenever a copy of an `IdTable` has
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

TEST(IdTableTest, push_back_and_assign) {
  // A lambda that is used as the `testCase` argument to the
  // `runtTestForDifferenTypes` function (see above for details).
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

    ASSERT_EQ(NUM_ROWS, t1.size());
    ASSERT_EQ(NUM_ROWS, t1.numRows());
    ASSERT_EQ(NUM_COLS, t1.numColumns());
    // Check the entries
    for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
      ASSERT_EQ(make(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
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

TEST(IdTableTest, insertAtEnd) {
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

TEST(IdTableTest, reserve_and_resize) {
  // A lambda that is used as the `testCase` argument to the
  // `runtTestForDifferenTypes` function (see above for details).
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

TEST(IdTableTest, copyAndMove) {
  // A lambda that is used as the `testCase` argument to the
  // `runtTestForDifferenTypes` function (see above for details).
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

TEST(IdTableTest, erase) {
  constexpr size_t NUM_ROWS = 12;
  constexpr size_t NUM_COLS = 4;

  IdTable t1(NUM_COLS, allocator());
  // Fill the rows with numbers counting up from 1
  for (size_t j = 0; j < 2 * NUM_ROWS; j++) {
    size_t i = j / 2;
    t1.push_back({I(i * NUM_COLS + 1), I(i * NUM_COLS + 2), I(i * NUM_COLS + 3),
                  I(i * NUM_COLS + 4)});
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
    ASSERT_EQ(I(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  t1.erase(t1.begin(), t1.end());
  ASSERT_EQ(0u, t1.size());
}

TEST(IdTableTest, iterating) {
  constexpr size_t NUM_ROWS = 42;
  constexpr size_t NUM_COLS = 17;

  IdTable t1(NUM_COLS, allocator());
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.emplace_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = I(i * NUM_COLS + 1 + j);
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
      ASSERT_EQ(I(row_index * NUM_COLS + i + 1), row[i]);
    }
    row_index++;
  }
}

TEST(IdTableTest, sortTest) {
  IdTable test(2, allocator());
  test.push_back({I(3), I(1)});
  test.push_back({I(8), I(9)});
  test.push_back({I(1), I(5)});
  test.push_back({I(0), I(4)});
  test.push_back({I(5), I(8)});
  test.push_back({I(6), I(2)});

  IdTable orig = test.clone();

  // First check for the requirements of the iterator:
  // Value Swappable : swap rows 0 and 2
  IdTable::iterator i1 = test.begin();
  IdTable::iterator i2 = i1 + 2;
  std::iter_swap(i1, i2);
  ASSERT_EQ(orig[0], test[2]);
  ASSERT_EQ(orig[2], test[0]);

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
  std::sort(test.begin(), test.end(),
            [](const auto& v1, const auto& v2) { return v1[0] < v2[0]; });

  // The sorted order of the orig tables should be:
  // 3, 2, 0, 4, 5, 1
  ASSERT_EQ(orig[3], test[0]);
  ASSERT_EQ(orig[2], test[1]);
  ASSERT_EQ(orig[0], test[2]);
  ASSERT_EQ(orig[4], test[3]);
  ASSERT_EQ(orig[5], test[4]);
  ASSERT_EQ(orig[1], test[5]);
}

// =============================================================================
// IdTableStatic tests
// =============================================================================

TEST(IdTableStaticTest, push_back_and_assign) {
  constexpr size_t NUM_ROWS = 30;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{allocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back({I(i * NUM_COLS + 1), I(i * NUM_COLS + 2), I(i * NUM_COLS + 3),
                  I(i * NUM_COLS + 4)});
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.numRows());
  ASSERT_EQ(NUM_COLS, t1.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(I(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  // assign new values to the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t1(i / NUM_COLS, i % NUM_COLS) = I((NUM_ROWS * NUM_COLS) - i);
  }

  // test for the new entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(I((NUM_ROWS * NUM_COLS) - i), t1(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, insert) {
  IdTableStatic<4> t1{allocator()};
  t1.push_back({I(7), I(2), I(4), I(1)});
  t1.push_back({I(0), I(22), I(1), I(4)});

  IdTableStatic<4> init{allocator()};
  init.push_back({I(1), I(0), I(6), I(3)});
  init.push_back({I(3), I(1), I(8), I(2)});
  init.push_back({I(0), I(6), I(8), I(5)});
  init.push_back({I(9), I(2), I(6), I(8)});

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
  IdTableStatic<NUM_COLS> t1{allocator()};
  t1.reserve(NUM_ROWS);

  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.emplace_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = I(i * NUM_COLS + 1 + j);
    }
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.numRows());
  ASSERT_EQ(NUM_COLS, t1.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(I(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  // Test a resize call instead of insertions
  IdTableStatic<NUM_COLS> t2{allocator()};
  t2.resize(NUM_ROWS);

  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t2(i / NUM_COLS, i % NUM_COLS) = I(i + 1);
  }

  ASSERT_EQ(NUM_ROWS, t2.size());
  ASSERT_EQ(NUM_ROWS, t2.numRows());
  ASSERT_EQ(NUM_COLS, t2.numColumns());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(I(i + 1), t2(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, copyAndMove) {
  constexpr size_t NUM_ROWS = 100;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{allocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back({I(i * NUM_COLS + 1), I(i * NUM_COLS + 2), I(i * NUM_COLS + 3),
                  I(i * NUM_COLS + 4)});
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
    ASSERT_EQ(I(i + 1), t2(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(I(i + 1), t3(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(I(i + 1), t4(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(I(i + 1), t5(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, erase) {
  constexpr size_t NUM_ROWS = 12;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{allocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t j = 0; j < 2 * NUM_ROWS; j++) {
    size_t i = j / 2;
    t1.push_back({I(i * NUM_COLS + 1), I(i * NUM_COLS + 2), I(i * NUM_COLS + 3),
                  I(i * NUM_COLS + 4)});
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
    ASSERT_EQ(I(i + 1), t1(i / NUM_COLS, i % NUM_COLS));
  }

  t1.erase(t1.begin(), t1.end());
  ASSERT_EQ(0u, t1.size());
}

TEST(IdTableStaticTest, iterating) {
  constexpr size_t NUM_ROWS = 42;
  constexpr size_t NUM_COLS = 17;

  IdTableStatic<NUM_COLS> t1{allocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.emplace_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = I(i * NUM_COLS + 1 + j);
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
      ASSERT_EQ(I(row_index * NUM_COLS + i + 1), row[i]);
    }
    row_index++;
  }
}

// =============================================================================
// Conversion Tests
// =============================================================================
TEST(IdTableTest, conversion) {
  IdTable table(3, allocator());
  table.push_back({I(4), I(1), I(0)});
  table.push_back({I(1), I(7), I(8)});
  table.push_back({I(7), I(12), I(2)});
  table.push_back({I(9), I(3), I(4)});

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
  IdTable tableVar(6, allocator());
  tableVar.push_back({I(1), I(2), I(3), I(6), I(5), I(9)});
  tableVar.push_back({I(0), I(4), I(3), I(4), I(5), I(3)});
  tableVar.push_back({I(3), I(2), I(3), I(2), I(5), I(6)});
  tableVar.push_back({I(5), I(5), I(9), I(4), I(7), I(0)});

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

TEST(IdTableTest, staticAsserts) {
  static_assert(std::is_trivially_copyable_v<IdTableStatic<1>::iterator>);
  static_assert(std::is_trivially_copyable_v<IdTableStatic<1>::const_iterator>);
}

// Check that we can completely instantiate `IdTable`s with a different value
// type and a different underlying storage.

// Note: Clang 13 and 14 don't handle the `requires` clauses in the `clone`
// member function correctly, so we have to disable these checks for those
// compiler versions.
// TODO<joka921> throw these checks out as soon as we don't support these
// compiler versions anymore.

#if not(defined(__clang__)) || (__clang_major__ != 13 && __clang_major__ != 14)
template class columnBasedIdTable::IdTable<char, 0>;
static_assert(!std::is_copy_constructible_v<ad_utility::BufferedVector<char>>);
template class columnBasedIdTable::IdTable<char, 0,
                                           ad_utility::BufferedVector<char>>;
#endif
