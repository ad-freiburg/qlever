
// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "../src/engine/IdTable.h"
#include "../src/global/Id.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

TEST(IdTableTest, push_back_and_assign) {
  constexpr size_t NUM_ROWS = 30;
  constexpr size_t NUM_COLS = 4;

  IdTable t1{NUM_COLS, allocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back({i * NUM_COLS + 1, i * NUM_COLS + 2, i * NUM_COLS + 3,
                  i * NUM_COLS + 4});
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.rows());
  ASSERT_EQ(NUM_COLS, t1.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t1(i / NUM_COLS, i % NUM_COLS));
  }

  // assign new values to the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t1(i / NUM_COLS, i % NUM_COLS) = (NUM_ROWS * NUM_COLS) - i;
  }

  // test for the new entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ((NUM_ROWS * NUM_COLS) - i, t1(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableTest, insert) {
  IdTable t1{4, allocator()};
  t1.push_back({7, 2, 4, 1});
  t1.push_back({0, 22, 1, 4});

  IdTable init(4, allocator());
  init.push_back({1, 0, 6, 3});
  init.push_back({3, 1, 8, 2});
  init.push_back({0, 6, 8, 5});
  init.push_back({9, 2, 6, 8});

  IdTable t2(init);

  // Test inserting at the beginning
  t2.insert(t2.begin(), t1.begin(), t1.end());
  for (size_t i = 0; i < t1.size(); i++) {
    ASSERT_EQ(t1[i], t2[i]);
  }
  for (size_t i = 0; i < init.size(); i++) {
    ASSERT_EQ(init[i], t2[i + t1.size()]);
  }

  // Test inserting at the end
  t2 = init;
  t2.insert(t2.end(), t1.begin(), t1.end());
  for (size_t i = 0; i < init.size(); i++) {
    ASSERT_EQ(init[i], t2[i]);
  }
  for (size_t i = 0; i < t1.size(); i++) {
    ASSERT_EQ(t1[i], t2[i + init.size()]);
  }

  // Test inserting at the center
  t2 = init;
  t2.insert(t2.begin() + 3, t1.begin(), t1.end());
  for (size_t i = 0; i < t2.size(); i++) {
    if (i < 3) {
      ASSERT_EQ(init[i], t2[i]);
    } else if (i < 5) {
      ASSERT_EQ(t1[i - 3], t2[i]);
    } else {
      ASSERT_EQ(init[i - 2], t2[i]);
    }
  }
}

TEST(IdTableTest, reserve_and_resize) {
  constexpr size_t NUM_ROWS = 34;
  constexpr size_t NUM_COLS = 20;

  // Test a reserve call before insertions
  IdTable t1(NUM_COLS, allocator());
  t1.reserve(NUM_ROWS);

  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = i * NUM_COLS + 1 + j;
    }
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.rows());
  ASSERT_EQ(NUM_COLS, t1.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t1(i / NUM_COLS, i % NUM_COLS));
  }

  // Test a resize call instead of insertions
  IdTable t2(NUM_COLS, allocator());
  t2.resize(NUM_ROWS);

  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t2(i / NUM_COLS, i % NUM_COLS) = i + 1;
  }

  ASSERT_EQ(NUM_ROWS, t2.size());
  ASSERT_EQ(NUM_ROWS, t2.rows());
  ASSERT_EQ(NUM_COLS, t2.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t2(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableTest, copyAndMove) {
  constexpr size_t NUM_ROWS = 100;
  constexpr size_t NUM_COLS = 4;

  IdTable t1(NUM_COLS, allocator());
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back({i * NUM_COLS + 1, i * NUM_COLS + 2, i * NUM_COLS + 3,
                  i * NUM_COLS + 4});
  }

  // Test all copy and move constructors and assignment operators
  IdTable t2(t1);
  IdTable t3 = t1;
  IdTable tmp = t1;
  IdTable t4(std::move(t1));
  IdTable t5 = std::move(tmp);

  ASSERT_EQ(NUM_ROWS, t2.size());
  ASSERT_EQ(NUM_ROWS, t2.rows());
  ASSERT_EQ(NUM_COLS, t2.cols());

  ASSERT_EQ(NUM_ROWS, t3.size());
  ASSERT_EQ(NUM_ROWS, t3.rows());
  ASSERT_EQ(NUM_COLS, t3.cols());

  ASSERT_EQ(NUM_ROWS, t4.size());
  ASSERT_EQ(NUM_ROWS, t4.rows());
  ASSERT_EQ(NUM_COLS, t4.cols());

  ASSERT_EQ(NUM_ROWS, t5.size());
  ASSERT_EQ(NUM_ROWS, t5.rows());
  ASSERT_EQ(NUM_COLS, t5.cols());

  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t2(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(i + 1, t3(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(i + 1, t4(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(i + 1, t5(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableTest, erase) {
  constexpr size_t NUM_ROWS = 12;
  constexpr size_t NUM_COLS = 4;

  IdTable t1(NUM_COLS, allocator());
  // Fill the rows with numbers counting up from 1
  for (size_t j = 0; j < 2 * NUM_ROWS; j++) {
    size_t i = j / 2;
    t1.push_back({i * NUM_COLS + 1, i * NUM_COLS + 2, i * NUM_COLS + 3,
                  i * NUM_COLS + 4});
  }
  // i will underflow and then be larger than 2 * NUM_ROWS
  for (size_t i = 2 * NUM_ROWS - 1; i <= 2 * NUM_ROWS; i -= 2) {
    t1.erase(t1.begin() + i);
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.rows());
  ASSERT_EQ(NUM_COLS, t1.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t1(i / NUM_COLS, i % NUM_COLS));
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
    t1.push_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = i * NUM_COLS + 1 + j;
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
      ASSERT_EQ(row_index * NUM_COLS + i + 1, row[i]);
    }
    row_index++;
  }
}

TEST(IdTableTest, sortTest) {
  IdTable test(2, allocator());
  test.push_back({3, 1});
  test.push_back({8, 9});
  test.push_back({1, 5});
  test.push_back({0, 4});
  test.push_back({5, 8});
  test.push_back({6, 2});

  IdTable orig(test);

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
  test = orig;
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
    t1.push_back({i * NUM_COLS + 1, i * NUM_COLS + 2, i * NUM_COLS + 3,
                  i * NUM_COLS + 4});
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.rows());
  ASSERT_EQ(NUM_COLS, t1.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t1(i / NUM_COLS, i % NUM_COLS));
  }

  // assign new values to the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t1(i / NUM_COLS, i % NUM_COLS) = (NUM_ROWS * NUM_COLS) - i;
  }

  // test for the new entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ((NUM_ROWS * NUM_COLS) - i, t1(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, insert) {
  IdTableStatic<4> t1{allocator()};
  t1.push_back({7, 2, 4, 1});
  t1.push_back({0, 22, 1, 4});

  IdTableStatic<4> init{allocator()};
  init.push_back({1, 0, 6, 3});
  init.push_back({3, 1, 8, 2});
  init.push_back({0, 6, 8, 5});
  init.push_back({9, 2, 6, 8});

  IdTableStatic<4> t2(init);

  // Test inserting at the beginning
  t2.insert(t2.begin(), t1.begin(), t1.end());
  for (size_t i = 0; i < t1.size(); i++) {
    ASSERT_EQ(t1[i], t2[i]);
  }
  for (size_t i = 0; i < init.size(); i++) {
    ASSERT_EQ(init[i], t2[i + t1.size()]);
  }

  // Test inserting at the end
  t2 = init;
  t2.insert(t2.end(), t1.begin(), t1.end());
  for (size_t i = 0; i < init.size(); i++) {
    ASSERT_EQ(init[i], t2[i]);
  }
  for (size_t i = 0; i < t1.size(); i++) {
    ASSERT_EQ(t1[i], t2[i + init.size()]);
  }

  // Test inserting at the center
  t2 = init;
  t2.insert(t2.begin() + 3, t1.begin(), t1.end());
  for (size_t i = 0; i < t2.size(); i++) {
    if (i < 3) {
      ASSERT_EQ(init[i], t2[i]);

    } else if (i < 5) {
      ASSERT_EQ(t1[i - 3], t2[i]);

    } else {
      ASSERT_EQ(init[i - 2], t2[i]);
    }
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
    t1.push_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = i * NUM_COLS + 1 + j;
    }
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.rows());
  ASSERT_EQ(NUM_COLS, t1.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t1(i / NUM_COLS, i % NUM_COLS));
  }

  // Test a resize call instead of insertions
  IdTableStatic<NUM_COLS> t2{allocator()};
  t2.resize(NUM_ROWS);

  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    t2(i / NUM_COLS, i % NUM_COLS) = i + 1;
  }

  ASSERT_EQ(NUM_ROWS, t2.size());
  ASSERT_EQ(NUM_ROWS, t2.rows());
  ASSERT_EQ(NUM_COLS, t2.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t2(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, copyAndMove) {
  constexpr size_t NUM_ROWS = 100;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{allocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t i = 0; i < NUM_ROWS; i++) {
    t1.push_back({i * NUM_COLS + 1, i * NUM_COLS + 2, i * NUM_COLS + 3,
                  i * NUM_COLS + 4});
  }

  // Test all copy and move constructors and assignment operators
  IdTableStatic<NUM_COLS> t2(t1);
  IdTableStatic<NUM_COLS> t3 = t1;
  IdTableStatic<NUM_COLS> tmp = t1;
  IdTableStatic<NUM_COLS> t4(std::move(t1));
  IdTableStatic<NUM_COLS> t5 = std::move(tmp);

  ASSERT_EQ(NUM_ROWS, t2.size());
  ASSERT_EQ(NUM_ROWS, t2.rows());
  ASSERT_EQ(NUM_COLS, t2.cols());

  ASSERT_EQ(NUM_ROWS, t3.size());
  ASSERT_EQ(NUM_ROWS, t3.rows());
  ASSERT_EQ(NUM_COLS, t3.cols());

  ASSERT_EQ(NUM_ROWS, t4.size());
  ASSERT_EQ(NUM_ROWS, t4.rows());
  ASSERT_EQ(NUM_COLS, t4.cols());

  ASSERT_EQ(NUM_ROWS, t5.size());
  ASSERT_EQ(NUM_ROWS, t5.rows());
  ASSERT_EQ(NUM_COLS, t5.cols());

  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t2(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(i + 1, t3(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(i + 1, t4(i / NUM_COLS, i % NUM_COLS));
    ASSERT_EQ(i + 1, t5(i / NUM_COLS, i % NUM_COLS));
  }
}

TEST(IdTableStaticTest, erase) {
  constexpr size_t NUM_ROWS = 12;
  constexpr size_t NUM_COLS = 4;

  IdTableStatic<NUM_COLS> t1{allocator()};
  // Fill the rows with numbers counting up from 1
  for (size_t j = 0; j < 2 * NUM_ROWS; j++) {
    size_t i = j / 2;
    t1.push_back({i * NUM_COLS + 1, i * NUM_COLS + 2, i * NUM_COLS + 3,
                  i * NUM_COLS + 4});
  }
  // i will underflow and then be larger than 2 * NUM_ROWS
  for (size_t i = 2 * NUM_ROWS - 1; i <= 2 * NUM_ROWS; i -= 2) {
    t1.erase(t1.begin() + i);
  }

  ASSERT_EQ(NUM_ROWS, t1.size());
  ASSERT_EQ(NUM_ROWS, t1.rows());
  ASSERT_EQ(NUM_COLS, t1.cols());
  // check the entries
  for (size_t i = 0; i < NUM_ROWS * NUM_COLS; i++) {
    ASSERT_EQ(i + 1, t1(i / NUM_COLS, i % NUM_COLS));
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
    t1.push_back();
    for (size_t j = 0; j < NUM_COLS; j++) {
      t1(i, j) = i * NUM_COLS + 1 + j;
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
      ASSERT_EQ(row_index * NUM_COLS + i + 1, row[i]);
    }
    row_index++;
  }
}

// =============================================================================
// Conversion Tests
// =============================================================================
TEST(IdTableTest, conversion) {
  IdTable table(3, allocator());
  table.push_back({4, 1, 0});
  table.push_back({1, 7, 8});
  table.push_back({7, 12, 2});
  table.push_back({9, 3, 4});

  IdTable initial = table;

  IdTableStatic<3> s = std::move(table).moveToStatic<3>();
  ASSERT_EQ(4u, s.size());
  ASSERT_EQ(3u, s.cols());
  for (size_t i = 0; i < s.size(); i++) {
    for (size_t j = 0; j < s.cols(); j++) {
      ASSERT_EQ(initial(i, j), s(i, j));
    }
  }

  table = std::move(s).moveToDynamic();
  ASSERT_EQ(4u, table.size());
  ASSERT_EQ(3u, table.cols());
  for (size_t i = 0; i < table.size(); i++) {
    for (size_t j = 0; j < table.cols(); j++) {
      ASSERT_EQ(initial(i, j), table(i, j));
    }
  }

  auto view = table.asStaticView<3>();
  static_assert(std::is_same_v<decltype(view), IdTableView<3>>);
  ASSERT_EQ(4u, view.size());
  ASSERT_EQ(3u, view.cols());
  for (size_t i = 0; i < view.size(); i++) {
    for (size_t j = 0; j < view.cols(); j++) {
      ASSERT_EQ(initial(i, j), view(i, j));
    }
  }

  // Test with more than 5 columns
  IdTable tableVar(6, allocator());
  tableVar.push_back({1, 2, 3, 6, 5, 9});
  tableVar.push_back({0, 4, 3, 4, 5, 3});
  tableVar.push_back({3, 2, 3, 2, 5, 6});
  tableVar.push_back({5, 5, 9, 4, 7, 0});

  IdTable initialVar = tableVar;

  IdTableStatic<6> staticVar = std::move(tableVar).moveToStatic<6>();
  ASSERT_EQ(initialVar.size(), staticVar.size());
  ASSERT_EQ(initialVar.cols(), staticVar.cols());
  for (size_t i = 0; i < staticVar.size(); i++) {
    for (size_t j = 0; j < staticVar.cols(); j++) {
      ASSERT_EQ(initialVar(i, j), staticVar(i, j));
    }
  }

  IdTable dynamicVar = std::move(staticVar).moveToDynamic();
  ASSERT_EQ(initialVar.size(), dynamicVar.size());
  ASSERT_EQ(initialVar.cols(), dynamicVar.cols());
  for (size_t i = 0; i < dynamicVar.size(); i++) {
    for (size_t j = 0; j < dynamicVar.cols(); j++) {
      ASSERT_EQ(initialVar(i, j), dynamicVar(i, j));
    }
  }

  auto viewVar = std::move(dynamicVar).asStaticView<6>();
  static_assert(std::is_same_v<decltype(viewVar), IdTableView<6>>);
  ASSERT_EQ(initialVar.size(), viewVar.size());
  ASSERT_EQ(initialVar.cols(), viewVar.cols());
  for (size_t i = 0; i < viewVar.size(); i++) {
    for (size_t j = 0; j < viewVar.cols(); j++) {
      ASSERT_EQ(initialVar(i, j), viewVar(i, j));
    }
  }
}

TEST(IdTableTest, staticAsserts) {
  static_assert(std::is_trivially_copyable_v<IdTableStatic<1>::iterator>);
  static_assert(std::is_trivially_copyable_v<IdTableStatic<1>::const_iterator>);
}
