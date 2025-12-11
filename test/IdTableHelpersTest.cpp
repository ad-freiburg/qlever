// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <ranges>
#include <utility>
#include <vector>

#include "./util/IdTableHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "util/Algorithm.h"
#include "util/ConstexprUtils.h"
#include "util/IdTestHelpers.h"
#include "util/Iterators.h"
#include "util/RandomTestHelpers.h"

/*
@brief Calculate all sub-sets of a container of elements. Note: Duplicated
elements will not be ignored.

@tparam R The type of the range.
@tparam E The type of the content in `setToCalculateFor`.

@param setToCalculateFor The container to calculate all sub-sets for. Will only
be read.
*/
CPP_template(typename R,
             typename E = std::iter_value_t<ql::ranges::iterator_t<R>>)(
    requires ql::ranges::forward_range<R>)
    std::vector<std::vector<E>> calculateAllSubSets(R&& setToCalculateFor) {
  // Getting rid of duplicated elements.

  std::vector<std::vector<E>> calculatedSubSets;
  // There will be exactly $setToCalculateFor.size()^2$ items added.
  calculatedSubSets.reserve(
      ad_utility::pow(2, ql::ranges::size(setToCalculateFor)));

  // The empty set is always a sub-set.
  calculatedSubSets.push_back({});

  // Calculate all sub-sets.
  ql::ranges::for_each(setToCalculateFor, [&calculatedSubSets](const E& entry) {
    ad_utility::appendVector(
        calculatedSubSets,
        ad_utility::transform(calculatedSubSets,
                              [&entry](std::vector<E> subSet) {
                                subSet.push_back(entry);
                                return subSet;
                              }));
  });

  return calculatedSubSets;
}

TEST(IdTableHelpersHelpersTest, calculateAllSubSets) {
  // Calculate the sub sets and compare with an given vector.
  auto doTest = [](const std::vector<size_t>& input,
                   std::vector<std::vector<size_t>> expectedOutput) {
    std::vector<std::vector<size_t>> result{calculateAllSubSets(input)};

    // For comparison, we have to sort both vectors.
    ql::ranges::sort(expectedOutput, ql::ranges::lexicographical_compare);
    ql::ranges::sort(result, ql::ranges::lexicographical_compare);

    ASSERT_EQ(expectedOutput, result);
  };

  // An empty vector should result in a vector with only an empty vector inside.
  doTest({}, {{}});

  // Single element.
  doTest({1uL}, {{}, {1uL}});

  // Three elements.
  doTest({4uL, 2uL, 5uL}, {{},
                           {4uL, 2uL, 5uL},
                           {4uL},
                           {2uL},
                           {5uL},
                           {4uL, 5uL},
                           {2uL, 5uL},
                           {4uL, 2uL}});
}

// _____________________________________________________________________________
// Tests for makeRangeVectorTable.

TEST(IdTableHelpersTest, makeRangeVectorTableBasicRanges) {
  // Empty range when a == b.
  {
    auto table = makeRangeVectorTable(0, 0);
    EXPECT_TRUE(table.empty());
  }
  // Simple small range [0, 3).
  {
    auto table = makeRangeVectorTable(0, 3);
    ASSERT_EQ(table.size(), 3u);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(table[i].size(), 1u);
      ASSERT_TRUE(std::holds_alternative<int64_t>(table[i][0]));
      EXPECT_EQ(std::get<int64_t>(table[i][0]), static_cast<int64_t>(i));
    }
  }
  // Non-zero start: [2, 5) -> 2, 3, 4.
  {
    auto table = makeRangeVectorTable(2, 5);
    ASSERT_EQ(table.size(), 3u);
    for (size_t row = 0; row < table.size(); ++row) {
      ASSERT_EQ(table[row].size(), 1u);
      ASSERT_TRUE(std::holds_alternative<int64_t>(table[row][0]));
      EXPECT_EQ(std::get<int64_t>(table[row][0]),
                static_cast<int64_t>(row + 2));
    }
  }
  // a > b should yield an empty table.
  {
    auto table = makeRangeVectorTable(5, 2);
    EXPECT_TRUE(table.empty());
  }
}

// Checks, if the given `IdTable` fulfills all wanted criteria.
void generalIdTableCheck(const IdTable& table,
                         const size_t& expectedNumberOfRows,
                         const size_t& expectedNumberOfColumns,
                         bool allEntriesWereSet) {
  ASSERT_EQ(table.numRows(), expectedNumberOfRows);
  ASSERT_EQ(table.numColumns(), expectedNumberOfColumns);

  if (allEntriesWereSet) {
    ASSERT_TRUE(ql::ranges::all_of(table, [](const auto& row) {
      return ql::ranges::all_of(row, [](const ValueId& entry) {
        return ad_utility::testing::VocabId(0) <= entry &&
               entry <= ad_utility::testing::VocabId(ValueId::maxIndex);
      });
    }));
  }
}

// The overloads that don't take generators.
TEST(IdTableHelpersTest, createRandomlyFilledIdTableWithoutGenerators) {
  // Table with zero rows/columns.
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(0, 0, JoinColumnAndBounds{0, 0, 1}));
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(1, 0, JoinColumnAndBounds{0, 0, 1}));
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      0, 0, std::vector{JoinColumnAndBounds{0, 0, 1}}));
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      1, 0, std::vector{JoinColumnAndBounds{0, 0, 1}}));
  {
    auto table = createRandomlyFilledIdTable(
        0, 1, std::vector{JoinColumnAndBounds{0, 0, 1}});
    EXPECT_EQ(table.numRows(), 0);
    EXPECT_EQ(table.numColumns(), 1);
  }

  // Table with out of bounds join column.
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(5, 5, JoinColumnAndBounds{6, 0, 1}));
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      5, 5, std::vector{JoinColumnAndBounds{6, 0, 1}}));

  // Table with lower bound, that is higher than the upper bound.
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(5, 5, JoinColumnAndBounds{0, 3, 2}));
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      5, 5, std::vector{JoinColumnAndBounds{0, 3, 2}}));

  // Checks, if all entries of are within a given inclusive range.
  auto checkColumn = [](const IdTable& table, const size_t& columnNumber,
                        const size_t& lowerBound, const size_t& upperBound) {
    ASSERT_TRUE(ql::ranges::all_of(
        table.getColumn(columnNumber),
        [&lowerBound, &upperBound](const ValueId& entry) {
          return ad_utility::testing::VocabId(lowerBound) <= entry &&
                 entry <= ad_utility::testing::VocabId(upperBound);
        }));
  };

  // Sample request for the overload that takes a single `JoinColumnAndBounds`.
  IdTable result{
      createRandomlyFilledIdTable(5, 5, JoinColumnAndBounds{0, 0, 10})};
  generalIdTableCheck(result, 5, 5, true);
  checkColumn(result, 0, 0, 10);

  result = createRandomlyFilledIdTable(50, 58, JoinColumnAndBounds{0, 30, 42});
  generalIdTableCheck(result, 50, 58, true);
  checkColumn(result, 0, 30, 42);

  // No join columns with explicit generators are specified, so all columns are
  // filled randomly.
  result =
      createRandomlyFilledIdTable(50, 58, std::vector<JoinColumnAndBounds>{});
  generalIdTableCheck(result, 50, 58, true);

  /*
  Exhaustive input test for the overload that takes a vector of
  `JoinColumnAndBounds`, in the case of generating tables with 40 rows and
  10 columns.
  */
  ql::ranges::for_each(
      calculateAllSubSets(std::vector<size_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      [&checkColumn, &result](const std::vector<size_t>& joinColumns) {
        result = createRandomlyFilledIdTable(
            40, 10, ad_utility::transform(joinColumns, [](const size_t& jc) {
              return JoinColumnAndBounds{jc, jc * 10, jc * 10 + 9};
            }));

        // General check.
        generalIdTableCheck(result, 40, 10, true);

        // Are the join columns like we wanted them?
        ql::ranges::for_each(joinColumns,
                             [&result, &checkColumn](const size_t& jc) {
                               checkColumn(result, jc, jc * 10, jc * 10 + 9);
                             });
      });
}

// The overloads that take generators for creating the content of the join
// columns.
TEST(IdTableHelpersTest, createRandomlyFilledIdTableWithGenerators) {
  // Creates a 'generator', that counts one up, every time it's called.
  auto createCountUpGenerator = []() {
    return [i = 0]() mutable { return ad_utility::testing::VocabId(i++); };
  };

  // Compares the content of a specific column with a given vector.
  auto compareColumnsWithVectors =
      [](const IdTable& table, const size_t& columnNumber,
         const std::vector<size_t>& expectedContent) {
        ASSERT_EQ(table.numRows(), expectedContent.size());
        for (size_t i = 0; i < table.numRows(); i++) {
          ASSERT_EQ(table(i, columnNumber),
                    ad_utility::testing::VocabId(expectedContent.at(i)));
        }
      };

  // Assigning a generator to a column outside of the table size.
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(10, 10, {{10, createCountUpGenerator()}}));
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(10, 10, {10}, createCountUpGenerator()));

  // Assigning a generator to the same column twice.
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      10, 10, {{1, createCountUpGenerator()}, {1, createCountUpGenerator()}}));
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(10, 10, {1, 1}, createCountUpGenerator()));

  // Giving an empty function.
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      10, 10, {{1, createCountUpGenerator()}, {1, {}}}));
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(10, 10, {1}, std::function<ValueId()>{}));

  // Creating an empty table of size (0,0).
  {
    auto table = createRandomlyFilledIdTable(
        0, 0, std::vector<std::pair<size_t, std::function<ValueId()>>>{});
    EXPECT_EQ(table.numRows(), 0);
    EXPECT_EQ(table.numColumns(), 0);
  }
  ASSERT_ANY_THROW(
      createRandomlyFilledIdTable(0, 0, {}, std::function<ValueId()>{}));

  // Exhaustive test, if the creation of a randomly filled table works,
  // regardless of the amount of join columns and their position.
  ql::ranges::for_each(
      calculateAllSubSets(std::vector<size_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      [&createCountUpGenerator,
       &compareColumnsWithVectors](const std::vector<size_t>& joinColumns) {
        IdTable resultMultiGenerator = createRandomlyFilledIdTable(
            10, 10,
            ad_utility::transform(
                joinColumns, [&createCountUpGenerator](const size_t& num) {
                  return std::make_pair(
                      num, std::function<ValueId()>{createCountUpGenerator()});
                }));
        IdTable resultSingleGenerator = createRandomlyFilledIdTable(
            10, 10, joinColumns, createCountUpGenerator());

        // Check, if every entry of the tables was set and if the join columns
        // have the correct content.
        generalIdTableCheck(resultMultiGenerator, 10, 10, true);
        generalIdTableCheck(resultSingleGenerator, 10, 10, true);
        ql::ranges::for_each(
            joinColumns,
            [&resultMultiGenerator, &resultSingleGenerator, &joinColumns,
             &compareColumnsWithVectors](const size_t& num) {
              compareColumnsWithVectors(resultMultiGenerator, num,
                                        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

              const size_t indexOfTheColumn =
                  ql::ranges::find(joinColumns, num) - joinColumns.begin();
              compareColumnsWithVectors(
                  resultSingleGenerator, num,
                  {indexOfTheColumn, indexOfTheColumn + joinColumns.size(),
                   indexOfTheColumn + joinColumns.size() * 2,
                   indexOfTheColumn + joinColumns.size() * 3,
                   indexOfTheColumn + joinColumns.size() * 4,
                   indexOfTheColumn + joinColumns.size() * 5,
                   indexOfTheColumn + joinColumns.size() * 6,
                   indexOfTheColumn + joinColumns.size() * 7,
                   indexOfTheColumn + joinColumns.size() * 8,
                   indexOfTheColumn + joinColumns.size() * 9});
            });
      });

  // Simple test, if the function actually uses different generators, if told
  // to.
  IdTable result = createRandomlyFilledIdTable(
      10, 10,
      {{0, createCountUpGenerator()},
       {1, []() { return ad_utility::testing::VocabId(42); }}});
  generalIdTableCheck(result, 10, 10, true);
  compareColumnsWithVectors(result, 0, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  compareColumnsWithVectors(result, 1,
                            {42, 42, 42, 42, 42, 42, 42, 42, 42, 42});
}

// _____________________________________________________________________________
// Tests for createLazyIdTables.

TEST(IdTableHelpersTest, createLazyIdTablesSingleBlock) {
  // One block with a couple of rows; all entries are ints, so IntId is used.
  VectorTable block;
  block.push_back({IntOrId{int64_t{0}}, IntOrId{int64_t{1}}});
  block.push_back({IntOrId{int64_t{2}}, IntOrId{int64_t{3}}});

  std::vector<VectorTable> blocks;
  blocks.push_back(block);

  auto idTables = createLazyIdTables(blocks);
  ASSERT_EQ(idTables.size(), 1u);

  const auto& t = idTables[0];
  ASSERT_EQ(t.numRows(), 2u);
  ASSERT_EQ(t.numColumns(), 2u);

  // Each integer is transformed via IntId; compare via MatchesIdTableFromVector
  // using the same IntId transformation as in createLazyIdTables.
  VectorTable expected = block;
  EXPECT_THAT(t,
              matchesIdTableFromVector(expected, ad_utility::testing::IntId));
}

TEST(IdTableHelpersTest, createLazyIdTablesMultipleBlocks) {
  // Two blocks, with different shapes.
  VectorTable block1;
  block1.push_back({IntOrId{int64_t{0}}});
  block1.push_back({IntOrId{int64_t{1}}});

  VectorTable block2;
  block2.push_back({IntOrId{int64_t{10}}, IntOrId{int64_t{11}}});

  std::vector<VectorTable> blocks;
  blocks.push_back(block1);
  blocks.push_back(block2);

  auto idTables = createLazyIdTables(blocks);
  ASSERT_EQ(idTables.size(), 2u);

  // First block.
  {
    const auto& t1 = idTables[0];
    ASSERT_EQ(t1.numRows(), 2u);
    ASSERT_EQ(t1.numColumns(), 1u);
    EXPECT_THAT(t1,
                matchesIdTableFromVector(block1, ad_utility::testing::IntId));
  }

  // Second block.
  {
    const auto& t2 = idTables[1];
    ASSERT_EQ(t2.numRows(), 1u);
    ASSERT_EQ(t2.numColumns(), 2u);
    EXPECT_THAT(t2,
                matchesIdTableFromVector(block2, ad_utility::testing::IntId));
  }
}

TEST(IdTableHelpersTest, generateIdTable) {
  /*
  Creates a 'generator', that returns a row of the given length, were every
  entry contains the same number. The number starts with 0 and goes on up
  with every call.
  */
  auto createCountUpGenerator = [](const size_t& width) {
    return [width, i = 0]() mutable {
      // Create the row.
      std::vector<ValueId> row(width);

      // Fill the row.
      ql::ranges::fill(row, ad_utility::testing::VocabId(i));

      i++;
      return row;
    };
  };

  // A row generator should always have the correct width.
  ASSERT_ANY_THROW(generateIdTable(5, 5, createCountUpGenerator(0)));
  ASSERT_ANY_THROW(generateIdTable(5, 5, createCountUpGenerator(4)));
  ASSERT_ANY_THROW(generateIdTable(5, 5, [i = 0]() mutable {
    // Create the row.
    std::vector<ValueId> row(i < 3 ? 5 : 20);

    // Fill the row.
    ql::ranges::fill(row, ad_utility::testing::VocabId(4));

    i++;
    return row;
  }));

  // Create a `IdTable` and check it's content.
  IdTable table{generateIdTable(5, 5, createCountUpGenerator(5))};
  generalIdTableCheck(table, 5, 5, true);
  for (size_t row = 0; row < 5; row++) {
    ASSERT_TRUE(ql::ranges::all_of(table[row], [&row](const auto& entry) {
      return entry == ad_utility::testing::VocabId(row);
    }));
  }

  table = generateIdTable(0, 0, createCountUpGenerator(0));
  EXPECT_EQ(table.numRows(), 0);
  EXPECT_EQ(table.numColumns(), 0);
}

/*
Quick check, if identical calls, with the same random number generator seed,
create the same `IdTable`.
*/
TEST(IdTableHelpersTest, randomSeed) {
  // How big should the tables be.
  constexpr size_t NUM_ROWS = 100;
  constexpr size_t NUM_COLUMNS = 200;

  ql::ranges::for_each(
      createArrayOfRandomSeeds<5>(), [](const ad_utility::RandomSeed seed) {
        // Simply generate and compare.
        ASSERT_EQ(
            createRandomlyFilledIdTable(
                NUM_ROWS, NUM_COLUMNS,
                std::vector<std::pair<size_t, std::function<ValueId()>>>{},
                seed),
            createRandomlyFilledIdTable(
                NUM_ROWS, NUM_COLUMNS,
                std::vector<std::pair<size_t, std::function<ValueId()>>>{},
                seed));
        ASSERT_EQ(createRandomlyFilledIdTable(
                      NUM_ROWS, NUM_COLUMNS, std::vector<size_t>{},
                      []() { return ad_utility::testing::VocabId(1); }, seed),
                  createRandomlyFilledIdTable(
                      NUM_ROWS, NUM_COLUMNS, std::vector<size_t>{},
                      []() { return ad_utility::testing::VocabId(1); }, seed));
        ASSERT_EQ(createRandomlyFilledIdTable(NUM_ROWS, NUM_COLUMNS,
                                              JoinColumnAndBounds{}, seed),
                  createRandomlyFilledIdTable(NUM_ROWS, NUM_COLUMNS,
                                              JoinColumnAndBounds{}, seed));
        ASSERT_EQ(createRandomlyFilledIdTable(
                      NUM_ROWS, NUM_COLUMNS, std::vector<JoinColumnAndBounds>{},
                      seed),
                  createRandomlyFilledIdTable(
                      NUM_ROWS, NUM_COLUMNS, std::vector<JoinColumnAndBounds>{},
                      seed));
        ASSERT_EQ(createRandomlyFilledIdTable(NUM_ROWS, NUM_COLUMNS, seed),
                  createRandomlyFilledIdTable(NUM_ROWS, NUM_COLUMNS, seed));
      });
}
