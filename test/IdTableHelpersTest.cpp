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
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "util/Algorithm.h"
#include "util/ConstexprUtils.h"
#include "util/IdTestHelpers.h"
#include "util/Iterators.h"

/*
@brief Calculate all sub-sets of a container of elements. Note: Duplicated
elements will not be ignored.

@tparam R The type of the range.
@tparam E The type of the content in `setToCalculateFor`.

@param setToCalculateFor The container to calculate all sub-sets for. Will only
be read.
*/
template <std::ranges::forward_range R,
          typename E = std::iter_value_t<std::ranges::iterator_t<R>>>
std::vector<std::vector<E>> calculateAllSubSets(R&& setToCalculateFor) {
  // Getting rid of duplicated elements.

  std::vector<std::vector<E>> calculatedSubSets;
  // There will be exactly $setToCalculateFor.size()^2$ items added.
  calculatedSubSets.reserve(
      ad_utility::pow(2, std::ranges::size(setToCalculateFor)));

  // The empty set is always a sub-set.
  calculatedSubSets.push_back({});

  // Calculate all sub-sets.
  std::ranges::for_each(
      setToCalculateFor, [&calculatedSubSets](const E& entry) {
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
    std::ranges::sort(expectedOutput, std::ranges::lexicographical_compare);
    std::ranges::sort(result, std::ranges::lexicographical_compare);

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

TEST(IdTableHelpersTest, createRandomlyFilledIdTable) {
  IdTable result{10, ad_utility::testing::makeAllocator()};
  result.resize(10);

  // Creates a 'generator', that counts one up, everytime it's called.
  auto createCountUpGenerator = []() {
    return [i = 0]() mutable { return ad_utility::testing::VocabId(i++); };
  };

  // Checks, if a given `IdTable` has all it's entries set.
  auto checkIfAllEntriesSet = [](const IdTable& table) {
    ASSERT_TRUE(std::ranges::all_of(table, [](const auto& row) {
      return std::ranges::all_of(row, [](const ValueId& entry) {
        return ad_utility::testing::VocabId(0) <= entry &&
               entry <= ad_utility::testing::VocabId(ValueId::maxIndex);
      });
    }));
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

  // Assigning a generator to the same column twice.
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      10, 10, {{1, createCountUpGenerator()}, {1, createCountUpGenerator()}}));

  // Giving an empty function.
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(
      10, 10, {{1, createCountUpGenerator()}, {1, {}}}));

  // Creating an empty table of size (0,0).
  ASSERT_ANY_THROW(createRandomlyFilledIdTable(0, 0, {}));

  // Exhaustive test, if the creation of a randomly filled table works,
  // regardless of the amount of join columns and their position.
  std::ranges::for_each(
      calculateAllSubSets(std::vector<size_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
      [&result, &createCountUpGenerator, &compareColumnsWithVectors,
       &checkIfAllEntriesSet](const std::vector<size_t>& joinColumns) {
        result = createRandomlyFilledIdTable(
            10, 10,
            ad_utility::transform(
                joinColumns, [&createCountUpGenerator](const size_t& num) {
                  return std::make_pair(
                      num, std::function<ValueId()>{createCountUpGenerator()});
                }));

        // Check, if every entry of the table was set and if the join columns
        // have the correct content.
        checkIfAllEntriesSet(result);
        std::ranges::for_each(
            joinColumns,
            [&result, &compareColumnsWithVectors](const size_t& num) {
              compareColumnsWithVectors(result, num,
                                        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
            });
      });

  // Simple test, if the function actually uses different generators, if told
  // to.
  result = createRandomlyFilledIdTable(
      10, 10,
      {{0, createCountUpGenerator()},
       {1, []() { return ad_utility::testing::VocabId(42); }}});
  checkIfAllEntriesSet(result);
  compareColumnsWithVectors(result, 0, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  compareColumnsWithVectors(result, 1,
                            {42, 42, 42, 42, 42, 42, 42, 42, 42, 42});
}
