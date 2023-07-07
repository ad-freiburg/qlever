// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (Januar of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <tuple>

#include "./AllocatorTestHelpers.h"
#include "./GTestHelpers.h"
#include "./IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/Algorithm.h"
#include "util/Forward.h"
#include "util/Random.h"

/*
 * Does what it says on the tin: Save an IdTable with the corresponding
 * join column.
 */
struct IdTableAndJoinColumn {
  IdTable idTable;
  size_t joinColumn;
};

// For easier reading. We repeat that type combination so often, that this
// will make things a lot easier in terms of reading and writing.
using IntOrId = std::variant<int64_t, Id>;
using VectorTable = std::vector<std::vector<IntOrId>>;

/*
 * Return an 'IdTable' with the given `content` by applying the
 * `transformation` to each of them. All rows of `content` must have the
 * same length.
 */
template <typename Transformation = decltype(ad_utility::testing::VocabId)>
inline IdTable makeIdTableFromVector(const VectorTable& content,
                                     Transformation transformation = {}) {
  size_t numCols = content.empty() ? 0UL : content.at(0).size();
  IdTable result{numCols, ad_utility::testing::makeAllocator()};
  result.reserve(content.size());
  for (const auto& row : content) {
    AD_CONTRACT_CHECK(row.size() == result.numColumns());
    result.emplace_back();
    for (size_t i = 0; i < result.numColumns(); ++i) {
      if (std::holds_alternative<Id>(row.at(i))) {
        result.back()[i] = std::get<Id>(row.at(i));
      } else {
        result.back()[i] = transformation(std::get<int64_t>(row.at(i)));
      }
    }
  }
  return result;
}

/*
 * @brief Tests, whether the given IdTable has the same content as the sample
 * solution and, if the option was choosen, if the IdTable is sorted by
 * the join column.
 *
 * @param table The IdTable that should be tested.
 * @param expectedContent The sample solution. Doesn't need to be sorted,
 *  or the same order of rows as the table.
 * @param resultMustBeSortedByJoinColumn If this is true, it will also be
 * tested, if the table is sorted by the join column.
 * @param joinColumn The join column of the table.
 * @param l Ignore it. It's only here for being able to make better messages,
 *  if a IdTable fails the comparison.
 */
inline void compareIdTableWithExpectedContent(
    const IdTable& table, const IdTable& expectedContent,
    const bool resultMustBeSortedByJoinColumn = false,
    const size_t joinColumn = 0,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating more informative messages, when failing the comparison.
  std::stringstream traceMessage{};

  auto writeIdTableToStream = [&traceMessage](const IdTable& idTable) {
    std::ranges::for_each(idTable,
                          [&traceMessage](const auto& row) {
                            // TODO<C++23> Use std::views::join_with for both
                            // loops.
                            for (size_t i = 0; i < row.numColumns(); i++) {
                              traceMessage << row[i] << " ";
                            }
                            traceMessage << "\n";
                          },
                          {});
  };

  traceMessage << "compareIdTableWithExpectedContent comparing IdTable\n";
  writeIdTableToStream(table);
  traceMessage << "with IdTable \n";
  writeIdTableToStream(expectedContent);
  auto trace{generateLocationTrace(l, traceMessage.str())};

  // Because we compare tables later by sorting them, so that every table has
  // one definit form, we need to create local copies.
  IdTable localTable{table.clone()};
  IdTable localExpectedContent{expectedContent.clone()};

  if (resultMustBeSortedByJoinColumn) {
    // Is the table sorted by join column?
    ASSERT_TRUE(std::ranges::is_sorted(localTable.getColumn(joinColumn)));
  }

  // Sort both the table and the expectedContent, so that both have a definite
  // form for comparison.
  std::ranges::sort(localTable, std::ranges::lexicographical_compare);
  std::ranges::sort(localExpectedContent, std::ranges::lexicographical_compare);

  ASSERT_EQ(localTable, localExpectedContent);
}

/*
 * @brief Sorts an IdTable in place, in the same way, that we sort them during
 * normal programm usage.
 */
inline void sortIdTableByJoinColumnInPlace(IdTableAndJoinColumn& table) {
  CALL_FIXED_SIZE((std::array{table.idTable.numColumns()}), &Engine::sort,
                  &table.idTable, table.joinColumn);
}

/*
@brief Creates a `IdTable`, where the rows are created via generator.

@param numberRows numberColumns The number of rows and columns, the table should
have.
@param rowGenerator Creates the rows for the to be returned `IdTable`. The
generated row must ALWAYS have size `numberColumns`. Otherwise an exception will
be thrown.
*/
inline IdTable generateIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::function<IdTable::row_type()>& rowGenerator) {
  AD_CONTRACT_CHECK(numberRows > 0 && numberColumns > 0);

  // Creating the table and setting it to the wanted size.
  IdTable table{numberColumns, ad_utility::testing::makeAllocator()};
  table.resize(numberRows);

  // Fill the table.
  std::ranges::generate(table, [&numberColumns, &rowGenerator]() {
    // Make sure, that the generated row has the right size, before passing it
    // on.
    IdTable::row_type row = rowGenerator();

    if (row.numColumns() != numberColumns) {
      throw std::runtime_error(
          "The row generator generated a IdTable::row_type, which didn't have "
          "the wanted size.");
    }

    return row;
  });

  return table;
}

/*
@brief Creates a `IdTable`, where the content of the join column is given via
functions and all other columns are randomly filled with numbers.

@param numberRows numberColumns The number of rows and columns, the table should
have.
@param joinColumnWithGenerator Every pair describes the position of a join
column and the function, which will be called, to generate it's entries.
*/
inline IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<std::pair<size_t, std::function<ValueId()>>>&
        joinColumnWithGenerator) {
  AD_CONTRACT_CHECK(numberRows > 0 && numberColumns > 0);

  // Views for clearer access.
  auto joinColumnNumberView = std::views::keys(joinColumnWithGenerator);
  auto joinColumnGeneratorView = std::views::values(joinColumnWithGenerator);

  // Are all the join column numbers within the max column number?
  if (std::ranges::any_of(joinColumnNumberView,
                          [&numberColumns](const size_t& num) {
                            return num >= numberColumns;
                          })) {
    throw std::runtime_error(
        "At least one of the join columns is out of bounds.");
  }

  // Are there no duplicates in the join column numbers?
  for (auto it = joinColumnNumberView.begin(); it != joinColumnNumberView.end();
       it++) {
    if (std::ranges::any_of(it + 1, joinColumnNumberView.end(),
                            [&it](const size_t& num) { return *it == num; })) {
      throw std::runtime_error(
          "A join column got at least two generator assigned to it.");
    }
  }

  // Are all the functions for generating join column entries not nullptr?
  if (std::ranges::any_of(joinColumnGeneratorView,
                          [](auto func) { return func == nullptr; })) {
    throw std::runtime_error(
        "At least one of the generator function pointer was a nullptr.");
  }

  // The random number generators for normal entries.
  SlowRandomIntGenerator<size_t> randomNumberGenerator(0, ValueId::maxIndex);
  std::function<ValueId()> normalEntryGenerator = [&randomNumberGenerator]() {
    // `IdTable`s don't take raw numbers, you have to transform them first.
    return ad_utility::testing::VocabId(randomNumberGenerator());
  };

  // Assiging the column number to a generator function.
  std::vector<const std::function<ValueId()>*> columnToGenerator(
      numberColumns, &normalEntryGenerator);
  std::ranges::for_each(joinColumnWithGenerator,
                        [&columnToGenerator](auto& pair) {
                          columnToGenerator.at(pair.first) = &pair.second;
                        });

  // Creating the table.
  return generateIdTable(
      numberRows, numberColumns, [&numberColumns, &columnToGenerator]() {
        IdTable::row_type row(numberColumns);

        // Filling the row.
        for (size_t column = 0; column < numberColumns; column++) {
          row[column] = (*columnToGenerator.at(column))();
        }

        return row;
      });
}

/*
@brief Creates a `IdTable`, where the content of the join columns is given via
a function and all other columns are randomly filled with numbers.

@param numberRows numberColumns The number of rows and columns, the table should
have.
@param joinColumns The join columns.
@param generator The generator for the join columns. Order of calls: Row per
row, starting from row 0, and in a row for every join column, with the join
columns ordered by their column. Starting from column 0.
*/
inline IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<size_t>& joinColumns,
    const std::function<ValueId()>& generator) {
  // Is the generator not empty?
  if (generator == nullptr) {
    throw std::runtime_error("The generator function was empty.");
  }

  // Creating the table.
  return createRandomlyFilledIdTable(
      numberRows, numberColumns,
      ad_utility::transform(joinColumns, [&generator](const size_t& num) {
        /*
        Simply passing `generator` doesn't work, because it would be copied,
        which would lead to different behavior. After all, the columns would no
        longer share a function with one internal state, but each have their own
        separate function with its own internal state.
        */
        return std::make_pair(
            num, std::function{[&generator]() { return generator(); }});
      }));
}

/*
 * @brief Return a IdTable, that is randomly filled. The range of numbers
 *  being entered in the join column can be defined.
 *
 * @param numberRows, numberColumns The size of the IdTable, that is to be
 *  returned.
 * @param joinColumn The joinColumn of the IdTable, that is to be returned.
 * @param joinColumnLowerBound, joinColumnUpperBound The range of the entries
 *  in the join column, definied as
 *  [joinColumnLowerBound, joinColumnUpperBound].
 */
inline IdTable createRandomlyFilledIdTable(const size_t numberRows,
                                           const size_t numberColumns,
                                           const size_t joinColumn,
                                           const size_t joinColumnLowerBound,
                                           const size_t joinColumnUpperBound) {
  // Entries in IdTables have a max size.
  constexpr size_t maxIdSize = ValueId::maxIndex;

  // Is the lower bound smaller, or equal, to the upper bound?
  AD_CONTRACT_CHECK(joinColumnLowerBound <= joinColumnUpperBound);
  // Is the upper bound smaller, or equal, to the maximum size of an IdTable
  // entry?
  AD_CONTRACT_CHECK(joinColumnUpperBound <= maxIdSize);

  // The random number generators for normal entries and join column entries.
  // Both can be found in util/Random.h
  SlowRandomIntGenerator<size_t> normalEntryGenerator(
      0, maxIdSize);  // Entries in IdTables have a max size.
  SlowRandomIntGenerator<size_t> joinColumnEntryGenerator(joinColumnLowerBound,
                                                          joinColumnUpperBound);

  // Creating the table and setting it to the wanted size.
  IdTable table{numberColumns, ad_utility::testing::makeAllocator()};
  table.resize(numberRows);

  // `IdTable`s don't take raw numbers, you have to transform them first.
  auto transform = ad_utility::testing::VocabId;

  // Fill table with random content.
  for (size_t row = 0; row < numberRows; row++) {
    for (size_t i = 0; i < joinColumn; i++) {
      table[row][i] = transform(normalEntryGenerator());
    }
    table[row][joinColumn] = transform(joinColumnEntryGenerator());
    for (size_t i = joinColumn + 1; i < numberColumns; i++) {
      table[row][i] = transform(normalEntryGenerator());
    }
  }

  return table;
}
