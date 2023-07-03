// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (Januar of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <sstream>
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

/*
 * @brief Sorts an IdTable in place, in the same way, that we sort them during
 * normal programm usage.
 */
inline void sortIdTableByJoinColumnInPlace(IdTableAndJoinColumn& table) {
  CALL_FIXED_SIZE((std::array{table.idTable.numColumns()}), &Engine::sort,
                  &table.idTable, table.joinColumn);
}
