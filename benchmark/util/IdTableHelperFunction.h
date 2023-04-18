// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <algorithm>
#include <cstdio>

#include "../test/util/AllocatorTestHelpers.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Random.h"

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
IdTable createRandomlyFilledIdTable(const size_t numberRows,
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
void sortIdTableByJoinColumnInPlace(IdTableAndJoinColumn& table) {
  CALL_FIXED_SIZE((std::array{table.idTable.numColumns()}), &Engine::sort,
                  &table.idTable, table.joinColumn);
}
