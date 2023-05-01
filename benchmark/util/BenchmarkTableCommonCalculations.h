// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <type_traits>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"

namespace ad_benchmark {

// Helper function, because this one line call happens very often in this file.
static float getTableEntryAsFloat(ResultTable* table, const size_t& row,
                                  const size_t& column) {
  /*
  This will cause an exception, if the row and column is bigger
  than the table. However, such a situation should only happen, if one of the
  function using this helper got the wrong arguments to begin with. In which
  case, it can only really be fixed by the user.
  */
  return table->getEntry<float>(row, column);
}

/*
@brief Reads two columns, calculates the relativ speedup between their entries
and writes it in a third column.

@param table The result table, in which columns those actions will take place.
@param columnToCalculateFor, columnToCompareAgainst The columns, with which
the question "How much faster than the entries of `columnToCompareAgainst`
are the entires of `columnToCalculateFor`?".
@param columnToPlaceResultIn This is where the speedup calculation results
will be placed in.
*/
void calculateSpeedupOfColumn(ResultTable* table,
                              const size_t& columnToCalculateFor,
                              const size_t& columnToCompareAgainst,
                              const size_t& columnToPlaceResultIn) {
  // Go through every row.
  for (size_t row = 0; row < table->numRows(); row++) {
    const float speedUp =
        getTableEntryAsFloat(table, row, columnToCompareAgainst) /
        getTableEntryAsFloat(table, row, columnToCalculateFor);

    table->setEntry(row, columnToPlaceResultIn, speedUp);
  }
}

template <typename Type, typename... Ts>
concept AllTheSameType = (std::is_same_v<Type, Ts> && ...);

/*
@brief Adds multiple columns together and writes the result in a designated
column.

@tparam ColumnNumbers Must all be `size_t`.

@param table The `ResultTable` to do this in.
@param columnToPlaceResultIn Where to place the results.
@param columnToSumUp All the columns, who shall be added up.
*/
template <typename... ColumnNumbers>
requires AllTheSameType<size_t, ColumnNumbers...>
void sumUpColumns(ResultTable* table, const size_t& columnToPlaceResultIn,
                  const ColumnNumbers&... columnToSumUp) {
  // Go through every row.
  for (size_t row = 0; row < table->numRows(); row++) {
    table->setEntry(row, columnToPlaceResultIn,
                    (getTableEntryAsFloat(table, row, columnToSumUp) + ...));
  }
}
}  // namespace ad_benchmark
