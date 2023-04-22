// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"

namespace ad_benchmark {
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
  /*
  Get an entry from the table as a float. For easier usage.
  Note: This will cause an exception, if the row and column is bigger
  than the table. However, such a situation should only happen, if this
  function was called with the wrong column numbers, in which case it
  ain't our problem and can only be fixed by the user.
  */
  auto getEntryAsFloat = [&table](const size_t& row, const size_t& column) {
    return table->getEntry<float>(row, column);
  };

  // Go through every row.
  for (size_t row = 0; row < table->numRows(); row++) {
    const float speedUp = getEntryAsFloat(row, columnToCompareAgainst) /
                          getEntryAsFloat(row, columnToCalculateFor);

    table->setEntry(row, columnToPlaceResultIn, speedUp);
  }
}
}  // namespace ad_benchmark
