// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

/*
For doing a column based operations, that is, on all the entries.
For example: Adding two columns together, calculating speed up between the
entries of two columns, etc.
*/
namespace ad_benchmark {

/*
Column number together with the type of value, that can be found inside the
column. Note, that **all** entries in the column must have the same type,
because of `ResultTable::getEntry`.
*/
template <ad_utility::SameAsAnyTypeIn<ResultTable::EntryType> Type>
struct ColumnNumWithType {
  using ColumnType = Type;
  const size_t columnNum_;
};

template <typename ColumnReturnType, typename... ColumnInputTypes>
requires(sizeof...(ColumnInputTypes) > 0) void generateColumnWithColumnInput(
    ResultTable* const table,
    ad_utility::InvocableWithSimilarReturnType<
        ColumnReturnType, const ColumnInputTypes&...> auto&& generator,
    const ColumnNumWithType<ColumnReturnType>& columnToPutResultIn,
    const ColumnNumWithType<ColumnInputTypes>&... inputColumns) {
  // Using a column more than once is the sign of an error.
  std::array<size_t, sizeof...(ColumnInputTypes)> allColumnNums{
      {inputColumns.columnNum_...}};
  std::ranges::sort(allColumnNums);
  AD_CONTRACT_CHECK(std::ranges::adjacent_find(allColumnNums) ==
                    allColumnNums.end());

  // Fill the result column.
  for (size_t row = 0; row < table->numRows(); row++) {
    table->setEntry(
        row, columnToPutResultIn.columnNum_,
        std::invoke(generator, table->getEntry<ColumnInputTypes>(
                                   row, inputColumns.columnNum_)...));
  }
}

/*
@brief Vector addition with `ResultTable` columns.
*/
template <typename ColumnReturnType,
          std::same_as<ColumnNumWithType<ColumnReturnType>>... ColumnInputTypes>
requires(sizeof...(ColumnInputTypes) > 1) void sumUpColumns(
    ResultTable* const table,
    const ColumnNumWithType<ColumnReturnType>& columnToPutResultIn,
    const ColumnInputTypes&... columnsToSumUp) {
  // We can simply pass this to `generateColumnWithColumnInput`.
  generateColumnWithColumnInput(
      table,
      [](const ColumnInputTypes::ColumnType&... values) -> ColumnReturnType {
        return (values + ...);
      },
      columnToPutResultIn, columnsToSumUp...);
}

/*
@brief Reads two floating point columns, calculates the relativ speedup between
their entries and writes it in a third column.

@param columnToCalculateFor, columnToCompareAgainst The columns, with which
the question "How much faster than the entries of `columnToCompareAgainst`
are the entires of `columnToCalculateFor`?".
@param columnToPlaceResultIn This is where the speedup calculation results
will be placed in.
*/
inline void calculateSpeedupOfColumn(
    ResultTable* const table,
    const ColumnNumWithType<float>& columnToPlaceResultIn,
    const ColumnNumWithType<float>& columnToCalculateFor,
    const ColumnNumWithType<float>& columnToCompareAgainst) {
  // We can simply pass this to `generateColumnWithColumnInput`.
  generateColumnWithColumnInput(
      table,
      [](const float compareAgainst, const float calculateFor) -> float {
        /*
        Speedup calculations only makes sense, if ALL values are bigger than
        0.
        */
        AD_CONTRACT_CHECK(compareAgainst > 0.f && calculateFor > 0.f);
        return compareAgainst / calculateFor;
      },
      columnToPlaceResultIn, columnToCompareAgainst, columnToCalculateFor);
}
}  // namespace ad_benchmark
