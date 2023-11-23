// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
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
template <typename Type>
requires ad_utility::isTypeContainedIn<Type, ResultTable::EntryType>
struct ColumnNumWithType {
  using ColumnType = Type;
  const size_t columnNum_;
};

template <typename ColumnReturnType, typename... ColumnInputType>
requires ad_utility::isInstantiation<ColumnReturnType, ColumnNumWithType> &&
         (ad_utility::isInstantiation<ColumnInputType, ColumnNumWithType> &&
          ...)
void generateColumnWithColumnInput(
    ResultTable* const table,
    ad_utility::InvocableWithSimilarReturnType<
        typename ColumnReturnType::ColumnType,
        typename ColumnInputType::ColumnType...> auto&& generator,
    const ColumnReturnType& columnToPutResultIn,
    const ColumnInputType&... inputColumns);
}  // namespace ad_benchmark
