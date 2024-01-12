// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <cstddef>
#include <sstream>
#include <type_traits>
#include <variant>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "util/TypeTraits.h"

/*
@brief Generate a dummy value of the given type. Used for generating test values
for `ad_benchmark::ResultTable`.
*/
template <
    ad_utility::SimilarToAnyTypeIn<ad_benchmark::ResultTable::EntryType> Type>
Type createDummyValueEntryType();

/*
@brief Call the lambda with each of the alternatives in
`ad_benchmark::ResultTable::EntryType`, except `std::monostate`, as template
parameter.

@tparam Function The loop body should be a templated function, with one
`typename` template argument and no more. It also shouldn't take any function
arguments. Should be passed per deduction.
*/
template <typename Function>
static void doForTypeInResultTableEntryType(Function function) {
  ad_utility::forEachTypeInTemplateType<ad_benchmark::ResultTable::EntryType>(
      [&function]<typename IndexType>() {
        // `std::monostate` is not important for these kinds of tests.
        if constexpr (!ad_utility::isSimilar<IndexType, std::monostate>) {
          function.template operator()<IndexType>();
        }
      });
}
