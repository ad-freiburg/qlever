// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_TEST_UTIL_BENCHMARKMEASUREMENTCONTAINERHELPERS_H
#define QLEVER_TEST_UTIL_BENCHMARKMEASUREMENTCONTAINERHELPERS_H

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
CPP_template(typename Type)(requires ad_utility::SimilarToAnyTypeIn<
                            Type, ad_benchmark::ResultTable::EntryType>) Type
    createDummyValueEntryType();

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
  ad_utility::forEachTypeInTemplateTypeWithTI(
      ad_utility::use_type_identity::ti<ad_benchmark::ResultTable::EntryType>,
      [&function](auto t) {
        using IndexType = typename decltype(t)::type;
        // `std::monostate` is not important for these kinds of tests.
        if constexpr (!ad_utility::isSimilar<IndexType, std::monostate>) {
          function(ad_utility::use_type_identity::ti<IndexType>);
        }
      });
}

#endif  // QLEVER_TEST_UTIL_BENCHMARKMEASUREMENTCONTAINERHELPERS_H
