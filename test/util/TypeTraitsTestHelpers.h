// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023,
// schlegea@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_TEST_UTIL_TYPETRAITSTESTHELPERS_H
#define QLEVER_TEST_UTIL_TYPETRAITSTESTHELPERS_H

#include "util/ConstexprUtils.h"

/*
@brief Call the given template function with the cartesian product of the
parameter type list with itself, as template parameters. For example: If given
`<int, const int>`, then the function will be called as `Func<int, int>`,
`Func<int, const int>`, `Func<const int, int>` and `Func<const int, const int>`.
*/
// TODO Why not replace `Func` with `auto`?
template <typename... Parameters>
constexpr void passCartesianPorductToLambda(auto&& func) {
  ad_utility::forEachTypeInParameterPackWithTI<Parameters...>([&func](auto t1) {
    ad_utility::forEachTypeInParameterPackWithTI<Parameters...>(
        [&t1, &func](auto t2) { func(t1, t2); });
  });
}

/*
@brief Call the given template function with every type in the parameter type
list as template parameter.
For example: If given `<int, const int>`, then the function will be called as
`func<int>` and `func<const int>`.
*/
template <typename... Parameters>
constexpr void passListOfTypesToLambda(auto&& func) {
  ad_utility::forEachTypeInParameterPackWithTI<Parameters...>(func);
}

#endif  // QLEVER_TEST_UTIL_TYPETRAITSTESTHELPERS_H
