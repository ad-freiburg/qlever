// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023,
// schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_TEST_UTIL_TYPETRAITSTESTHELPERS_H
#define QLEVER_TEST_UTIL_TYPETRAITSTESTHELPERS_H

/*
@brief Call the given template function with the cartesian product of the
parameter type list with itself, as template parameters. For example: If given
`<int, const int>`, then the function will be called as `Func<int, int>`,
`Func<int, const int>`, `Func<const int, int>` and `Func<const int, const int>`.
*/
// TODO Why not replace `Func` with `auto`?
#include <concepts>
template <typename Func, typename... Parameter>
constexpr void passCartesianPorductToLambda(Func func) {
  (
      [&func]<typename T>() {
        (func.template operator()<T, Parameter>(), ...);
      }.template operator()<Parameter>(),
      ...);
}

/*
@brief Call the given template function with every type in the parameter type
list as template parameter.
For example: If given `<int, const int>`, then the function will be called as
`func<int>` and `func<const int>`.
*/
template <typename... Parameters>
constexpr void passListOfTypesToLambda(auto func) {
  (func.template operator()<Parameters>(), ...);
}

#endif  // QLEVER_TEST_UTIL_TYPETRAITSTESTHELPERS_H
