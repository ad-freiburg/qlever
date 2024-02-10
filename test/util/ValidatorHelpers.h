// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <cstddef>
#include <sstream>
#include <type_traits>

#include "util/ConfigManager/ConfigOption.h"
#include "util/TypeTraits.h"

/*
@brief Generate a value of the given type. Used for generating test values in
cooperation with `generateSingleParameterValidatorFunction`, while keeping the
invariant of `generateValidatorFunction` true.
`variant` slightly changes the returned value.
*/
template <
    ad_utility::SameAsAnyTypeIn<ad_utility::ConfigOption::AvailableTypes> Type>
Type createDummyValueForValidator(size_t variant);

/*
@brief For easily creating `Validator` functions, that compare given values to
values created using the `createDummyValueForValidator` function.

The following invariant should always be true, except for when `bool` is one of
the types in `ParameterTypes`: A validator function, that was generated with
`variant` number $x$, returns false, when given
`createDummyValueForValidator<ParameterTypes>(x)...`. Otherwise, it always
returns true.

Special behavior for `bool` types: Because we only have 2 values for `bool`, I
decided, that the comparison for any `bool` argument `b` is always `b == false`.
In other words, when only looking at the `bool` arguments given to the generated
validator, than the given arguments are valid, as long as they are all `false`.

@tparam ParameterType The parameter type for the parameter of the
function.

@param variant Changes the generated function slightly. Allows the easier
creation of multiple different validator functions. For more information,
what the exact difference is, see the code in `createDummyValueForValidator`.
*/
template <typename... ParameterTypes>
requires((ad_utility::SameAsAnyTypeIn<
             ParameterTypes, ad_utility::ConfigOption::AvailableTypes>) &&
         ...)
auto generateDummyNonExceptionValidatorFunction(size_t variant) {
  return [... dummyValuesToCompareTo =
              createDummyValueForValidator<ParameterTypes>(variant)](
             const ParameterTypes&... args) {
    // Special handeling for `args` of type bool is needed. For the reasoning:
    // See the doc string.
    auto compare = []<typename T>(const T& arg,
                                  const T& dummyValueToCompareTo) {
      if constexpr (std::is_same_v<T, bool>) {
        return arg == false;
      } else {
        return arg != dummyValueToCompareTo;
      }
    };
    return (compare(args, dummyValuesToCompareTo) || ...);
  };
};
