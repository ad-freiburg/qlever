// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once
#include <concepts>
#include <cstddef>
#include <sstream>
#include <type_traits>

#include "util/ConfigManager/ValidatorConcept.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

/*
@brief Generate a value of the given type. Used for generating test values in
cooperation with `generateSingleParameterValidatorFunction`, while keeping the
invariant of `generateValidatorFunction` true.
`variant` slightly changes the returned value.
*/
template <typename Type>
Type createDummyValueForValidator(size_t variant) {
  if constexpr (std::is_same_v<Type, bool>) {
    return variant % 2;
  } else if constexpr (std::is_same_v<Type, std::string>) {
    // Create a string counting up to `variant`.
    std::ostringstream stream;
    for (size_t i = 0; i <= variant; i++) {
      stream << i;
    }
    return stream.str();
  } else if constexpr (std::is_same_v<Type, int> ||
                       std::is_same_v<Type, size_t>) {
    // Return uneven numbers.
    return static_cast<Type>(variant) * 2 + 1;
  } else if constexpr (std::is_same_v<Type, float>) {
    return 43.70137416518735163649172636491957f * static_cast<Type>(variant);
  } else {
    // Must be a vector and we can go recursive.
    AD_CORRECTNESS_CHECK(ad_utility::isVector<Type>);
    Type vec;
    vec.reserve(variant + 1);
    for (size_t i = 0; i <= variant; i++) {
      vec.push_back(createDummyValueForValidator<typename Type::value_type>(i));
    }
    return vec;
  }
};

/*
@brief For easily creating single parameter validator functions, that compare
given values to values created using the `createDummyValueForValidator`
function.

The following invariant should always be true, except for `bool`: A validator
function with `variant` number $x$ returns false, when given
`createDummyValue(x)`. Otherwise, it always returns true.

@tparam ParameterType The parameter type for the parameter of the
function.

@param variant Changes the generated function slightly. Allows the easier
creation of multiple different validator functions. For more information,
what the exact difference is, see the code.
*/
template <typename ParameterType>
auto generateSingleParameterValidatorFunction(size_t variant) {
  if constexpr (std::is_same_v<ParameterType, bool> ||
                std::is_same_v<ParameterType, std::string> ||
                std::is_same_v<ParameterType, int> ||
                std::is_same_v<ParameterType, size_t> ||
                std::is_same_v<ParameterType, float>) {
    return [compareTo = createDummyValueForValidator<ParameterType>(variant)](
               const ParameterType& n) { return n != compareTo; };
  } else {
    // Must be a vector and we can go recursive.
    AD_CORRECTNESS_CHECK(ad_utility::isVector<ParameterType>);

    // Just call the validator function for the non-vector version of
    // the types on the elements of the vector.
    return [variant](const ParameterType& v) {
      if (v.size() != variant + 1) {
        return true;
      }

      for (size_t i = 0; i < variant + 1; i++) {
        if (generateSingleParameterValidatorFunction<
                typename ParameterType::value_type>(i)(v.at(i))) {
          return true;
        }
      }

      return false;
    };
  }
};
