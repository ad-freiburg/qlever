// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "../test/util/ValidatorHelpers.h"

#include "util/TypeTraits.h"

// ____________________________________________________________________________
template <
    ad_utility::SameAsAnyTypeIn<ad_utility::ConfigOption::AvailableTypes> Type>
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
Explicit instantiation for all types in
`ad_utility::ConfigOption::AvailableTypes`.
*/
template bool createDummyValueForValidator<bool>(size_t);
template std::string createDummyValueForValidator<std::string>(size_t);
template int createDummyValueForValidator<int>(size_t);
template size_t createDummyValueForValidator<size_t>(size_t);
template float createDummyValueForValidator<float>(size_t);
template std::vector<bool> createDummyValueForValidator<std::vector<bool>>(
    size_t);
template std::vector<std::string>
    createDummyValueForValidator<std::vector<std::string>>(size_t);
template std::vector<int> createDummyValueForValidator<std::vector<int>>(
    size_t);
template std::vector<size_t> createDummyValueForValidator<std::vector<size_t>>(
    size_t);
template std::vector<float> createDummyValueForValidator<std::vector<float>>(
    size_t);
