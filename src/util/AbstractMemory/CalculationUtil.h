// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <cmath>
#include <cstddef>

#include "util/ConstexprUtils.h"

// Just the number of bytes per memory unit.
constexpr size_t numberOfBytesPerKB = ad_utility::pow<size_t>(2, 10);
constexpr size_t numberOfBytesPerMB = ad_utility::pow<size_t>(2, 20);
constexpr size_t numberOfBytesPerGB = ad_utility::pow<size_t>(2, 30);
constexpr size_t numberOfBytesPerTB = ad_utility::pow<size_t>(2, 40);
constexpr size_t numberOfBytesPerPB = ad_utility::pow<size_t>(2, 50);

/*
@brief Calculate the amount of bytes for a given amount of untis and a given
amount of bytes per unit.

@return The amount of bytes. Rounded up, if needed.
*/
template <typename T>
requires std::integral<T> || std::floating_point<T>
size_t constexpr convertMemoryUnitsToBytes(const T& amountOfUnits,
                                           const size_t& numberOfBytesPerUnit) {
  if constexpr (std::is_floating_point_v<T>) {
    // We (maybe) have to round up.
    return static_cast<size_t>(std::ceil(amountOfUnits * numberOfBytesPerUnit));
  } else {
    AD_CORRECTNESS_CHECK(std::is_integral_v<T>);
    return amountOfUnits * numberOfBytesPerUnit;
  }
}
