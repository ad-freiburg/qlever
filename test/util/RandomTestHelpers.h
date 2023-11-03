// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once
#include <algorithm>
#include <array>
#include <cstddef>
#include <random>

#include "util/Random.h"

// Create a array of random seeds for use with random number generators.
template <size_t NumSeeds>
inline std::array<ad_utility::RandomSeed, NumSeeds> createArrayOfRandomSeeds(
    ad_utility::RandomSeed seed =
        ad_utility::RandomSeed::make(std::random_device{}())) {
  ad_utility::FastRandomIntGenerator<unsigned int> generator{std::move(seed)};
  std::array<ad_utility::RandomSeed, NumSeeds> seeds{};
  std::ranges::generate(seeds, [&generator]() {
    return ad_utility::RandomSeed::make(std::invoke(generator));
  });
  return seeds;
}
