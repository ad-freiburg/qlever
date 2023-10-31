// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once
#include <algorithm>
#include <array>
#include <cstddef>
#include <random>

#include "util/Random.h"

// Create a array of non-deterministic random seeds for use with random number
// generators.
template <size_t NumSeeds>
static std::array<Seed, NumSeeds> createArrayOfRandomSeeds() {
  std::array<Seed, NumSeeds> seeds{};
  std::ranges::generate(seeds,
                        []() { return Seed::make(std::random_device{}()); });
  return seeds;
}
