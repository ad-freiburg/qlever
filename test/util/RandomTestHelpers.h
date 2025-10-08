// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023, schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_TEST_UTIL_RANDOMTESTHELPERS_H
#define QLEVER_TEST_UTIL_RANDOMTESTHELPERS_H

#include <algorithm>
#include <array>
#include <cstddef>
#include <random>

#include "backports/functional.h"
#include "util/Random.h"

// A simple pseudo random generator for instances of `ad_utility::RandomSeed`.
class RandomSeedGenerator {
  // For generating the number, that will be transformed into a
  // `ad_utility::RandomSeed`.
  ad_utility::FastRandomIntGenerator<unsigned int> numberGenerator_;

 public:
  explicit RandomSeedGenerator(
      ad_utility::RandomSeed seed =
          ad_utility::RandomSeed::make(std::random_device{}()))
      : numberGenerator_{ad_utility::FastRandomIntGenerator<unsigned int>{
            std::move(seed)}} {}

  // Generate a new `ad_utility::RandomSeed`.
  ad_utility::RandomSeed operator()();
};

// Create a array of random seeds for use with random number generators.
template <size_t NumSeeds>
inline std::array<ad_utility::RandomSeed, NumSeeds> createArrayOfRandomSeeds(
    ad_utility::RandomSeed seed =
        ad_utility::RandomSeed::make(std::random_device{}())) {
  RandomSeedGenerator generator{std::move(seed)};
  std::array<ad_utility::RandomSeed, NumSeeds> seeds{};
  ql::ranges::generate(seeds,
                       [&generator]() { return std::invoke(generator); });
  return seeds;
}

#endif  // QLEVER_TEST_UTIL_RANDOMTESTHELPERS_H
