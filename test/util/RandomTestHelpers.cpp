// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "../test/util/RandomTestHelpers.h"

// ____________________________________________________________________________
ad_utility::RandomSeed RandomSeedGenerator::operator()() {
  return ad_utility::RandomSeed::make(std::invoke(numberGenerator_));
}
