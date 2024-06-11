// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <array>

#include "global/Id.h"

// Should we have an own class for this? We need this at several places.
// TODO<qup42> some matching comparison operators between
// idtriple/permutedtriple would be nice
using IdTriple = std::array<Id, 3>;
