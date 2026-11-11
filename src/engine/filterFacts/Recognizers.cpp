// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/filterFacts/Recognizers.h"

#include <cmath>
#include <limits>

#include "util/Exception.h"

namespace filterFacts {

using valueIdComparators::Comparison;

// _____________________________________________________________________________
std::optional<DistanceBound> recognizeDistanceBound(const Conjunct& conjunct) {
  // Only a `DistanceComparison` bounds a distance, and only between two
  // different operands, at least one of which is a variable.
  const auto* comparison =
      std::get_if<DistanceComparison>(&conjunct.canonical_);
  if (comparison == nullptr) {
    return std::nullopt;
  }
  const Distance& distance = comparison->lhs_;
  if (distance.a_ == distance.b_ ||
      (!distance.a_.isVariable() && !distance.b_.isVariable())) {
    return std::nullopt;
  }

  // Return `std::nullopt` if the number is infinite or NaN (also after the
  // conversion to meters, e.g. for `< 1e306` in kilometers).
  if (!std::isfinite(comparison->rhs_)) {
    return std::nullopt;
  }

  // Compute the maximum distance (for `<`, the largest `double` below the
  // number), and return `std::nullopt` if it is negative.
  AD_CORRECTNESS_CHECK(comparison->op_ == Comparison::LT ||
                       comparison->op_ == Comparison::LE);
  double maxMeters = comparison->rhs_;
  if (comparison->op_ == Comparison::LT) {
    maxMeters =
        std::nextafter(maxMeters, -std::numeric_limits<double>::infinity());
  }
  if (maxMeters < 0) {
    return std::nullopt;
  }
  return DistanceBound{distance, maxMeters};
}

}  // namespace filterFacts
