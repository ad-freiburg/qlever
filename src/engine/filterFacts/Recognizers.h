// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_FILTERFACTS_RECOGNIZERS_H
#define QLEVER_SRC_ENGINE_FILTERFACTS_RECOGNIZERS_H

#include <optional>

#include "engine/filterFacts/Canonicalizer.h"

// The recognition of typed facts in the canonical conjuncts of a FILTER (see
// `Canonicalizer.h`). A fact holds iff its conjunct holds (up to the rounding
// of the conversion of units, see `canonicalize`), so a rewrite of the query
// may replace the conjunct by an operation that implements the fact.
namespace filterFacts {

// The `distance_` (between two different operands, at least one of them a
// variable) is at most `maxMeters_`.
struct DistanceBound {
  Distance distance_;
  double maxMeters_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(DistanceBound, distance_, maxMeters_)
};

// Recognize `distance(a, b) <= c` and `distance(a, b) < c` for a finite number
// `c` and a maximum distance that is not negative. For `<`, the maximum
// distance is the largest `double` below `c` (because a `SpatialJoin` includes
// the pairs at exactly its maximum distance).
std::optional<DistanceBound> recognizeDistanceBound(const Conjunct& conjunct);

}  // namespace filterFacts

#endif  // QLEVER_SRC_ENGINE_FILTERFACTS_RECOGNIZERS_H
