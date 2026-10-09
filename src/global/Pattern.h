// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//          Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_GLOBAL_PATTERN_H
#define QLEVER_SRC_GLOBAL_PATTERN_H

#include <cstdint>
#include <limits>
#include <vector>

#include "global/Id.h"

/**
 * @brief This represents a set of relations of a single entity.
 *        (e.g. a set of books that all have an author and a title).
 *        This information can then be used to efficiently count the relations
 *        that a set of entities has (e.g. for autocompletion of relations
 *        while writing a query).
 */
struct Pattern : std::vector<Id> {
  using PatternId = int32_t;
  static constexpr PatternId NoPattern = std::numeric_limits<PatternId>::max();
};

#endif  // QLEVER_SRC_GLOBAL_PATTERN_H
