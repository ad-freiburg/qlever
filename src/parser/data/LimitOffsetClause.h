//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include <cstdint>
#include <limits>

// Represents the data returned by a limitOffsetClause.
struct LimitOffsetClause {
  uint64_t _limit = std::numeric_limits<uint64_t>::max();
  uint64_t _textLimit = 1;
  uint64_t _offset = 0;

  bool operator==(const LimitOffsetClause&) const = default;
};
