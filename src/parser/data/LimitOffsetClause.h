//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include <cstdint>
#include <limits>

// Represents the data returned by a limitOffsetClause.
struct LimitOffsetClause {
  std::optional<uint64_t> _limit;
  uint64_t _textLimit = 1;
  uint64_t _offset = 0;

  // If a limit is specified, return the limit, else return the maximal
  // representable limit.
  uint64_t limitOrDefault() const {
    return _limit.value_or(std::numeric_limits<uint64_t>::max());
  }

  uint64_t upperBound(uint64_t inputSize) const {
    auto val = limitOrDefault() + _offset;
    val = val >= std::max(limitOrDefault(), _offset)
              ? val
              : std::numeric_limits<uint64_t>::max();
    return std::min(val, inputSize);
  }

  uint64_t actualOffset(uint64_t inputSize) const {
    return std::min(inputSize, _offset);
  }

  uint64_t actualSize(uint64_t inputSize) const {
    auto upper = upperBound(inputSize);
    auto offset = actualOffset(inputSize);
    AD_CORRECTNESS_CHECK(upper >= offset);
    return upper - offset;
  }

  bool operator==(const LimitOffsetClause&) const = default;
};
