//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include <cstdint>
#include <limits>
#include <optional>

#include "global/Constants.h"
#include "util/Exception.h"

// Represents the data returned by a limitOffsetClause.
struct LimitOffsetClause {
  std::optional<uint64_t> _limit;
  uint64_t _textLimit = TEXT_LIMIT_DEFAULT;
  uint64_t _offset = 0;

  // If a limit is specified, return the limit, else return the maximal
  // representable limit.
  uint64_t limitOrDefault() const {
    return _limit.value_or(std::numeric_limits<uint64_t>::max());
  }

  // Return the minimum of the offset and the `actualSize` of a query result.
  // That way, if the offset is too large, the result after applying it will be
  // empty, but there will be no out of bounds errors when using the result of
  // this function to resize a result.
  uint64_t actualOffset(uint64_t actualSize) const {
    return std::min(actualSize, _offset);
  }

  // When applying the limit and offset to a table of `actualSize`, what is the
  // actual upper bound (as an index into the table) for the resulting elements.
  // In the most simple case this is `limit + offset`, but this function handles
  // all possible overflows. The result will always be `<= actualSize`.
  uint64_t upperBound(uint64_t actualSize) const {
    auto val = limitOrDefault() + _offset;
    val = val >= std::max(limitOrDefault(), _offset)
              ? val
              : std::numeric_limits<uint64_t>::max();
    return std::min(val, actualSize);
  }

  // Return the resulting number of elements when applying the limit and offset
  // to a table of `actualSize`. This is exactly `upperBound(actualSize) -
  // actualOffset(actualSize)`.
  uint64_t actualSize(uint64_t actualSize) const {
    auto upper = upperBound(actualSize);
    auto offset = actualOffset(actualSize);
    AD_CORRECTNESS_CHECK(upper >= offset);
    return upper - offset;
  }

  bool operator==(const LimitOffsetClause&) const = default;
};
