// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_DATA_LIMITOFFSETCLAUSE_H
#define QLEVER_SRC_PARSER_DATA_LIMITOFFSETCLAUSE_H

#include <cstdint>
#include <limits>
#include <optional>

#include "util/Exception.h"

// Represents the data returned by a limitOffsetClause.
struct LimitOffsetClause {
  std::optional<uint64_t> _limit = std::nullopt;
  uint64_t _offset = 0;
  std::optional<uint64_t> textLimit_ = std::nullopt;
  std::optional<uint64_t> exportLimit_ = std::nullopt;

  // If a limit is specified, return the limit, else return the maximal
  // representable limit.
  uint64_t limitOrDefault() const {
    return _limit.value_or(std::numeric_limits<uint64_t>::max());
  }
  uint64_t exportLimitOrDefault() const {
    return exportLimit_.value_or(std::numeric_limits<uint64_t>::max());
  }

  // Return the minimum of the offset and the `actualSize` of a query result.
  // That way, if the offset is too large, the result after applying it will be
  // empty, but there will be no out of bounds errors when using the result of
  // this function to resize a result.
  uint64_t actualOffset(uint64_t actualSize) const {
    return std::min(actualSize, _offset);
  }

  // Return the largest index into a table of size `actualSize` when applying
  // the limit and offset. When a limit and offset are specified and the table
  // is large enough, this is simply `limit + offset`. Otherwise, it is
  // appropriately clamped.
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

  // Return true iff there is neither a limit nor an offset clause.
  // Note: The `TEXTLIMIT` is ignored for this function, as it is irrelevant
  // almost always.
  bool isUnconstrained() const { return !_limit.has_value() && _offset == 0; }

  bool operator==(const LimitOffsetClause&) const = default;
};

#endif  // QLEVER_SRC_PARSER_DATA_LIMITOFFSETCLAUSE_H
