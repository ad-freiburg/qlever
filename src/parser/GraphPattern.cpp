//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/GraphPattern.h"

// These includes are required because of the forward declaration of
// `GraphPatternOperation` in `GraphPattern.h` and its subsequent usage inside
// `std::vector`.
#include "parser/GraphPatternOperation.h"
#include "parser/ParsedQuery.h"

namespace parsedQuery {
GraphPattern::GraphPattern(GraphPattern&& other) noexcept = default;
GraphPattern::GraphPattern(const GraphPattern& other) = default;
GraphPattern& GraphPattern::operator=(const GraphPattern& other) = default;
GraphPattern& GraphPattern::operator=(GraphPattern&& other) noexcept = default;
GraphPattern::~GraphPattern() = default;
}  // namespace parsedQuery
