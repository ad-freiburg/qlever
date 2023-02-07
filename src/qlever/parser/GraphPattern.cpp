//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "qlever/parser/GraphPattern.h"

#include "qlever/parser/GraphPatternOperation.h"
#include "qlever/parser/ParsedQuery.h"

namespace parsedQuery {
GraphPattern::GraphPattern(GraphPattern&& other) = default;
GraphPattern::GraphPattern(const GraphPattern& other) = default;
GraphPattern& GraphPattern::operator=(const GraphPattern& other) = default;
GraphPattern& GraphPattern::operator=(GraphPattern&& other) noexcept = default;
GraphPattern::~GraphPattern() = default;
}  // namespace parsedQuery
