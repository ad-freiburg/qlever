//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/GraphPattern.h"

// These includes are required because of the forward declarations of
// `GraphPatternOperation` in `GraphPattern.h` and of `SparqlTriple` in
// `GraphPatternOperation.h`. Both types are stored inside a `std::vector`, so
// they must be complete for the defaulted special member functions below.
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"

namespace parsedQuery {
GraphPattern::GraphPattern(GraphPattern&& other) noexcept = default;
GraphPattern::GraphPattern(const GraphPattern& other) = default;
GraphPattern& GraphPattern::operator=(const GraphPattern& other) = default;
GraphPattern& GraphPattern::operator=(GraphPattern&& other) noexcept = default;
GraphPattern::~GraphPattern() = default;
}  // namespace parsedQuery
