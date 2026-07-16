// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H

#include <optional>
#include <string>

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
// If (and only if) `regex` starts with `^`, return the longest literal prefix
// that every string matching `regex` is guaranteed to start with. This is used
// for prefiltering: a `REGEX` filter can never match a value that does not
// start with this prefix, so blocks that only contain values outside of the
// prefix range can be skipped. The actual regex is still evaluated afterwards.
//
// The prefix is returned without the leading `^` and with all escaping undone.
// Scanning stops at the first "special" regex construct (e.g. `.`, `[`, `(`, a
// quantifier, or a character-class escape like `\d`); such constructs only
// shorten the prefix, they no longer cause the whole optimization to be
// dropped. If the regex contains a top-level alternation (`|`) the prefix
// guarantee does not hold, so `std::nullopt` is returned. `std::nullopt` is
// also returned if `regex` does not start with `^` or the prefix would be
// empty.
//
// This function assumes that `regex` is a valid regex (which is enforced by
// `makeRegexExpression` before this function is called).
std::optional<std::string> getPrefixRegex(std::string regex);

// Make a standard SPARQL `REGEX` expression. The resulting expression always
// evaluates the actual regex (using Google's RE2 library). If the regex is a
// prefix regex (e.g. `^prefix` or `^prefix[0-9]`) on a plain variable, the
// expression additionally supports prefiltering via
// `getPrefilterExpressionForMetadata`.
SparqlExpression::Ptr makeRegexExpression(SparqlExpression::Ptr string,
                                          SparqlExpression::Ptr regex,
                                          SparqlExpression::Ptr flags);
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H
