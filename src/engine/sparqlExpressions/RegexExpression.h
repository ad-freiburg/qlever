// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

// Make a standard SPARQL `REGEX` expression. The resulting expression always
// evaluates the actual regex (using Google's RE2 library). If the regex is a
// prefix regex (e.g. `^prefix` or `^prefix[0-9]`) on a plain variable, the
// expression additionally supports prefiltering via
// `getPrefilterExpressionForMetadata`, but each value that passes the prefilter
// is still checked against the actual regex.
SparqlExpression::Ptr makeRegexExpression(SparqlExpression::Ptr string,
                                          SparqlExpression::Ptr regex,
                                          SparqlExpression::Ptr flags);
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_REGEXEXPRESSION_H
