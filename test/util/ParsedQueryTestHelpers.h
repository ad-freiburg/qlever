// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PARSEDQUERYTESTHELPERS_H
#define QLEVER_TEST_UTIL_PARSEDQUERYTESTHELPERS_H

#include <string>
#include <vector>

#include "parser/SparqlParser.h"

namespace ad_utility::testing {
// Return a pointer to a single, lazily-initialized `EncodedIriManager`
// instance shared across all tests.
inline const EncodedIriManager* encodedIriManager() {
  static EncodedIriManager instance;
  return &instance;
}

// Parse a SPARQL query string into a typed `ParsedQuery` object.
inline auto parseQuery(std::string query,
                       const std::vector<DatasetClause>& datasets = {}) {
  return SparqlParser::parseQuery(encodedIriManager(), std::move(query),
                                  datasets);
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_PARSEDQUERYTESTHELPERS_H
