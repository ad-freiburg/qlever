// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PARSEQUERYTESTHELPERS_H
#define QLEVER_TEST_UTIL_PARSEQUERYTESTHELPERS_H

#include <string>
#include <vector>

#include "parser/SparqlParser.h"

namespace ad_utility::testing {
using namespace ad_utility::url_parser::sparqlOperation;

// Create a shared `EncodedIriManager` instance used for testing.
inline const EncodedIriManager* encodedIriManager() {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
}

// Helper function that parses a SPARL query string into a typed `ParsedQuery`
// object.
inline auto parseQuery(std::string query,
                       const std::vector<DatasetClause>& datasets = {}) {
  return SparqlParser::parseQuery(encodedIriManager(), std::move(query),
                                  datasets);
}

}  // namespace ad_utility::testing

#endif
