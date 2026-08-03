// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tomas Damek <tomas.damek@email.uni-freiburg.de>

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

// Help function that parses a SPARL query string into a typed `ParsedQuery`
// object.
inline auto parseQuery(std::string query,
                       const std::vector<DatasetClause>& datasets = {}) {
  return SparqlParser::parseQuery(encodedIriManager(), std::move(query),
                                  datasets);
}

}  // namespace ad_utility::testing

#endif
