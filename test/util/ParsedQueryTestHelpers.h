#ifndef QLEVER_TEST_UTIL_PARSEQUERYTESTHELPERS_H
#define QLEVER_TEST_UTIL_PARSEQUERYTESTHELPERS_H

#include <string>
#include <vector>

#include "parser/SparqlParser.h"

namespace ad_utility::testing {
using namespace ad_utility::url_parser::sparqlOperation;

inline const EncodedIriManager* encodedIriManager() {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
}

inline auto parseQuery(std::string query,
                       const std::vector<DatasetClause>& datasets = {}) {
  return SparqlParser::parseQuery(encodedIriManager(), std::move(query),
                                  datasets);
}

}  // namespace ad_utility::testing

#endif
