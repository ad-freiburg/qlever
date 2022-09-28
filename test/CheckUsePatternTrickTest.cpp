//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/CheckUsePatternTrick.h"
#include "gtest/gtest.h"
#include "parser/SparqlParser.h"

using namespace checkUsePatternTrick;

auto expectContained = [](const std::string& whereClause,
                          const std::string& variable) {
  std::string query = absl::StrCat("SELECT * WHERE {", whereClause, "}");
  auto pq = SparqlParser::parseQuery(query);
  EXPECT_TRUE(isVariableContainedInGraphPattern(Variable{variable},
                                                pq._rootGraphPattern, nullptr));
};
// TODO<joka921> duplication
auto expectNotContained = [](const std::string& whereClause,
                             const std::string& variable) {
  std::string query = absl::StrCat("SELECT * WHERE {", whereClause, "}");
  auto pq = SparqlParser::parseQuery(query);
  EXPECT_FALSE(isVariableContainedInGraphPattern(
      Variable{variable}, pq._rootGraphPattern, nullptr));
};

auto expectXYZContained(const std::string& whereClause) {
  expectContained(whereClause, "?x");
  expectContained(whereClause, "?y");
  expectContained(whereClause, "?z");
  expectNotContained(whereClause, "?not");
}

TEST(CheckUsePatternTrick, isVariableContainedInGraphPattern) {
  expectXYZContained("?x ?y ?z");
  expectXYZContained("OPTIONAL {?x ?y ?z}");
  expectXYZContained("MINUS {?x ?y ?z}");
  expectXYZContained("{{{?x ?y ?z}}}");
  expectXYZContained("{?x <is-a> ?y} UNION {?z <is-a> <something>}");
  expectXYZContained("?x <is-a> ?y {SELECT ?z WHERE {?z <is-a> ?not}}");
  expectXYZContained("BIND (3 AS ?x) . ?y <is-a> ?z");
  expectXYZContained("?x <is-a> ?z. BIND(?y AS ?t)");
  expectXYZContained("VALUES ?x {<a> <b>}. ?y <is-a> ?z");
  expectXYZContained("VALUES ?x {<a> <b>}. ?y <is-a> ?z");

  // TODO<joka921> source_location
  // TODO<joka921> test for the working and not working transitive paths.
}
