// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "util/Exception.h"

namespace {

using namespace materializedViewsTestHelpers;

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, Basic) {
  // Write a simple view
  qlv().writeMaterializedView("testView1",
                              "SELECT * { ?s ?p ?o . BIND(1 AS ?g) }");
  qlv().loadMaterializedView("testView1");

  // Test index scan on materialized view
  std::vector<std::string> equivalentQueries{
      R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:testView1:g ?x .
      }
    )",
      R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SERVICE view: {
          _:config view:name "testView1" ;
                   view:scan-column ?s ;
                   view:payload-g ?x .
        }
      }
    )",
  };
  for (const auto& query : equivalentQueries) {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(query);
    auto res = qet->getResult(false);
    EXPECT_EQ(res->idTable().numRows(), 4);
    auto col = qet->getVariableColumn(Variable{"?x"});
    for (size_t i = 0; i < res->idTable().numRows(); ++i) {
      auto id = res->idTable().at(i, col);
      EXPECT_EQ(id.getDatatype(), Datatype::Int);
      EXPECT_EQ(id.getInt(), 1);
    }
  }

  // Join between index scan on view and regular index scan
  qlv().writeMaterializedView(
      "testView2", "SELECT * { ?s <p1> ?o . BIND(42 AS ?g) . BIND(3 AS ?x) }");
  qlv().loadMaterializedView("testView2");
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:testView2:o ?x .
        ?s <p2> ?y .
      }
    )");
    auto res = qet->getResult(false);
    EXPECT_EQ(res->idTable().numRows(), 1);
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, InvalidInputToWriter) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().writeMaterializedView("testView1", "SELECT * { ?s ?p ?o }"),
      ::testing::HasSubstr("Currently the query used to write a materialized "
                           "view needs to have at least four columns"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().writeMaterializedView("Something Out!of~the.ordinary",
                                  "SELECT * { ?s ?p ?o . BIND(1 AS ?g) }"),
      ::testing::HasSubstr("not a valid name for a materialized view"));
}

}  // namespace
