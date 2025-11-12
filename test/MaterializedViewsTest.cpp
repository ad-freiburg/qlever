// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include "./MaterializedViewsTestHelpers.h"
#include "engine/MaterializedViews.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/GTestHelpers.h"

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

    EXPECT_THAT(qet->getRootOperation()->getCacheKey(),
                ::testing::HasSubstr("on materialized view testView1"));
    const auto& rtDetails =
        qet->getRootOperation()->getRuntimeInfoPointer()->details_;
    ASSERT_TRUE(rtDetails.contains("scan-on-materialized-view"));
    EXPECT_EQ(rtDetails["scan-on-materialized-view"], "testView1");

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

  // Wrong queries
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().query(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          ?s view:testView1:blabliblu ?x .
        }
      )"),
      ::testing::HasSubstr("The column '?blabliblu' does not exist in the "
                           "materialized view 'testView1'"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().query(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          ?s view:testViewXYZ:g ?x .
        }
      )"),
      ::testing::HasSubstr(
          "The materialized view 'testViewXYZ' does not exist"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().query(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view: {
            _:config view:name "testView1" ;
                    view:payload-g ?x .
          }
        }
      )"),
      ::testing::HasSubstr(
          "You must set a variable, IRI or literal for the scan column of a "
          "materialized view"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().query(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view: {
            _:config view:scan-column ?s ;
                    view:payload-g ?x .
          }
        }
      )"),
      ::testing::HasSubstr(
          "To read from a materialized view its name must be set in the "
          "query configuration"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().query(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view: {
            _:config view:name "testView1" ;
                    view:scan-column ?s ;
                    view:payload-g ?x .
            { ?s ?p ?o }
          }
        }
      )"),
      ::testing::HasSubstr("A materialized view query may not have a child "
                           "group graph pattern"));
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
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().writeMaterializedView(
          "testView2",
          "SELECT * { ?s ?p ?o . BIND(\"localVocabString\" AS ?g) }"),
      ::testing::HasSubstr("Materialized views cannot contain entries from a "
                           "local vocabulary currently."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().loadMaterializedView("doesNotExist"),
      ::testing::HasSubstr(
          "The materialized view 'doesNotExist' does not exist."));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, ManualConfigurations) {
  auto plan = qlv().parseAndPlanQuery("SELECT * { ?s ?p ?o . BIND(1 AS ?g) }");

  MaterializedViewWriter writer{"testView1", plan};
  writer.writeViewToDisk();

  MaterializedViewsManager manager{testIndexBase_};
  auto view = manager.getView("testView1");
  ASSERT_TRUE(view != nullptr);
  EXPECT_EQ(view->getName(), "testView1");

  MaterializedViewsManager managerNoBaseName;
  AD_EXPECT_THROW_WITH_MESSAGE(
      managerNoBaseName.getView("testView1"),
      ::testing::HasSubstr("index base filename was not set"));

  using ViewQuery = parsedQuery::MaterializedViewQuery;
  using Triple = SparqlTripleSimple;
  using V = Variable;
  auto iri = [](const std::string& ref) {
    return ad_utility::triple_component::Iri::fromIriref(ref);
  };
  auto lit = [](const std::string& literal) {
    return ad_utility::triple_component::Literal::fromStringRepresentation(
        literal);
  };

  V placeholderP{"?placeholder_p"};
  V placeholderO{"?placeholder_o"};

  // Request for reading an extra payload column
  {
    ViewQuery query{SparqlTriple{
        V{"?s"},
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1:g>"),
        V{"?o"}}};

    auto t = view->makeScanConfig(query, placeholderP, placeholderO);
    Triple expected{V{"?s"}, placeholderP, placeholderO, {{3, V{"?o"}}}};
    EXPECT_EQ(t, expected);
  }
  {
    ViewQuery query;
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<name>"), lit("\"testView1\"")});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<scan-column>"), V{"?s"}});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<payload-g>"), V{"?o"}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<blabliblu>"), V{"?o"}}),
        ::testing::HasSubstr("Unknown parameter"));

    auto t = view->makeScanConfig(query, placeholderP, placeholderO);
    Triple expected{V{"?s"}, placeholderP, placeholderO, {{3, V{"?o"}}}};
    EXPECT_EQ(t, expected);
  }

  // Request for reading a payload column from the first three columns of the
  // view
  {
    ViewQuery query{SparqlTriple{
        V{"?s"},
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1:o>"),
        V{"?o"}}};

    auto t = view->makeScanConfig(query, placeholderP, placeholderO);
    Triple expected{V{"?s"}, placeholderP, V{"?o"}};
    EXPECT_EQ(t, expected);
  }

  // Invalid inputs
  {
    ViewQuery query;
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<blabliblu>"), V{"?o"}}),
        ::testing::HasSubstr("Unknown parameter"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<name>"), V{"?o"}}),
        ::testing::HasSubstr("Parameter <name> to materialized view query "
                             "needs to be a literal"));

    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<payload-g>"), V{"?o"}});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<name>"), lit("\"testView1\"")});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<scan-column>"), V{"?s"}});

    AD_EXPECT_THROW_WITH_MESSAGE(
        view->makeScanConfig(query, V{"?a"}, V{"?a"}),
        ::testing::HasSubstr("Placeholders for predicate and object must not "
                             "be the same variable"));

    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<payload-g>"), V{"?o"}}),
        ::testing::HasSubstr("Each payload column may only be requested once"));

    auto query2 = query;
    query2.addParameter(
        SparqlTriple{iri("<config>"), iri("<payload-o>"), V{"?o"}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        view->makeScanConfig(query2),
        ::testing::HasSubstr(
            "Each target variable for a payload column may only be "
            "associated with one column"));

    auto query3 = query;
    query3.addParameter(
        SparqlTriple{iri("<config>"), iri("<payload-o>"), V{"?s"}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        view->makeScanConfig(query3),
        ::testing::HasSubstr(
            "The variable for the scan column of a materialized "
            "view may not also be used for a payload column"));

    auto query4 = query;
    query4.addParameter(
        SparqlTriple{iri("<config>"), iri("<payload-s>"), V{"?y"}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        view->makeScanConfig(query4),
        ::testing::HasSubstr(
            "The scan column of a materialized view may not be requested as "
            "payload"));

    AD_EXPECT_THROW_WITH_MESSAGE(
        ViewQuery(SparqlTriple{V{"?s"},
                               iri("<https://qlever.cs.uni-freiburg.de/"
                                   "materializedView/testView1>"),
                               V{"?o"}}),
        ::testing::HasSubstr("Special triple for materialized view has an "
                             "invalid predicate"));
  }
}

}  // namespace
