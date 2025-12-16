// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "engine/Server.h"
#include "gmock/gmock.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, NotImplemented) {
  // TODO<ullingerc> Remove this test when materialized views implementation is
  // added. These are valid queries - the error means that they were completely
  // parsed (and the implementation is still missing).
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().parseAndPlanQuery(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view:testView1 {
            _:config view:column-s ?s ;
                     view:column-g ?x .
          }
        }
      )"),
      ::testing::HasSubstr(
          "Materialized views are currently not supported yet"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().parseAndPlanQuery(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          ?s view:testView1-g ?x .
        }
      )"),
      ::testing::HasSubstr(
          "Materialized views are currently not supported yet"));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, ManualConfigurations) {
  auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);

  MaterializedViewWriter::writeViewToDisk("testView1", plan);

  MaterializedViewsManager manager{testIndexBase_};
  auto view = manager.getView("testView1");
  ASSERT_TRUE(view != nullptr);
  EXPECT_EQ(view->name(), "testView1");

  MaterializedViewsManager managerNoBaseName;
  AD_EXPECT_THROW_WITH_MESSAGE(
      managerNoBaseName.getView("testView1"),
      ::testing::HasSubstr("index base filename was not set"));

  using ViewQuery = parsedQuery::MaterializedViewQuery;
  using V = Variable;
  auto iri = [](const std::string& ref) {
    return ad_utility::triple_component::Iri::fromIriref(ref);
  };

  V placeholderP{"?placeholder_p"};
  V placeholderO{"?placeholder_o"};

  // Request for reading an extra payload column
  {
    ViewQuery query{SparqlTriple{
        V{"?s"},
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1-g>"),
        V{"?o"}}};

    auto t = view->makeScanConfig(query, placeholderP, placeholderO);
    Triple expected{V{"?s"}, placeholderP, placeholderO, {{3, V{"?o"}}}};
    EXPECT_EQ(t, expected);
  }
  {
    ViewQuery query{
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1>")};
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<column-s>"), V{"?s"}});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<column-g>"), V{"?o"}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<blabliblu>"), V{"?o"}}),
        ::testing::HasSubstr("Unknown parameter"));
    EXPECT_EQ(query.name(), "materialized view query");

    auto t = view->makeScanConfig(query, placeholderP, placeholderO);
    Triple expected{V{"?s"}, placeholderP, placeholderO, {{3, V{"?o"}}}};
    EXPECT_EQ(t, expected);
  }

  // Request for reading from a view with a fixed value for the scan column
  {
    ViewQuery query{SparqlTriple{
        iri("<s1>"),
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1-p>"),
        V{"?p"}}};
    auto t = view->makeScanConfig(query, placeholderP, placeholderO);
    Triple expected{iri("<s1>"), V{"?p"}, placeholderO};
    EXPECT_EQ(t, expected);
    std::vector<Variable> expectedVars{V{"?p"}};
    EXPECT_THAT(query.getVarsToKeep(),
                ::testing::UnorderedElementsAreArray(expectedVars));
  }

  // Test that we can write a view from a fully materialized result
  {
    auto plan = qlv().parseAndPlanQuery(
        "SELECT * { BIND(1 AS ?s) BIND(2 AS ?p) BIND(3 AS ?o) BIND(4 AS ?g) }");
    auto qet = std::get<0>(plan);
    auto res = qet->getResult(true);
    EXPECT_TRUE(res->isFullyMaterialized());
    MaterializedViewWriter::writeViewToDisk("testView4", plan);
  }

  // Invalid inputs
  {
    AD_EXPECT_THROW_WITH_MESSAGE(
        ViewQuery(iri("<https://qlever.cs.uni-freiburg.de/materializedView/>")),
        ::testing::HasSubstr("The IRI for the materialized view SERVICE should "
                             "specify the view name"));

    ViewQuery query{
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1>")};
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<blabliblu>"), V{"?o"}}),
        ::testing::HasSubstr("Unknown parameter"));

    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<column-g>"), V{"?o"}});

    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<column-g>"), V{"?o"}}),
        ::testing::HasSubstr("Each column may only be requested once"));

    AD_EXPECT_THROW_WITH_MESSAGE(
        ViewQuery(SparqlTriple{V{"?s"},
                               iri("<https://qlever.cs.uni-freiburg.de/"
                                   "materializedView/testView1>"),
                               V{"?o"}}),
        ::testing::HasSubstr("Special triple for materialized view has an "
                             "invalid predicate"));

    AD_EXPECT_THROW_WITH_MESSAGE(
        ViewQuery(SparqlTriple{TripleComponent::UNDEF(),
                               iri("<https://qlever.cs.uni-freiburg.de/"
                                   "materializedView/testView1-o>"),
                               V{"?o"}}),
        ::testing::HasSubstr("The subject of the magic predicate for reading "
                             "from a materialized view may not be undef"));
  }
  {
    ViewQuery query{SparqlTriple{
        V{"?s"},
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1-o>"),
        V{"?o"}}};
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<column-s>"), V{"?x"}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        view->makeScanConfig(query, placeholderP, placeholderO),
        ::testing::HasSubstr(
            "The first column of a materialized view may not be requested "
            "twice, but '?x' violated this requirement."));
  }

  // Test column stripping helper.
  {
    ViewQuery query{SparqlTriple{V{"?s"},
                                 iri("<https://qlever.cs.uni-freiburg.de/"
                                     "materializedView/testView1-o>"),
                                 V{"?o"}}};
    EXPECT_THAT(query.getVarsToKeep(),
                ::testing::UnorderedElementsAre(::testing::Eq(V{"?s"}),
                                                ::testing::Eq(V{"?o"})));
  }
}
