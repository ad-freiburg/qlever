// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./ServerTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "engine/Server.h"
#include "index/EncodedIriManager.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/SparqlParser.h"
#include "parser/SparqlTriple.h"
#include "parser/TripleComponent.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, Basic) {
  // Write a simple view.
  clearLog();
  qlv().writeMaterializedView("testView1", simpleWriteQuery_);
  EXPECT_THAT(log_.str(), ::testing::HasSubstr(
                              "Materialized view testView1 written to disk"));
  EXPECT_FALSE(qlv().isMaterializedViewLoaded("testView1"));
  qlv().loadMaterializedView("testView1");
  EXPECT_THAT(log_.str(), ::testing::HasSubstr(
                              "Loading materialized view testView1 from disk"));
  EXPECT_TRUE(qlv().isMaterializedViewLoaded("testView1"));

  // Overwriting a materialized view automatically unloads it first.
  qlv().writeMaterializedView("testView1", simpleWriteQuery_);
  EXPECT_FALSE(qlv().isMaterializedViewLoaded("testView1"));
  qlv().loadMaterializedView("testView1");
  EXPECT_TRUE(qlv().isMaterializedViewLoaded("testView1"));

  // Test index scan on materialized view.
  std::vector<std::string> equivalentQueries{
      R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:testView1-g ?x .
      }
    )",
      R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SERVICE view:testView1 {
          _:config view:column-s ?s ;
                   view:column-g ?x .
        }
      }
    )",
      // Regression test (subquery).
      R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SELECT * {
          ?s view:testView1-g ?x .
        }
      }
    )",
  };

  // Query with the equivalent result to the expected result, but without
  // materialized views.
  auto expectedResult =
      getQueryResultAsIdTable("SELECT ?s ?x { ?s ?p ?o . BIND(1 AS ?x) }");

  for (const auto& query : equivalentQueries) {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(query);

    EXPECT_THAT(qet->getRootOperation()->getCacheKey(),
                ::testing::HasSubstr("testView1"));
    // For a full scan on a materialized view, the size estimate should be
    // exactly the number of rows in the view. This is also a regression test
    // for a bug introduced in #2680.
    EXPECT_EQ(qet->getSizeEstimate(), expectedResult.numRows());

    auto res = qet->getResult(false);
    ASSERT_TRUE(res->isFullyMaterialized());
    EXPECT_THAT(res->idTable(), matchesIdTable(expectedResult));
  }

  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().loadMaterializedView("doesNotExist"),
      ::testing::HasSubstr(
          "The materialized view 'doesNotExist' does not exist."));

  // Join between index scan on view and regular index scan.
  qlv().writeMaterializedView(
      "testView2", "SELECT * { ?s <p1> ?o . BIND(42 AS ?g) . BIND(3 AS ?x) }");
  qlv().loadMaterializedView("testView2");
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:testView2-o ?x .
        ?s <p2> ?y .
      }
    )");
    auto res = qet->getResult(false);
    EXPECT_EQ(res->idTable().numRows(), 1);
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, ParserConfigChecks) {
  // Helper that checks that parsing the given query produces the expected error
  // message.
  auto expectParserError = [&](std::string query, std::string expectedError,
                               ad_utility::source_location location =
                                   AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(location);
    EncodedIriManager encodedIriManager;
    AD_EXPECT_THROW_WITH_MESSAGE(
        SparqlParser::parseQuery(&encodedIriManager, std::move(query), {}),
        ::testing::HasSubstr(expectedError));
  };

  expectParserError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view: {
            _:config view:column-s ?s ;
                     view:column-g ?x .
          }
        }
      )",
      "The IRI for the materialized view SERVICE should specify the view name");
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, MetadataDependentConfigChecks) {
  // Simple materialized view for testing the checks when querying.
  auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);
  MaterializedViewsManager manager{testIndexBase_};
  manager.writeViewToDisk("testView1", plan);

  // Helper that parses a query, but doesn't feed it to the `QueryPlanner` but
  // instead inputs the `MaterializedViewQuery` directly into a
  // `MaterializedViewsManager`.
  auto expectMakeIndexScanError = [&](std::string query,
                                      std::string expectedError,
                                      ad_utility::source_location location =
                                          AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(location);

    // Parse query.
    EncodedIriManager encodedIriManager;
    auto parsed =
        SparqlParser::parseQuery(&encodedIriManager, std::move(query), {});
    ASSERT_TRUE(parsed.hasSelectClause());
    ASSERT_EQ(parsed.children().size(), 1);

    // Extract `MaterializedViewQuery` from `SERVICE` or special triple.
    auto viewQuery = parsed.children().at(0).visit(
        [](const auto& contained) -> parsedQuery::MaterializedViewQuery {
          using T = std::decay_t<decltype(contained)>;
          if constexpr (std::is_same_v<T, parsedQuery::MaterializedViewQuery>) {
            // `SERVICE` is visited automatically during parsing.
            return contained;
          } else if constexpr (std::is_same_v<T,
                                              parsedQuery::BasicGraphPattern>) {
            // Special triple has to be processed after parsing.
            if (contained._triples.size() != 1) {
              throw std::runtime_error("Invalid graph pattern");
            }
            return parsedQuery::MaterializedViewQuery{contained._triples.at(0)};
          } else {
            throw std::runtime_error(
                "Only for testing materialized view predicate or SERVICE.");
          }
        });

    // Run `makeIndexScan` and check the error message.
    QueryExecutionContext qec{*std::get<1>(plan)};
    AD_EXPECT_THROW_WITH_MESSAGE(manager.makeIndexScan(&qec, viewQuery),
                                 ::testing::HasSubstr(expectedError));
  };

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          ?s view:testView1-blabliblu ?x .
        }
      )",
      "The column '?blabliblu' does not exist in the "
      "materialized view 'testView1'");

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          ?s view:testViewXYZ-g ?x .
        }
      )",
      "The materialized view 'testViewXYZ' does not exist");

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view:testView1 {
            _:config view:column-g ?x .
          }
        }
      )",
      "The first column of a materialized view must always be read to a "
      "variable or restricted to a fixed value");

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view:testView1 {
            _:config view:column-s ?s ;
                     view:column-g ?x .
            { ?s ?p ?o }
          }
        }
      )",
      "A materialized view query may not have a child "
      "group graph pattern");

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view:testView1 {
            _:config view:column-s ?s ;
                     view:column-g ?s .
          }
        }
      )",
      "Each target variable for a reading from a materialized "
      "view may only be associated with one column. However '?s' was "
      "requested multiple times");

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view:testView1 {
            _:config view:column-s ?s ;
                     view:column-g <http://example.com/> .
          }
        }
      )",
      "Currently only the first three columns of a materialized view may "
      "be restricted to fixed values. All other columns must be variables, "
      "but column '?g' was fixed to '<http://example.com/>'.");

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view:testView1 {
            _:config view:column-s ?s ;
                     view:column-p <http://example.com/> ;
                     view:column-g ?x .
          }
        }
      )",
      "When setting the second column of a materialized view to a fixed "
      "value, the first column must also be fixed.");

  expectMakeIndexScanError(
      R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view:testView1 {
            _:config view:column-s <http://example.com/s> ;
                     view:column-p ?p ;
                     view:column-o <http://example.com/> ;
                     view:column-g ?x .
          }
        }
      )",
      "When setting the third column of a materialized view to a fixed "
      "value, the first two columns must also be fixed.");
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, ColumnPermutation) {
  MaterializedViewsManager manager{testIndexBase_};

  // Helper to get all column names from a view via its `VariableToColumnMap`.
  auto columnNames = [](const MaterializedView& view) {
    const auto& varToCol = view.variableToColumnMap();
    std::vector<Variable> vars =
        varToCol | ql::views::keys | ::ranges::to<std::vector>();
    ql::ranges::sort(
        vars.begin(), vars.end(), [&](const auto& a, const auto& b) {
          return varToCol.at(a).columnIndex_ < varToCol.at(b).columnIndex_;
        });
    return vars;
  };

  // Test that the names and ordering of the columns in a newly written view
  // matches the names (including aliases) and ordering requested by the
  // `SELECT` statement.
  {
    const std::string reorderedQuery =
        "SELECT ?p ?o (?s AS ?x) ?g { ?s ?p ?o . BIND(3 AS ?g) }";
    manager.writeViewToDisk("testView3",
                            qlv().parseAndPlanQuery(reorderedQuery));
    MaterializedView view{testIndexBase_, "testView3"};
    EXPECT_EQ(columnNames(view).at(0), V{"?p"});
    const auto& map = view.variableToColumnMap();
    EXPECT_EQ(map.at(V{"?p"}).columnIndex_, 0);
    EXPECT_EQ(map.at(V{"?o"}).columnIndex_, 1);
    EXPECT_EQ(map.at(V{"?x"}).columnIndex_, 2);
    EXPECT_EQ(map.at(V{"?g"}).columnIndex_, 3);
  }

  // Test that presorted results are not sorted again and that the presorting
  // check considers the correct columns.
  {
    clearLog();
    const std::string presortedQuery =
        "SELECT * { SELECT ?p ?o (?s AS ?x) ?g { ?s ?p ?o . BIND(3 AS ?g) } "
        "INTERNAL SORT BY ?p ?o ?x }";
    manager.writeViewToDisk("testView4",
                            qlv().parseAndPlanQuery(presortedQuery));
    EXPECT_THAT(log_.str(),
                ::testing::HasSubstr("Query result rows for materialized view "
                                     "testView4 are already sorted"));
    MaterializedView view{testIndexBase_, "testView4"};
    EXPECT_EQ(columnNames(view).at(0), V{"?p"});
    auto res = qlv().query(
        "PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>"
        "SELECT * { <p1> view:testView4-o ?o }",
        ad_utility::MediaType::tsv);
    EXPECT_EQ(res, "?o\n\"abc\"\n\"xyz\"\n");
  }

  // Test that writing and reading from a view with less than four columns is
  // possible.
  {
    clearLog();
    manager.writeViewToDisk("testView5",
                            qlv().parseAndPlanQuery("SELECT * { <s1> ?p ?o }"));
    MaterializedView view{testIndexBase_, "testView5"};
    EXPECT_THAT(columnNames(view),
                ::testing::ElementsAreArray(std::vector<V>{V{"?p"}, V{"?o"}}));
    EXPECT_THAT(log_.str(), ::testing::HasSubstr("2 empty column(s)"));
    auto res = qlv().query(
        "PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>"
        "SELECT * { <p1> view:testView5-o ?o }",
        ad_utility::MediaType::tsv);
    EXPECT_EQ(res, "?o\n\"abc\"\n");
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, InvalidInputToWriter) {
  MaterializedViewsManager manager{testIndexBase_};

  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.writeViewToDisk("Something Out!of~the.ordinary",
                              qlv().parseAndPlanQuery(simpleWriteQuery_)),
      ::testing::HasSubstr("not a valid name for a materialized view"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.writeViewToDisk(
          "testView2",
          qlv().parseAndPlanQuery(
              "SELECT * { ?s ?p ?o . BIND(\"localVocabString\" AS ?g) }")),
      ::testing::HasSubstr(
          "The query to write a materialized view returned a string not "
          "contained in the index (local vocabulary entry)"));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, ManualConfigurations) {
  MaterializedViewsManager manager{testIndexBase_};
  auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);
  manager.writeViewToDisk("testView1", plan);
  auto view = manager.getView("testView1");
  ASSERT_TRUE(view != nullptr);
  EXPECT_EQ(view->name(), "testView1");
  EXPECT_EQ(view->permutation()->permutation(), Permutation::Enum::SPO);
  EXPECT_EQ(view->permutation()->readableName(), "testView1");
  EXPECT_NE(view->locatedTriplesState(), nullptr);
  EXPECT_TRUE(manager.isViewLoaded("testView1"));
  EXPECT_FALSE(manager.isViewLoaded("something"));

  // Unloading a view that is not loaded is a no-op.
  manager.unloadViewIfLoaded("something");
  EXPECT_FALSE(manager.isViewLoaded("something"));
  EXPECT_THAT(view->originalQuery(),
              ::testing::Optional(::testing::Eq(simpleWriteQuery_)));

  MaterializedViewsManager managerNoBaseName;
  AD_EXPECT_THROW_WITH_MESSAGE(
      managerNoBaseName.getView("testView1"),
      ::testing::HasSubstr("index base filename was not set"));
  managerNoBaseName.setOnDiskBase(testIndexBase_);
  EXPECT_NE(managerNoBaseName.getView("testView1"), nullptr);

  using ViewQuery = parsedQuery::MaterializedViewQuery;
  using Triple = SparqlTripleSimple;
  using V = Variable;
  auto iri = [](const std::string& ref) {
    return ad_utility::triple_component::Iri::fromIriref(ref);
  };

  const V placeholderP{"?_ql_materialized_view_p"};
  const V placeholderO{"?_ql_materialized_view_o"};

  // Request for reading an extra payload column.
  {
    ViewQuery query{SparqlTriple{
        V{"?s"},
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1-g>"),
        V{"?o"}}};

    auto t = view->makeScanConfig(query);
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

    auto t = view->makeScanConfig(query);
    Triple expected{V{"?s"}, placeholderP, placeholderO, {{3, V{"?o"}}}};
    EXPECT_EQ(t, expected);
  }

  // Request for reading a payload column from the first three columns of the
  // view.
  {
    ViewQuery query{SparqlTriple{
        V{"?s"},
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1-o>"),
        V{"?o"}}};

    auto t = view->makeScanConfig(query);
    Triple expected{V{"?s"}, placeholderP, V{"?o"}};
    EXPECT_EQ(t, expected);
    std::vector<Variable> expectedVars{V{"?s"}, V{"?o"}};
    EXPECT_THAT(query.getVarsToKeep(),
                ::testing::UnorderedElementsAreArray(expectedVars));
  }

  // Request for reading from a view with a fixed value for the scan column.
  {
    ViewQuery query{SparqlTriple{
        iri("<s1>"),
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1-p>"),
        V{"?p"}}};
    auto t = view->makeScanConfig(query);
    Triple expected{iri("<s1>"), V{"?p"}, placeholderO};
    EXPECT_EQ(t, expected);
    std::vector<Variable> expectedVars{V{"?p"}};
    EXPECT_THAT(query.getVarsToKeep(),
                ::testing::UnorderedElementsAreArray(expectedVars));
  }

  // Test that we can write a view from a fully materialized result.
  {
    auto plan = qlv().parseAndPlanQuery(
        "SELECT * { BIND(1 AS ?s) BIND(2 AS ?p) BIND(3 AS ?o) BIND(4 AS ?g) }");
    auto qet = std::get<0>(plan);
    auto res = qet->getResult(true);
    EXPECT_TRUE(res->isFullyMaterialized());
    manager.writeViewToDisk("testView4", plan);
  }

  // Invalid inputs.
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
        view->makeScanConfig(query),
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

  // Test internal constructor.
  {
    ViewQuery query{"testView", ViewQuery::RequestedColumns{
                                    {V{"?s"}, V{"?s2"}}, {V{"?o"}, V{"?o2"}}}};
    EXPECT_EQ(query.viewName_, "testView");
    EXPECT_THAT(query.getVarsToKeep(),
                ::testing::UnorderedElementsAre(::testing::Eq(V{"?s2"}),
                                                ::testing::Eq(V{"?o2"})));
  }

  // Unsupported format version.
  {
    auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);
    manager.writeViewToDisk("testView5", plan);
    {
      // Write fake view metadata with unsupported version.
      nlohmann::json viewInfo = {{"version", 0}};
      ad_utility::makeOfstream(
          "_materializedViewsTestIndex.view.testView5.viewinfo.json")
          << viewInfo.dump() << std::endl;
    }
    AD_EXPECT_THROW_WITH_MESSAGE(
        MaterializedView(testIndexBase_, "testView5"),
        ::testing::HasSubstr(
            "The materialized view 'testView5' is saved with format version "
            "0, however this version of QLever expects"));
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, serverIntegration) {
  using namespace serverTestHelpers;
  SimulateHttpRequest simulateHttpRequest{testIndexBase_};

  // Write a new materialized view using the `writeMaterializedView` method of
  // the `Server` class.
  {
    // Initialize but do not start a `Server` instance on our test index.
    Server server{4321, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
    server.initialize(testIndexBase_, false);

    ad_utility::url_parser::sparqlOperation::Query query{simpleWriteQuery_, {}};
    ad_utility::Timer requestTimer{ad_utility::Timer::InitialStatus::Started};
    auto cancellationHandle =
        std::make_shared<ad_utility::CancellationHandle<>>();
    static constexpr size_t dummyTimeLimit = 1000 * 60 * 60;  // 1 hour
    std::chrono::milliseconds timeLimit{dummyTimeLimit};
    server.writeMaterializedView("testViewFromServer", query, requestTimer,
                                 cancellationHandle, timeLimit);
  }

  // Test the preloading of materialized views on server start.
  {
    qlv().writeMaterializedView("testViewForServerPreload", simpleWriteQuery_);
    Server server{4321, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
    server.initialize(testIndexBase_, false, true, true, false,
                      {"testViewForServerPreload"});
    EXPECT_TRUE(server.materializedViewsManager_.isViewLoaded(
        "testViewForServerPreload"));
  }

  // Try loading the new view.
  {
    qlv().loadMaterializedView("testViewFromServer");
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(
        "SELECT * { ?s "
        "<https://qlever.cs.uni-freiburg.de/materializedView/"
        "testViewFromServer-o> ?o }");
    auto expectedIdTable = getQueryResultAsIdTable(
        "SELECT ?s ?o { ?s ?p ?o } INTERNAL SORT BY ?s ?p ?o");
    auto res = qet->getResult(false);
    ASSERT_TRUE(res->isFullyMaterialized());
    EXPECT_THAT(res->idTable(), matchesIdTable(expectedIdTable));
  }

  // Write a materialized view through a simulated HTTP POST request.
  {
    clearLog();
    auto request = makePostRequest(
        "/?cmd=write-materialized-view&view-name=testViewFromHTTP&access-token="
        "accessToken",
        "application/sparql-query", simpleWriteQuery_);
    auto response = simulateHttpRequest(request);

    // Check HTTP response.
    ASSERT_TRUE(response.has_value());
    ASSERT_TRUE(response.value().contains("materialized-view-written"));
    EXPECT_EQ(response.value()["materialized-view-written"],
              "testViewFromHTTP");

    // Check correct logging.
    EXPECT_THAT(log_.str(),
                ::testing::HasSubstr(
                    "Materialized view testViewFromHTTP written to disk"));
  }

  // Write a materialized view through a simulated HTTP GET request.
  {
    clearLog();
    auto request = makeGetRequest(
        "/?cmd=write-materialized-view&view-name=testViewFromHTTP2"
        "&access-token=accessToken"
        "&query=SELECT%20*%20%7B%20%3Fs%20%3Fp%20%3Fo%20.%20BIND(1%"
        "20AS%20%3Fg)%20%7D");
    auto response = simulateHttpRequest(request);

    // Check HTTP response.
    ASSERT_TRUE(response.has_value());
    ASSERT_TRUE(response.value().contains("materialized-view-written"));
    EXPECT_EQ(response.value()["materialized-view-written"],
              "testViewFromHTTP2");

    // Check correct logging.
    EXPECT_THAT(log_.str(),
                ::testing::HasSubstr(
                    "Materialized view testViewFromHTTP2 written to disk"));
  }

  // Load a materialized view through a simulated HTTP GET request.
  {
    clearLog();
    auto request = makeGetRequest(
        "/?cmd=load-materialized-view&view-name=testViewFromHTTP2"
        "&access-token=accessToken");
    auto response = simulateHttpRequest(request);

    // Check HTTP response.
    ASSERT_TRUE(response.has_value());
    ASSERT_TRUE(response.value().contains("materialized-view-loaded"));
    EXPECT_EQ(response.value()["materialized-view-loaded"],
              "testViewFromHTTP2");

    // Check correct logging.
    EXPECT_THAT(log_.str(),
                ::testing::HasSubstr(
                    "Loading materialized view testViewFromHTTP2 from disk"));
  }

  // Test error message for wrong query type.
  {
    auto request = makePostRequest(
        "/?cmd=write-materialized-view&view-name=testViewFromHTTP3&"
        "access-token=accessToken",
        "application/sparql-update", "INSERT DATA { <a> <b> <c> }");
    AD_EXPECT_THROW_WITH_MESSAGE(
        simulateHttpRequest(request),
        ::testing::HasSubstr(
            "Action 'write-materialized-view' requires a 'SELECT' query"));
  }

  // Test access token check.
  {
    auto request = makePostRequest(
        "/?cmd=write-materialized-view&view-name=testViewFromHTTP3",
        "application/sparql-query", simpleWriteQuery_);
    AD_EXPECT_THROW_WITH_MESSAGE(
        simulateHttpRequest(request),
        ::testing::HasSubstr("write-materialized-view requires a valid access "
                             "token but no access token was provided"));
  }

  // Test check for name of the view (missing).
  {
    auto request = makePostRequest(
        "/?cmd=write-materialized-view&access-token=accessToken",
        "application/sparql-query", simpleWriteQuery_);
    AD_EXPECT_THROW_WITH_MESSAGE(
        simulateHttpRequest(request),
        ::testing::HasSubstr(
            "Writing a materialized view requires a name to be set "
            "via the 'view-name' parameter"));
  }

  // Test check for name of the view (empty).
  {
    auto request = makePostRequest(
        "/?cmd=write-materialized-view&view-name=&access-token=accessToken",
        "application/sparql-query", simpleWriteQuery_);
    AD_EXPECT_THROW_WITH_MESSAGE(
        simulateHttpRequest(request),
        ::testing::HasSubstr("The name for the view may not be empty"));
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTestLarge, LazyScan) {
  // Write a simple view, inflated 10x using cartesian product with a values
  // clause.
  auto writePlan = qlv().parseAndPlanQuery(
      "SELECT * { ?s ?p ?o ."
      " VALUES ?g { 1 2 3 4 5 6 7 8 9 10 } }");
  MaterializedViewsManager manager{testIndexBase_};
  manager.writeViewToDisk("testView1", writePlan);
  auto view = manager.getView("testView1");
  using ViewQuery = parsedQuery::MaterializedViewQuery;

  // Run a simple query and consume its result lazily.
  {
    ViewQuery query{SparqlTriple{Variable{"?s"},
                                 ad_utility::triple_component::Iri::fromIriref(
                                     "<https://qlever.cs.uni-freiburg.de/"
                                     "materializedView/testView1-o>"),
                                 Variable{"?o"}}};
    QueryExecutionContext qec{*std::get<1>(writePlan)};
    auto scan = manager.makeIndexScan(&qec, query);
    auto res = scan->getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    size_t numRows = 0;
    size_t numBlocks = 0;

    ASSERT_FALSE(res->isFullyMaterialized());

    for (const auto& [idTable, localVocab] : res->idTables()) {
      EXPECT_TRUE(localVocab.empty());
      EXPECT_EQ(idTable.numColumns(), 2);
      numRows += idTable.numRows();
      ++numBlocks;
    }

    EXPECT_EQ(numRows, 2 * numFakeSubjects_);
    AD_LOG_INFO << "Lazy scan had " << numRows << " rows from " << numBlocks
                << " block(s)" << std::endl;

    EXPECT_THAT(scan->getCacheKey(), ::testing::HasSubstr("testView1"));
    EXPECT_THAT(scan->getDescriptor(), ::testing::HasSubstr("testView1"));
  }

  // Regression test for `COUNT(*)`.
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(
        "SELECT (COUNT(*) AS ?cnt) { ?s "
        "<https://qlever.cs.uni-freiburg.de/materializedView/testView1-o> ?o "
        "}");
    auto res = qet->getResult();
    ASSERT_TRUE(res->isFullyMaterialized());
    auto col = qet->getVariableColumn(Variable{"?cnt"});
    auto count = res->idTable().at(0, col);
    ASSERT_TRUE(count.getDatatype() == Datatype::Int);
    EXPECT_EQ(count.getInt(), 2 * numFakeSubjects_);
  }
}
