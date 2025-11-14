// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "engine/MaterializedViews.h"
#include "engine/Server.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;

}  // namespace

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, Basic) {
  // Write a simple view
  clearLog();
  qlv().writeMaterializedView("testView1", simpleWriteQuery_);
  EXPECT_THAT(log_.str(), ::testing::HasSubstr(
                              "Materialized view testView1 written to disk"));
  qlv().loadMaterializedView("testView1");
  EXPECT_THAT(log_.str(), ::testing::HasSubstr(
                              "Loading materialized view testView1 from disk"));

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
      // Regression test (subquery)
      R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SELECT * {
          ?s view:testView1:g ?x .
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
                                  simpleWriteQuery_),
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
  auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);

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
    std::vector<Variable> expectedVars{V{"?s"}, V{"?o"}};
    EXPECT_THAT(query.getVarsToKeep(),
                ::testing::UnorderedElementsAreArray(expectedVars));
  }

  // Request for reading from a view with a fixed value for the scan column
  {
    ViewQuery query{SparqlTriple{
        iri("<s1>"),
        iri("<https://qlever.cs.uni-freiburg.de/materializedView/testView1:p>"),
        V{"?p"}}};
    auto t = view->makeScanConfig(query, placeholderP, placeholderO);
    Triple expected{iri("<s1>"), V{"?p"}, placeholderO};
    EXPECT_EQ(t, expected);
    std::vector<Variable> expectedVars{V{"?p"}};
    EXPECT_THAT(query.getVarsToKeep(),
                ::testing::UnorderedElementsAreArray(expectedVars));
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

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, serverIntegration) {
  // Write a new materialized view using the `writeMaterializedView` method of
  // the `Server` class
  {
    // Initialize but do not start a `Server` instance on our test index
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

  // Try loading the new view
  {
    qlv().loadMaterializedView("testViewFromServer");
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(
        "SELECT * { ?s "
        "<https://qlever.cs.uni-freiburg.de/materializedView/"
        "testViewFromServer:o> ?o }");
    auto res = qet->getResult(false);
    ASSERT_TRUE(res->isFullyMaterialized());
    EXPECT_EQ(res->idTable().numColumns(), 2);
    EXPECT_EQ(res->idTable().numRows(), 4);
  }

  // Test the HTTP request processing of the `Server` class regarding writing
  // and loading materialized views.
  using ReqT = http::request<http::string_body>;
  using ResT = std::optional<http::response<http::string_body>>;
  auto simulateHttpRequest =
      [&](const ReqT& request,
          ad_utility::source_location location =
              AD_CURRENT_SOURCE_LOC()) -> std::optional<nlohmann::json> {
    auto l = generateLocationTrace(location);
    boost::asio::io_context io;
    std::future<ResT> fut = co_spawn(
        io,
        [](auto request, auto indexName) -> boost::asio::awaitable<ResT> {
          // Initialize but do not start a `Server` instance on our test index
          Server server{4321, 1, ad_utility::MemorySize::megabytes(1),
                        "accessToken"};
          server.initialize(indexName, false);

          // Simulate receiving the HTTP request
          co_return co_await server
              .template onlyForTestingProcess<decltype(request), ResT>(request);
        }(request, testIndexBase_),
        boost::asio::use_future);
    io.run();
    auto response = fut.get();
    if (!response.has_value()) {
      return std::nullopt;
    }
    return std::optional{nlohmann::json::parse(response.value().body())};
  };

  // Write a materialized view through a simulated HTTP POST request.
  {
    clearLog();
    auto request = makePostRequest(
        "/?cmd=write-materialized-view&view-name=testViewFromHTTP&access-token="
        "accessToken",
        "application/sparql-query", simpleWriteQuery_);
    auto response = simulateHttpRequest(request);

    // Check HTTP response
    ASSERT_TRUE(response.has_value());
    ASSERT_TRUE(response.value().contains("materialized-view-written"));
    EXPECT_EQ(response.value()["materialized-view-written"],
              "testViewFromHTTP");

    // Check correct logging
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

    // Check HTTP response
    ASSERT_TRUE(response.has_value());
    ASSERT_TRUE(response.value().contains("materialized-view-written"));
    EXPECT_EQ(response.value()["materialized-view-written"],
              "testViewFromHTTP2");

    // Check correct logging
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

    // Check HTTP response
    ASSERT_TRUE(response.has_value());
    ASSERT_TRUE(response.value().contains("materialized-view-loaded"));
    EXPECT_EQ(response.value()["materialized-view-loaded"],
              "testViewFromHTTP2");

    // Check correct logging
    EXPECT_THAT(log_.str(),
                ::testing::HasSubstr(
                    "Loading materialized view testViewFromHTTP2 from disk"));
  }

  // Test error message for wrong query type
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

  // Test access token check
  {
    auto request = makePostRequest(
        "/?cmd=write-materialized-view&view-name=testViewFromHTTP3",
        "application/sparql-query", simpleWriteQuery_);
    AD_EXPECT_THROW_WITH_MESSAGE(
        simulateHttpRequest(request),
        ::testing::HasSubstr("write-materialized-view requires a valid access "
                             "token but no access token was provided"));
  }

  // Test check for name of the view (missing)
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

  // Test check for name of the view (empty)
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
  // clause
  qlv().writeMaterializedView(
      "testView1",
      "SELECT * { ?s ?p ?o . VALUES ?g { 1 2 3 4 5 6 7 8 9 10 } }");
  qlv().loadMaterializedView("testView1");

  // Run a simple query and consume its result lazily
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(
        "SELECT * { ?s "
        "<https://qlever.cs.uni-freiburg.de/materializedView/"
        "testView1:o> ?o }");

    auto res = qet->getResult(true);
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

    EXPECT_THAT(qet->getRootOperation()->getCacheKey(),
                ::testing::HasSubstr("on materialized view testView1"));
    const auto& rtDetails =
        qet->getRootOperation()->getRuntimeInfoPointer()->details_;
    ASSERT_TRUE(rtDetails.contains("scan-on-materialized-view"));
    EXPECT_EQ(rtDetails["scan-on-materialized-view"], "testView1");
  }

  // Regression test for `COUNT(*)`
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(
        "SELECT (COUNT(*) AS ?cnt) { ?s "
        "<https://qlever.cs.uni-freiburg.de/materializedView/"
        "testView1:o> ?o }");
    auto res = qet->getResult();
    ASSERT_TRUE(res->isFullyMaterialized());
    auto col = qet->getVariableColumn(Variable{"?cnt"});
    auto count = res->idTable().at(0, col);
    ASSERT_TRUE(count.getDatatype() == Datatype::Int);
    EXPECT_EQ(count.getInt(), 2 * numFakeSubjects_);
  }
}
