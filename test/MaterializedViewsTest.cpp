// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "engine/Server.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/SparqlParser.h"
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
  MaterializedViewWriter::writeViewToDisk(testIndexBase_, "testView1", plan);
  MaterializedViewsManager manager{testIndexBase_};

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
  // `SELECT` statement
  {
    const std::string reorderedQuery =
        "SELECT ?p ?o (?s AS ?x) ?g { ?s ?p ?o . BIND(3 AS ?g) }";
    MaterializedViewWriter::writeViewToDisk(
        testIndexBase_, "testView3", qlv().parseAndPlanQuery(reorderedQuery));
    MaterializedView view{testIndexBase_, "testView3"};
    EXPECT_EQ(columnNames(view).at(0), V{"?p"});
    const auto& map = view.variableToColumnMap();
    EXPECT_EQ(map.at(V{"?p"}).columnIndex_, 0);
    EXPECT_EQ(map.at(V{"?o"}).columnIndex_, 1);
    EXPECT_EQ(map.at(V{"?x"}).columnIndex_, 2);
    EXPECT_EQ(map.at(V{"?g"}).columnIndex_, 3);
  }

  // Test that presorted results are not sorted again and that the presorting
  // check considers the correct columns
  {
    clearLog();
    const std::string presortedQuery =
        "SELECT * { SELECT ?p ?o (?s AS ?x) ?g { ?s ?p ?o . BIND(3 AS ?g) } "
        "INTERNAL SORT BY ?p ?o ?x }";
    MaterializedViewWriter::writeViewToDisk(
        testIndexBase_, "testView4", qlv().parseAndPlanQuery(presortedQuery));
    EXPECT_THAT(log_.str(),
                ::testing::HasSubstr("Query result rows for materialized view "
                                     "testView4 are already sorted"));
    MaterializedView view{testIndexBase_, "testView4"};
    EXPECT_EQ(columnNames(view).at(0), V{"?p"});
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, InvalidInputToWriter) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      MaterializedViewWriter::writeViewToDisk(
          testIndexBase_, "testView1",
          qlv().parseAndPlanQuery("SELECT * { ?s ?p ?o }")),
      ::testing::HasSubstr("Currently the query used to write a materialized "
                           "view needs to have at least four columns"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      MaterializedViewWriter::writeViewToDisk(
          testIndexBase_, "Something Out!of~the.ordinary",
          qlv().parseAndPlanQuery(simpleWriteQuery_)),
      ::testing::HasSubstr("not a valid name for a materialized view"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      MaterializedViewWriter::writeViewToDisk(
          testIndexBase_, "testView2",
          qlv().parseAndPlanQuery(
              "SELECT * { ?s ?p ?o . BIND(\"localVocabString\" AS ?g) }")),
      ::testing::HasSubstr(
          "The query to write a materialized view returned a string not "
          "contained in the index (local vocabulary entry)"));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, ManualConfigurations) {
  auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);

  MaterializedViewWriter::writeViewToDisk(testIndexBase_, "testView1", plan);

  MaterializedViewsManager manager{testIndexBase_};
  auto view = manager.getView("testView1");
  ASSERT_TRUE(view != nullptr);
  EXPECT_EQ(view->name(), "testView1");
  EXPECT_EQ(view->permutation()->permutation(), Permutation::Enum::SPO);
  EXPECT_NE(view->locatedTriplesSnapshot(), nullptr);

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

  // Request for reading an extra payload column
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

  // Request for reading from a view with a fixed value for the scan column
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

  // Test that we can write a view from a fully materialized result
  {
    auto plan = qlv().parseAndPlanQuery(
        "SELECT * { BIND(1 AS ?s) BIND(2 AS ?p) BIND(3 AS ?o) BIND(4 AS ?g) }");
    auto qet = std::get<0>(plan);
    auto res = qet->getResult(true);
    EXPECT_TRUE(res->isFullyMaterialized());
    MaterializedViewWriter::writeViewToDisk(testIndexBase_, "testView4", plan);
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
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTestLarge, LazyScan) {
  // Write a simple view, inflated 10x using cartesian product with a values
  // clause
  auto writePlan = qlv().parseAndPlanQuery(
      "SELECT * { ?s ?p ?o ."
      " VALUES ?g { 1 2 3 4 5 6 7 8 9 10 } }");
  MaterializedViewWriter::writeViewToDisk(testIndexBase_, "testView1",
                                          writePlan);
  MaterializedViewsManager manager{testIndexBase_};
  auto view = manager.getView("testView1");
  using ViewQuery = parsedQuery::MaterializedViewQuery;

  // Run a simple query and consume its result lazily
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
  }
}
