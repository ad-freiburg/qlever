// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <functional>

#include "./MaterializedViewsTestHelpers.h"
#include "./QueryPlannerTestHelpers.h"
#include "./ServerTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "engine/MaterializedViewsQueryAnalysis.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Server.h"
#include "engine/SpatialJoinConfig.h"
#include "engine/StripColumns.h"
#include "engine/VariableToColumnMap.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "index/EncodedIriManager.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/SparqlParser.h"
#include "parser/SparqlTriple.h"
#include "parser/TripleComponent.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/CompilerWarnings.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, Basic) {
  SKIP_IF_LOGLEVEL_IS_LOWER(INFO);
  // Write a simple view.
  clearLog();
  qlv().writeMaterializedView("testView1", simpleWriteQuery_);
  EXPECT_THAT(
      log_.str(),
      ::testing::HasSubstr("Materialized view \"testView1\" written to disk"));
  EXPECT_FALSE(qlv().isMaterializedViewLoaded("testView1"));
  qlv().loadMaterializedView("testView1");
  EXPECT_THAT(log_.str(),
              ::testing::HasSubstr(
                  "Loading materialized view \"testView1\" from disk"));
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
    EXPECT_THAT(qet->getRootOperation()->getDescriptor(),
                ::testing::HasSubstr("?x"));
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

  // Test the `IndexScan` operation's descriptor when reading from columns not
  // within the first three.
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SERVICE view:testView1 {
          _:config view:column-s ?s ;
                   view:column-p ?p ;
                   view:column-o ?o ;
                   view:column-g ?g .
        }
      }
    )");
    EXPECT_EQ(qet->getRootOperation()->getDescriptor(),
              "IndexScan testView1 ?s ?p ?o ?g");
  }

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
  SKIP_IF_LOGLEVEL_IS_LOWER(INFO);
  MaterializedViewsManager manager{testIndexBase_};

  // Helper to get all column names from a view via its `VariableToColumnMap`.
  auto columnNames = [](const MaterializedView& view) {
    const auto& varToCol = view.variableToColumnMap();
    DISABLE_AGGRESSIVE_LOOP_OPT_WARNINGS
    std::vector<Variable> vars =
        varToCol | ql::views::keys | ::ranges::to<std::vector>();
    GCC_REENABLE_WARNINGS
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
                                     "\"testView4\" are already sorted"));
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

  // Test that the column `UndefStatus` is set correctly in the view's
  // `VariableToColumnMap`. Also test the `UndefStatus` stored in the
  // `Permutation` object and in an `IndexScan`'s `VariableToColumnMap`.
  {
    // In this view, ?s and ?o are always defined, but ?u is undefined for one
    // of the two rows in the view.
    manager.writeViewToDisk("testView6", qlv().parseAndPlanQuery(R"(
      SELECT ?s ?o ?u {
        ?s <p1> ?o .
        OPTIONAL {
          ?s <p2> ?u .
        }
      }
    )"));
    auto view = manager.getView("testView6");

    // `UndefStatus` in `VariableToColumnMap`.
    auto map = view->variableToColumnMap();
    VariableToColumnMap expected{{V{"?s"}, makeAlwaysDefinedColumn(0)},
                                 {V{"?o"}, makeAlwaysDefinedColumn(1)},
                                 {V{"?u"}, makePossiblyUndefinedColumn(2)}};
    EXPECT_THAT(map, ::testing::UnorderedElementsAreArray(expected));

    // `UndefStatus` is stored correctly in `Permutation`.
    auto permutation = view->permutation();
    EXPECT_EQ(permutation->getColumnUndefStatus(0),
              ColumnIndexAndTypeInfo::AlwaysDefined);
    EXPECT_EQ(permutation->getColumnUndefStatus(1),
              ColumnIndexAndTypeInfo::AlwaysDefined);
    EXPECT_EQ(permutation->getColumnUndefStatus(2),
              ColumnIndexAndTypeInfo::PossiblyUndefined);

    // `UndefStatus` in `IndexScan`.
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SERVICE view:testView6 {
          _:config view:column-s ?s ;
                   view:column-o ?o ;
                   view:column-u ?u .
        }
      }
    )");
    auto scanMap = qet->getVariableColumns();
    // When all columns are requested, the `IndexScan`'s `VariableToColumnMap`
    // should be equal to that of the `MaterializedView` itself.
    EXPECT_THAT(scanMap, ::testing::UnorderedElementsAreArray(expected));

    // Check that `columnOriginatesFromGraphOrUndef` of `IndexScan` is disabled
    // for the materialized view.
    for (const auto& var : expected | ql::views::keys) {
      EXPECT_FALSE(
          qet->getRootOperation()->columnOriginatesFromGraphOrUndef(var));
    }
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
  EXPECT_EQ(view->permutation()->permutationType(),
            Permutation::Type::MATERIALIZED_VIEW);
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

  // Backward compatibility: View with no saved query.
  {
    auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);
    manager.writeViewToDisk("testView6", plan);
    {
      // Remove the `query` key from the metadata JSON file.
      nlohmann::json viewInfo;
      const std::string metadataFilename =
          "_materializedViewsTestIndex.view.testView6.viewinfo.json";
      ad_utility::makeIfstream(metadataFilename) >> viewInfo;
      viewInfo.erase("query");
      ad_utility::makeOfstream(metadataFilename)
          << viewInfo.dump() << std::endl;
    }
    // Load the view: It can be loaded correctly, but does not have an original
    // query set.
    auto view = manager.getView("testView6");
    EXPECT_FALSE(view->originalQuery().has_value());
    EXPECT_FALSE(view->parsedQuery().has_value());
  }

  // Backward compatibility: View with columns saved without `UndefStatus`.
  {
    auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);
    manager.writeViewToDisk("testView7", plan);
    {
      // Store only the column names in the `columns` array of the metadata JSON
      // file, in particular, no `UndefStatus`.
      nlohmann::json viewInfo;
      const std::string metadataFilename =
          "_materializedViewsTestIndex.view.testView7.viewinfo.json";
      ad_utility::makeIfstream(metadataFilename) >> viewInfo;
      viewInfo.erase("columns");
      viewInfo["columns"] = std::vector<std::string>{"?s", "?p", "?o", "?g"};
      ad_utility::makeOfstream(metadataFilename)
          << viewInfo.dump() << std::endl;
    }
    // Load the view: The view can be loaded correctly, but all columns are
    // possibly undefined because the information is missing.
    auto view = manager.getView("testView7");
    for (size_t i = 0; i < 4; ++i) {
      EXPECT_EQ(view->permutation()->getColumnUndefStatus(i),
                ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined);
    }
  }

  // View with no parsed query is skipped by `QueryPatternCache::analyzeView`.
  {
    qlv().writeMaterializedView("testView7", simpleWriteQuery_);
    auto view = std::make_shared<MaterializedView>(testIndexBase_, "testView7");
    view->parsedQuery_ = std::nullopt;
    materializedViewsQueryAnalysis::QueryPatternCache c;
    EXPECT_FALSE(c.analyzeView(view));
  }

  // Test assertions on `Permutation::Type`.
  {
    Permutation testPermutation{Permutation::Enum::SPO,
                                ad_utility::makeUnlimitedAllocator<Id>()};
    const std::string testView1Filename =
        "_materializedViewsTestIndex.view.testView1";
    // A materialized view permutation does not have a corresponding internal
    // permutation.
    EXPECT_ANY_THROW(testPermutation.loadFromDisk(
        testView1Filename, true, Permutation::Type::MATERIALIZED_VIEW));
    // If a permutation is loaded as `Type::NORMAL`, no reference to an owning
    // materialized view can be set.
    EXPECT_NO_THROW(testPermutation.loadFromDisk(testView1Filename, false,
                                                 Permutation::Type::NORMAL));
    EXPECT_ANY_THROW(testPermutation.setMaterializedView(view));
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, serverIntegration) {
  SKIP_IF_LOGLEVEL_IS_LOWER(INFO);
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
                    "Materialized view \"testViewFromHTTP\" written to disk"));
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
                    "Materialized view \"testViewFromHTTP2\" written to disk"));
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
    EXPECT_THAT(
        log_.str(),
        ::testing::HasSubstr(
            "Loading materialized view \"testViewFromHTTP2\" from disk"));
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

    EXPECT_EQ(numRows, 20 * numFakeSubjects_);
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
    EXPECT_EQ(count.getInt(), 20 * numFakeSubjects_);
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, BindToColumnMap) {
  qlv().writeMaterializedView("testView1", simpleWriteQuery_);
  MaterializedViewsManager manager{testIndexBase_};
  auto view = manager.getView("testView1");
  EXPECT_TRUE(view->parsedQuery().has_value());

  // `BIND` is contained.
  {
    auto expr = sparqlExpression::SparqlExpressionPimpl{
        std::make_shared<sparqlExpression::IdExpression>(
            ValueId::makeFromInt(1)),
        "1"};
    auto cacheKey = expr.getCacheKey({});
    EXPECT_THAT(view->lookupBindTargetColumn(cacheKey),
                ::testing::Optional(::testing::Eq(3)));
  }

  // `BIND` is not contained.
  {
    auto expr = sparqlExpression::SparqlExpressionPimpl{
        std::make_shared<sparqlExpression::IdExpression>(
            ValueId::makeFromDouble(5.0)),
        "5.0"};
    auto cacheKey = expr.getCacheKey({});
    EXPECT_FALSE(view->lookupBindTargetColumn(cacheKey).has_value());
  }

  // Variable in `BIND` must be mapped to the correct column by the caller.
  qlv().writeMaterializedView("testView2", R"(
    SELECT ?s ?p ?o ?g {
      ?s ?p ?o .
      BIND(?o AS ?g)
    }
  )");
  auto view2 = manager.getView("testView2");
  {
    auto expr = sparqlExpression::SparqlExpressionPimpl{
        std::make_shared<sparqlExpression::VariableExpression>(V{"?x"}), "?x"};

    // `BIND` is found using correct mapping despite different variable name.
    VariableToColumnMap correctVarToCol{{V{"?x"}, makeAlwaysDefinedColumn(2)}};
    auto cacheKey1 = expr.getCacheKey(correctVarToCol);
    EXPECT_THAT(view2->lookupBindTargetColumn(cacheKey1),
                ::testing::Optional(::testing::Eq(3)));

    // `BIND` is not found with different column index.
    VariableToColumnMap wrongVarToCol{{V{"?x"}, makeAlwaysDefinedColumn(1)}};
    auto cacheKey2 = expr.getCacheKey(wrongVarToCol);
    EXPECT_FALSE(view2->lookupBindTargetColumn(cacheKey2).has_value());
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, NoDuplicateRemovalOnScan) {
  // Test that rows from materialized views are not implicitly deduplicated when
  // scanning. If the first three columns have duplicates (differing only in an
  // unselected additional column), all rows should be returned.

  // Write a view where each row is duplicated with two different values of
  // the fourth column `?x`.
  const std::string dupQuery =
      "SELECT ?s ?p ?o ?g { ?s ?p ?o . VALUES ?g { 1 2 } }";
  qlv().writeMaterializedView("dupView", dupQuery);
  qlv().loadMaterializedView("dupView");

  // Base case: Query the view selecting all 4 columns: we expect exactly the
  // original result.
  auto allColsExpected =
      getQueryResultAsIdTable(dupQuery + " INTERNAL SORT BY ?s ?p ?o ?g");
  auto allColsResult = getQueryResultAsIdTable(R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * {
      SERVICE view:dupView {
        _:config view:column-s ?s ;
                 view:column-p ?p ;
                 view:column-o ?o ;
                 view:column-g ?g .
      }
    }
  )");
  EXPECT_THAT(allColsResult, matchesIdTable(allColsExpected));

  // No-deduplication case: Select only the first 3 columns, but expect each of
  // them twice.
  auto numRowsDedup =
      getQueryResultAsIdTable("SELECT ?s ?p ?o { ?s ?p ?o }").numRows();
  auto threeColsExpected = getQueryResultAsIdTable(
      "SELECT ?s ?p ?o { ?s ?p ?o . VALUES ?g { 1 2 } } "
      "INTERNAL SORT BY ?s ?p ?o");
  auto threeColsResult = getQueryResultAsIdTable(R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * {
      SERVICE view:dupView {
        _:config view:column-s ?s ;
                 view:column-p ?p ;
                 view:column-o ?o .
      }
    }
  )");
  EXPECT_EQ(threeColsResult.numRows(), 2 * numRowsDedup);
  EXPECT_THAT(threeColsResult, matchesIdTable(threeColsExpected));

  // Graph column regression from #2708: Previously the graph column would
  // overwrite an additional column if it was not selected.
  {
    qlv().writeMaterializedView("graphColRegression", R"(
      SELECT ?a ?b ?c ?d ?e {
        VALUES (?a ?b ?c ?d ?e) {
          (1 2 3 4 5)
          (11 12 13 14 15)
        }
      }
    )");
    auto res = getQueryResultAsIdTable(R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT ?e {
        SERVICE view:graphColRegression {
          [
            view:column-a 1 ;
            view:column-e ?e
          ]
        }
      }
    )");
    // If we always select the graph column, but do not read it into a dummy
    // variable, we would get `4` instead of `5` here.
    auto expected = getQueryResultAsIdTable("SELECT (5 AS ?e) {}");
    EXPECT_THAT(res, matchesIdTable(expected));
  }
}

// Queries for testing `BIND` rewriting.
constexpr std::string_view bindWriteQuery =
    R"(
      PREFIX math: <http://www.w3.org/2005/xpath-functions/math#>
      SELECT ?s ?o ?b1 ?b2 ?b3 {
        ?s <p2> ?o .
        BIND(15 AS ?b1)
        BIND(2 * ?o + 1 AS ?b2)
        BIND(math:cos(?o - 1) + 4 AS ?b3)
      }
    )";

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, BindRewrite) {
  qlv().writeMaterializedView("bindView", std::string{bindWriteQuery});
  qlv().loadMaterializedView("bindView");

  // We fix the first columns of the `IndexScan` matcher because we are only
  // interested in the additional columns. The number of columns after stripping
  // is 3, for S, P and one additional column.
  auto bindView = std::bind_front(&viewScan, "bindView", "?s", "?o",
                                  "?_ql_materialized_view_o", 3);
  // Matcher for a view scan with no additional columns.
  auto viewScanNoBind =
      viewScan("bindView", "?s", "?o", "?_ql_materialized_view_o", 2);

  using AC = std::vector<std::pair<ColumnIndex, Variable>>;

  // Simple `BIND` rewrites.
  {
    constexpr std::string_view simpleBind = R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT ?bind {
        ?s view:bindView-o ?o .
        BIND(2 * ?o + 1 AS ?bind)
      }
    )";
    qpExpect(qlv(), simpleBind, bindView(AC{{3, V{"?bind"}}}));

    auto actual = getQueryResultAsIdTable(std::string{simpleBind});
    auto expected = getQueryResultAsIdTable("SELECT (3 AS ?bind) {}");
    EXPECT_THAT(actual, matchesIdTable(expected));
  }

  // A `BIND` is pushed down through a `Join` operation.
  {
    constexpr std::string_view bindThroughJoin = R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT ?s ?x ?o ?bind {
        {
          # Force the `JOIN` first, then the `BIND`.
          ?s <p1> ?x .
          ?s view:bindView-o ?o .
        }
        BIND(2 * ?o + 1 AS ?bind)
      }
    )";
    qpExpect(qlv(), bindThroughJoin,
             h::Join(h::IndexScanFromStrings("?s", "<p1>", "?x"),
                     bindView(AC{{3, V{"?bind"}}})));
  }

  // A `BIND` is pushed down through a `SpatialJoin` operation.
  {
    constexpr std::string_view bindThroughSpatialJoin = R"(
      PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT ?s ?o ?s2 ?o2 ?bind {
        {
          # Force the `SpatialJoin` first.
          ?s view:bindView-o ?o .
          ?s2 view:bindView-o ?o2 .
          FILTER(geof:metricDistance(?o, ?o2) <= 100)
        }
        BIND(2 * ?o2 + 1 AS ?bind)
      }
    )";
    auto viewScanWithBind =
        viewScan("bindView", "?s2", "?o2", "?_ql_materialized_view_o", 3,
                 AC{{3, V{"?bind"}}});
    qpExpect(
        qlv(), bindThroughSpatialJoin,
        h::spatialJoin(
            100, -1, V{"?o"}, V{"?o2"}, std::nullopt, PayloadVariables::all(),
            SpatialJoinAlgorithm::LIBSPATIALJOIN, SpatialJoinType::WITHIN_DIST,
            // Matcher for left child of `SpatialJoin`: Scan on
            // view without `BIND` push down.
            viewScanNoBind,
            // Matcher for right child of `SpatialJoin`: Scan on
            // view with `BIND` push down due to matching variables.
            viewScanWithBind));
  }

  // The `2 * ?o + 1` expression.
  auto bindExpr = sparqlExpression::makeAddExpression(
      sparqlExpression::makeMultiplyExpression(
          std::make_unique<sparqlExpression::IdExpression>(
              ValueId::makeFromInt(2)),
          std::make_unique<sparqlExpression::VariableExpression>(V{"?o"})),
      std::make_unique<sparqlExpression::IdExpression>(
          ValueId::makeFromInt(1)));
  const parsedQuery::Bind bind{sparqlExpression::SparqlExpressionPimpl{
                                   std::move(bindExpr), "2 * ?o + 1"},
                               V{"?bind"}};

  // Trying to push down a `BIND` into a `SpatialJoin` which does not have its
  // children yet is not possible. But the child `nullptr` should not crash.
  {
    SpatialJoinConfiguration config{
        LibSpatialJoinConfig{SpatialJoinType::INTERSECTS}, V{"?a"}, V{"?b"}};
    auto plan = qlv().parseAndPlanQuery("SELECT * { ?s ?p ?o }");
    QueryExecutionContext qec{*std::get<1>(plan)};
    // `SpatialJoin` has no children.
    SpatialJoin sj{&qec, config, std::nullopt, std::nullopt};
    EXPECT_FALSE(sj.makeTreeWithBindColumn(bind).has_value());
  }

  // A `BIND` is pushed down through a `StripColumns` operation.
  const std::set<Variable> varsToKeep{V{"?o"}};
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView-o ?o .
      }
    )");

    // `StripColumns` with a single column.
    auto stripCols =
        ad_utility::makeExecutionTree<StripColumns>(qec.get(), qet, varsToKeep);
    EXPECT_EQ(stripCols->getResultWidth(), 1);

    auto stripWithBind =
        stripCols->getRootOperation()->makeTreeWithBindColumn(bind);
    ASSERT_TRUE(stripWithBind.has_value());
    EXPECT_THAT(*stripWithBind.value(),
                h::RootOperation<StripColumns>(::testing::AllOf(
                    // The new `StripColumns` now includes the `BIND` column and
                    // therefore has two columns, while the old `StripColumns`
                    // only had one.
                    AD_PROPERTY(StripColumns, getResultWidth, ::testing::Eq(2)),
                    h::children(bindView(AC{{3, V{"?bind"}}})))));
  }

  // A `BIND` cannot be pushed into a regular `IndexScan` (not a materialized
  // view) or a `StripColumns` operation containing a regular `IndexScan`.
  {
    auto [qet, qec, parsed] =
        qlv().parseAndPlanQuery("SELECT * { ?s <p2> ?o }");
    EXPECT_FALSE(
        qet->getRootOperation()->makeTreeWithBindColumn(bind).has_value());
    auto stripCols =
        ad_utility::makeExecutionTree<StripColumns>(qec.get(), qet, varsToKeep);
    EXPECT_FALSE(stripCols->getRootOperation()
                     ->makeTreeWithBindColumn(bind)
                     .has_value());
  }

  // A `BIND` cannot be pushed into a scan on a view if the scan doesn't select
  // all columns needed for the `BIND`.
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView-b3 ?b3 .
      }
    )");
    EXPECT_FALSE(
        qet->getRootOperation()->makeTreeWithBindColumn(bind).has_value());
  }

  // A `BIND` cannot be pushed into a scan on a view if the scan already selects
  // values into the `BIND`'s target column.
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SERVICE view:bindView {
          [
            view:column-s ?s ;
            view:column-o ?o ;
            view:column-b3 ?bind
          ]
        }
      }
    )");
    EXPECT_FALSE(
        qet->getRootOperation()->makeTreeWithBindColumn(bind).has_value());
  }

  // A `BIND` cannot be pushed into a scan on a view if the scan already selects
  // the column that contains the `BIND` results into a variable with a
  // different name.
  {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        SERVICE view:bindView {
          [
            view:column-s ?s ;
            view:column-o ?o ;
            view:column-b2 ?other_variable
          ]
        }
      }
    )");
    EXPECT_FALSE(
        qet->getRootOperation()->makeTreeWithBindColumn(bind).has_value());
  }

  // A `BIND` that is not contained in the view cannot be pushed down.
  {
    const std::string notContainedBind = R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView-o ?o .
        BIND(42 * ?o AS ?bind)
      }
    )";
    qpExpect(qlv(), notContainedBind,
             h::Bind(viewScanNoBind, "42 * ?o", V{"?bind"}));
  }

  // The `BIND` is the second column of the view.
  {
    qlv().writeMaterializedView(
        "bindView2",
        "SELECT ?a ?b ?c { <s1> <p2> ?a . BIND(2 * ?a AS ?b) BIND(42 AS ?c) }");
    qlv().loadMaterializedView("bindView2");
    const std::string secondColBind = R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView2-c ?b2 .
        BIND(2 * ?s AS ?b1)
      }
    )";
    qpExpect(qlv(), secondColBind,
             viewScan("bindView2", "?s", "?b1", "?b2", 3));

    // Column already selected: Rewrite is not possible.
    const std::string secondColBindIllegal = R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView2-b ?b2 .
        BIND(2 * ?s AS ?b1)
      }
    )";
    qpExpect(qlv(), secondColBindIllegal,
             h::Bind(viewScan("bindView2", "?s", "?b2",
                              "?_ql_materialized_view_o", 2),
                     "2 * ?s", V{"?b1"}));
  }

  // The `BIND` is the third column of the view.
  {
    const std::string thirdColBind = R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView-o ?o .
        BIND(15 AS ?bind)
      }
    )";
    qpExpect(qlv(), thirdColBind, viewScan("bindView", "?s", "?o", "?bind", 3));

    // Column already selected: Rewrite is not possible.
    const std::string thirdColBindIllegal = R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView-b1 ?x2 .
        BIND(2 * ?s AS ?x1)
      }
    )";
    qpExpect(qlv(), thirdColBindIllegal,
             h::Bind(viewScan("bindView", "?s", "?_ql_materialized_view_p",
                              "?x2", 2),
                     "2 * ?s", V{"?x1"}));
  }

  // Test the variable to permutation column index map.
  {
    // The column `?b3` is the fifth column in the permutation, but the second
    // in the scan result.
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        ?s view:bindView-b3 ?b3 .
      }
    )");
    auto indexScan = dynamic_cast<IndexScan&>(*qet->getRootOperation());

    VariableToColumnMap expectedVarToColResult{
        {V{"?s"}, makeAlwaysDefinedColumn(0)},
        {V{"?b3"}, makePossiblyUndefinedColumn(1)},
    };
    EXPECT_THAT(indexScan.getExternallyVisibleVariableColumns(),
                ::testing::UnorderedElementsAreArray(expectedVarToColResult));

    VariableToColumnMap expectedVarToColPermutation{
        {V{"?s"}, makeAlwaysDefinedColumn(0)},
        {V{"?b3"}, makePossiblyUndefinedColumn(4)},
    };
    EXPECT_THAT(
        indexScan.computePermutationColumnIndices(),
        ::testing::UnorderedElementsAreArray(expectedVarToColPermutation));
  }
  {
    // The same as above, but including `BIND` rewriting, a fixed subject for
    // the materialized view scan and different variable names.
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      PREFIX math: <http://www.w3.org/2005/xpath-functions/math#>
      PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
      SELECT * {
        <s1> view:bindView-o ?x .
        BIND(math:cos(?x - 1) + 4 AS ?y)
      }
    )");
    auto indexScan = dynamic_cast<IndexScan&>(*qet->getRootOperation());

    VariableToColumnMap expectedVarToColResult{
        {V{"?x"}, makeAlwaysDefinedColumn(0)},
        {V{"?y"}, makeAlwaysDefinedColumn(1)},
    };
    EXPECT_THAT(indexScan.getExternallyVisibleVariableColumns(),
                ::testing::UnorderedElementsAreArray(expectedVarToColResult));

    VariableToColumnMap expectedVarToColPermutation{
        {V{"?x"}, makeAlwaysDefinedColumn(1)},
        {V{"?y"}, makeAlwaysDefinedColumn(4)},
    };
    EXPECT_THAT(
        indexScan.computePermutationColumnIndices(),
        ::testing::UnorderedElementsAreArray(expectedVarToColPermutation));
  }
}

// Example dataset for usage of `MaterializedView` with `SpatialJoin`.
constexpr std::string_view geoTtl = R"'(
    @prefix geo: <http://www.opengis.net/ont/geosparql#> .
    <s1> geo:hasGeometry <m1> .
    <m1> geo:asWKT "POINT(1 2)"^^geo:wktLiteral .
    <s2> geo:hasGeometry <m2> .
    <m2> geo:asWKT "LINESTRING(7.8341918 48.014192,7.8342038 48.0135509)"^^geo:wktLiteral .
    <s3> geo:hasGeometry <m3> .
    <m3> geo:asWKT "POINT(7.841295 47.997731)"^^geo:wktLiteral .
    <s1> <name> "Demo" .
    <s2> <is> <demo> .
    <s3> <is> <demo> .
    <s3> <is> <demo2> .
    <s4> geo:hasGeometry <m4> .
    <m4> geo:asWKT "An Invalid Wkt Literal Does Not Throw"^^geo:wktLiteral .
)'";
constexpr std::string_view geoBoundingBoxesViewQuery = R"(
  PREFIX geo: <http://www.opengis.net/ont/geosparql#>
  PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
  SELECT ?osm_id ?intermediate ?geometry ?lower_left ?upper_right ?centroid {
    ?osm_id geo:hasGeometry ?intermediate .
    ?intermediate geo:asWKT ?geometry .
    BIND (ql:envelopeLowerLeft(?geometry) AS ?lower_left)
    BIND (ql:envelopeUpperRight(?geometry) AS ?upper_right)
    BIND (geof:centroid(?geometry) AS ?centroid)
  }
)";

// Automatic `BIND` push-down for bounding boxes in `SpatialJoin`.
TEST(MaterializedViewsSpatialJoinTest, BoundingBoxBindRewrite) {
  const std::string onDiskBase = "_materializedViewRewriteSpatialJoin";
  const std::string viewName = "geoms";

  // Initialize engine on test index.
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, std::string{geoTtl});
  auto cleanUp = absl::MakeCleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};

  // Write geometries view with bounding boxes.
  qlv.writeMaterializedView(viewName, std::string{geoBoundingBoxesViewQuery});
  qlv.loadMaterializedView(viewName);

  // Running the same query for reading that was used for writing results in a
  // single `IndexScan` for all columns of the materialized view.
  qpExpect(qlv, geoBoundingBoxesViewQuery,
           viewScan(viewName, "?osm_id", "?intermediate", "?geometry", 6,
                    {
                        {3, V{"?lower_left"}},
                        {4, V{"?upper_right"}},
                        {5, V{"?centroid"}},
                    }));

  // Check for the correct `VariableToColumnMap`, in particular, the correct
  // `UndefStatus`.
  {
    auto [qet, qec, parsed] =
        qlv.parseAndPlanQuery(std::string{geoBoundingBoxesViewQuery});
    VariableToColumnMap expected{
        {V{"?osm_id"}, makeAlwaysDefinedColumn(0)},
        {V{"?intermediate"}, makeAlwaysDefinedColumn(1)},
        {V{"?geometry"}, makeAlwaysDefinedColumn(2)},
        {V{"?lower_left"}, makePossiblyUndefinedColumn(3)},
        {V{"?upper_right"}, makePossiblyUndefinedColumn(4)},
        {V{"?centroid"}, makePossiblyUndefinedColumn(5)}};
    EXPECT_THAT(qet->getVariableColumns(),
                ::testing::UnorderedElementsAreArray(expected));
  }

  // A `SpatialJoin` adds the bounding box columns to its child `IndexScan`s
  // automatically, even if there is a `Join` operation in between.
  const std::string spatialJoinQuery = R"(
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * {
        # Force materialized view (the test dataset is too small for the view's
        # query plan to reliably have lower cost than the standard plan).
        ?osm_id1 view:geoms-geometry ?geometry1 .
        ?osm_id2 view:geoms-geometry ?geometry2 .
        ?osm_id1 <is> <demo> .
        FILTER geof:sfIntersects(?geometry1, ?geometry2)
    }
  )";
  {
    auto [qet, qec, parsed] = qlv.parseAndPlanQuery(spatialJoinQuery);
    auto sjMatcher = h::spatialJoin(
        -1, -1, V{"?geometry1"}, V{"?geometry2"}, std::nullopt,
        PayloadVariables::all(), SpatialJoinAlgorithm::LIBSPATIALJOIN,
        SpatialJoinType::INTERSECTS,
        // Push down of automatic `BIND`s through a `Join`.
        h::Join(viewScan(viewName, "?osm_id1", "?_ql_materialized_view_p",
                         "?geometry1", 4,
                         {{3, V{"?_ql_sj_ll_geometry1"}},
                          {4, V{"?_ql_sj_ur_geometry1"}}}),
                h::IndexScanFromStrings("?osm_id1", "<is>", "<demo>")),
        // Push down of automatic `BIND`s into direct child.
        viewScan(
            viewName, "?osm_id2", "?_ql_materialized_view_p", "?geometry2", 4,
            {{3, V{"?_ql_sj_ll_geometry2"}}, {4, V{"?_ql_sj_ur_geometry2"}}}));
    // The query plan contains the pushed down columns.
    EXPECT_THAT(*qet, sjMatcher);

    // Prefiltering using the pushed down columns is actually applied on
    // evaluation of the query plan.
    auto res = qet->getResult();
    const auto& runtimeInfo = qet->getRootOperation()->runtimeInfo().details_;
    ASSERT_TRUE(runtimeInfo.contains("num-geoms-dropped-by-prefilter"));
    EXPECT_EQ(runtimeInfo.at("num-geoms-dropped-by-prefilter"), 3);
  }
}

// Example queries for testing query rewriting.
constexpr std::string_view simpleChain = "SELECT * { ?s <p1> ?m . ?m <p2> ?o }";
constexpr std::string_view simpleChainRenamed =
    "SELECT * { ?b <p2> ?c . ?a <p1> ?b }";
constexpr std::string_view simpleChainFixed =
    "SELECT * {  <s2> <p1>/<p2> ?c . }";
constexpr std::string_view simpleChainPlusJoin =
    "SELECT * { ?s <p1>/<p2> ?o . ?s <p3> ?o2 }";
constexpr std::string_view simpleChainRenamedPlusBind =
    "SELECT ?a ?b ?c ?x { ?b <p2> ?c . ?a <p1> ?b . BIND(5 AS ?x) }";
constexpr std::string_view simpleChainDifferentSort =
    "SELECT ?m ?s ?o { ?s <p1> ?m . ?m <p2> ?o }";
constexpr std::string_view overlappingChains =
    "SELECT * { ?s <p1> ?m . ?m <p2> ?o1 . ?m <p2> ?o2 }";

// _____________________________________________________________________________
TEST_P(MaterializedViewsChainRewriteTest, simpleChain) {
  RewriteTestParams p = GetParam();
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::queryPlanningBudget_>(
          p.queryPlanningBudget_);

  // Test dataset and query.
  const std::string chainTtl =
      " <s1> <p1> <m2> . \n"
      " <m1> <p2> <o1> . \n"
      " <s2> <p1> <m2> . \n"
      " <m2> <p2> <http://example.com/> . \n"
      " <m2> <p3> \"abc\" . \n"
      " <s2> <p3> <o3> . \n";
  const std::string onDiskBase = "_materializedViewRewriteChain";
  const std::string viewName = "testViewChain";

  // Initialized libqlever.
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, chainTtl);
  auto cleanUp = absl::MakeCleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};

  // Without the materialized view, a regular join is executed.
  h::expect(std::string{simpleChain},
            h::Join(h::IndexScanFromStrings("?s", "<p1>", "?m"),
                    h::IndexScanFromStrings("?m", "<p2>", "?o")));

  // Write a chain structure to the materialized view.
  qlv.writeMaterializedView(viewName, p.writeQuery_);
  qlv.loadMaterializedView(viewName);
  auto chainView = std::bind_front(&viewScanSimple, viewName);

  // With the materialized view loaded, an index scan on the view is performed
  // instead of a regular join.
  qpExpect(qlv, simpleChain, chainView("?s", "?m", "?o"));
  qpExpect(qlv, simpleChainRenamed, chainView("?a", "?b", "?c"));
  qpExpect(qlv, simpleChainFixed,
           chainView("<s2>", "?_QLever_internal_variable_qp_0", "?c"));
  qpExpect(qlv, simpleChainPlusJoin,
           h::Join(chainView("?s", "?_QLever_internal_variable_qp_0", "?o"),
                   h::IndexScanFromStrings("?s", "<p3>", "?o2")));

  // If the view is sorted such that the subject of the chain is not the first
  // column, rewriting cannot be applied with a fixed subject.
  qlv.writeMaterializedView(viewName, std::string{simpleChainDifferentSort});
  qlv.loadMaterializedView(viewName);
  qpExpect(qlv, simpleChainFixed,
           h::Join(h::IndexScanFromStrings("<s2>", "<p1>",
                                           "?_QLever_internal_variable_qp_0"),
                   h::IndexScanFromStrings("?_QLever_internal_variable_qp_0",
                                           "<p2>", "?c")));

  // Test overlapping view plans: the rewriting can be applied but the remaining
  // triple must be joined normally.
  auto firstRewritten = h::Join(chainView("?m", "?s", "?o1"),
                                h::IndexScanFromStrings("?m", "<p2>", "?o2"));
  auto secondRewritten = h::Join(chainView("?m", "?s", "?o2"),
                                 h::IndexScanFromStrings("?m", "<p2>", "?o1"));
  qpExpect(qlv, overlappingChains,
           ::testing::AnyOf(firstRewritten, secondRewritten));
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(
    MaterializedViewsTest, MaterializedViewsChainRewriteTest,
    ::testing::Values(
        // Default case.
        RewriteTestParams{std::string{simpleChain}, 1500},

        // Default query for writing the materialized view, but forced greedy
        // planning.
        RewriteTestParams{std::string{simpleChain}, 1},

        // An additional `BIND` is ignored and the view can still be used for
        // query rewriting. Also uses a different sorting.
        RewriteTestParams{std::string{simpleChainRenamedPlusBind}, 1500}));
