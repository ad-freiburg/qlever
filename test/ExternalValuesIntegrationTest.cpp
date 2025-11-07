//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Generated with Claude Code

#include <gtest/gtest.h>

#include "engine/ExternallySpecifiedValues.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryPlanner.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlParser.h"
#include "util/IndexTestHelpers.h"

using namespace parsedQuery;

// Test that SERVICE ql:external-values is parsed correctly
TEST(ExternalValuesIntegration, parseServiceClause) {
  std::string query = R"(
    PREFIX ql: <https://qlever.cs.uni-freiburg.de/>
    SELECT * WHERE {
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values-test123> {
        [] <variables> ?x .
        [] <variables> ?y .
      }
    }
  )";

  ParsedQuery pq = SparqlParser::parseQuery(query);

  // Check that the graph pattern contains an ExternalValuesQuery
  ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1u);

  const auto& graphPattern = pq._rootGraphPattern._graphPatterns[0];
  const auto* externalValuesQuery =
      std::get_if<ExternalValuesQuery>(&graphPattern);

  ASSERT_NE(externalValuesQuery, nullptr);
  EXPECT_EQ(externalValuesQuery->identifier_, "test123");
  ASSERT_EQ(externalValuesQuery->variables_.size(), 2u);
  EXPECT_EQ(externalValuesQuery->variables_[0], Variable{"?x"});
  EXPECT_EQ(externalValuesQuery->variables_[1], Variable{"?y"});
}

// Test that the query planner creates an ExternallySpecifiedValues operation
TEST(ExternalValuesIntegration, queryPlannerCreatesOperation) {
  std::string query = R"(
    PREFIX ql: <https://qlever.cs.uni-freiburg.de/>
    SELECT * WHERE {
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values-mytest> {
        [] <variables> ?a .
        [] <variables> ?b .
      }
    }
  )";

  auto qec = ad_utility::testing::getQec();
  ParsedQuery pq = SparqlParser::parseQuery(query);
  QueryPlanner qp(qec, pq.makeExecutionContext(qec));
  auto qet = qp.createExecutionTree(pq._rootGraphPattern);

  ASSERT_NE(qet, nullptr);

  // Get the operation from the execution tree
  auto* operation = qet->getRootOperation();
  ASSERT_NE(operation, nullptr);

  // Collect external values
  std::vector<ExternallySpecifiedValues*> externalValues;
  operation->getExternalValues(externalValues);

  ASSERT_EQ(externalValues.size(), 1u);
  EXPECT_EQ(externalValues[0]->getIdentifier(), "mytest");
  EXPECT_EQ(externalValues[0]->getResultWidth(), 2u);
}

// Test updating values and executing query
TEST(ExternalValuesIntegration, updateAndExecute) {
  std::string query = R"(
    PREFIX ql: <https://qlever.cs.uni-freiburg.de/>
    SELECT * WHERE {
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values-exec-test> {
        [] <variables> ?x .
        [] <variables> ?y .
      }
    }
  )";

  auto qec = ad_utility::testing::getQec("<a> <b> <c> .");
  ParsedQuery pq = SparqlParser::parseQuery(query);
  QueryPlanner qp(qec, pq.makeExecutionContext(qec));
  auto qet = qp.createExecutionTree(pq._rootGraphPattern);

  // Collect external values
  std::vector<ExternallySpecifiedValues*> externalValues;
  qet->getRootOperation()->getExternalValues(externalValues);

  ASSERT_EQ(externalValues.size(), 1u);
  auto* externalOp = externalValues[0];

  // Update with actual values
  using TC = TripleComponent;
  std::vector<std::vector<TC>> values{{TC{1}, TC{2}}, {TC{3}, TC{4}}};
  parsedQuery::SparqlValues newValues{{Variable{"?x"}, Variable{"?y"}}, values};
  externalOp->updateValues(std::move(newValues));

  // Execute and check result
  auto result = qet->getResult();
  ASSERT_NE(result, nullptr);
  const auto& table = result->idTable();

  EXPECT_EQ(table.numRows(), 2u);
  EXPECT_EQ(table.numColumns(), 2u);
}

// Test that duplicate variables are rejected
TEST(ExternalValuesIntegration, rejectDuplicateVariables) {
  std::string query = R"(
    PREFIX ql: <https://qlever.cs.uni-freiburg.de/>
    SELECT * WHERE {
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values-dup-test> {
        [] <variables> ?x .
        [] <variables> ?x .
      }
    }
  )";

  auto qec = ad_utility::testing::getQec();
  ParsedQuery pq = SparqlParser::parseQuery(query);
  QueryPlanner qp(qec, pq.makeExecutionContext(qec));

  // Should throw due to duplicate variables
  EXPECT_DEATH(qp.createExecutionTree(pq._rootGraphPattern), "");
}

// Test getExternalValues with nested operations
TEST(ExternalValuesIntegration, getExternalValuesNested) {
  std::string query = R"(
    PREFIX ql: <https://qlever.cs.uni-freiburg.de/>
    SELECT * WHERE {
      {
        SERVICE <https://qlever.cs.uni-freiburg.de/external-values-first> {
          [] <variables> ?a .
        }
      } UNION {
        SERVICE <https://qlever.cs.uni-freiburg.de/external-values-second> {
          [] <variables> ?b .
        }
      }
    }
  )";

  auto qec = ad_utility::testing::getQec();
  ParsedQuery pq = SparqlParser::parseQuery(query);
  QueryPlanner qp(qec, pq.makeExecutionContext(qec));
  auto qet = qp.createExecutionTree(pq._rootGraphPattern);

  // Collect all external values from the tree
  std::vector<ExternallySpecifiedValues*> externalValues;
  qet->getRootOperation()->getExternalValues(externalValues);

  // Should find both external values operations
  ASSERT_EQ(externalValues.size(), 2u);

  // Check identifiers (order may vary)
  std::set<std::string> identifiers;
  for (const auto* op : externalValues) {
    identifiers.insert(op->getIdentifier());
  }

  EXPECT_TRUE(identifiers.count("first") > 0);
  EXPECT_TRUE(identifiers.count("second") > 0);
}
