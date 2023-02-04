//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./IndexTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/ValueIdComparators.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"

#pragma once

namespace sparqlExpression {
// Dummy expression for testing, that for `evaluate` returns the `result` that
// is specified in the constructor.
struct DummyExpression : public SparqlExpression {
  explicit DummyExpression(ExpressionResult result)
      : _result{std::move(result)} {}
  mutable ExpressionResult _result;
  ExpressionResult evaluate(EvaluationContext*) const override {
    return std::move(_result);
  }
  vector<std::string> getUnaggregatedVariables() override { return {}; }
  string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return "DummyDummyDummDumm";
  }

  std::span<SparqlExpression::Ptr> children() override { return {}; }
};

// Make a `ValueId` from an int/ a double. Shorter name, as it will be used
// often.
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;

// Struct that stores a `sparqlExpression::EvaluationContext` and all the data
// structures that this context refers to. Most importantly it uses the
// `QueryExecutionContext` from `getQec()` (see above), and  has an `inputTable`
// that refers to a previous partial query result with multiple columns of
// various types. For details see the constructor.
struct TestContext {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  sparqlExpression::VariableToColumnAndResultTypeMap varToColMap;
  LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{*qec, varToColMap, table,
                                              qec->getAllocator(), localVocab};
  TestContext() {
    // First get some IDs for strings from the vocabulary to later reuse them
    Id alpha;
    Id aelpha;
    Id A;
    Id Beta;

    bool b = qec->getIndex().getId("\"alpha\"", &alpha);
    AD_CONTRACT_CHECK(b);
    b = qec->getIndex().getId("\"älpha\"", &aelpha);
    AD_CONTRACT_CHECK(b);
    b = qec->getIndex().getId("\"A\"", &A);
    AD_CONTRACT_CHECK(b);
    b = qec->getIndex().getId("\"Beta\"", &Beta);
    AD_CONTRACT_CHECK(b);

    // Set up the `table` that represents the previous partial query results. It
    // has five columns/variables: ?ints (only integers), ?doubles (only
    // doubles), ?numeric (int and double), ?vocab (only entries from the
    // vocabulary), ?mixed (all of the previous). None of the columns is sorted.
    table.setNumColumns(5);
    // Order of the columns:
    // ?ints ?doubles ?numeric ?vocab ?mixed
    table.push_back({IntId(1), DoubleId(0.1), IntId(1), Beta, IntId(1)});
    table.push_back(
        {IntId(0), DoubleId(-.1), DoubleId(-.1), alpha, DoubleId(-.1)});
    table.push_back({IntId(-1), DoubleId(2.8), DoubleId(3.4), aelpha, A});

    context._beginIndex = 0;
    context._endIndex = table.size();
    // Define the mapping from variable names to column indices.
    using V = Variable;
    varToColMap[V{"?ints"}] = {0, qlever::ResultType::KB};
    varToColMap[V{"?doubles"}] = {1, qlever::ResultType::KB};
    varToColMap[V{"?numeric"}] = {2, qlever::ResultType::KB};
    varToColMap[V{"?vocab"}] = {3, qlever::ResultType::KB};
    varToColMap[V{"?mixed"}] = {4, qlever::ResultType::KB};
  }

  // Get a test context where the rows are the same as by default, but sorted by
  // `variable`
  static TestContext sortedBy(const Variable& variable) {
    TestContext result;
    auto columnIndex = result.varToColMap.at(variable).first;
    std::sort(result.table.begin(), result.table.end(),
              [columnIndex](const auto& a, const auto& b) {
                return valueIdComparators::compareByBits(a[columnIndex],
                                                         b[columnIndex]);
              });
    result.context._columnsByWhichResultIsSorted.push_back(columnIndex);
    return result;
  }
};
}  // namespace sparqlExpression

// Add output for `SetOfIntervals for Gtest.
namespace ad_utility {
inline void PrintTo(const SetOfIntervals& set, std::ostream* os) {
  for (auto [first, second] : set._intervals) {
    *os << '{' << first << ", " << second << '}';
  }
}
}  // namespace ad_utility
