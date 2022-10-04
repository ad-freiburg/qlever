//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./IndexTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/ValueIdComparators.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"

#pragma once

namespace sparqlExpression {
/// Dummy expression for testing, that for `evaluate` returns the `result`
/// that is specified in the constructor.
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

// Make a `ValueId` from an int. Shorter name, as it will be used often.
ValueId IntId(int64_t i) { return ValueId::makeFromInt(i); }

// Make a `ValueId` from a double. Shorter name, as it will be used often.
ValueId DoubleId(double d) { return ValueId::makeFromDouble(d); }

// Create an `Index`, the vocabulary of which contains the literals `"alpha",
// "älpha", "A", "Beta"`. The subjects and predicates of the triples are not
// important for these unit tests.
Index makeTestIndex() {
  // Ignore the (irrelevant) log output of the index building and loading during
  // these tests.
  static std::ostringstream ignoreLogStream;
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  std::string filename = "relationalExpressionTestIndex.ttl";
  std::string dummyKb =
      "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . <x> "
      "<label> \"Beta\".";

  FILE_BUFFER_SIZE() = 1000;
  std::fstream f(filename, std::ios_base::out);
  f << dummyKb;
  f.close();
  std::string indexBasename = "_relationalExpressionTestIndex";
  {
    Index index = makeIndexWithTestSettings();
    index.setOnDiskBase(indexBasename);
    index.createFromFile<TurtleParserAuto>(filename);
  }
  Index index;
  index.createFromOnDiskIndex(indexBasename);
  return index;
}

// Return a static  `QueryExecutionContext` that refers to an index that was
// build using `makeTestIndex()` (see above). The index (most notably its
// vocabulary) is the only part of the `QueryExecutionContext` that is actually
// relevant for these tests, so the other members are defaultet.
static QueryExecutionContext* getQec() {
  static ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)};
  static const Index index = makeTestIndex();
  static const Engine engine{};
  static QueryResultCache cache{};
  static QueryExecutionContext qec{
      index,
      engine,
      &cache,
      ad_utility::AllocatorWithLimit<Id>{
          ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)},
      {}};
  return &qec;
}

// Struct that stores a `sparqlExpression::EvaluationContext` and all the data
// structures that this context refers to. Most importantly it uses the
// `QueryExecutionContext` from `getQec()` (see above), and  has an `inputTable`
// that refers to a previous partial query result with multiple columns of
// various types. For details see the constructor.
struct TestContext {
  QueryExecutionContext* qec = getQec();
  sparqlExpression::VariableToColumnAndResultTypeMap varToColMap;
  ResultTable::LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{*getQec(), varToColMap, table,
                                              qec->getAllocator(), localVocab};
  TestContext() {
    // First get some IDs for strings from the vocabulary to later reuse them
    Id alpha;
    Id aelpha;
    Id A;
    Id Beta;

    bool b = qec->getIndex().getId("\"alpha\"", &alpha);
    AD_CHECK(b);
    b = qec->getIndex().getId("\"älpha\"", &aelpha);
    AD_CHECK(b);
    b = qec->getIndex().getId("\"A\"", &A);
    AD_CHECK(b);
    b = qec->getIndex().getId("\"Beta\"", &Beta);
    AD_CHECK(b);

    // Set up the `table` that represents the previous partial query results. It
    // has five columns/variables: ?ints (only integers), ?doubles (only
    // doubles), ?numeric (int and double), ?vocab (only entries from the
    // vocabulary), ?mixed (all of the previous). None of the columns is sorted.
    table.setCols(5);
    // Order of the columns:
    // ?ints ?doubles ?numeric ?vocab ?mixed
    table.push_back({IntId(1), DoubleId(0.1), IntId(1), Beta, IntId(1)});
    table.push_back(
        {IntId(0), DoubleId(-.1), DoubleId(-.1), alpha, DoubleId(-.1)});
    table.push_back({IntId(-1), DoubleId(2.8), DoubleId(3.4), aelpha, A});

    context._beginIndex = 0;
    context._endIndex = table.size();
    // Define the mapping from variable names to column indices.
    varToColMap["?ints"] = {0, qlever::ResultType::KB};
    varToColMap["?doubles"] = {1, qlever::ResultType::KB};
    varToColMap["?numeric"] = {2, qlever::ResultType::KB};
    varToColMap["?vocab"] = {3, qlever::ResultType::KB};
    varToColMap["?mixed"] = {4, qlever::ResultType::KB};
  }

  // Get a test context where the rows are the same as by default, but sorted by
  // `variable`
  static TestContext sortedBy(const Variable& variable) {
    TestContext result;
    auto columnIndex = result.varToColMap.at(variable.name()).first;
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
void PrintTo(const SetOfIntervals& set, std::ostream* os) {
  for (auto [first, second] : set._intervals) {
    *os << '{' << first << ", " << second << '}';
  }
}
}  // namespace ad_utility
