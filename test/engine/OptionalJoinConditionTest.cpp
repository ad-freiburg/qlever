// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for an `OPTIONAL` with a `FILTER` that uses variables from outside of
// the `OPTIONAL`. By the SPARQL 1.1 standard (section 18.2.2.6 and the
// definition of `LeftJoin` in section 18.5), such a `FILTER` is the condition
// of the left join:
//
//   LeftJoin(Ω1, Ω2, expr) = Filter(expr, Join(Ω1, Ω2)) ∪ Diff(Ω1, Ω2, expr)
//
// The first part of the tests compares `OptionalJoin` with a condition to a
// direct implementation of this definition on random inputs, for all the
// different join algorithms. The second part runs complete queries.

#include <gmock/gmock.h>

#include <map>
#include <random>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/ParsedQueryTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryPlanner.h"
#include "engine/StripColumns.h"
#include "global/RuntimeParameters.h"

using namespace ad_utility::testing;

namespace {

constexpr auto U = Id::makeUndefined();
using Row = std::vector<Id>;
using Binding = std::map<std::string, Id>;
using Condition = std::function<bool(const Binding&)>;

// A relation with named columns, e.g. `{"?x", "?y"}`.
struct Relation {
  std::vector<std::string> variables_;
  std::vector<Row> rows_;
};

// The value of `var` in `binding` as an integer, or `std::nullopt` if it is
// unbound. All test data consists of integers.
std::optional<int64_t> getInt(const Binding& binding, const std::string& var) {
  auto it = binding.find(var);
  if (it == binding.end()) {
    return std::nullopt;
  }
  return it->second.getInt();
}

// Direct implementation of `LeftJoin(left, right, condition)` from section
// 18.5 of the SPARQL 1.1 standard. UNDEF means that the variable is not bound.
// The result has one column per variable in `resultVariables`.
std::vector<Row> referenceLeftJoin(const Relation& left, const Relation& right,
                                   const Condition& condition,
                                   const std::vector<std::string>& resultVars) {
  auto toBinding = [](const Relation& relation, const Row& row) {
    Binding binding;
    for (size_t i = 0; i < row.size(); ++i) {
      if (!row[i].isUndefined()) {
        binding[relation.variables_[i]] = row[i];
      }
    }
    return binding;
  };
  std::vector<Row> result;
  auto addToResult = [&](const Binding& binding) {
    Row row;
    for (const auto& var : resultVars) {
      auto it = binding.find(var);
      row.push_back(it == binding.end() ? U : it->second);
    }
    result.push_back(std::move(row));
  };
  for (const auto& leftRow : left.rows_) {
    Binding mu1 = toBinding(left, leftRow);
    bool hasMatch = false;
    for (const auto& rightRow : right.rows_) {
      Binding mu2 = toBinding(right, rightRow);
      bool compatible = ql::ranges::all_of(mu2, [&mu1](const auto& el) {
        auto it = mu1.find(el.first);
        return it == mu1.end() || it->second == el.second;
      });
      if (!compatible) {
        continue;
      }
      Binding merged = mu1;
      merged.insert(mu2.begin(), mu2.end());
      // First part: `Filter(expr, Join(Ω1, Ω2))`.
      if (condition(merged)) {
        addToResult(merged);
        hasMatch = true;
      }
    }
    // Second part: `Diff(Ω1, Ω2, expr)`, the left rows without any compatible
    // partner for which the condition holds.
    if (!hasMatch) {
      addToResult(mu1);
    }
  }
  return result;
}

// Parse a SPARQL expression, e.g. `?y < ?z`.
sparqlExpression::SparqlExpressionPimpl parseExpression(
    const std::string& expression) {
  auto query = parseQuery(
      absl::StrCat("SELECT * { ?s ?p ?o FILTER(", expression, ") }"));
  return query._rootGraphPattern._filters.at(0).expression_;
}

// The conditions that are used in the tests, as SPARQL and as a C++ function
// with the same semantics (an unbound variable leads to an error, which is not
// `true`).
struct TestCondition {
  std::string sparql_;
  Condition function_;
};

const std::vector<TestCondition>& testConditions() {
  static const std::vector<TestCondition> conditions{
      // Uses a non-join variable from each side.
      {"?y < ?z",
       [](const Binding& b) {
         auto y = getInt(b, "?y");
         auto z = getInt(b, "?z");
         return y && z && *y < *z;
       }},
      // Also uses the join variable `?x`, which in the merged row can come
      // from the right side if it is UNDEF in the left row.
      {"?x + ?y > ?z",
       [](const Binding& b) {
         auto x = getInt(b, "?x");
         auto y = getInt(b, "?y");
         auto z = getInt(b, "?z");
         return x && y && z && *x + *y > *z;
       }},
      // Uses only a variable from the left side.
      {"?y >= 5",
       [](const Binding& b) {
         auto y = getInt(b, "?y");
         return y && *y >= 5;
       }},
      // Never true.
      {"?y < ?y", [](const Binding&) { return false; }},
  };
  return conditions;
}

// Random relation with `numRows` rows over `variables`. Each value is an
// integer in `[0, maxValue]` or UNDEF with probability `undefProbability[i]`
// for column `i`.
Relation randomRelation(std::mt19937& gen, std::vector<std::string> variables,
                        size_t numRows, int64_t maxValue,
                        std::vector<double> undefProbability) {
  Relation relation{std::move(variables), {}};
  std::uniform_int_distribution<int64_t> valueDist{0, maxValue};
  std::uniform_real_distribution<double> undefDist{0.0, 1.0};
  for (size_t i = 0; i < numRows; ++i) {
    Row row;
    for (size_t col = 0; col < relation.variables_.size(); ++col) {
      if (undefDist(gen) < undefProbability.at(col)) {
        row.push_back(U);
      } else {
        row.push_back(IntId(valueDist(gen)));
      }
    }
    relation.rows_.push_back(std::move(row));
  }
  return relation;
}

IdTable toIdTable(const Relation& relation) {
  IdTable table{relation.variables_.size(), makeAllocator()};
  for (const auto& row : relation.rows_) {
    table.emplace_back();
    for (size_t col = 0; col < row.size(); ++col) {
      table.back()[col] = row[col];
    }
  }
  return table;
}

std::vector<std::optional<Variable>> toVariables(const Relation& relation) {
  std::vector<std::optional<Variable>> variables;
  for (const auto& var : relation.variables_) {
    variables.emplace_back(Variable{var});
  }
  return variables;
}

std::vector<Row> toRows(const IdTableView<0>& table) {
  std::vector<Row> rows;
  for (const auto& row : table) {
    rows.emplace_back(row.begin(), row.end());
  }
  return rows;
}

// Split the rows of `relation` into blocks of `blockSize` rows.
std::vector<IdTable> splitIntoBlocks(const Relation& relation,
                                     size_t blockSize) {
  std::vector<IdTable> blocks;
  for (size_t i = 0; i < relation.rows_.size(); i += blockSize) {
    Relation block{relation.variables_, {}};
    for (size_t j = i; j < std::min(i + blockSize, relation.rows_.size());
         ++j) {
      block.rows_.push_back(relation.rows_[j]);
    }
    blocks.push_back(toIdTable(block));
  }
  return blocks;
}

// An `OptionalJoin` of the materialized relations `left` and `right` with the
// conjunction of the SPARQL expressions `conditions` as its condition.
OptionalJoin makeJoin(const Relation& left, const Relation& right,
                      const std::vector<std::string>& conditions,
                      bool keepJoinColumns = true) {
  auto* qec = getQec();
  qec->getQueryTreeCache().clearAll();
  OptionalJoin::JoinCondition condition;
  for (const auto& expression : conditions) {
    condition.push_back(parseExpression(expression));
  }
  return OptionalJoin{qec,
                      ad_utility::makeExecutionTree<ValuesForTesting>(
                          qec, toIdTable(left), toVariables(left)),
                      ad_utility::makeExecutionTree<ValuesForTesting>(
                          qec, toIdTable(right), toVariables(right)),
                      keepJoinColumns, std::move(condition)};
}

// How the inputs are passed to the `OptionalJoin`.
enum struct InputMode {
  // Both inputs are fully materialized and sorted by a `Sort` operation.
  Materialized,
  // The inputs are already sorted and the right input is split into several
  // blocks that are yielded lazily.
  LazyRightInBlocks,
};

// Sort `relation` by the columns `cols` (in this order). UNDEF is the smallest
// `Id`, as required by the join algorithms.
void sortBy(Relation& relation, const std::vector<ColumnIndex>& cols) {
  ql::ranges::stable_sort(relation.rows_, [&cols](const Row& a, const Row& b) {
    for (auto col : cols) {
      if (a[col] != b[col]) {
        return a[col] < b[col];
      }
    }
    return false;
  });
}

// Run the `OptionalJoin` of `left` and `right` with `condition` and compare
// the result to `referenceLeftJoin`. The join columns are the variables that
// occur in both relations.
void testAgainstReference(
    Relation left, Relation right, const TestCondition& condition,
    InputMode mode, size_t blockSize = 3,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto* qec = getQec();
  qec->getQueryTreeCache().clearAll();

  std::shared_ptr<QueryExecutionTree> leftTree;
  std::shared_ptr<QueryExecutionTree> rightTree;
  if (mode == InputMode::Materialized) {
    // No sorted columns are given, so the `OptionalJoin` adds `Sort`
    // operations, which yield materialized results.
    leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, toIdTable(left), toVariables(left));
    rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, toIdTable(right), toVariables(right));
  } else {
    // Only a single join column `?x` in column 0 is supported here.
    AD_CONTRACT_CHECK(left.variables_.at(0) == "?x" &&
                      right.variables_.at(0) == "?x");
    sortBy(left, {0});
    sortBy(right, {0});
    leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, toIdTable(left), toVariables(left), false,
        std::vector<ColumnIndex>{0});
    auto blocks = splitIntoBlocks(right, blockSize);
    if (blocks.empty()) {
      blocks.push_back(toIdTable(right));
    }
    rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(blocks), toVariables(right), false,
        std::vector<ColumnIndex>{0});
  }

  OptionalJoin join{
      qec, leftTree, rightTree, true,
      OptionalJoin::JoinCondition{parseExpression(condition.sparql_)}};
  auto result = join.computeResultOnlyForTesting(false);
  ASSERT_TRUE(result.isFullyMaterialized());

  // The variables of the result in the order of the columns.
  std::vector<std::string> resultVars(join.getResultWidth());
  for (const auto& [var, info] : join.getExternallyVisibleVariableColumns()) {
    resultVars.at(info.columnIndex_) = var.name();
  }

  auto actual = toRows(result.idTableView());
  auto expected =
      referenceLeftJoin(left, right, condition.function_, resultVars);

  // The result must be sorted as promised by `resultSortedOn`.
  auto sortedOn = join.resultSortedOn();
  EXPECT_TRUE(ql::ranges::is_sorted(actual, [&sortedOn](const Row& a,
                                                        const Row& b) {
    for (auto col : sortedOn) {
      if (a[col] != b[col]) {
        return a[col] < b[col];
      }
    }
    return false;
  })) << "Result is not sorted by the join columns";

  ql::ranges::sort(actual);
  ql::ranges::sort(expected);
  EXPECT_EQ(actual, expected) << "Condition: " << condition.sparql_;
}

}  // namespace

// _____________________________________________________________________________
// Small examples that are easy to check by hand.
TEST(OptionalJoinWithCondition, handwrittenExamples) {
  auto I = IntId;
  Relation left{{"?x", "?y"},
                {{I(1), I(10)}, {I(2), I(20)}, {I(3), I(30)}, {I(4), I(40)}}};
  // `?x = 1`: two partners, one satisfies `?y < ?z`.
  // `?x = 2`: two partners, none satisfies it -> exactly one UNDEF row.
  // `?x = 3`: no partner -> one UNDEF row.
  // `?x = 4`: two partners that both satisfy it.
  Relation right{{"?x", "?z"},
                 {{I(1), I(5)},
                  {I(1), I(15)},
                  {I(2), I(1)},
                  {I(2), I(2)},
                  {I(4), I(41)},
                  {I(4), I(42)}}};
  const auto& lessThan = testConditions().at(0);
  auto join = makeJoin(left, right, {lessThan.sparql_});
  auto result = join.computeResultOnlyForTesting(false);
  auto expected = makeIdTableFromVector({{I(1), I(10), I(15)},
                                         {I(2), I(20), U},
                                         {I(3), I(30), U},
                                         {I(4), I(40), I(41)},
                                         {I(4), I(40), I(42)}});
  EXPECT_EQ(result.idTableView(), expected);
  EXPECT_THAT(join.getDescriptor(), ::testing::HasSubstr("?y < ?z"));
  EXPECT_THAT(join.getCacheKey(), ::testing::HasSubstr("condition: ["));

  // Duplicates in the left input are kept: each copy of `?x = 2` gets its own
  // UNDEF row, each copy of `?x = 1` its own match.
  left.rows_ = {{I(1), I(10)}, {I(1), I(10)}, {I(2), I(20)}, {I(2), I(20)}};
  testAgainstReference(left, right, lessThan, InputMode::Materialized);
  testAgainstReference(left, right, lessThan, InputMode::LazyRightInBlocks, 1);

  // Empty inputs.
  testAgainstReference(Relation{{"?x", "?y"}, {}}, right, lessThan,
                       InputMode::Materialized);
  testAgainstReference(left, Relation{{"?x", "?z"}, {}}, lessThan,
                       InputMode::Materialized);
  testAgainstReference(left, Relation{{"?x", "?z"}, {}}, lessThan,
                       InputMode::LazyRightInBlocks);
}

// _____________________________________________________________________________
// The cache key must contain the condition, otherwise an `OptionalJoin` with a
// condition could reuse the cached result of one without (or with another).
TEST(OptionalJoinWithCondition, cacheKey) {
  Relation left{{"?x", "?y"}, {{IntId(1), IntId(1)}}};
  Relation right{{"?x", "?z"}, {{IntId(1), IntId(2)}}};
  auto noCondition = makeJoin(left, right, {}).getCacheKey();
  auto lessThan = makeJoin(left, right, {"?y < ?z"}).getCacheKey();
  auto greaterThan = makeJoin(left, right, {"?y > ?z"}).getCacheKey();
  EXPECT_NE(noCondition, lessThan);
  EXPECT_NE(lessThan, greaterThan);
}

// _____________________________________________________________________________
// A condition with `RAND()` makes the `OptionalJoin` non-deterministic, so its
// result must not be cached and the operation must not be cloned.
TEST(OptionalJoinWithCondition, nonDeterministicCondition) {
  Relation left{{"?x", "?y"}, {{IntId(1), IntId(1)}}};
  Relation right{{"?x", "?z"}, {{IntId(1), IntId(2)}}};
  auto deterministic = makeJoin(left, right, {"?y < ?z"});
  EXPECT_TRUE(deterministic.isDeterministic());
  EXPECT_TRUE(deterministic.canResultBeCached());
  auto withRand = makeJoin(left, right, {"?y < ?z", "RAND() < 2"});
  EXPECT_FALSE(withRand.isDeterministic());
  EXPECT_FALSE(withRand.canResultBeCached());
}

// _____________________________________________________________________________
// One join column. Depending on the sizes and the UNDEF values this uses the
// zipper join, the galloping join, the zipper join with UNDEF values in the
// materialized case, and the block-wise zipper join in the lazy case.
TEST(OptionalJoinWithCondition, randomOneJoinColumn) {
  std::mt19937 gen{42};
  struct Config {
    size_t sizeLeft_;
    size_t sizeRight_;
    std::vector<double> undefLeft_;
    std::vector<double> undefRight_;
  };
  std::vector<Config> configs{
      // No UNDEF in the join column.
      {20, 20, {0, 0.1}, {0, 0.1}},
      {50, 10, {0, 0}, {0, 0}},
      {1, 1, {0, 0}, {0, 0}},
      // Galloping join (right is more than 1000 times larger).
      {2, 3000, {0, 0}, {0, 0.1}},
      // UNDEF in the join column of the left input.
      {20, 20, {0.3, 0.1}, {0, 0}},
      // UNDEF in the join column of the right input, or both.
      {20, 20, {0, 0}, {0.3, 0.1}},
      {30, 30, {0.3, 0.1}, {0.3, 0.1}},
      // Only UNDEF in the join columns.
      {5, 5, {1, 0}, {1, 0}},
  };
  for (const auto& condition : testConditions()) {
    for (const auto& c : configs) {
      for (size_t iteration = 0; iteration < 5; ++iteration) {
        auto left =
            randomRelation(gen, {"?x", "?y"}, c.sizeLeft_, 6, c.undefLeft_);
        auto right =
            randomRelation(gen, {"?x", "?z"}, c.sizeRight_, 6, c.undefRight_);
        testAgainstReference(left, right, condition, InputMode::Materialized);
        testAgainstReference(left, right, condition,
                             InputMode::LazyRightInBlocks, 1 + iteration);
      }
    }
  }
}

// _____________________________________________________________________________
// Two join columns `?x` and `?w`. With UNDEF values only in the last join
// column of the left input, the `specialOptionalJoin` is used, otherwise the
// general zipper join with UNDEF values.
TEST(OptionalJoinWithCondition, randomTwoJoinColumns) {
  std::mt19937 gen{7};
  struct Config {
    std::vector<double> undefLeft_;
    std::vector<double> undefRight_;
  };
  std::vector<Config> configs{
      {{0, 0, 0}, {0, 0, 0}},
      {{0, 0.3, 0.1}, {0, 0, 0.1}},
      {{0.2, 0.2, 0.1}, {0.2, 0.2, 0.1}},
  };
  for (const auto& condition : testConditions()) {
    for (const auto& c : configs) {
      for (size_t iteration = 0; iteration < 5; ++iteration) {
        auto left =
            randomRelation(gen, {"?x", "?w", "?y"}, 25, 3, c.undefLeft_);
        auto right =
            randomRelation(gen, {"?x", "?w", "?z"}, 25, 3, c.undefRight_);
        testAgainstReference(left, right, condition, InputMode::Materialized);
      }
    }
  }
}

// _____________________________________________________________________________
// No shared variables: every pair is compatible (Cartesian product), the
// condition decides which pairs are kept, and a left row without a kept pair
// gets UNDEF on the right.
TEST(OptionalJoinWithCondition, noJoinColumns) {
  std::mt19937 gen{3};
  TestCondition condition{"?y < ?z", testConditions().at(0).function_};
  for (size_t sizeRight : {0, 1, 7}) {
    for (size_t iteration = 0; iteration < 5; ++iteration) {
      auto left = randomRelation(gen, {"?u", "?y"}, 10, 8, {0.2, 0.2});
      auto right = randomRelation(gen, {"?v", "?z"}, sizeRight, 8, {0.2, 0.2});
      testAgainstReference(left, right, condition, InputMode::Materialized);
    }
  }
}

// _____________________________________________________________________________
// Complete queries, the expected results are computed by hand.
namespace {

// Athletes with height and weights, like the example in the olympics dataset.
// <a1>: height 180, weights 70 and 90.
// <a2>: height 150, weight 60.
// <a3>: height 170, no weight.
// <a4>: height 160, weights 50 and 55.
// <a5>: no height, weight 75.
const std::string kg =
    "<a1> <height> 180 . <a1> <weight> 70 . <a1> <weight> 90 . "
    "<a2> <height> 150 . <a2> <weight> 60 . "
    "<a3> <height> 170 . "
    "<a4> <height> 160 . <a4> <weight> 50 . <a4> <weight> 55 . "
    "<a5> <weight> 75 . "
    "<a1> <age> 30 . <a2> <age> 25 . <a4> <age> 10 .";

// Run `query` on the knowledge graph `turtle` and return the TSV result
// without the header, one string per row.
std::vector<std::string> runQueryOnKg(const std::string& turtle,
                                      const std::string& query) {
  auto qec = getQec(turtle);
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string tsv;
  for (const auto& block : ExportQueryExecutionTrees::computeResult(
           pq, *qet, ad_utility::MediaType::tsv, timer,
           std::move(cancellationHandle))) {
    tsv += block;
  }
  std::vector<std::string> rows = absl::StrSplit(tsv, '\n', absl::SkipEmpty());
  rows.erase(rows.begin());
  return rows;
}

// Run `query` on `kg`.
std::vector<std::string> runQuery(const std::string& query) {
  return runQueryOnKg(kg, query);
}

// The cache key of the execution tree for `query`.
std::string getCacheKey(const std::string& query) {
  auto qec = getQec(kg);
  QueryPlanner qp{qec, std::make_shared<ad_utility::CancellationHandle<>>()};
  auto pq = parseQuery(query);
  return qp.createExecutionTree(pq)->getCacheKey();
}

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
}  // namespace

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryFilterUsesOuterVariable) {
  // The example from the documentation: weights that are more than height
  // minus 100. <a4> has two weights that both don't qualify, it must appear
  // exactly once with an unbound weight.
  auto query =
      "SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> ?w "
      "FILTER(?w > ?h - 100) } } ORDER BY ?a ?w";
  EXPECT_THAT(runQuery(query),
              ElementsAre("<a1>\t90", "<a2>\t60", "<a3>\t", "<a4>\t"));
  EXPECT_THAT(getCacheKey(query), ::testing::HasSubstr("condition: ["));

  // Two FILTERs form a conjunction.
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?w > ?h - 100) FILTER(?w < 80) } } "
                       "ORDER BY ?a ?w"),
              ElementsAre("<a1>\t", "<a2>\t60", "<a3>\t", "<a4>\t"));

  // A FILTER that only uses variables of the OPTIONAL stays a filter on the
  // OPTIONAL group (no condition), together with one that is a condition.
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?w > 65) FILTER(?w > ?h - 100) } } "
                       "ORDER BY ?a ?w"),
              ElementsAre("<a1>\t90", "<a2>\t", "<a3>\t", "<a4>\t"));

  // The condition uses the join variable.
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?a != <a1> && ?w > ?h - 100) } } "
                       "ORDER BY ?a ?w"),
              ElementsAre("<a1>\t", "<a2>\t60", "<a3>\t", "<a4>\t"));

  // The right side is a join of two triples instead of a single index scan.
  EXPECT_THAT(
      runQuery("SELECT ?a ?w ?g { ?a <height> ?h OPTIONAL { ?a "
               "<weight> ?w . ?a <age> ?g FILTER(?w > ?h - 100) } } "
               "ORDER BY ?a ?w"),
      ElementsAre("<a1>\t90\t30", "<a2>\t60\t25", "<a3>\t\t", "<a4>\t\t"));

  // With a LIMIT, which is pushed to the left side of the `OptionalJoin`.
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?w > ?h - 100) } } ORDER BY ?a LIMIT 2"),
              ElementsAre("<a1>\t90", "<a2>\t60"));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryFilterOnlyUsesInnerVariables) {
  // This was already correct before, the FILTER is applied inside the group.
  auto query =
      "SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> ?w FILTER(?w > "
      "65) } } ORDER BY ?a ?w";
  EXPECT_THAT(runQuery(query), ElementsAre("<a1>\t70", "<a1>\t90", "<a2>\t",
                                           "<a3>\t", "<a4>\t"));
  EXPECT_THAT(getCacheKey(query),
              ::testing::Not(::testing::HasSubstr("condition: [")));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryNestedGroupsAndOptionals) {
  // A FILTER in a nested group (not the outermost level of the OPTIONAL) only
  // sees the variables of this group, `?h` is unbound there, so the FILTER is
  // never true.
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { { ?a <weight> "
                       "?w FILTER(?w > ?h - 100) } } } ORDER BY ?a ?w"),
              ElementsAre("<a1>\t", "<a2>\t", "<a3>\t", "<a4>\t"));

  // In a nested OPTIONAL, the condition can use the variables of the
  // enclosing OPTIONAL group (here `?w`), and the result of the inner
  // OPTIONAL is then joined with the outer one.
  EXPECT_THAT(
      runQuery("SELECT ?a ?w ?g { ?a <height> ?h OPTIONAL { ?a <weight> ?w "
               "OPTIONAL { ?a <age> ?g FILTER(?g < ?w - 50) } } } "
               "ORDER BY ?a ?w"),
      ElementsAre("<a1>\t70\t", "<a1>\t90\t30", "<a2>\t60\t", "<a3>\t\t",
                  "<a4>\t50\t", "<a4>\t55\t"));

  // The inner OPTIONAL can not see `?h`, which is bound outside of the outer
  // OPTIONAL (the groups are evaluated from the inside out).
  EXPECT_THAT(
      runQuery("SELECT ?a ?w ?g { ?a <height> ?h OPTIONAL { ?a <weight> ?w "
               "OPTIONAL { ?a <age> ?g FILTER(?g < ?h) } } } ORDER BY ?a ?w"),
      ElementsAre("<a1>\t70\t", "<a1>\t90\t", "<a2>\t60\t", "<a3>\t\t",
                  "<a4>\t50\t", "<a4>\t55\t"));

  // Two OPTIONALs after each other, the second one uses a variable of the
  // first. <a5> has no age and the weights of <a4> are too high.
  EXPECT_THAT(
      runQuery("SELECT ?a ?w ?g { ?a <weight> ?w OPTIONAL { ?a <height> ?h } "
               "OPTIONAL { ?a <age> ?g FILTER(?g * 3 > ?w) } } "
               "ORDER BY ?a ?w"),
      ElementsAre("<a1>\t70\t30", "<a1>\t90\t", "<a2>\t60\t25", "<a4>\t50\t",
                  "<a4>\t55\t", "<a5>\t75\t"));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryOptionalInsideExists) {
  // The group of an `EXISTS` is planned several times (once for each `Filter`
  // operation that the query planner considers), and the condition has to be
  // found each time. With the condition, the weight 90 of <a1> and the weights
  // of <a4> have no matching age. Without it, all the ages would be bound and
  // the result of the first query would be empty.
  //
  // NOTE: All FILTERs inside the `EXISTS` only use variables that are bound
  // inside the `EXISTS`, so this test does not depend on how variables from
  // the outside are treated there.
  auto query = [](std::string_view exists) {
    return absl::StrCat(
        "SELECT ?a { ?a <height> ?h FILTER ", exists,
        " { ?a <weight> ?w OPTIONAL { ?a <age> ?g FILTER(?g * 3 > ?w) } "
        "FILTER(!BOUND(?g)) } } ORDER BY ?a");
  };
  EXPECT_THAT(runQuery(query("EXISTS")), ElementsAre("<a1>", "<a4>"));
  EXPECT_THAT(runQuery(query("NOT EXISTS")), ElementsAre("<a2>", "<a3>"));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryUndefAndDuplicatesOnTheLeft) {
  // The join variable `?a` is UNDEF in some rows of the left side, so these
  // rows are compatible with all weights. The row with height 200 has no
  // partner that satisfies the condition, and appears once with `?a` and `?w`
  // unbound. Duplicate rows on the left are kept.
  EXPECT_THAT(
      runQuery("SELECT ?a ?h ?w { VALUES (?a ?h) { (<a1> 180) (UNDEF 165) "
               "(UNDEF 200) (<a4> 160) (<a4> 160) } OPTIONAL { ?a <weight> ?w "
               "FILTER(?w > ?h - 100) } } ORDER BY ?h ?a ?w"),
      ElementsAreArray(std::vector<std::string>{
          "<a4>\t160\t", "<a4>\t160\t", "<a1>\t165\t70", "<a1>\t165\t90",
          "<a5>\t165\t75", "<a1>\t180\t90", "\t200\t"}));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryWithoutSharedVariables) {
  // The example from https://github.com/ad-freiburg/qlever/issues/2509: the
  // OPTIONAL shares no variable with the preceding VALUES clause, so every
  // row is compatible, and the condition decides.
  EXPECT_THAT(
      runQuery("SELECT ?X ?Y { VALUES ?X { 1 2 } OPTIONAL { BIND(\"OK\" "
               "AS ?Y) FILTER(?X >= 2) } } ORDER BY ?X"),
      ElementsAre("1\t", "2\t\"OK\""));

  // The variable `?X` of the condition is not part of the result.
  EXPECT_THAT(
      runQuery("SELECT (COUNT(?Y) AS ?c) (COUNT(*) AS ?n) { VALUES ?X { 1 2 } "
               "OPTIONAL { BIND(\"OK\" AS ?Y) FILTER(?X >= 2) } }"),
      ElementsAre("1\t2"));

  // Without a condition, nothing changes.
  EXPECT_THAT(
      runQuery("SELECT ?X ?Y { VALUES ?X { 1 2 } OPTIONAL { BIND(\"OK\" "
               "AS ?Y) } } ORDER BY ?X"),
      ElementsAre("1\t\"OK\"", "2\t\"OK\""));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryErrorsInCondition) {
  // A variable that is bound nowhere: the condition is always an error, which
  // counts as not satisfied, so all weights are unbound.
  auto query =
      "SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> ?w "
      "FILTER(?w > ?h - ?nowhere) } } ORDER BY ?a ?w";
  EXPECT_THAT(runQuery(query),
              ElementsAre("<a1>\t", "<a2>\t", "<a3>\t", "<a4>\t"));
  EXPECT_THAT(getCacheKey(query), ::testing::HasSubstr("condition: ["));

  // The same for a FILTER that stays in the group, because it uses no
  // variable from outside of the OPTIONAL.
  query =
      "SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> ?w "
      "FILTER(?w > ?nowhere) } } ORDER BY ?a ?w";
  EXPECT_THAT(runQuery(query),
              ElementsAre("<a1>\t", "<a2>\t", "<a3>\t", "<a4>\t"));
  EXPECT_THAT(getCacheKey(query),
              ::testing::Not(::testing::HasSubstr("condition: [")));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, queryFilterWithExists) {
  // A FILTER with `EXISTS` is never part of the condition, it stays in the
  // group. <a1>, <a2> and <a4> have an age.
  auto query = [](std::string_view filter) {
    return absl::StrCat(
        "SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> ?w FILTER(",
        filter, ") } } ORDER BY ?a ?w");
  };
  auto q = query("EXISTS { ?a <age> ?g }");
  EXPECT_THAT(runQuery(q), ElementsAre("<a1>\t70", "<a1>\t90", "<a2>\t60",
                                       "<a3>\t", "<a4>\t50", "<a4>\t55"));
  EXPECT_THAT(getCacheKey(q),
              ::testing::Not(::testing::HasSubstr("condition: [")));

  q = query("NOT EXISTS { ?a <age> ?g }");
  EXPECT_THAT(runQuery(q), ElementsAre("<a1>\t", "<a2>\t", "<a3>\t", "<a4>\t"));
}

// _____________________________________________________________________________
// Regression test for a bug in the block-wise zipper join that is used for
// lazy inputs (independent of conditions): if the rows of the right input with
// the same join value span more than a few blocks, the left rows with this
// value were additionally added as rows without a match.
TEST(OptionalJoinWithCondition, lazyJoinWithLargeGroupsWithoutCondition) {
  auto* qec = getQec();
  std::mt19937 gen{42};
  for (size_t sizeRight : {10, 100, 3000}) {
    for (size_t blockSize : {1, 2, 3, 5}) {
      for (bool leftIsLazy : {false, true}) {
        for (size_t iteration = 0; iteration < 5; ++iteration) {
          qec->getQueryTreeCache().clearAll();
          auto left = randomRelation(gen, {"?x", "?y"}, 2, 6, {0, 0});
          auto right =
              randomRelation(gen, {"?x", "?z"}, sizeRight, 6, {0, 0.1});
          sortBy(left, {0});
          sortBy(right, {0});
          auto blocks = splitIntoBlocks(right, blockSize);
          std::shared_ptr<QueryExecutionTree> leftTree;
          if (leftIsLazy) {
            std::vector<IdTable> leftBlocks;
            leftBlocks.push_back(toIdTable(left));
            leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
                qec, std::move(leftBlocks), toVariables(left), false,
                std::vector<ColumnIndex>{0});
          } else {
            leftTree = ad_utility::makeExecutionTree<ValuesForTesting>(
                qec, toIdTable(left), toVariables(left), false,
                std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
          }
          auto rightTree = ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, std::move(blocks), toVariables(right), false,
              std::vector<ColumnIndex>{0});
          OptionalJoin join{qec, leftTree, rightTree};
          auto result = join.computeResultOnlyForTesting(false);
          auto actual = toRows(result.idTableView());
          auto expected = referenceLeftJoin(left, right,
                                            [](const Binding&) { return true; },
                                            {"?x", "?y", "?z"});
          ql::ranges::sort(actual);
          ql::ranges::sort(expected);
          EXPECT_EQ(actual, expected)
              << "size of right: " << sizeRight << ", block size " << blockSize
              << ", left is lazy: " << leftIsLazy;
        }
      }
    }
  }
}

namespace {
// <s1> and <s2> have a <q>, <s3> and <s4> don't, so `?z` is UNDEF for them
// after the first OPTIONAL.
const std::string kgTwoColumns =
    "<s1> <p> 1 . <s2> <p> 2 . <s3> <p> 3 . <s4> <p> 4 . "
    "<s1> <q> <z1> . <s2> <q> <z2> . "
    "<s1> <r> <z1> . <s2> <r> <z2> . <s3> <r> <z3> . <s3> <r> <z4> . "
    "<s4> <r> <z5> .";
}  // namespace

// Two join columns (`?s` and `?z`), UNDEF only in the second one on the left,
// and the right side is a single index scan: this is the prefiltered
// `specialOptionalJoinForBlocks`.
TEST(OptionalJoinWithCondition, twoJoinColumnsWithIndexScan) {
  // <s4> has the partner <z5>, but the condition is false for it, so it must
  // appear once with `?z` unbound.
  EXPECT_THAT(
      runQueryOnKg(kgTwoColumns,
                   "SELECT ?s ?o ?z { ?s <p> ?o OPTIONAL { ?s <q> ?z } "
                   "OPTIONAL { ?s <r> ?z FILTER(?o != 4) } } ORDER BY ?s ?z"),
      ElementsAre("<s1>\t1\t<z1>", "<s2>\t2\t<z2>", "<s3>\t3\t<z3>",
                  "<s3>\t3\t<z4>", "<s4>\t4\t"));
  // The condition is false for all partners of <s3>.
  EXPECT_THAT(
      runQueryOnKg(kgTwoColumns,
                   "SELECT ?s ?o ?z { ?s <p> ?o OPTIONAL { ?s <q> ?z } "
                   "OPTIONAL { ?s <r> ?z FILTER(?o != 3) } } ORDER BY ?s ?z"),
      ElementsAre("<s1>\t1\t<z1>", "<s2>\t2\t<z2>", "<s3>\t3\t",
                  "<s4>\t4\t<z5>"));
  // The condition uses the merged value of `?z`.
  EXPECT_THAT(
      runQueryOnKg(kgTwoColumns,
                   "SELECT ?s ?o ?z { ?s <p> ?o OPTIONAL { ?s <q> ?z } "
                   "OPTIONAL { ?s <r> ?z FILTER(?z != <z4> && ?o > 0) } } "
                   "ORDER BY ?s ?z"),
      ElementsAre("<s1>\t1\t<z1>", "<s2>\t2\t<z2>", "<s3>\t3\t<z3>",
                  "<s4>\t4\t<z5>"));
}

// The same queries without the prefiltered join with the index scan.
TEST(OptionalJoinWithCondition, twoJoinColumnsWithoutPrefilteredScan) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::prefilteredOptionalJoin_>(
          false);
  EXPECT_THAT(
      runQueryOnKg(kgTwoColumns,
                   "SELECT ?s ?o ?z { ?s <p> ?o OPTIONAL { ?s <q> ?z } "
                   "OPTIONAL { ?s <r> ?z FILTER(?o != 4) } } ORDER BY ?s ?z"),
      ElementsAre("<s1>\t1\t<z1>", "<s2>\t2\t<z2>", "<s3>\t3\t<z3>",
                  "<s3>\t3\t<z4>", "<s4>\t4\t"));
  EXPECT_THAT(
      runQueryOnKg(kgTwoColumns,
                   "SELECT ?s ?o ?z { ?s <p> ?o OPTIONAL { ?s <q> ?z } "
                   "OPTIONAL { ?s <r> ?z FILTER(?o != 3) } } ORDER BY ?s ?z"),
      ElementsAre("<s1>\t1\t<z1>", "<s2>\t2\t<z2>", "<s3>\t3\t",
                  "<s4>\t4\t<z5>"));
}

// The join column and the variable `?h` of the condition are not selected.
TEST(OptionalJoinWithCondition, strippedColumns) {
  // NOTE: `runQuery` skips empty lines, so a single unbound column would be
  // lost in the TSV, therefore the COALESCE.
  EXPECT_THAT(runQuery("SELECT (COALESCE(?w, \"none\") AS ?x) { ?a <height> ?h "
                       "OPTIONAL { ?a <weight> ?w FILTER(?w > ?h - 100) } } "
                       "ORDER BY ?w"),
              ElementsAre("\"none\"", "\"none\"", "60", "90"));
  EXPECT_THAT(runQuery("SELECT (COUNT(*) AS ?c) { ?a <height> ?h OPTIONAL { ?a "
                       "<weight> ?w FILTER(?w > ?h - 100) } }"),
              ElementsAre("4"));
  EXPECT_THAT(runQuery("SELECT DISTINCT ?w (1 AS ?one) { ?a <height> ?h "
                       "OPTIONAL { ?a <weight> ?w FILTER(?w > ?h - 100) } } "
                       "ORDER BY ?w"),
              ElementsAre("\t1", "60\t1", "90\t1"));
}

// If the `OptionalJoin` with stripped columns still produces variables that
// were not requested, a `StripColumns` operation on top removes them.
TEST(OptionalJoinWithCondition, strippedColumnsThatAreStillProduced) {
  auto* qec = getQec();
  Relation left{{"?x", "?w", "?y"}, {{IntId(1), IntId(2), IntId(3)}}};
  Relation right{{"?x", "?w", "?z"}, {{IntId(1), IntId(2), IntId(4)}}};
  auto strip = [&](const std::vector<std::string>& conditions,
                   const std::set<Variable>& requestedVariables) {
    OptionalJoin::JoinCondition condition;
    for (const auto& expression : conditions) {
      condition.push_back(parseExpression(expression));
    }
    auto tree = ad_utility::makeExecutionTree<OptionalJoin>(
        qec,
        ad_utility::makeExecutionTree<ValuesForTesting>(qec, toIdTable(left),
                                                        toVariables(left)),
        ad_utility::makeExecutionTree<ValuesForTesting>(qec, toIdTable(right),
                                                        toVariables(right)),
        true, std::move(condition));
    return QueryExecutionTree::makeTreeWithStrippedColumns(std::move(tree),
                                                           requestedVariables);
  };
  auto isStripColumns = [](const auto& tree) {
    return std::dynamic_pointer_cast<StripColumns>(tree->getRootOperation()) !=
           nullptr;
  };
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};

  // No join column is requested, so the join columns are dropped by the
  // `OptionalJoin` itself.
  auto tree = strip({}, {y, z});
  EXPECT_FALSE(isStripColumns(tree));
  EXPECT_EQ(tree->getResultWidth(), 2u);

  // Only one of the two join columns is requested, but the `OptionalJoin`
  // can only keep all of them or none.
  tree = strip({}, {x, z});
  EXPECT_TRUE(isStripColumns(tree));
  EXPECT_EQ(tree->getResultWidth(), 2u);

  // The variable `?y` of the condition is not requested.
  tree = strip({"?y < ?z"}, {z});
  EXPECT_TRUE(isStripColumns(tree));
  EXPECT_EQ(tree->getResultWidth(), 1u);
}

// BIND, subquery and VALUES inside the OPTIONAL group.
TEST(OptionalJoinWithCondition, bindSubqueryValuesInOptional) {
  // `?d` is bound inside the group, the condition compares it with `?h`.
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w BIND(?w + 100 AS ?d) FILTER(?d > ?h) } } "
                       "ORDER BY ?a ?w"),
              ElementsAre("<a1>\t90", "<a2>\t60", "<a3>\t", "<a4>\t"));
  // A BIND inside the group can not see `?h`, so `?d` is unbound.
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w BIND(?h - 100 AS ?d) FILTER(?w > ?d) } } "
                       "ORDER BY ?a ?w"),
              ElementsAre("<a1>\t", "<a2>\t", "<a3>\t", "<a4>\t"));
  // Subquery with the maximum weight (a1: 90, a2: 60, a4: 55).
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { { SELECT ?a "
                       "(MAX(?w0) AS ?w) { ?a <weight> ?w0 } GROUP BY ?a } "
                       "FILTER(?w > ?h - 100) } } ORDER BY ?a ?w"),
              ElementsAre("<a1>\t90", "<a2>\t60", "<a3>\t", "<a4>\t"));
  // VALUES without a shared variable.
  EXPECT_THAT(runQuery("SELECT ?a ?t { ?a <height> ?h OPTIONAL { VALUES ?t { "
                       "155 175 } FILTER(?t < ?h) } } ORDER BY ?a ?t"),
              ElementsAre("<a1>\t155", "<a1>\t175", "<a2>\t", "<a3>\t155",
                          "<a4>\t155"));
  // The group only consists of a FILTER: LeftJoin(G, {{}}, F) = G.
  EXPECT_THAT(runQuery("SELECT ?a ?h { ?a <height> ?h OPTIONAL { FILTER(?h > "
                       "165) } } ORDER BY ?a"),
              ElementsAre("<a1>\t180", "<a2>\t150", "<a3>\t170", "<a4>\t160"));
}

// Strings that are created during the evaluation of the condition, and
// strings from the left side that are not part of the index.
TEST(OptionalJoinWithCondition, stringsAndLocalVocab) {
  EXPECT_THAT(
      runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> ?w "
               "FILTER(CONCAT(STR(?h), \"-\", STR(?w)) = \"180-90\") } } "
               "ORDER BY ?a ?w"),
      ElementsAre("<a1>\t90", "<a2>\t", "<a3>\t", "<a4>\t"));
  EXPECT_THAT(
      runQuery("SELECT ?a ?l ?w { VALUES (?a ?l) { (<a1> \"x\") (<a2> \"y\") } "
               "OPTIONAL { ?a <weight> ?w FILTER(?l = \"x\") } } "
               "ORDER BY ?a ?w"),
      ElementsAre("<a1>\t\"x\"\t70", "<a1>\t\"x\"\t90", "<a2>\t\"y\"\t"));
}

// The variable of the condition is bound on the left by an earlier OPTIONAL
// (so it can be UNDEF there) and also bound inside the group.
TEST(OptionalJoinWithConditionReview, conditionVariableBoundOnBothSides) {
  // Left: (a1,70,180) (a1,90,180) (a2,60,150) (a4,50,160) (a4,55,160)
  // (a5,75,UNDEF). Right: heights again. Only for <a5> the value of `?h` in
  // the merged row could come from the right, but <a5> has no height.
  EXPECT_THAT(
      runQuery("SELECT ?a ?w ?h { ?a <weight> ?w OPTIONAL { ?a <height> ?h } "
               "OPTIONAL { ?a <height> ?h FILTER(?h > 155 && ?w > 50) } } "
               "ORDER BY ?a ?w"),
      ElementsAre("<a1>\t70\t180", "<a1>\t90\t180", "<a2>\t60\t150",
                  "<a4>\t50\t160", "<a4>\t55\t160", "<a5>\t75\t"));
}

// The cost estimate of an `OptionalJoin` without join columns must not
// overflow.
TEST(OptionalJoinWithCondition, costEstimateWithoutJoinColumns) {
  auto qec = getQec(kg);
  QueryPlanner qp{qec, std::make_shared<ad_utility::CancellationHandle<>>()};
  auto pq = parseQuery(
      "SELECT ?X ?Y { VALUES ?X { 1 2 } OPTIONAL { BIND(\"OK\" AS ?Y) "
      "FILTER(?X >= 2) } }");
  auto qet = qp.createExecutionTree(pq);
  EXPECT_LT(qet->getCostEstimate(), size_t{1'000'000});
  // The join itself costs at least four times the size of its result.
  EXPECT_GE(qet->getCostEstimate(), 4 * qet->getSizeEstimate());
}

// More than 100'000 pairs, so that `AddCombinedRowToIdTable` flushes several
// times and the condition is evaluated in several chunks.
TEST(OptionalJoinWithCondition, manyPairs) {
  std::mt19937 gen{11};
  for (const auto& condition : testConditions()) {
    // Values in `[0, 1]`, so about 2 * 250 * 250 = 125'000 pairs.
    auto left = randomRelation(gen, {"?x", "?y"}, 500, 1, {0, 0});
    auto right = randomRelation(gen, {"?x", "?z"}, 500, 1, {0, 0});
    testAgainstReference(left, right, condition, InputMode::Materialized);
    testAgainstReference(left, right, condition, InputMode::LazyRightInBlocks,
                         50);
  }
}
