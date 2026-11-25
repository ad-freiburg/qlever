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
// The tests start with complete queries, followed by the `OptionalJoin` with a
// condition on small handwritten inputs and the `OptionalJoinConditionFilter`
// alone. At the end, the `OptionalJoin` is compared to a direct implementation
// of the definition above on random inputs.

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <array>
#include <map>
#include <random>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/LocalVocabEntryTestHelpers.h"
#include "../util/ParsedQueryTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/OptionalJoinConditionFilter.h"
#include "engine/QueryPlanner.h"
#include "engine/operation/OptionalJoin.h"

using namespace ad_utility::testing;
using ::testing::ElementsAre;

namespace {

// Athletes with a height, weights, and for some an age.
// <a1>: height 180, weights 70 and 90.
// <a2>: height 150, weight 60.
// <a3>: height 170, no weight.
// <a4>: height 160, weights 50 and 55.
// <a5>: no height, weight 75.
const std::string athletes =
    "<a1> <height> 180 . <a1> <weight> 70 . <a1> <weight> 90 . "
    "<a2> <height> 150 . <a2> <weight> 60 . "
    "<a3> <height> 170 . "
    "<a4> <height> 160 . <a4> <weight> 50 . <a4> <weight> 55 . "
    "<a5> <weight> 75 . "
    "<a1> <age> 30 . <a2> <age> 25 . <a4> <age> 10 .";

// Run `query` on `kg` and return the TSV result without the header, one string
// per row.
std::vector<std::string> runQuery(const std::string& query,
                                  const std::string& kg = athletes) {
  auto qec = getQec(kg);
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
  // A row with a single empty column is an empty line, so empty lines are not
  // skipped. The last line is empty because of the final newline.
  std::vector<std::string> lines = absl::StrSplit(tsv, '\n');
  AD_CORRECTNESS_CHECK(lines.size() >= 2 && lines.back().empty());
  lines.pop_back();
  lines.erase(lines.begin());
  return lines;
}

}  // namespace

// _____________________________________________________________________________
// The FILTER uses `?h` from outside of the OPTIONAL, so it is the condition of
// the left join. <a1> has a weight that is more than its height minus 100.
// <a4> has two weights, but none of them is, so <a4> is in the result exactly
// once, with `?w` unbound.
TEST(OptionalJoinWithConditionQuery, basicExample) {
  std::string kg =
      "<a1> <height> 180 . <a1> <weight> 70 . <a1> <weight> 90 . "
      "<a4> <height> 160 . <a4> <weight> 50 . <a4> <weight> 55 .";
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?w > ?h - 100) } } ORDER BY ?a",
                       kg),
              ElementsAre("<a1>\t90", "<a4>\t"));
}

// _____________________________________________________________________________
// For <a1>, the weight 90 fulfills the first FILTER, but not the second one.
TEST(OptionalJoinWithConditionQuery, twoFiltersAreConjunction) {
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?w > ?h - 100) FILTER(?w < 80) } } "
                       "ORDER BY ?a ?w"),
              ElementsAre("<a1>\t", "<a2>\t60", "<a3>\t", "<a4>\t"));
}

// _____________________________________________________________________________
// `FILTER(?w > 65)` only uses a variable of the OPTIONAL, so it stays a filter
// on the OPTIONAL group, next to the condition. It removes the weight 60 of
// <a2>.
TEST(OptionalJoinWithConditionQuery,
     filterOnlyOnOptionalVariablesStaysInGroup) {
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?w > 65) FILTER(?w > ?h - 100) } } "
                       "ORDER BY ?a ?w"),
              ElementsAre("<a1>\t90", "<a2>\t", "<a3>\t", "<a4>\t"));
}

// _____________________________________________________________________________
// The right side is a join of two triples instead of a single index scan.
TEST(OptionalJoinWithConditionQuery, rightSideIsJoin) {
  EXPECT_THAT(
      runQuery("SELECT ?a ?w ?g { ?a <height> ?h OPTIONAL { ?a "
               "<weight> ?w . ?a <age> ?g FILTER(?w > ?h - 100) } } "
               "ORDER BY ?a ?w"),
      ElementsAre("<a1>\t90\t30", "<a2>\t60\t25", "<a3>\t\t", "<a4>\t\t"));
}

// _____________________________________________________________________________
// Without ORDER BY, the LIMIT is pushed to the left side of the
// `OptionalJoin`. This is correct with a condition, too, because each left row
// gives at least one row of the result.
TEST(OptionalJoinWithConditionQuery, limit) {
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <weight> "
                       "?w FILTER(?w > ?h - 100) } } LIMIT 2"),
              ElementsAre("<a1>\t90", "<a2>\t60"));
}

// _____________________________________________________________________________
// The join variable `?a` and the variable `?h` of the condition are not
// selected, so their columns are stripped above the `OptionalJoin`.
TEST(OptionalJoinWithConditionQuery, variablesOfConditionAreNotSelected) {
  EXPECT_THAT(runQuery("SELECT ?w { ?a <height> ?h OPTIONAL { ?a <weight> ?w "
                       "FILTER(?w > ?h - 100) } } ORDER BY ?w"),
              ElementsAre("", "", "60", "90"));
}

// _____________________________________________________________________________
// Two shared variables, `?a` and `?g`. <a3> has no age, so it is not on the
// left side.
TEST(OptionalJoinWithConditionQuery, twoSharedVariables) {
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h . ?a <age> ?g OPTIONAL { "
                       "?a <weight> ?w . ?a <age> ?g FILTER(?w > ?h - 100) } } "
                       "ORDER BY ?a"),
              ElementsAre("<a1>\t90", "<a2>\t60", "<a4>\t"));
}

// _____________________________________________________________________________
// The shared variable `?g` is UNDEF on the left for <a3>.
TEST(OptionalJoinWithConditionQuery, sharedVariableUndefOnLeft) {
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?a <age> ?g } "
                       "OPTIONAL { ?a <weight> ?w . ?a <age> ?g "
                       "FILTER(?w > ?h - 100) } } ORDER BY ?a"),
              ElementsAre("<a1>\t90", "<a2>\t60", "<a3>\t", "<a4>\t"));
}

// _____________________________________________________________________________
// The shared variable `?g` might be UNDEF on the right.
TEST(OptionalJoinWithConditionQuery, sharedVariableMaybeUndefOnRight) {
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h . ?a <age> ?g OPTIONAL { "
                       "?a <weight> ?w OPTIONAL { ?a <age> ?g } "
                       "FILTER(?w > ?h - 100) } } ORDER BY ?a"),
              ElementsAre("<a1>\t90", "<a2>\t60", "<a4>\t"));
}

// _____________________________________________________________________________
// An OPTIONAL that shares no variable with the left side is computed like
// before the condition was determined: the FILTER is applied to the group
// alone, with a warning (see `OptionalJoinConditionPlanningTest`). There, `?h`
// is unbound, so the FILTER removes all rows of the group.
TEST(OptionalJoinWithConditionQuery, conditionIsAppliedToGroup) {
  EXPECT_THAT(runQuery("SELECT ?a ?w { ?a <height> ?h OPTIONAL { ?b <weight> "
                       "?w FILTER(?w > ?h - 100) } } ORDER BY ?a"),
              ElementsAre("<a1>\t", "<a2>\t", "<a3>\t", "<a4>\t"));
}

namespace {

constexpr auto U = Id::makeUndefined();
auto I = IntId;
using Row = std::vector<Id>;

// A relation with named columns, e.g. `{"?x", "?y"}`.
struct Relation {
  std::vector<std::string> variables_;
  std::vector<Row> rows_;
};

std::vector<Row> toRows(const IdTableView<0>& table) {
  std::vector<Row> result;
  for (const auto& row : table) {
    result.emplace_back(row.begin(), row.end());
  }
  return result;
}

// The rows of `table`, with `IntId`s for the integers, e.g. `{{1, U}}`.
std::vector<Row> rows(const VectorTable& table) {
  return toRows(makeIdTableFromVector(table, IntId).asStaticView<0>());
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
  if (blocks.empty()) {
    blocks.push_back(toIdTable(relation));
  }
  return blocks;
}

// A `ValuesForTesting` for `relation`. If `inBlocks` is true, the relation has
// to be sorted by its first column and is split into blocks of `blockSize`
// rows.
std::shared_ptr<QueryExecutionTree> makeTree(const Relation& relation,
                                             bool inBlocks = false,
                                             size_t blockSize = 1) {
  auto* qec = getQec();
  if (!inBlocks) {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, toIdTable(relation), toVariables(relation));
  }
  return ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, splitIntoBlocks(relation, blockSize), toVariables(relation), false,
      std::vector<ColumnIndex>{0});
}

// Parse a SPARQL expression, e.g. `?y < ?z`.
sparqlExpression::SparqlExpressionPimpl parseExpression(
    const std::string& expression) {
  auto query = parseQuery(
      absl::StrCat("SELECT * { ?s ?p ?o FILTER(", expression, ") }"));
  return query._rootGraphPattern._filters.at(0).expression_;
}

// An `OptionalJoin` of `left` and `right` with the conjunction of the SPARQL
// expressions `conditions` as its condition.
OptionalJoin makeJoin(std::shared_ptr<QueryExecutionTree> left,
                      std::shared_ptr<QueryExecutionTree> right,
                      const std::vector<std::string>& conditions,
                      bool keepJoinColumns = true) {
  auto* qec = getQec();
  qec->getQueryTreeCache().clearAll();
  OptionalJoin::JoinCondition condition;
  for (const auto& expression : conditions) {
    condition.push_back(parseExpression(expression));
  }
  return OptionalJoin{qec, std::move(left), std::move(right), keepJoinColumns,
                      std::move(condition)};
}

OptionalJoin makeJoin(const Relation& left, const Relation& right,
                      const std::vector<std::string>& conditions,
                      bool keepJoinColumns = true) {
  return makeJoin(makeTree(left), makeTree(right), conditions, keepJoinColumns);
}

// How the inputs are passed to the `OptionalJoin`.
enum struct InputMode {
  // Both inputs are fully materialized.
  Materialized,
  // The inputs are sorted by their first column, and one or both of them are
  // split into several blocks that can be yielded lazily.
  LazyLeft,
  LazyRight,
  LazyBoth,
};
constexpr std::array allInputModes{InputMode::Materialized, InputMode::LazyLeft,
                                   InputMode::LazyRight, InputMode::LazyBoth};

// An `OptionalJoin` of `left` and `right` with `condition`, where the inputs
// are passed as described by `mode`, in blocks of one row.
OptionalJoin makeJoinForMode(const Relation& left, const Relation& right,
                             const std::string& condition, InputMode mode) {
  bool leftInBlocks =
      mode == InputMode::LazyLeft || mode == InputMode::LazyBoth;
  bool rightInBlocks =
      mode == InputMode::LazyRight || mode == InputMode::LazyBoth;
  return makeJoin(makeTree(left, leftInBlocks), makeTree(right, rightInBlocks),
                  {condition});
}

// The rows of the result of `join`, which is requested lazily if
// `requestLaziness` is true.
std::vector<Row> computeRows(OptionalJoin& join, bool requestLaziness = false) {
  auto result = join.computeResultOnlyForTesting(requestLaziness);
  if (result.isFullyMaterialized()) {
    return toRows(result.idTableView());
  }
  std::vector<Row> rowsOfAllBlocks;
  for (auto& [table, localVocab] : result.idTables()) {
    (void)localVocab;
    ql::ranges::move(toRows(table.asStaticView<0>()),
                     std::back_inserter(rowsOfAllBlocks));
  }
  return rowsOfAllBlocks;
}

// Whether `rows` are sorted as promised by `join.resultSortedOn()`.
bool isSortedAsPromised(OptionalJoin& join, const std::vector<Row>& result) {
  auto sortedOn = join.resultSortedOn();
  return ql::ranges::is_sorted(result, [&sortedOn](const Row& a, const Row& b) {
    for (auto col : sortedOn) {
      if (a[col] != b[col]) {
        return a[col] < b[col];
      }
    }
    return false;
  });
}

// Check that the `OptionalJoin` of `left` and `right` (sorted by their first
// column) with `condition` gives the `expected` rows in any order. This is
// checked for all `InputMode`s, with the result requested lazily or not, and
// with a buffer size of 1 (each row is written by its own call to `flush`) and
// a large one.
void expectOptionalJoin(
    const Relation& left, const Relation& right, const std::string& condition,
    const std::vector<Row>& expected,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  for (auto mode : allInputModes) {
    for (size_t bufferSize : {1, 100'000}) {
      auto join = makeJoinForMode(left, right, condition, mode);
      join.setRowAdderBufferSizeForTesting(bufferSize);
      for (bool requestLaziness : {false, true}) {
        getQec()->getQueryTreeCache().clearAll();
        auto actual = computeRows(join, requestLaziness);
        EXPECT_TRUE(isSortedAsPromised(join, actual));
        EXPECT_THAT(actual, ::testing::UnorderedElementsAreArray(expected))
            << "input mode " << static_cast<int>(mode) << ", buffer size "
            << bufferSize;
      }
    }
  }
}

}  // namespace

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, basicFiltering) {
  Relation left{{"?x", "?y"}, rows({{1, 10}, {2, 20}, {3, 30}, {4, 40}})};
  Relation right{{"?x", "?z"},
                 rows({{1, 5}, {1, 15}, {2, 1}, {2, 2}, {4, 41}, {4, 42}})};
  // `?x = 1`: two partners, one fulfills `?y < ?z`.
  // `?x = 2`: two partners, none fulfills it, so exactly one row with UNDEF.
  // `?x = 3`: no partner, one row with UNDEF.
  // `?x = 4`: two partners that both fulfill it.
  expectOptionalJoin(
      left, right, "?y < ?z",
      rows({{1, 10, 15}, {2, 20, U}, {3, 30, U}, {4, 40, 41}, {4, 40, 42}}));
}

// _____________________________________________________________________________
// Each copy of a left row is a row of its own: each copy of `?x = 1` gets its
// match, each copy of `?x = 2` its row with UNDEF.
TEST(OptionalJoinWithCondition, duplicatesInLeftInput) {
  Relation left{{"?x", "?y"}, rows({{1, 10}, {1, 10}, {2, 20}, {2, 20}})};
  Relation right{{"?x", "?z"}, rows({{1, 5}, {1, 15}, {2, 1}})};
  expectOptionalJoin(left, right, "?y < ?z",
                     rows({{1, 10, 15}, {1, 10, 15}, {2, 20, U}, {2, 20, U}}));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, emptyLeftInput) {
  Relation left{{"?x", "?y"}, {}};
  Relation right{{"?x", "?z"}, rows({{1, 5}, {1, 15}})};
  expectOptionalJoin(left, right, "?y < ?z", {});
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, emptyRightInput) {
  Relation left{{"?x", "?y"}, rows({{1, 10}, {2, 20}})};
  Relation right{{"?x", "?z"}, {}};
  expectOptionalJoin(left, right, "?y < ?z", rows({{1, 10, U}, {2, 20, U}}));
}

// _____________________________________________________________________________
// A left row with `?x = UNDEF` is compatible with every right row. The first
// one has no match, so its row with UNDEF keeps `?x` unbound, although its
// pairs bind it. The second one has a match with `?x = 1` and `?x = 4`.
TEST(OptionalJoinWithCondition, undefInJoinColumnOfLeft) {
  Relation left{{"?x", "?y"}, rows({{U, 100}, {U, 3}, {2, 20}})};
  Relation right{{"?x", "?z"}, rows({{1, 5}, {2, 1}, {4, 41}})};
  expectOptionalJoin(left, right, "?y < ?z",
                     rows({{U, 100, U}, {1, 3, 5}, {4, 3, 41}, {2, 20, U}}));
}

// _____________________________________________________________________________
// If the right input is more than 1000 times larger than the left one, the
// `OptionalJoin` uses the galloping join. The inputs are the ones from
// `basicFiltering`, with 4000 rows without a partner at the beginning of the
// right input.
TEST(OptionalJoinWithCondition, gallopingJoin) {
  Relation left{{"?x", "?y"}, rows({{1, 10}, {2, 20}, {3, 30}, {4, 40}})};
  Relation right{{"?x", "?z"}, std::vector<Row>(4'000, Row{I(0), I(0)})};
  ql::ranges::copy(rows({{1, 5}, {1, 15}, {2, 1}, {2, 2}, {4, 41}, {4, 42}}),
                   std::back_inserter(right.rows_));
  expectOptionalJoin(
      left, right, "?y < ?z",
      rows({{1, 10, 15}, {2, 20, U}, {3, 30, U}, {4, 40, 41}, {4, 40, 42}}));
}

// _____________________________________________________________________________
// Without `keepJoinColumns`, the result only has the columns `?y` and `?z`.
TEST(OptionalJoinWithCondition, joinColumnIsNotKept) {
  Relation left{{"?x", "?y"}, rows({{1, 10}, {2, 20}})};
  Relation right{{"?x", "?z"}, rows({{1, 15}, {2, 5}})};
  auto join = makeJoin(left, right, {"?y < ?z"}, false);
  EXPECT_EQ(join.getResultWidth(), 2u);
  EXPECT_THAT(computeRows(join),
              ::testing::UnorderedElementsAreArray(rows({{10, 15}, {20, U}})));
}

// _____________________________________________________________________________
// If the condition uses the join variable `?x`, the join column is kept, even
// without `keepJoinColumns`.
TEST(OptionalJoinWithCondition, joinColumnIsKeptIfConditionUsesIt) {
  Relation left{{"?x", "?y"}, rows({{1, 10}, {2, 20}})};
  Relation right{{"?x", "?z"}, rows({{1, 15}, {2, 5}})};
  auto join = makeJoin(left, right, {"?x + ?y > ?z"}, false);
  EXPECT_EQ(join.getResultWidth(), 3u);
  EXPECT_THAT(computeRows(join), ::testing::UnorderedElementsAreArray(
                                     rows({{1, 10, U}, {2, 20, 5}})));
}

// _____________________________________________________________________________
TEST(OptionalJoinWithCondition, descriptor) {
  Relation left{{"?x", "?y"}, rows({{1, 1}})};
  Relation right{{"?x", "?z"}, rows({{1, 2}})};
  EXPECT_THAT(makeJoin(left, right, {"?y < ?z"}).getDescriptor(),
              ::testing::HasSubstr("with condition (?y < ?z)"));
}

// _____________________________________________________________________________
// The cache key must contain the condition, otherwise an `OptionalJoin` with a
// condition could reuse the cached result of one without (or with another).
TEST(OptionalJoinWithCondition, cacheKey) {
  Relation left{{"?x", "?y"}, rows({{1, 1}})};
  Relation right{{"?x", "?z"}, rows({{1, 2}})};
  auto noCondition = makeJoin(left, right, {}).getCacheKey();
  auto lessThan = makeJoin(left, right, {"?y < ?z"}).getCacheKey();
  auto greaterThan = makeJoin(left, right, {"?y > ?z"}).getCacheKey();
  EXPECT_THAT(lessThan, ::testing::HasSubstr("condition: ["));
  EXPECT_NE(noCondition, lessThan);
  EXPECT_NE(lessThan, greaterThan);
}

// _____________________________________________________________________________
// A condition with `RAND()` makes the `OptionalJoin` non-deterministic, so its
// result must not be cached.
TEST(OptionalJoinWithCondition, nonDeterministicCondition) {
  Relation left{{"?x", "?y"}, rows({{1, 1}})};
  Relation right{{"?x", "?z"}, rows({{1, 2}})};
  auto deterministic = makeJoin(left, right, {"?y < ?z"});
  EXPECT_TRUE(deterministic.isDeterministic());
  auto withRand = makeJoin(left, right, {"?y < ?z", "RAND() < 2"});
  EXPECT_FALSE(withRand.isDeterministic());
  EXPECT_FALSE(withRand.canResultBeCached());
}

// _____________________________________________________________________________
// The rows with UNDEF on the right of the left rows without a match keep the
// words of their left rows in the local vocab of the result.
TEST(OptionalJoinWithCondition, resultWithLocalVocab) {
  auto* qec = getQec();
  auto word = [qec](int64_t i) {
    return localVocabEntryFromStringRepresentation(absl::StrCat("\"s", i, "\""),
                                                   qec->getLocalVocabContext());
  };
  // Left: `?x = i / 3`, `?y = i % 3`, `?s = "s<i>"`, in blocks of 700 rows.
  constexpr int64_t numLeft = 3'000;
  std::vector<Result::IdTableVocabPair> leftBlocks;
  for (int64_t begin = 0; begin < numLeft; begin += 700) {
    IdTable table{3, makeAllocator()};
    LocalVocab localVocab = makeLocalVocab();
    for (int64_t i = begin; i < std::min(begin + 700, numLeft); ++i) {
      Id s = Id::makeFromLocalVocabIndex(
          localVocab.getIndexAndAddIfNotContained(word(i)));
      table.push_back({I(i / 3), I(i % 3), s});
    }
    leftBlocks.emplace_back(std::move(table), std::move(localVocab));
  }
  // Right: `?z` 0 and 1 for each `?x < 500`.
  Relation right{{"?x", "?z"}, {}};
  for (int64_t i = 0; i < 1'000; ++i) {
    right.rows_.push_back({I(i / 2), I(i % 2)});
  }
  auto join = makeJoin(ad_utility::makeExecutionTree<ValuesForTesting>(
                           qec, std::move(leftBlocks),
                           std::vector<std::optional<Variable>>{
                               Variable{"?x"}, Variable{"?y"}, Variable{"?s"}},
                           false, std::vector<ColumnIndex>{0}),
                       makeTree(right, true, 100), {"?y < ?z"});
  join.setRowAdderBufferSizeForTesting(7);
  auto result = join.computeResultOnlyForTesting(true);

  const auto& columns = join.getExternallyVisibleVariableColumns();
  auto col = [&columns](const std::string& var) {
    return columns.at(Variable{var}).columnIndex_;
  };
  // Look up the expected words instead of dereferencing the `Id`s, which would
  // be undefined behavior if the words were not kept alive.
  ad_utility::HashSet<std::string> words;
  for (const auto& entry : result.localVocab().getAllWordsForTesting()) {
    words.insert(std::string{entry.toStringRepresentation()});
  }
  size_t numUndef = 0;
  for (const auto& row : result.idTableView()) {
    int64_t i = row[col("?x")].getInt() * 3 + row[col("?y")].getInt();
    ASSERT_EQ(row[col("?s")].getDatatype(), Datatype::LocalVocabIndex);
    EXPECT_TRUE(words.contains(word(i).toStringRepresentation()));
    numUndef += row[col("?z")].isUndefined();
  }
  // For each `?x`, `(?y, ?z) = (0, 1)` is the only match, the left rows with
  // `?y = 1` and `?y = 2` have partners, but get a row with UNDEF.
  EXPECT_EQ(result.idTableView().size(), 3'000u);
  EXPECT_EQ(numUndef, 2'500u);
}

namespace {

// _____________________________________________________________________________
IdTable intTable(const VectorTable& content) {
  return makeIdTableFromVector(content, IntId);
}

// A block of an input with its own local vocab, like the blocks of a lazy
// result, as an input of `AddCombinedRowToIdTable::setInput`.
struct BlockWithLocalVocab {
  IdTable table_;
  LocalVocab localVocab_;
  template <size_t NumColumns>
  IdTableView<NumColumns> asStaticView() const {
    return table_.asStaticView<NumColumns>();
  }
  const LocalVocab& getLocalVocab() const { return localVocab_; }
};

// The columns `?x ?y ?z` of the rows that the `rowAdder_` of
// `FilterOnRowAdder` writes.
VariableToColumnMap xyzColumns() {
  return VariableToColumnMap{{{Variable{"?x"}, makePossiblyUndefinedColumn(0)},
                              {Variable{"?y"}, makeAlwaysDefinedColumn(1)},
                              {Variable{"?z"}, makePossiblyUndefinedColumn(2)}},
                             makeAllocator()};
}

// An `AddCombinedRowToIdTable` for the inputs `left_` and `right_` (columns
// `?x ?y` and `?x ?z`) that uses an `OptionalJoinConditionFilter` with the
// condition `?y < ?z`.
struct FilterOnRowAdder {
  IdTable left_;
  IdTable right_;
  OptionalJoinConditionFilter::Condition condition_{parseExpression("?y < ?z")};
  OptionalJoinConditionFilter filter_;
  // The rows that the blockwise callback of the `rowAdder_` has seen last.
  IdTable seenByCallback_{3, makeAllocator()};
  ad_utility::AddCombinedRowToIdTable rowAdder_;

  explicit FilterOnRowAdder(
      size_t bufferSize,
      IdTable left = intTable({{1, 10}, {1, 20}, {2, 30}, {3, 40}}),
      IdTable right = intTable({{1, 15}, {1, 25}, {2, 5}, {3, 50}}),
      ad_utility::SharedCancellationHandle handleOfFilter =
          std::make_shared<ad_utility::CancellationHandle<>>())
      : left_{std::move(left)},
        right_{std::move(right)},
        filter_{*getQec(),
                condition_,
                xyzColumns(),
                2,
                std::move(handleOfFilter),
                makeAllocator()},
        rowAdder_{1,
                  left_.asStaticView<0>(),
                  right_.asStaticView<0>(),
                  IdTable{3, makeAllocator()},
                  std::make_shared<ad_utility::CancellationHandle<>>(),
                  true,
                  bufferSize,
                  [this](IdTable& table, LocalVocab&) {
                    seenByCallback_ = table.clone();
                  }} {
    rowAdder_.setRowFilter(&filter_);
  }
};

}  // namespace

// _____________________________________________________________________________
// The pairs for which the condition doesn't hold are removed. The left row 1
// (`?y = 20`) only has a match in its second pair, the left row 2 has no
// match, so it gets a row with UNDEF on the right at the end.
TEST(OptionalJoinConditionFilter, filterAndAddRowsWithoutMatch) {
  for (size_t bufferSize : {1, 2, 3, 100}) {
    FilterOnRowAdder f{bufferSize};
    auto& r = f.rowAdder_;
    // Join value 1: (10, 15) true, (10, 25) true, (20, 15) false, (20, 25)
    // true.
    r.addRow(0, 0);
    r.addRow(0, 1);
    r.addRow(1, 0);
    r.addRow(1, 1);
    // Join value 2: (30, 5) false.
    r.addRow(2, 2);
    // Join value 3: (40, 50) true.
    r.addRow(3, 3);
    auto result = std::move(r).resultTable();
    EXPECT_EQ(
        result,
        intTable(
            {{1, 10, 15}, {1, 10, 25}, {1, 20, 25}, {3, 40, 50}, {2, 30, U}}))
        << "buffer size " << bufferSize;
    EXPECT_EQ(f.filter_.numRowsWithoutMatch(), 1u);
  }
}

// _____________________________________________________________________________
// The row with UNDEF for a left row without a match is only written when the
// left rows are finished, because the left row might get another partner.
TEST(OptionalJoinConditionFilter, finishPreviousLeftRows) {
  FilterOnRowAdder f{1};
  auto& r = f.rowAdder_;
  r.addRow(2, 2);
  r.flush();
  EXPECT_EQ(f.seenByCallback_.size(), 0);
  r.finishPreviousLeftRows();
  EXPECT_EQ(f.seenByCallback_, intTable({{2, 30, U}}));
  // Rows without a partner are not affected by the condition.
  r.addOptionalRow(3);
  EXPECT_EQ(std::move(r).resultTable(), intTable({{2, 30, U}, {3, 40, U}}));
  EXPECT_EQ(f.filter_.numRowsWithoutMatch(), 1u);
}

// _____________________________________________________________________________
// The row with UNDEF for a left row without a match takes the values of the
// left row, also in the join column, which is UNDEF there, but bound in the
// joined row.
TEST(OptionalJoinConditionFilter, rowWithoutMatchHasValuesOfLeftRow) {
  FilterOnRowAdder f{100, intTable({{U, 10}}), intTable({{1, 5}})};
  f.rowAdder_.addRow(0, 0);
  EXPECT_EQ(std::move(f.rowAdder_).resultTable(), intTable({{U, 10, U}}));
}

// _____________________________________________________________________________
// A candidate keeps the local vocab of its left row, even if the row adder has
// dropped that vocab in the meantime. This happens if the input changes to
// another left block while the result is empty.
TEST(OptionalJoinConditionFilter, candidatesKeepTheirLocalVocab) {
  auto* qec = getQec();
  auto word = localVocabEntryFromStringRepresentation(
      "\"a\"", qec->getLocalVocabContext());
  // Columns `?x ?y ?s`, the word is only in the local vocab of block `a`.
  BlockWithLocalVocab a{intTable({{1, 10, 0}}), makeLocalVocab()};
  a.table_(0, 2) = Id::makeFromLocalVocabIndex(
      a.localVocab_.getIndexAndAddIfNotContained(word));
  BlockWithLocalVocab b{intTable({{1, 20, 0}}), makeLocalVocab()};
  IdTable right = intTable({{1, 5}});

  OptionalJoinConditionFilter::Condition condition{parseExpression("?y < ?z")};
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  OptionalJoinConditionFilter filter{
      *qec,
      condition,
      VariableToColumnMap{{{Variable{"?x"}, makeAlwaysDefinedColumn(0)},
                           {Variable{"?y"}, makeAlwaysDefinedColumn(1)},
                           {Variable{"?s"}, makeAlwaysDefinedColumn(2)},
                           {Variable{"?z"}, makePossiblyUndefinedColumn(3)}},
                          makeAllocator()},
      3,
      handle,
      makeAllocator()};
  // Like a lazy join, yield each non-empty result.
  std::vector<Result::IdTableVocabPair> yielded;
  ad_utility::AddCombinedRowToIdTable rowAdder{
      1,
      IdTable{4, makeAllocator()},
      handle,
      true,
      1,
      [&yielded](IdTable& table, LocalVocab& localVocab) {
        if (!table.empty()) {
          yielded.emplace_back(std::move(table), std::move(localVocab));
        }
      }};
  rowAdder.setRowFilter(&filter);
  // The only pair of the left row in `a` fails, it becomes a candidate.
  rowAdder.setInput(a, right);
  rowAdder.addRow(0, 0);
  // The result is still empty, so the row adder drops the local vocab of `a`.
  rowAdder.setInput(b, right);
  rowAdder.addRow(0, 0);
  rowAdder.finishPreviousLeftRows();
  EXPECT_TRUE(std::move(rowAdder).toIdTableVocabPair().idTable_.empty());

  ASSERT_EQ(yielded.size(), 1u);
  const auto& [table, localVocab] = yielded.at(0);
  EXPECT_EQ(table, makeIdTableFromVector({{I(1), I(10), a.table_(0, 2), U},
                                          {I(1), I(20), I(0), U}}));
  EXPECT_THAT(localVocab.getAllWordsForTesting(), ::testing::Contains(word));
}

// _____________________________________________________________________________
// The evaluation of the condition can be cancelled.
TEST(OptionalJoinConditionFilter, cancellation) {
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  FilterOnRowAdder f{100, intTable({{1, 10}}), intTable({{1, 15}}), handle};
  f.rowAdder_.addRow(0, 0);
  EXPECT_THROW(f.rowAdder_.flush(), ad_utility::CancellationException);
}

// Random inputs, compared to a direct implementation of `LeftJoin`.
namespace {

using Binding = std::map<std::string, Id>;
using Condition = std::function<bool(const Binding&)>;

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
// The result has one column per variable in `resultVars`.
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
      // Also uses the join variable `?x`.
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
        row.push_back(I(valueDist(gen)));
      }
    }
    relation.rows_.push_back(std::move(row));
  }
  return relation;
}

// `relation` as text for the message of a failed test, e.g.
// `?x ?y: (1 U) (2 3)`.
std::string relationToString(const Relation& relation) {
  auto formatId = [](std::string* out, Id id) {
    absl::StrAppend(out, id.isUndefined() ? "U" : std::to_string(id.getInt()));
  };
  std::string result = absl::StrJoin(relation.variables_, " ") + ":";
  for (const auto& row : relation.rows_) {
    absl::StrAppend(&result, " (", absl::StrJoin(row, " ", formatId), ")");
  }
  return result;
}

// Compute the result of `join` (an `OptionalJoin` of `left` and `right` with
// `condition`) and compare it to `referenceLeftJoin`. A failure shows the
// inputs.
void expectResultMatchesReference(
    OptionalJoin& join, const Relation& left, const Relation& right,
    const TestCondition& condition,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto actual = computeRows(join);
  // The variables of the result in the order of the columns.
  std::vector<std::string> resultVars(join.getResultWidth());
  for (const auto& [var, info] : join.getExternallyVisibleVariableColumns()) {
    resultVars.at(info.columnIndex_) = var.name();
  }
  auto expected =
      referenceLeftJoin(left, right, condition.function_, resultVars);
  std::string inputs = absl::StrCat("Condition: ", condition.sparql_,
                                    "\nLeft:  ", relationToString(left),
                                    "\nRight: ", relationToString(right));
  EXPECT_TRUE(isSortedAsPromised(join, actual)) << inputs;
  EXPECT_THAT(actual, ::testing::UnorderedElementsAreArray(expected)) << inputs;
}

}  // namespace

// _____________________________________________________________________________
// Random inputs with one, two, and three join columns, with and without UNDEF
// in the join columns on either side. This covers the implementations of
// `optionalJoin` except for the galloping join (see `gallopingJoin`): the
// zipper join without UNDEF, the `specialOptionalJoin` (UNDEF only in the last
// join column on the left), and the general zipper join with UNDEF, which can
// write rows out of order.
TEST(OptionalJoinWithCondition, joinColumnsWithUndefRandomInputs) {
  std::mt19937 gen{7};
  struct Config {
    size_t sizeLeft_;
    size_t sizeRight_;
    int64_t maxValue_;
  };
  std::vector<Config> configs{
      {20, 20, 2},
      {50, 10, 3},
      {10, 50, 3},
      {1, 1, 2},
      // Few join values with many rows each.
      {30, 30, 1},
  };
  // The probability of UNDEF in the join columns of the left and the right
  // input, and whether only the last join column of the left input can be
  // UNDEF.
  struct Undef {
    double left_;
    double right_;
    bool onlyLastLeftColumn_;
  };
  std::vector<Undef> undefs{{0, 0, false},
                            {0.2, 0, true},
                            {0.2, 0, false},
                            {0, 0.2, false},
                            {0.2, 0.2, false}};
  for (std::vector<std::string> joinVariables :
       {std::vector<std::string>{"?x"}, {"?x", "?w"}, {"?x", "?w", "?v"}}) {
    const size_t numJoinColumns = joinVariables.size();
    auto withVariable = [&joinVariables](std::string variable) {
      auto variables = joinVariables;
      variables.push_back(std::move(variable));
      return variables;
    };
    for (const auto& undef : undefs) {
      std::vector<double> undefLeft(numJoinColumns, undef.left_);
      if (undef.onlyLastLeftColumn_) {
        ql::ranges::fill(undefLeft.begin(), undefLeft.end() - 1, 0.0);
      }
      std::vector<double> undefRight(numJoinColumns, undef.right_);
      // UNDEF also in the columns that are not join columns.
      undefLeft.push_back(0.1);
      undefRight.push_back(0.1);
      for (const auto& condition : testConditions()) {
        for (const auto& c : configs) {
          for (size_t iteration = 0; iteration < 2; ++iteration) {
            auto left = randomRelation(gen, withVariable("?y"), c.sizeLeft_,
                                       c.maxValue_, undefLeft);
            auto right = randomRelation(gen, withVariable("?z"), c.sizeRight_,
                                        c.maxValue_, undefRight);
            for (bool keepJoinColumns : {true, false}) {
              for (size_t bufferSize : {1, 100'000}) {
                auto join =
                    makeJoin(left, right, {condition.sparql_}, keepJoinColumns);
                join.setRowAdderBufferSizeForTesting(bufferSize);
                expectResultMatchesReference(join, left, right, condition);
              }
            }
          }
        }
      }
    }
  }
}
