// Copyright 2018 - 2026 The QLever Authors, in particular:
//
// 2018 Florian Kramer <florian.kramer@netpun.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for the `Join` operation with more than one join column.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/NeutralOptional.h"
#include "engine/Sort.h"
#include "engine/ValuesForTesting.h"
#include "util/AllocatorTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

using ad_utility::testing::makeAllocator;
namespace {
auto V = ad_utility::testing::VocabId;
constexpr auto U = Id::makeUndefined();
using Vars = std::vector<std::optional<Variable>>;
using JoinColumns = Join::JoinColumns;
using Tree = std::shared_ptr<QueryExecutionTree>;
using Rows = std::vector<std::vector<Id>>;
using OptSize = std::optional<size_t>;

// Return a vector of `Variable`s with the given `names`.
template <typename... Names>
std::vector<Variable> vars(const Names&... names) {
  return std::vector<Variable>{Variable{names}...};
}

// Create a `Join` of `a` and `b` on all their common variables.
Tree joinOnCommonVars(QueryExecutionContext* qec, const Tree& a,
                      const Tree& b) {
  return ad_utility::makeExecutionTree<Join>(
      qec, a, b, QueryExecutionTree::getJoinColumns(*a, *b));
}

// Create an `IndexScan` for the triple `subject predicate object` (where the
// `subject` and `object` are variables) with the given `permutation`.
Tree makeScan(QueryExecutionContext* qec, Permutation::Enum permutation,
              std::string_view subject, std::string_view predicate,
              std::string_view object) {
  return ad_utility::makeExecutionTree<IndexScan>(
      qec, permutation,
      SparqlTripleSimple{
          Variable{std::string{subject}},
          ad_utility::triple_component::Iri::fromIriref(predicate),
          Variable{std::string{object}}});
}

// Return the contents of `table` as rows of the values of the given `vars` (in
// this order), sorted lexicographically. That way, results can be compared
// independently of the order of their rows and columns.
Rows toSortedRows(const IdTable& table, const VariableToColumnMap& varToCol,
                  const std::vector<Variable>& vars) {
  Rows rows;
  for (size_t i = 0; i < table.numRows(); ++i) {
    std::vector<Id> row;
    for (const auto& var : vars) {
      row.push_back(table(i, varToCol.at(var).columnIndex_));
    }
    rows.push_back(std::move(row));
  }
  ql::ranges::sort(rows);
  return rows;
}

// Return the variables of the `varToCol` map, sorted by their name.
std::vector<Variable> sortedVariables(const VariableToColumnMap& varToCol) {
  std::vector<Variable> vars;
  for (const auto& [var, _] : varToCol) {
    vars.push_back(var);
  }
  ql::ranges::sort(vars, [](const Variable& a, const Variable& b) {
    return a.name() < b.name();
  });
  return vars;
}

// Compute the result of the `tree` (which must be the root of a fresh tree, or
// the cache must have been cleared), either lazily or fully materialized, and
// return it as a single `IdTable`. If `checkSorted` is true, also check that
// the result is sorted as promised by `resultSortedOn()`.
IdTable computeResult(QueryExecutionTree& tree, bool requestLaziness,
                      bool checkSorted = true) {
  auto& op = *tree.getRootOperation();
  auto res = op.getResult(false, requestLaziness
                                     ? ComputationMode::LAZY_IF_SUPPORTED
                                     : ComputationMode::FULLY_MATERIALIZED);
  IdTable table =
      res->isFullyMaterialized()
          ? res->idTableView().clone()
          : aggregateTables(res->idTables(), op.getResultWidth()).first;
  if (checkSorted) {
    auto sortedOn = tree.resultSortedOn();
    EXPECT_TRUE(std::is_sorted(table.begin(), table.end(),
                               [&sortedOn](const auto& a, const auto& b) {
                                 for (auto col : sortedOn) {
                                   if (a[col] != b[col]) {
                                     return a[col] < b[col];
                                   }
                                 }
                                 return false;
                               }));
  }
  return table;
}

// Compute the result of `tree` and return it as rows of the values of all its
// variables sorted by their names (see `toSortedRows` above).
Rows sortedResultRows(QueryExecutionTree& tree, bool requestLaziness) {
  return toSortedRows(computeResult(tree, requestLaziness),
                      tree.getVariableColumns(),
                      sortedVariables(tree.getVariableColumns()));
}

// Compute the join of the (fully materialized) results of `left` and `right`
// on all their common variables with a simple nested-loop join. Return the
// result as rows of the values of all variables sorted by their names (see
// `toSortedRows` above).
Rows bruteForceJoin(QueryExecutionTree& left, QueryExecutionTree& right) {
  auto leftTable = computeResult(left, false, false);
  auto rightTable = computeResult(right, false, false);
  const auto& leftVars = left.getVariableColumns();
  const auto& rightVars = right.getVariableColumns();
  VariableToColumnMap allVars = leftVars;
  for (const auto& [var, col] : rightVars) {
    allVars.try_emplace(var, col);
  }
  auto vars = sortedVariables(allVars);

  Rows result;
  for (size_t i = 0; i < leftTable.numRows(); ++i) {
    for (size_t j = 0; j < rightTable.numRows(); ++j) {
      std::vector<Id> row;
      bool compatible = true;
      for (const auto& var : vars) {
        auto l = leftVars.contains(var)
                     ? leftTable(i, leftVars.at(var).columnIndex_)
                     : U;
        auto r = rightVars.contains(var)
                     ? rightTable(j, rightVars.at(var).columnIndex_)
                     : U;
        if (!l.isUndefined() && !r.isUndefined() && l != r) {
          compatible = false;
          break;
        }
        row.push_back(l.isUndefined() ? r : l);
      }
      if (compatible) {
        result.push_back(std::move(row));
      }
    }
  }
  ql::ranges::sort(result);
  return result;
}

// Check that the result of `join` (lazily and fully materialized) is equal to
// the brute-force join of its children.
void expectJoinMatchesBruteForce(
    QueryExecutionContext* qec, const Tree& join,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto children = join->getRootOperation()->getChildren();
  ASSERT_EQ(children.size(), 2u);
  auto expected = bruteForceJoin(*children.at(0), *children.at(1));
  for (bool requestLaziness : {false, true}) {
    qec->getQueryTreeCache().clearAll();
    EXPECT_EQ(sortedResultRows(*join, requestLaziness), expected)
        << "requestLaziness: " << requestLaziness;
  }
  qec->getQueryTreeCache().clearAll();
}

// Create a `ValuesForTesting` with the given `variables` from the `rows`. If
// `blockSize` is set, the result is split into blocks of this size and can be
// lazily computed. The result is declared to be sorted by `sortedOn` (the
// caller is responsible for actually sorting the `rows`).
Tree makeValues(QueryExecutionContext* qec, const Rows& rows,
                const std::vector<Variable>& variables,
                std::optional<size_t> blockSize = std::nullopt,
                std::vector<ColumnIndex> sortedOn = {}) {
  Vars vars(variables.begin(), variables.end());
  std::vector<IdTable> tables;
  for (size_t i = 0; i < rows.size() || tables.empty();
       i += blockSize.value_or(std::max(rows.size(), size_t{1}))) {
    IdTable table{variables.size(), makeAllocator()};
    for (size_t j = i;
         j < std::min(rows.size(), i + blockSize.value_or(rows.size())); ++j) {
      table.emplace_back();
      for (size_t col = 0; col < variables.size(); ++col) {
        table(table.numRows() - 1, col) = rows.at(j).at(col);
      }
    }
    tables.push_back(std::move(table));
  }
  if (!blockSize.has_value()) {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(tables.at(0)), std::move(vars), false,
        std::move(sortedOn), LocalVocab{}, std::nullopt, true);
  }
  return ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(tables), std::move(vars), false, std::move(sortedOn));
}

// Return the variables of the result of `join` that the `resultSortedOn()`
// refers to.
std::vector<Variable> sortedOnVariables(const QueryExecutionTree& join) {
  std::vector<Variable> result;
  for (auto col : join.resultSortedOn()) {
    result.push_back(join.getVariableAndInfoByColumnIndex(col).first);
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(MultiColumnJoin, basic) {
  auto* qec = ad_utility::testing::getQec();
  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  auto a = makeValues(qec,
                      {{V(4), V(1), V(2)},
                       {V(2), V(1), V(3)},
                       {V(1), V(1), V(4)},
                       {V(2), V(2), V(1)},
                       {V(1), V(3), V(1)}},
                      vars("?a", "?x", "?y"));
  auto b = makeValues(qec,
                      {{V(3), V(3), V(1)},
                       {V(1), V(8), V(1)},
                       {V(4), V(2), V(2)},
                       {V(1), V(1), V(3)}},
                      vars("?b", "?y", "?x"));
  auto join = ad_utility::makeExecutionTree<Join>(qec, a, b,
                                                  JoinColumns{{1, 2}, {2, 1}});
  EXPECT_EQ(join->getResultWidth(), 4u);
  // The variables sorted by name are `?a ?b ?x ?y`.
  Rows expected{{V(1), V(1), V(3), V(1)}, {V(2), V(3), V(1), V(3)}};
  EXPECT_EQ(sortedResultRows(*join, false), expected);
  expectJoinMatchesBruteForce(qec, join);

  // Test the multi column join with wider inputs.
  auto va = makeValues(qec,
                       {{V(1), V(2), V(3), V(4), V(5), V(6)},
                        {V(1), V(2), V(3), V(7), V(5), V(6)},
                        {V(7), V(6), V(5), V(4), V(3), V(2)}},
                       vars("?a", "?x", "?y", "?b", "?c", "?d"));
  auto vb = makeValues(
      qec, {{V(2), V(3), V(4)}, {V(2), V(3), V(5)}, {V(6), V(7), V(4)}},
      vars("?x", "?y", "?e"));
  auto join2 = ad_utility::makeExecutionTree<Join>(qec, va, vb,
                                                   JoinColumns{{1, 0}, {2, 1}});
  EXPECT_EQ(join2->getResultWidth(), 7u);
  EXPECT_EQ(computeResult(*join2, false).numRows(), 4u);
  expectJoinMatchesBruteForce(qec, join2);
}

// Test the join with UNDEF values in the join columns. An UNDEF value matches
// every value, and the join value of a result row is the one of the side that
// is not UNDEF. Such inputs always take the fully materialized path.
TEST(MultiColumnJoin, undefInJoinColumns) {
  auto* qec = ad_utility::testing::getQec();
  auto leftVars = vars("?x", "?y");
  auto rightVars = vars("?x", "?y", "?z");

  auto test = [&](Rows left, Rows right, const Rows& expected,
                  ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(loc);
    ql::ranges::sort(left);
    ql::ranges::sort(right);
    for (OptSize blockSize : {OptSize{}, OptSize{1}}) {
      auto join = joinOnCommonVars(
          qec, makeValues(qec, left, leftVars, blockSize, {0, 1}),
          makeValues(qec, right, rightVars, blockSize, {0, 1}));
      // The variables sorted by name are `?x ?y ?z`.
      EXPECT_EQ(sortedResultRows(*join, false), expected);
      expectJoinMatchesBruteForce(qec, join);
    }
  };

  // UNDEF only in a join column of the right input.
  test({{V(1), V(10)}, {V(2), V(20)}},
       {{U, V(10), V(100)}, {V(2), V(20), V(200)}},
       {{V(1), V(10), V(100)}, {V(2), V(20), V(200)}});
  // UNDEF only in a join column of the left input.
  test({{U, V(10)}, {V(2), V(20)}},
       {{V(1), V(10), V(100)}, {V(2), V(20), V(200)}},
       {{V(1), V(10), V(100)}, {V(2), V(20), V(200)}});
  // UNDEF in a join column of both inputs. The result is out of order and has
  // to be sorted.
  test({{U, V(10)}, {V(2), V(20)}}, {{V(1), V(10), V(100)}, {V(2), U, V(200)}},
       {{V(1), V(10), V(100)}, {V(2), V(10), V(200)}, {V(2), V(20), V(200)}});
  // UNDEF only in the second join column.
  test({{V(1), U}, {V(2), V(20)}},
       {{V(1), V(10), V(100)}, {V(1), V(11), V(101)}, {V(2), V(21), V(200)}},
       {{V(1), V(10), V(100)}, {V(1), V(11), V(101)}});
}

// _____________________________________________________________________________
TEST(MultiColumnJoin, descriptorCacheKeyAndEstimates) {
  auto* qec = ad_utility::testing::getQec();
  auto a = makeValues(qec, {{V(1), V(2), V(3)}}, vars("?x", "?y", "?a"));
  auto b = makeValues(qec, {{V(2), V(1)}}, vars("?y", "?x"));
  auto join = joinOnCommonVars(qec, a, b);
  const auto& op = *join->getRootOperation();
  // No child is sorted, so the join columns are sorted by the column indices
  // of the child with the smaller cache key, which yields either `?x ?y` or
  // `?y ?x`.
  EXPECT_THAT(op.getDescriptor(),
              ::testing::AnyOf("Join on ?x ?y", "Join on ?y ?x"));
  EXPECT_THAT(op.getCacheKey(), ::testing::HasSubstr("join-column: [0 & 1]"));
  EXPECT_EQ(op.getResultWidth(), 3u);
  EXPECT_EQ(sortedOnVariables(*join).size(), 2u);

  // The cache key and the order of the join columns don't depend on the order
  // of the children.
  auto joinSwapped = joinOnCommonVars(qec, b, a);
  EXPECT_EQ(op.getCacheKey(), joinSwapped->getRootOperation()->getCacheKey());
  EXPECT_EQ(op.getDescriptor(),
            joinSwapped->getRootOperation()->getDescriptor());

  // Check that the size estimate and multiplicities are computed.
  auto& mutableOp = *join->getRootOperation();
  EXPECT_GT(mutableOp.getSizeEstimate(), 0u);
  for (size_t i = 0; i < mutableOp.getResultWidth(); ++i) {
    EXPECT_GT(mutableOp.getMultiplicity(i), 0.0f);
  }
  EXPECT_GT(mutableOp.getCostEstimate(), 0u);

  // Removing the join columns is only supported for a single join column.
  AD_EXPECT_THROW_WITH_MESSAGE(
      Join(qec, a, b, JoinColumns{{0, 1}, {1, 0}}, false),
      ::testing::HasSubstr("only supported for a single join column"));
  // There must be at least one join column.
  EXPECT_ANY_THROW(Join(qec, a, b, JoinColumns{}));
}

// Test that the order of the join columns is chosen s.t. one of the children
// doesn't have to be sorted.
TEST(MultiColumnJoin, chooseJoinColumnOrder) {
  auto* qec = ad_utility::testing::getQec("<s> <p> <o> . <s> <q> <o> .");
  // Sorted by `?y`, then `?x`.
  auto sortedByYX =
      makeValues(qec, {{V(1), V(2)}}, vars("?x", "?y"), {}, {1, 0});
  // Sorted by `?x`, then `?y`.
  auto sortedByXY =
      makeValues(qec, {{V(1), V(2), V(3)}}, vars("?x", "?y", "?z"), {}, {0, 1});
  auto unsorted = makeValues(qec, {{V(2), V(1)}}, vars("?y", "?x"));

  // Join `a` and `b`, or `b` and `a` if `swap` is true.
  auto join = [qec](const Tree& a, const Tree& b, bool swap) {
    return swap ? joinOnCommonVars(qec, b, a) : joinOnCommonVars(qec, a, b);
  };
  auto isSort = [](const QueryExecutionTree* tree) {
    return std::dynamic_pointer_cast<const Sort>(tree->getRootOperation()) !=
           nullptr;
  };
  auto numSorts = [&isSort](const Tree& join) {
    auto children = join->getRootOperation()->getChildren();
    return ql::ranges::count_if(children, isSort);
  };

  // The sorted child determines the order, independent of the order of the
  // children.
  for (bool swap : {false, true}) {
    auto joinTree = join(sortedByYX, unsorted, swap);
    EXPECT_EQ(sortedOnVariables(*joinTree), vars("?y", "?x"));
    EXPECT_EQ(numSorts(joinTree), 1);
  }

  // If both children are sorted on the join columns, but in different orders,
  // one of them has to be sorted.
  {
    auto joinTree = joinOnCommonVars(qec, sortedByYX, sortedByXY);
    EXPECT_EQ(numSorts(joinTree), 1);
    expectJoinMatchesBruteForce(qec, joinTree);
  }

  // A scan is preferred over another sorted child.
  auto scanPSO = makeScan(qec, Permutation::PSO, "?x", "<p>", "?y");
  auto scanPOS = makeScan(qec, Permutation::POS, "?x", "<p>", "?y");
  for (bool swap : {false, true}) {
    auto joinTree = join(scanPSO, sortedByYX, swap);
    EXPECT_EQ(sortedOnVariables(*joinTree), vars("?x", "?y"));
    // The scan has to be the right child, and it must not be sorted.
    auto children = joinTree->getRootOperation()->getChildren();
    EXPECT_TRUE(IndexScan::getIfSupportsPrefilteredJoin(*children.at(1)));
    expectJoinMatchesBruteForce(qec, joinTree);

    auto joinPOS = join(scanPOS, sortedByXY, swap);
    EXPECT_EQ(sortedOnVariables(*joinPOS), vars("?y", "?x"));
    EXPECT_TRUE(IndexScan::getIfSupportsPrefilteredJoin(
        *joinPOS->getRootOperation()->getChildren().at(1)));
  }
}

// Test the special implementations for joins with `IndexScan`s (with
// prefiltering of the blocks) for multiple join columns.
TEST(MultiColumnJoin, joinWithIndexScans) {
  // Create a knowledge graph with many triples for `<p>` and `<q>`, s.t. the
  // scans consist of multiple blocks.
  std::string turtle;
  for (size_t i = 0; i < 40; ++i) {
    for (size_t j = 0; j < 5; ++j) {
      absl::StrAppend(&turtle, "<s", i, "> <p> <o", (i * j) % 7, "> .\n");
      if ((i + j) % 3 != 0) {
        absl::StrAppend(&turtle, "<s", i, "> <q> <o", (i + j) % 7, "> .\n");
      }
    }
  }
  ad_utility::testing::TestIndexConfig config;
  config.turtleInput = turtle;
  config.rowsPerBlock = 4;
  auto* qec = ad_utility::testing::getQec(config);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  auto scanP = makeScan(qec, Permutation::PSO, "?s", "<p>", "?o");
  auto scanQ = makeScan(qec, Permutation::PSO, "?s", "<q>", "?o");
  auto scanQPOS = makeScan(qec, Permutation::POS, "?s", "<q>", "?o");

  // Values for `?s ?o ?x`, sorted by `?s ?o` with some pairs that are not
  // contained in the index.
  Rows values;
  for (size_t i = 0; i < 40; i += 3) {
    values.push_back({getId(absl::StrCat("<s", i, ">")),
                      getId(absl::StrCat("<o", (i * 2) % 7, ">")), V(i)});
    values.push_back({getId(absl::StrCat("<s", i, ">")),
                      getId(absl::StrCat("<o", (i + 3) % 7, ">")), V(i + 1)});
  }
  ql::ranges::sort(values);
  auto valueVars = vars("?s", "?o", "?x");

  for (size_t threshold : {0, 1'000'000}) {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::lazyIndexScanMaxSizeMaterialization_>(threshold);
    auto join = [qec](const Tree& a, const Tree& b) {
      return joinOnCommonVars(qec, a, b);
    };
    // Two scans.
    expectJoinMatchesBruteForce(qec, join(scanP, scanQ));
    expectJoinMatchesBruteForce(qec, join(scanQ, scanP));
    // Two scans with different orders.
    expectJoinMatchesBruteForce(qec, join(scanP, scanQPOS));
    // A scan and a fully materialized and a lazy input.
    for (OptSize blockSize : {OptSize{}, OptSize{1}, OptSize{4}}) {
      auto valuesTree = makeValues(qec, values, valueVars, blockSize, {0, 1});
      expectJoinMatchesBruteForce(qec, join(scanP, valuesTree));
      expectJoinMatchesBruteForce(qec, join(valuesTree, scanQ));
    }
    // An input with UNDEF values in the join columns takes the fully
    // materialized code path.
    Rows valuesWithUndef = values;
    valuesWithUndef.push_back({U, getId("<o3>"), V(1000)});
    valuesWithUndef.push_back({getId("<s5>"), U, V(1001)});
    ql::ranges::sort(valuesWithUndef);
    for (OptSize blockSize : {OptSize{}, OptSize{2}}) {
      auto valuesTree =
          makeValues(qec, valuesWithUndef, valueVars, blockSize, {0, 1});
      expectJoinMatchesBruteForce(qec, join(scanP, valuesTree));
    }
  }
}

// Randomized test that compares all code paths of the `Join` for one to four
// join columns, with and without UNDEF values, with a brute-force join.
TEST(MultiColumnJoin, randomizedComparisonWithBruteForce) {
  auto* qec = ad_utility::testing::getQec();
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::lazyIndexScanMaxSizeMaterialization_>(0);
  std::mt19937 gen{42};
  auto randomInt = [&gen](size_t min, size_t max) {
    return std::uniform_int_distribution<size_t>{min, max}(gen);
  };

  // Create random rows for the `numJoinCols` join columns followed by
  // `numExtraCols` other columns, sorted by the join columns.
  auto makeRows = [&](size_t numRows, size_t numJoinCols, size_t numExtraCols,
                      double undefProbability) {
    std::bernoulli_distribution isUndef{undefProbability};
    Rows rows;
    for (size_t i = 0; i < numRows; ++i) {
      std::vector<Id> row;
      for (size_t col = 0; col < numJoinCols; ++col) {
        row.push_back(isUndef(gen) ? U : V(randomInt(0, 3)));
      }
      for (size_t col = 0; col < numExtraCols; ++col) {
        row.push_back(V(randomInt(0, 1000)));
      }
      rows.push_back(std::move(row));
    }
    ql::ranges::sort(rows);
    return rows;
  };

  for (size_t iteration = 0; iteration < 400; ++iteration) {
    size_t numJoinCols = randomInt(1, 4);
    double undefProbability = randomInt(0, 2) == 0 ? 0.15 : 0.0;
    // Sometimes use very different sizes to trigger the galloping join.
    bool unbalanced = iteration % 10 == 0;
    size_t numRowsLeft = unbalanced ? randomInt(0, 3) : randomInt(0, 40);
    size_t numRowsRight = unbalanced ? 5000 : randomInt(0, 40);
    size_t numExtraLeft = randomInt(0, 2);
    size_t numExtraRight = randomInt(0, 2);

    auto makeVars = [numJoinCols](size_t numExtra, std::string_view prefix) {
      std::vector<Variable> vars;
      for (size_t i = 0; i < numJoinCols; ++i) {
        vars.emplace_back(absl::StrCat("?j", i));
      }
      for (size_t i = 0; i < numExtra; ++i) {
        vars.emplace_back(absl::StrCat("?", prefix, i));
      }
      return vars;
    };
    auto leftRows =
        makeRows(numRowsLeft, numJoinCols, numExtraLeft, undefProbability);
    auto rightRows =
        makeRows(numRowsRight, numJoinCols, numExtraRight, undefProbability);
    std::vector<ColumnIndex> sortedOn;
    JoinColumns joinColumns;
    for (size_t i = 0; i < numJoinCols; ++i) {
      sortedOn.push_back(i);
      joinColumns.push_back({i, i});
    }

    auto makeTree = [&](const Rows& rows, const std::vector<Variable>& vars) {
      OptSize blockSize;
      if (randomInt(0, 1) == 0) {
        blockSize = randomInt(1, 10);
      }
      return makeValues(qec, rows, vars, blockSize, sortedOn);
    };
    auto left = makeTree(leftRows, makeVars(numExtraLeft, "l"));
    auto right = makeTree(rightRows, makeVars(numExtraRight, "r"));
    auto trace = generateLocationTrace(
        AD_CURRENT_SOURCE_LOC(),
        absl::StrCat("iteration ", iteration, ", numJoinCols ", numJoinCols,
                     ", undef ", undefProbability));
    expectJoinMatchesBruteForce(qec, ad_utility::makeExecutionTree<Join>(
                                         qec, left, right, joinColumns));
    expectJoinMatchesBruteForce(qec, ad_utility::makeExecutionTree<Join>(
                                         qec, right, left, joinColumns));
  }
}

// _____________________________________________________________________________
TEST(MultiColumnJoin, clone) {
  auto* qec = ad_utility::testing::getQec();
  auto a = makeValues(qec, {{V(4), V(1), V(2)}}, vars("?a", "?b", "?c"));
  Join join{qec, a, a, JoinColumns{{0, 0}, {1, 1}, {2, 2}}};

  auto clone = join.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(join, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), join.getDescriptor());
  EXPECT_EQ(clone->getDescriptor(), "Join on ?a ?b ?c");
}

// Test `makeTreeWithStrippedColumns`, which for multiple join columns must
// never remove the join columns.
TEST(MultiColumnJoin, makeTreeWithStrippedColumns) {
  auto* qec = ad_utility::testing::getQec();
  auto a = makeValues(qec, {{V(1), V(2), V(3)}}, vars("?x", "?y", "?a"));
  auto b = makeValues(qec, {{V(1), V(2), V(4)}}, vars("?x", "?y", "?b"));
  auto join = joinOnCommonVars(qec, a, b);
  auto stripped =
      QueryExecutionTree::makeTreeWithStrippedColumns(join, {Variable{"?a"}});
  ASSERT_TRUE(
      std::dynamic_pointer_cast<const Join>(stripped->getRootOperation()));
  const auto& varCols = stripped->getVariableColumns();
  EXPECT_TRUE(varCols.contains(Variable{"?x"}));
  EXPECT_TRUE(varCols.contains(Variable{"?y"}));
  EXPECT_TRUE(varCols.contains(Variable{"?a"}));
  EXPECT_EQ(stripped->getRootOperation()->getDescriptor(), "Join on ?x ?y");
  expectJoinMatchesBruteForce(qec, stripped);
}

// _____________________________________________________________________________
TEST(MultiColumnJoin, columnOriginatesFromGraphOrUndef) {
  using ad_utility::triple_component::Iri;
  auto* qec = ad_utility::testing::getQec();
  // Not in graph no undef
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  // Not in graph, potentially undef
  auto values3 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{Id::makeUndefined(), Id::makeUndefined()}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto values4 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{Id::makeUndefined(), Id::makeUndefined()}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  // In graph, no undef
  auto index1 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?c"}});
  auto index2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?b"}});
  // In graph, potential undef
  auto index3 = ad_utility::makeExecutionTree<NeutralOptional>(
      qec, ad_utility::makeExecutionTree<IndexScan>(
               qec, Permutation::PSO,
               SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                                  Variable{"?c"}}));
  auto index4 = ad_utility::makeExecutionTree<NeutralOptional>(
      qec, ad_utility::makeExecutionTree<IndexScan>(
               qec, Permutation::PSO,
               SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                                  Variable{"?b"}}));

  auto testWithTrees = [qec](std::shared_ptr<QueryExecutionTree> left,
                             std::shared_ptr<QueryExecutionTree> right, bool a,
                             bool b, bool c,
                             ad_utility::source_location location =
                                 AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(location);

    auto joinColumns = QueryExecutionTree::getJoinColumns(*left, *right);
    Join join{
        qec,  std::move(left), std::move(right), std::move(joinColumns), true,
        false};
    EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?a"}), a);
    EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?b"}), b);
    EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?c"}), c);
    EXPECT_THROW(
        join.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
        ad_utility::Exception);
  };

  testWithTrees(index3, index4, true, true, true);
  testWithTrees(index3, index2, true, true, true);
  testWithTrees(index3, values4, false, false, true);
  testWithTrees(index3, values2, false, false, true);
  testWithTrees(index1, index4, true, true, true);
  testWithTrees(index1, index2, true, true, true);
  testWithTrees(index1, values4, true, false, true);
  testWithTrees(index1, values2, true, false, true);
  testWithTrees(values4, index3, false, false, true);
  testWithTrees(values4, index1, true, false, true);
  testWithTrees(values4, values3, false, false, false);
  testWithTrees(values4, values1, false, false, false);
  testWithTrees(values2, index3, false, false, true);
  testWithTrees(values2, index1, true, false, true);
  testWithTrees(values2, values3, false, false, false);
  testWithTrees(values2, values1, false, false, false);
}
