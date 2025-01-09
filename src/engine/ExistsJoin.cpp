//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/ExistsJoin.h"

#include "engine/QueryPlanner.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

// _____________________________________________________________________________
ExistsJoin::ExistsJoin(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> left,
                       std::shared_ptr<QueryExecutionTree> right,
                       Variable existsVariable)
    : Operation{qec},
      left_{std::move(left)},
      right_{std::move(right)},
      joinColumns_{QueryExecutionTree::getJoinColumns(*left_, *right_)},
      existsVariable_{std::move(existsVariable)} {
  std::tie(left_, right_) = QueryExecutionTree::createSortedTrees(
      std::move(left_), std::move(right_), joinColumns_);
}

// _____________________________________________________________________________
string ExistsJoin::getCacheKeyImpl() const {
  return absl::StrCat("EXISTS JOIN left: ", left_->getCacheKey(),
                      " right: ", right_->getCacheKey());
}

// _____________________________________________________________________________
string ExistsJoin::getDescriptor() const { return "Exists Join"; }

// ____________________________________________________________________________
VariableToColumnMap ExistsJoin::computeVariableToColumnMap() const {
  auto res = left_->getVariableColumns();
  AD_CONTRACT_CHECK(
      !res.contains(existsVariable_),
      "The target variable of an exists scan must be a new variable");
  res[existsVariable_] = makeAlwaysDefinedColumn(getResultWidth() - 1);
  return res;
}

// ____________________________________________________________________________
size_t ExistsJoin::getResultWidth() const {
  // We add one column to the input.
  return left_->getResultWidth() + 1;
}

// ____________________________________________________________________________
vector<ColumnIndex> ExistsJoin::resultSortedOn() const {
  return left_->resultSortedOn();
}

// ____________________________________________________________________________
float ExistsJoin::getMultiplicity(size_t col) {
  if (col < getResultWidth() - 1) {
    return left_->getMultiplicity(col);
  }
  // The multiplicity of the boolean column can be a dummy value, as it should
  // be never used for joins etc.
  return 1;
}

// ____________________________________________________________________________
uint64_t ExistsJoin::getSizeEstimateBeforeLimit() {
  return left_->getSizeEstimate();
}

// ____________________________________________________________________________
size_t ExistsJoin::getCostEstimate() {
  // The implementation is a linear zipper join.
  return left_->getCostEstimate() + right_->getCostEstimate() +
         left_->getSizeEstimate() + right_->getSizeEstimate();
}

// ____________________________________________________________________________
ProtoResult ExistsJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  auto leftRes = left_->getResult();
  auto rightRes = right_->getResult();
  const auto& left = leftRes->idTable();
  const auto& right = rightRes->idTable();

  // We reuse the generic `zipperJoinWithUndef` utility in the following way:
  // It has (among others) two callbacks: One for each matching pair of rows
  // from left and right, and one for rows in the left input that have no
  // matching counterpart in the right input. The first callback can be a noop,
  // and the second callback gives us exactly `NOT EXISTS`.

  // Only extract the join columns from both inputs to make the following code
  // easier.
  ad_utility::JoinColumnMapping joinColumnData{joinColumns_, left.numColumns(),
                                               right.numColumns()};
  IdTableView<0> joinColumnsLeft =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> joinColumnsRight =
      right.asColumnSubsetView(joinColumnData.jcsRight());

  checkCancellation();

  // `isCheap` is true iff there are no UNDEF values in the join columns. In
  // this case we can use a much cheaper algorithm.
  // TODO<joka921> There are many other cases where a cheaper implementation can
  // be chosen, but we leave those for another PR, this is the most common case.
  namespace stdr = ql::ranges;
  size_t numJoinColumns = joinColumnsLeft.numColumns();
  AD_CORRECTNESS_CHECK(numJoinColumns == joinColumnsRight.numColumns());
  bool isCheap = stdr::none_of(
      ad_utility::integerRange(numJoinColumns), [&](const auto& col) {
        return (stdr::any_of(joinColumnsRight.getColumn(col),
                             &Id::isUndefined)) ||
               (stdr::any_of(joinColumnsLeft.getColumn(col), &Id::isUndefined));
      });

  // Nothing to do for the actual matches.
  auto noopRowAdder = ad_utility::noop;

  // Store the indices of rows for which `exists` is `false`.
  std::vector<size_t, ad_utility::AllocatorWithLimit<size_t>> notExistsIndices{
      allocator()};
  // The callback is called with iterators, so we convert them back to indices.
  auto actionForNotExisting =
      [&notExistsIndices, begin = joinColumnsLeft.begin()](const auto& itLeft) {
        notExistsIndices.push_back(itLeft - begin);
      };

  // Run the actual zipper join, with the possible optimization if we know, that
  // there can be no UNDEF values.
  auto checkCancellationLambda = [this] { checkCancellation(); };
  auto runZipperJoin = [&](auto findUndef) {
    [[maybe_unused]] auto numOutOfOrder = ad_utility::zipperJoinWithUndef(
        joinColumnsLeft, joinColumnsRight, ql::ranges::lexicographical_compare,
        noopRowAdder, findUndef, findUndef, actionForNotExisting,
        checkCancellationLambda);
  };
  if (isCheap) {
    runZipperJoin(ad_utility::noop);
  } else {
    runZipperJoin(ad_utility::findSmallerUndefRanges);
  }

  // Set up the result;
  IdTable result = left.clone();
  result.addEmptyColumn();
  decltype(auto) existsCol = result.getColumn(getResultWidth() - 1);
  ql::ranges::fill(existsCol, Id::makeFromBool(true));
  for (size_t notExistsIndex : notExistsIndices) {
    existsCol[notExistsIndex] = Id::makeFromBool(false);
  }

  // The result is a copy of the left input + and additional columns with only
  // boolean values, so the local vocab of the left input is sufficient.
  return {std::move(result), resultSortedOn(), leftRes->getCopyOfLocalVocab()};
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> ExistsJoin::addExistsJoinsToSubtree(
    const sparqlExpression::SparqlExpressionPimpl& expression,
    std::shared_ptr<QueryExecutionTree> subtree, QueryExecutionContext* qec,
    const ad_utility::SharedCancellationHandle& cancellationHandle) {
  // First extract all the `EXISTS` functions from the expression.
  std::vector<const sparqlExpression::SparqlExpression*> existsExpressions;
  expression.getPimpl()->getExistsExpressions(existsExpressions);

  // For each of the EXISTS functions add one `ExistsJoin`
  for (auto* expr : existsExpressions) {
    const auto& exists =
        dynamic_cast<const sparqlExpression::ExistsExpression&>(*expr);
    // Currently some FILTERs are applied multiple times especially when there
    // are OPTIONAL joins in the query. In these cases we have to make sure that
    // the `ExistsScan` is added only once.
    if (subtree->isVariableCovered(exists.variable())) {
      continue;
    }
    QueryPlanner qp{qec, cancellationHandle};
    auto pq = exists.argument();
    auto tree =
        std::make_shared<QueryExecutionTree>(qp.createExecutionTree(pq));
    subtree = ad_utility::makeExecutionTree<ExistsJoin>(
        qec, std::move(subtree), std::move(tree), exists.variable());
  }
  return subtree;
}
