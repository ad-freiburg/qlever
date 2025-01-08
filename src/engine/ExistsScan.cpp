//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/ExistsScan.h"

#include "engine/QueryPlanner.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

// _____________________________________________________________________________
ExistsScan::ExistsScan(QueryExecutionContext* qec,
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
string ExistsScan::getCacheKeyImpl() const {
  return absl::StrCat("EXISTS SCAN left: ", left_->getCacheKey(),
                      " right: ", right_->getCacheKey());
}

// _____________________________________________________________________________
string ExistsScan::getDescriptor() const { return "EXISTS scan"; }

// ____________________________________________________________________________
VariableToColumnMap ExistsScan::computeVariableToColumnMap() const {
  auto res = left_->getVariableColumns();
  AD_CONTRACT_CHECK(
      !res.contains(existsVariable_),
      "The target variable of an exists scan must be a new variable");
  res[existsVariable_] = makeAlwaysDefinedColumn(getResultWidth() - 1);
  return res;
}

// ____________________________________________________________________________
size_t ExistsScan::getResultWidth() const {
  // We add one column to the input.
  return left_->getResultWidth() + 1;
}

// ____________________________________________________________________________
vector<ColumnIndex> ExistsScan::resultSortedOn() const {
  return left_->resultSortedOn();
}

// ____________________________________________________________________________
float ExistsScan::getMultiplicity(size_t col) {
  if (col < getResultWidth() - 1) {
    return left_->getMultiplicity(col);
  }
  // The multiplicity of the boolean column can be a dummy value, as it should
  // be never used for joins etc.
  return 1;
}

// ____________________________________________________________________________
uint64_t ExistsScan::getSizeEstimateBeforeLimit() {
  return left_->getSizeEstimate();
}

// ____________________________________________________________________________
size_t ExistsScan::getCostEstimate() {
  return left_->getCostEstimate() + right_->getCostEstimate() +
         left_->getSizeEstimate() + right_->getSizeEstimate();
}

// ____________________________________________________________________________
ProtoResult ExistsScan::computeResult([[maybe_unused]] bool requestLaziness) {
  auto leftRes = left_->getResult();
  auto rightRes = right_->getResult();
  const auto& left = leftRes->idTable();
  const auto& right = rightRes->idTable();

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
  size_t numJoinColumns = joinColumnsLeft.size();
  AD_CORRECTNESS_CHECK(numJoinColumns == joinColumnsRight.size());
  bool isCheap = stdr::none_of(
      ad_utility::integerRange(numJoinColumns), [&](const auto& col) {
        return (stdr::any_of(joinColumnsRight.getColumn(col),
                             &Id::isUndefined)) ||
               (stdr::any_of(joinColumnsLeft.getColumn(col), &Id::isUndefined));
      });

  auto noopRowAdder = [](auto&&...) {};

  std::vector<size_t, ad_utility::AllocatorWithLimit<size_t>> notExistsIndices{
      allocator()};
  auto actionForNotExisting =
      [&notExistsIndices, begin = joinColumnsLeft.begin()](const auto& itLeft) {
        notExistsIndices.push_back(itLeft - begin);
      };

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
  return {std::move(result), resultSortedOn(), leftRes->getCopyOfLocalVocab()};
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> ExistsScan::addExistsScansToSubtree(
    const sparqlExpression::SparqlExpressionPimpl& expression,
    std::shared_ptr<QueryExecutionTree> subtree, QueryExecutionContext* qec,
    const ad_utility::SharedCancellationHandle& cancellationHandle) {
  std::vector<const sparqlExpression::SparqlExpression*> existsExpressions;
  expression.getPimpl()->getExistsExpressions(existsExpressions);
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
    subtree = ad_utility::makeExecutionTree<ExistsScan>(
        qec, std::move(subtree), std::move(tree), exists.variable());
  }
  return subtree;
}
