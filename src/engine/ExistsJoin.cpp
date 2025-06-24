// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/ExistsJoin.h"

#include "CallFixedSize.h"
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
  // Make sure that the left and right input are sorted on the join columns.
  std::tie(left_, right_) = QueryExecutionTree::createSortedTrees(
      std::move(left_), std::move(right_), joinColumns_);

  if (joinColumns_.empty()) {
    // For non-lazy results applying the limit introduces some overhead, but for
    // lazy results it ensures that we don't have to compute the whole result,
    // so we consider this a tradeoff worth to make.
    right_->applyLimit({1});
  }
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
      "The target variable of an EXISTS join must be a new variable");
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
  // We add one column to `left_`, but do not change the order of the rows.
  return left_->resultSortedOn();
}

// ____________________________________________________________________________
float ExistsJoin::getMultiplicity(size_t col) {
  // The multiplicities of all columns except the last one are the same as in
  // `left_`.
  if (col < getResultWidth() - 1) {
    return left_->getMultiplicity(col);
  }
  // For the added (Boolean) column we take a dummy value, assuming that it
  // will not be used for subsequent joins or other operations that make use of
  // the multiplicities.
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
Result ExistsJoin::computeResult(bool requestLaziness) {
  bool noJoinNecessary = joinColumns_.empty();
  auto leftRes = left_->getResult(noJoinNecessary && requestLaziness);
  auto rightRes = right_->getResult();
  const auto& right = rightRes->idTable();

  if (!leftRes->isFullyMaterialized()) {
    AD_CORRECTNESS_CHECK(noJoinNecessary);
    // Forward lazy result, otherwise let the existing code handle the join with
    // no column.
    return {Result::LazyResult{
                ad_utility::OwningView{leftRes->idTables()} |
                ql::views::transform([exists = !right.empty(),
                                      leftRes](Result::IdTableVocabPair& pair) {
                  // Make sure we keep this shared ptr alive until the result is
                  // completely consumed.
                  (void)leftRes;
                  auto& idTable = pair.idTable_;
                  idTable.addEmptyColumn();
                  ql::ranges::fill(idTable.getColumn(idTable.numColumns() - 1),
                                   Id::makeFromBool(exists));
                  return std::move(pair);
                })},
            leftRes->sortedBy()};
  }
  const auto& left = leftRes->idTable();

  // We reuse the generic `zipperJoinWithUndef` function, which has two two
  // callbacks: one for each matching pair of rows from `left` and `right`, and
  // one for rows in the left input that have no matching counterpart in the
  // right input. The first callback can be a noop, and the second callback
  // gives us exactly those rows, where the value in the to-be-added result
  // column should be `false`.

  // Extract the join columns from both inputs to make the following code
  // easier.
  ad_utility::JoinColumnMapping joinColumnData{joinColumns_, left.numColumns(),
                                               right.numColumns()};
  IdTableView<0> joinColumnsLeft =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> joinColumnsRight =
      right.asColumnSubsetView(joinColumnData.jcsRight());
  checkCancellation();

  // Compute `isCheap`, which is true iff there are no UNDEF values in the join
  // columns (in which case we can use a simpler and cheaper join algorithm).
  //
  // TODO<joka921> This is the most common case. There are many other cases
  // where the generic `zipperJoinWithUndef` can be optimized. This is work for
  // a future PR.
  size_t numJoinColumns = joinColumnsLeft.numColumns();
  AD_CORRECTNESS_CHECK(numJoinColumns == joinColumnsRight.numColumns());
  bool isCheap = ql::ranges::none_of(
      ad_utility::integerRange(numJoinColumns), [&](const auto& col) {
        return (ql::ranges::any_of(joinColumnsRight.getColumn(col),
                                   &Id::isUndefined)) ||
               (ql::ranges::any_of(joinColumnsLeft.getColumn(col),
                                   &Id::isUndefined));
      });

  // Nothing to do for the actual matches.
  auto noopRowAdder = ad_utility::noop;

  // Store the indices of rows for which the value of the `EXISTS` (in the added
  // Boolean column) should be `false`.
  std::vector<size_t, ad_utility::AllocatorWithLimit<size_t>> notExistsIndices{
      allocator()};
  // Helper lambda for computing the exists join with `callFixedSizeVi`, which
  // makes the number of join columns a template parameter.
  auto runForNumJoinCols = [&notExistsIndices, isCheap, &noopRowAdder,
                            &colsLeftDynamic = joinColumnsLeft,
                            &colsRightDynamic = joinColumnsRight,
                            this](auto NumJoinCols) {
    // The `actionForNotExisting` callback gets iterators as input, but
    // should output indices, hence the pointer arithmetic.
    auto joinColumnsLeft = colsLeftDynamic.asStaticView<NumJoinCols>();
    auto joinColumnsRight = colsRightDynamic.asStaticView<NumJoinCols>();
    auto actionForNotExisting =
        [&notExistsIndices, begin = joinColumnsLeft.begin()](
            const auto& itLeft) { notExistsIndices.push_back(itLeft - begin); };

    // Run `zipperJoinWithUndef` with the described callbacks and the
    // mentioned optimization in case we know that there are no UNDEF values
    // in the join columns.
    auto checkCancellationLambda = [this] { checkCancellation(); };
    auto runZipperJoin = [&](auto findUndef) {
      [[maybe_unused]] auto numOutOfOrder = ad_utility::zipperJoinWithUndef(
          joinColumnsLeft, joinColumnsRight,
          ql::ranges::lexicographical_compare, noopRowAdder, findUndef,
          findUndef, actionForNotExisting, checkCancellationLambda);
    };
    if (isCheap) {
      runZipperJoin(ad_utility::noop);
    } else {
      runZipperJoin(ad_utility::findSmallerUndefRanges);
    }
  };
  ad_utility::callFixedSizeVi(numJoinColumns, runForNumJoinCols);

  // Add the result column from the computed `notExistsIndices` (which tell us
  // where the value should be `false`).
  IdTable result = left.clone();
  result.addEmptyColumn();
  decltype(auto) existsCol = result.getColumn(getResultWidth() - 1);
  ql::ranges::fill(existsCol, Id::makeFromBool(true));
  for (size_t notExistsIndex : notExistsIndices) {
    existsCol[notExistsIndex] = Id::makeFromBool(false);
  }

  // The added column only contains Boolean values, and adds no new words to the
  // local vocabulary, so we can simply copy the local vocab from `leftRes`.
  return {std::move(result), resultSortedOn(), leftRes->getCopyOfLocalVocab()};
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> ExistsJoin::addExistsJoinsToSubtree(
    const sparqlExpression::SparqlExpressionPimpl& expression,
    std::shared_ptr<QueryExecutionTree> subtree, QueryExecutionContext* qec,
    const ad_utility::SharedCancellationHandle& cancellationHandle) {
  // For each `EXISTS` function, add the corresponding `ExistsJoin`.
  for (auto* expr : expression.getExistsExpressions()) {
    const auto& exists =
        dynamic_cast<const sparqlExpression::ExistsExpression&>(*expr);
    // If we have already considered this `EXIST` (which we can detect by its
    // variable), skip it. This can happen because some `FILTER`s (which may
    // contain `EXISTS` functions) are applied multiple times (for example,
    // when there are OPTIONAL joins in the query).
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

// _____________________________________________________________________________
std::unique_ptr<Operation> ExistsJoin::cloneImpl() const {
  auto newJoin = std::make_unique<ExistsJoin>(*this);
  newJoin->left_ = left_->clone();
  newJoin->right_ = right_->clone();
  return newJoin;
}

// _____________________________________________________________________________
bool ExistsJoin::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  if (variable == existsVariable_) {
    // NOTE: We could in theory check if the literals true and false are
    // contained in the knowledge graph, but that would makes things more
    // complicated for almost no benefit.
    return false;
  }
  return left_->getRootOperation()->columnOriginatesFromGraphOrUndef(variable);
}
