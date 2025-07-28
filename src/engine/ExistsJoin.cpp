// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/ExistsJoin.h"

#include "engine/CallFixedSize.h"
#include "engine/JoinHelpers.h"
#include "engine/QueryPlanner.h"
#include "engine/Result.h"
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
std::string ExistsJoin::getCacheKeyImpl() const {
  return absl::StrCat("EXISTS JOIN left: ", left_->getCacheKey(),
                      " right: ", right_->getCacheKey(), " join columns: [",
                      absl::StrJoin(joinColumns_, " ",
                                    [](std::string* out, const auto& array) {
                                      absl::StrAppend(out, "(", array[0], ",",
                                                      array[1], ")");
                                    }),
                      "]");
}

// _____________________________________________________________________________
std::string ExistsJoin::getDescriptor() const { return "Exists Join"; }

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
std::vector<ColumnIndex> ExistsJoin::resultSortedOn() const {
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
  // The lazy exists join implementation does only work if there's just a single
  // join column. This might be extended in the future.
  bool lazyJoinIsSupported = joinColumns_.size() == 1;
  auto leftRes = left_->getResult(requestLaziness &&
                                  (noJoinNecessary || lazyJoinIsSupported));
  auto rightRes = right_->getResult(!noJoinNecessary && lazyJoinIsSupported);

  if (noJoinNecessary && !leftRes->isFullyMaterialized()) {
    // Forward lazy result, otherwise let the existing code handle the join with
    // no column.
    return {Result::LazyResult{
                ad_utility::OwningView{leftRes->idTables()} |
                ql::views::transform([exists = !rightRes->idTable().empty(),
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
  if (!leftRes->isFullyMaterialized() || !rightRes->isFullyMaterialized()) {
    return lazyExistsJoin(std::move(leftRes), std::move(rightRes),
                          requestLaziness);
  }
  const auto& right = rightRes->idTable();
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
    // Hide non-visible variables in the subtree, so that they are not
    // accidentally joined, ideally collisions wouldn't happen in the first
    // place, but since we're creating our own instance of `QueryPlanner` we
    // can't prevent them without refactoring the code. This workaround has the
    // downside that it might look confusing
    tree->getRootOperation()->setSelectedVariablesForSubquery(
        pq.getVisibleVariables());
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

namespace {

// Implementation to add the `EXISTS` column to the result of a child operation
// of this class. Works with lazy and non-lazy results.
struct LazyExistsJoinImpl
    : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
  // Store child results.
  std::shared_ptr<const Result> left_;
  std::shared_ptr<const Result> right_;

  // Store the ranges of the child results. The left result is owned, the right
  // is just a view to the wrapped `IdTable`s.
  ad_utility::InputRangeTypeErased<Result::IdTableVocabPair> leftRange_;
  ad_utility::InputRangeTypeErased<std::reference_wrapper<const IdTable>>
      rightRange_;

  // Store the join columns.
  ColumnIndex leftJoinColumn_;
  ColumnIndex rightJoinColumn_;

  // Store the current result of the right child. This is a view to the current
  // `IdTable`.
  std::optional<std::reference_wrapper<const IdTable>> currentRight_ =
      std::nullopt;
  // Store the current index in the right child that was last being checked.
  size_t currentRightIndex_ = 0;

  // Helper enum to indicate if we can avoid expensive checks.
  enum class FastForwardState { Unknown, Yes, No };

  // If we found undef values on the right, or the right ranges have been
  // consumed, we can fast-forward and skip expensive checks.
  FastForwardState allRowsFromLeftExist_ = FastForwardState::Unknown;

  // Convert result to an owned range of `IdTableVocabPair`s. This is used for
  // the left side.
  static Result::LazyResult toOwnedRange(
      const std::shared_ptr<const Result>& result) {
    if (result->isFullyMaterialized()) {
      return ad_utility::InputRangeTypeErased{
          std::array{Result::IdTableVocabPair{result->idTable().clone(),
                                              result->getCopyOfLocalVocab()}}};
    }
    return result->idTables();
  }

  // Convert result to a view of `IdTable`s. This is used for the right side.
  static ad_utility::InputRangeTypeErased<std::reference_wrapper<const IdTable>>
  toRangeView(const std::shared_ptr<const Result>& result) {
    if (result->isFullyMaterialized()) {
      return ad_utility::InputRangeTypeErased{
          std::array{std::cref(result->idTable())}};
    }
    return ad_utility::InputRangeTypeErased{
        ad_utility::CachingTransformInputRange{
            result->idTables(),
            [](const auto& pair) { return std::cref(pair.idTable_); }}};
  }

  // Construct an instance of `LazyExistsJoinImpl` with the given left and right
  // join columns as well as the respective results.
  explicit LazyExistsJoinImpl(std::shared_ptr<const Result> left,
                              std::shared_ptr<const Result> right,
                              ColumnIndex leftJoinColumn,
                              ColumnIndex rightJoinColumn)
      : left_{std::move(left)},
        right_{std::move(right)},
        leftRange_{toOwnedRange(left_)},
        rightRange_{toRangeView(right_)},
        leftJoinColumn_{leftJoinColumn},
        rightJoinColumn_{rightJoinColumn} {}

  // Fetch and store the next non-empty result from `rightRange_` in
  // `currentRight_`.
  void fetchNextRightBlock() {
    do {
      currentRight_ = rightRange_.get();
    } while (currentRight_.has_value() && currentRight_.value().get().empty());
  }

  // Increment `currentRightIndex_` by one, or fetch the next non-empty element
  // from `rightRange_` and reset `currentRightIndex_` back to zero.
  void incrementToNextRow() {
    currentRightIndex_++;
    // Get the next block from the range if we couldn't find a matching value
    // in this one.
    if (currentRightIndex_ == currentRight_.value().get().size()) {
      fetchNextRightBlock();
      currentRightIndex_ = 0;
      // Optimization to copy all remaining blocks in one.
      if (!currentRight_.has_value()) {
        allRowsFromLeftExist_ = FastForwardState::No;
      }
    }
  }

  // Check if the `id` has a match on the right side. This will increment the
  // index until a match is found, or no matches exist.
  bool hasMatch(Id id) {
    if (id.isUndefined()) {
      // This is correct, because undefined values are processed first and
      // `currentRight_` is not-reassigned until a non-undefined value is
      // processed.
      return currentRight_.has_value();
    }
    // Search for the next match.
    while (currentRight_.has_value()) {
      AD_CORRECTNESS_CHECK(currentRightIndex_ <
                           currentRight_.value().get().size());
      auto comparison = currentRight_.value().get().at(currentRightIndex_,
                                                       rightJoinColumn_) <=> id;
      if (comparison == 0) {
        return true;
      }
      if (comparison > 0) {
        return false;
      }
      incrementToNextRow();
    }
    return false;
  }

  // Get the next result from the left side that is augmented with EXISTS
  // information.
  std::optional<Result::IdTableVocabPair> get() override {
    if (!currentRight_.has_value() &&
        allRowsFromLeftExist_ == FastForwardState::Unknown) {
      fetchNextRightBlock();
      if (currentRight_.has_value()) {
        if (currentRight_.value().get().at(0, rightJoinColumn_).isUndefined()) {
          allRowsFromLeftExist_ = FastForwardState::Yes;
        }
      } else {
        allRowsFromLeftExist_ = FastForwardState::No;
      }
    }

    auto result = leftRange_.get();

    if (result.has_value()) {
      auto& idTable = result.value().idTable_;
      idTable.addEmptyColumn();
      auto outputColumn = idTable.getColumn(idTable.numColumns() - 1);

      if (allRowsFromLeftExist_ != FastForwardState::Unknown) {
        ql::ranges::fill(outputColumn, Id::makeFromBool(allRowsFromLeftExist_ ==
                                                        FastForwardState::Yes));
      } else {
        auto leftJoinColumn = idTable.getColumn(leftJoinColumn_);

        for (size_t rowIndex = 0; rowIndex < leftJoinColumn.size();
             rowIndex++) {
          outputColumn[rowIndex] =
              Id::makeFromBool(hasMatch(leftJoinColumn[rowIndex]));
        }
      }
    }
    return result;
  }
};
}  // namespace

// _____________________________________________________________________________
Result ExistsJoin::lazyExistsJoin(std::shared_ptr<const Result> left,
                                  std::shared_ptr<const Result> right,
                                  bool requestLaziness) {
  // If both inputs are fully materialized, we can join them more
  // efficiently.
  AD_CONTRACT_CHECK(!left->isFullyMaterialized() ||
                    !right->isFullyMaterialized());
  // If `requestLaziness` is false, we expect the left result to be fully
  // materialized as well.
  AD_CONTRACT_CHECK(left->isFullyMaterialized() || requestLaziness);
  // Currently only supports a single join column.
  AD_CORRECTNESS_CHECK(joinColumns_.size() == 1);

  auto [leftCol, rightCol] = joinColumns_.at(0);

  Result::LazyResult generator{
      LazyExistsJoinImpl{std::move(left), std::move(right), leftCol, rightCol}};

  return requestLaziness
             ? Result{std::move(generator), resultSortedOn()}
             : Result{ad_utility::getSingleElement(std::move(generator)),
                      resultSortedOn()};
}
