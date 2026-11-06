// Copyright 2015 - 2026 The QLever Authors, in particular:

// 2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// 2018-2026 Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de), UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026 Mark Veser (mark.veser87@gmail.com)

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/JoinImpl.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <sstream>
#include <vector>

#include "JoinWithIndexScanHelpers.h"
#include "backports/functional.h"
#include "backports/type_traits.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/JoinHelpers.h"
#include "engine/Service.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "index/IdTableUtils.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Generators.h"
#include "util/HashMap.h"
#include "util/Iterators.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using namespace qlever::joinHelpers;
using namespace qlever::joinWithIndexScanHelpers;
using std::endl;
using std::string;

// _____________________________________________________________________________
JoinImpl::JoinImpl(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> t1,
                   std::shared_ptr<QueryExecutionTree> t2,
                   JoinColumns joinColumns, bool keepJoinColumn,
                   bool allowSwappingChildrenOnlyForTesting)
    : Operation(qec), keepJoinColumn_{keepJoinColumn} {
  AD_CONTRACT_CHECK(t1 && t2);
  AD_CONTRACT_CHECK(!joinColumns.empty());
  AD_CONTRACT_CHECK(keepJoinColumn || joinColumns.size() == 1,
                    "Removing the join columns from the result is only "
                    "supported for a single join column.");
  joinColumns = chooseJoinColumnOrder(*t1, *t2, std::move(joinColumns));
  // Currently all join algorithms require both inputs to be sorted, so we
  // enforce the sorting here.
  std::tie(t1, t2) = QueryExecutionTree::createSortedTrees(
      std::move(t1), std::move(t2), joinColumns);

  // Make the order of the two subtrees deterministic. That way, queries that
  // are identical except for the order of the join operands, are easier to
  // identify.
  auto swapChildren = [&]() {
    // This can be disabled by tests to fix the order of the subtrees.
    if (allowSwappingChildrenOnlyForTesting) {
      std::swap(t1, t2);
      for (auto& [col1, col2] : joinColumns) {
        std::swap(col1, col2);
      }
    }
  };
  if (t1->getCacheKey() > t2->getCacheKey()) {
    swapChildren();
  }
  // If one of the inputs is a SCAN (that supports prefiltered joins) and the
  // other one is not, always make the SCAN the right child (which also gives a
  // deterministic order of the subtrees). This simplifies several branches in
  // the `computeResult` method.
  if (IndexScan::getIfSupportsPrefilteredJoin(*t1) &&
      !IndexScan::getIfSupportsPrefilteredJoin(*t2)) {
    swapChildren();
  }
  left_ = std::move(t1);
  right_ = std::move(t2);
  joinColumns_ = std::move(joinColumns);
  sizeEstimate_ = 0;
  sizeEstimateComputed_ = false;
  multiplicities_.clear();
  for (auto [leftCol, rightCol] : joinColumns_) {
    const auto& joinVar = left_->getVariableAndInfoByColumnIndex(leftCol).first;
    AD_CONTRACT_CHECK(joinVar ==
                      right_->getVariableAndInfoByColumnIndex(rightCol).first);
    joinVars_.push_back(joinVar);
  }
}

// _____________________________________________________________________________
auto JoinImpl::chooseJoinColumnOrder(const QueryExecutionTree& t1,
                                     const QueryExecutionTree& t2,
                                     JoinColumns joinColumns) -> JoinColumns {
  if (joinColumns.size() <= 1) {
    return joinColumns;
  }
  // The candidates are the indices (0 for `t1`, 1 for `t2`) of the children
  // whose sort order we try to use. `IndexScan`s that support the prefiltered
  // joins are preferred, ties are broken by the cache key, which makes the
  // result independent of the order of `t1` and `t2`.
  std::array<const QueryExecutionTree*, 2> trees{&t1, &t2};
  std::array<size_t, 2> candidates{0, 1};
  auto isScan = [&trees](size_t i) {
    return IndexScan::getIfSupportsPrefilteredJoin(*trees[i]) != nullptr;
  };
  ql::ranges::sort(candidates, [&](size_t a, size_t b) {
    return std::tuple{!isScan(a), trees[a]->getCacheKey()} <
           std::tuple{!isScan(b), trees[b]->getCacheKey()};
  });

  for (size_t side : candidates) {
    auto sortedOn = trees[side]->resultSortedOn();
    if (sortedOn.size() < joinColumns.size()) {
      continue;
    }
    // Reorder the join columns according to the first columns by which the
    // child is sorted, if those are exactly the join columns.
    JoinColumns reordered;
    for (ColumnIndex col : ql::span{sortedOn}.first(joinColumns.size())) {
      auto it = ql::ranges::find_if(
          joinColumns, [&](const auto& jc) { return jc[side] == col; });
      if (it == joinColumns.end()) {
        break;
      }
      reordered.push_back(*it);
    }
    if (reordered.size() == joinColumns.size()) {
      return reordered;
    }
  }

  // No child is already sorted by the join columns, sort them by the column
  // indices of the child with the smaller cache key.
  size_t side = t1.getCacheKey() <= t2.getCacheKey() ? 0 : 1;
  ql::ranges::sort(joinColumns, [side](const auto& a, const auto& b) {
    return std::tuple{a[side], a[1 - side]} < std::tuple{b[side], b[1 - side]};
  });
  return joinColumns;
}

// _____________________________________________________________________________
auto JoinImpl::computeJoinColumns(
    const QueryExecutionTree& left, const QueryExecutionTree& right,
    const std::vector<Variable>& joinVars) -> JoinColumns {
  JoinColumns joinColumns;
  for (const auto& var : joinVars) {
    joinColumns.push_back(
        {left.getVariableColumn(var), right.getVariableColumn(var)});
  }
  return joinColumns;
}

// _____________________________________________________________________________
string JoinImpl::getCacheKeyImpl() const {
  auto joinColumnsToString = [this](size_t side) {
    return absl::StrJoin(joinColumnsOfSide(side), " & ");
  };
  std::ostringstream os;
  os << "JOIN\n"
     << left_->getCacheKey() << " join-column: [" << joinColumnsToString(0)
     << "]\n";
  os << "|X|\n"
     << right_->getCacheKey() << " join-column: [" << joinColumnsToString(1)
     << "]";
  os << "keep join Col " << keepJoinColumn_;
  return std::move(os).str();
}

// _____________________________________________________________________________
string JoinImpl::getDescriptor() const {
  return absl::StrCat(
      "Join on ",
      absl::StrJoin(joinVars_, " ", [](std::string* out, const Variable& v) {
        absl::StrAppend(out, v.name());
      }));
}

// _____________________________________________________________________________
template <typename F>
decltype(auto) JoinImpl::callWithNumJoinColumns(F&& f) const {
  static_assert(MAX_NUM_JOIN_COLS_FOR_LAZY_JOIN == 3);
  switch (joinColumns_.size()) {
    case 1:
      return f(std::integral_constant<size_t, 1>{});
    case 2:
      return f(std::integral_constant<size_t, 2>{});
    case 3:
      return f(std::integral_constant<size_t, 3>{});
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
Result JoinImpl::computeResult(bool requestLaziness) {
  AD_LOG_DEBUG << "Getting sub-results for join result computation..." << endl;
  if (left_->knownEmptyResult() || right_->knownEmptyResult()) {
    left_->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    right_->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    return createEmptyResult();
  }

  // If one of the RootOperations is a Service, precompute the result of its
  // sibling.
  Service::precomputeSiblingResult(left_->getRootOperation(),
                                   right_->getRootOperation(), false,
                                   requestLaziness);

  if (!lazyJoinAlgorithmsAreApplicable()) {
    return computeResultFullyMaterialized();
  }

  // Always materialize results that meet one of the following criteria:
  // * They are already present in the cache
  // * Their result is small
  // This is purely for performance reasons.
  auto getCachedOrSmallResult = [](const QueryExecutionTree& tree) {
    bool isSmall =
        tree.getRootOperation()->getSizeEstimate() <
        getRuntimeParameter<
            &RuntimeParameters::lazyIndexScanMaxSizeMaterialization_>();

    // The third argument means "only get the result if it can be read from the
    // cache". So effectively, this returns the result if it is small, or is
    // contained in the cache, otherwise `nullptr`.
    // TODO<joka921> Add a unit test that checks the correct conditions
    return tree.getRootOperation()->getResult(
        false, isSmall ? ComputationMode::FULLY_MATERIALIZED
                       : ComputationMode::ONLY_IF_CACHED);
  };

  auto leftResIfCached = getCachedOrSmallResult(*left_);
  checkCancellation();
  auto rightResIfCached = getCachedOrSmallResult(*right_);
  checkCancellation();

  auto leftIndexScan = IndexScan::getIfSupportsPrefilteredJoin(*left_);
  auto rightIndexScan = IndexScan::getIfSupportsPrefilteredJoin(*right_);
  if (leftIndexScan && rightIndexScan) {
    if (rightResIfCached && !leftResIfCached) {
      AD_CORRECTNESS_CHECK(rightResIfCached->isFullyMaterialized());
      return callWithNumJoinColumns([&](auto numJoinCols) {
        return computeResultForIndexScanAndIdTable<numJoinCols, true>(
            requestLaziness, std::move(rightResIfCached), leftIndexScan);
      });
    } else if (!leftResIfCached) {
      return callWithNumJoinColumns([&](auto numJoinCols) {
        return computeResultForTwoIndexScans<numJoinCols>(requestLaziness);
      });
    }
  }

  std::shared_ptr<const Result> leftRes =
      leftResIfCached ? leftResIfCached : left_->getResult(true);
  checkCancellation();
  if (leftRes->isFullyMaterialized() && leftRes->idTableView().empty()) {
    right_->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    return createEmptyResult();
  }

  // Note: If only one of the children is a scan, then we have made sure in the
  // constructor that it is the right child.
  if (rightIndexScan && !rightResIfCached) {
    return callWithNumJoinColumns([&](auto numJoinCols) {
      if (leftRes->isFullyMaterialized()) {
        return computeResultForIndexScanAndIdTable<numJoinCols, false>(
            requestLaziness, std::move(leftRes), rightIndexScan);
      }
      return computeResultForIndexScanAndLazyOperation<numJoinCols>(
          requestLaziness, std::move(leftRes), rightIndexScan);
    });
  }

  std::shared_ptr<const Result> rightRes =
      rightResIfCached ? rightResIfCached : right_->getResult(true);
  checkCancellation();
  if (leftRes->isFullyMaterialized() && rightRes->isFullyMaterialized()) {
    return computeResultForTwoMaterializedInputs(std::move(leftRes),
                                                 std::move(rightRes));
  }
  return lazyJoin(std::move(leftRes), std::move(rightRes), requestLaziness);
}

// _____________________________________________________________________________
bool JoinImpl::lazyJoinAlgorithmsAreApplicable() const {
  if (joinColumns_.size() == 1) {
    return true;
  }
  return joinColumns_.size() <= MAX_NUM_JOIN_COLS_FOR_LAZY_JOIN &&
         joinColumnsAreAlwaysDefined(joinColumns_, left_, right_);
}

// _____________________________________________________________________________
Result JoinImpl::computeResultFullyMaterialized() {
  auto leftRes = left_->getResult();
  checkCancellation();
  if (leftRes->idTableView().empty()) {
    right_->getRootOperation()->updateRuntimeInformationWhenOptimizedOut();
    return createEmptyResult();
  }
  auto rightRes = right_->getResult();
  checkCancellation();
  return computeResultForTwoMaterializedInputs(std::move(leftRes),
                                               std::move(rightRes));
}

// _____________________________________________________________________________
VariableToColumnMap JoinImpl::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      left_->getVariableColumns(), right_->getVariableColumns(), joinColumns_,
      BinOpType::Join, left_->getResultWidth(), keepJoinColumn_);
}

// _____________________________________________________________________________
size_t JoinImpl::getResultWidth() const {
  auto sumOfChildWidths = left_->getResultWidth() + right_->getResultWidth();
  size_t numJoinColumns = joinColumns_.size();
  AD_CORRECTNESS_CHECK(sumOfChildWidths >= 2 * numJoinColumns);
  return sumOfChildWidths - numJoinColumns -
         static_cast<size_t>(!keepJoinColumn_) * numJoinColumns;
}

// _____________________________________________________________________________
std::vector<ColumnIndex> JoinImpl::resultSortedOn() const {
  if (!keepJoinColumn_) {
    return {};
  }
  return ::ranges::to<std::vector<ColumnIndex>>(joinColumnsOfSide(0));
}

// _____________________________________________________________________________
float JoinImpl::getMultiplicity(size_t col) {
  if (multiplicities_.empty()) {
    computeSizeEstimateAndMultiplicities();
    sizeEstimateComputed_ = true;
  }
  return multiplicities_.at(col);
}

// _____________________________________________________________________________
size_t JoinImpl::getCostEstimate() {
  size_t costJoin = getSizeEstimateBeforeLimit() + left_->getSizeEstimate() +
                    right_->getSizeEstimate();
  if (size_t numJoinColumns = joinColumns_.size(); numJoinColumns > 1) {
    // A join on more than one column is slower than a join on a single column,
    // due to the more expensive comparisons and the handling of UNDEF values.
    // Additionally, make the join 7% more expensive per join column.
    costJoin =
        static_cast<size_t>(costJoin * 2 * (1 + (numJoinColumns - 1) * 0.07));
  }

  // TODO<joka921> once the `getCostEstimate` functions are `const`,
  // the argument can also be `const auto`
  auto costOfSubtree = [](auto& subtree) { return subtree->getCostEstimate(); };

  return costJoin + costOfSubtree(left_) + costOfSubtree(right_);
}

// _____________________________________________________________________________
void JoinImpl::computeSizeEstimateAndMultiplicities() {
  if (joinColumns_.size() > 1) {
    computeSizeEstimateAndMultiplicitiesMultiColumn();
    return;
  }
  auto [leftJoinCol, rightJoinCol] = joinColumns_.at(0);
  multiplicities_.clear();
  if (left_->getSizeEstimate() == 0 || right_->getSizeEstimate() == 0) {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      multiplicities_.emplace_back(1);
    }
    return;
  }

  size_t nofDistinctLeft = std::max(
      size_t(1), static_cast<size_t>(left_->getSizeEstimate() /
                                     left_->getMultiplicity(leftJoinCol)));
  size_t nofDistinctRight = std::max(
      size_t(1), static_cast<size_t>(right_->getSizeEstimate() /
                                     right_->getMultiplicity(rightJoinCol)));

  size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

  double adaptSizeLeft =
      left_->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctLeft);
  double adaptSizeRight =
      right_->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctRight);

  double corrFactor = _executionContext
                          ? _executionContext->getCostFactor(
                                "JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR")
                          : 1.0;

  double jcMultiplicityInResult = left_->getMultiplicity(leftJoinCol) *
                                  right_->getMultiplicity(rightJoinCol);
  sizeEstimate_ = std::max(
      size_t(1), static_cast<size_t>(corrFactor * jcMultiplicityInResult *
                                     nofDistinctInResult));

  AD_LOG_TRACE << "Estimated size as: " << sizeEstimate_ << " := " << corrFactor
               << " * " << jcMultiplicityInResult << " * "
               << nofDistinctInResult << std::endl;

  for (auto i = ColumnIndex{0}; i < left_->getResultWidth(); ++i) {
    double oldMult = left_->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * right_->getMultiplicity(rightJoinCol) * corrFactor);
    if (i != leftJoinCol && nofDistinctLeft != nofDistinctInResult) {
      double oldDist = left_->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeLeft);
      m = (sizeEstimate_ / corrFactor) / newDist;
    }
    if (i != leftJoinCol || keepJoinColumn_) {
      multiplicities_.emplace_back(m);
    }
  }
  for (auto i = ColumnIndex{0}; i < right_->getResultWidth(); ++i) {
    if (i == rightJoinCol) {
      continue;
    }
    double oldMult = right_->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * left_->getMultiplicity(leftJoinCol) * corrFactor);
    if (i != rightJoinCol && nofDistinctRight != nofDistinctInResult) {
      double oldDist = right_->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeRight);
      m = (sizeEstimate_ / corrFactor) / newDist;
    }
    multiplicities_.emplace_back(m);
  }
  assert(multiplicities_.size() == getResultWidth());
}

// _____________________________________________________________________________
void JoinImpl::computeSizeEstimateAndMultiplicitiesMultiColumn() {
  // The number of distinct entries in the result is at most the minimum of
  // the numbers of distinct entries in all join columns.
  // The multiplicity in the result is approximated by the product of the
  // minimum of the multiplicities of the join columns of each side.

  // Compute the minimum number of distinct elements in the join columns.
  size_t numDistinctLeft = std::numeric_limits<size_t>::max();
  size_t numDistinctRight = std::numeric_limits<size_t>::max();
  for (auto [leftCol, rightCol] : joinColumns_) {
    size_t dl = std::max(
        1.0f, left_->getSizeEstimate() / left_->getMultiplicity(leftCol));
    size_t dr = std::max(
        1.0f, right_->getSizeEstimate() / right_->getMultiplicity(rightCol));
    numDistinctLeft = std::min(numDistinctLeft, dl);
    numDistinctRight = std::min(numDistinctRight, dr);
  }
  size_t numDistinctResult = std::min(numDistinctLeft, numDistinctRight);

  // Compute an estimate for the multiplicity of the result.
  float multLeft = std::numeric_limits<float>::max();
  float multRight = std::numeric_limits<float>::max();
  for (auto [leftCol, rightCol] : joinColumns_) {
    multLeft = std::min(multLeft, left_->getMultiplicity(leftCol));
    multRight = std::min(multRight, right_->getMultiplicity(rightCol));
  }
  float multResult = multLeft * multRight;

  sizeEstimate_ = multResult * numDistinctResult;
  // Don't estimate 0 since then some parent operations (in particular joins)
  // using `knownEmptyResult()` will assume the size to be exactly zero.
  sizeEstimate_ += 1;

  // Compute estimates for the multiplicities of the result columns.
  multiplicities_.clear();
  for (size_t i = 0; i < left_->getResultWidth(); i++) {
    multiplicities_.push_back(left_->getMultiplicity(i) *
                              (multResult / multLeft));
  }
  for (size_t i = 0; i < right_->getResultWidth(); i++) {
    if (ad_utility::contains(joinColumnsOfSide(1), i)) {
      continue;
    }
    multiplicities_.push_back(right_->getMultiplicity(i) *
                              (multResult / multRight));
  }
  AD_CORRECTNESS_CHECK(multiplicities_.size() == getResultWidth());
}

namespace {
// Extract the result table and the local vocab from the `rowAdder`.
Result::IdTableVocabPair toIdTableVocabPair(
    ad_utility::AddCombinedRowToIdTable&& rowAdder) {
  auto localVocab = std::move(rowAdder.localVocab());
  return {std::move(rowAdder).resultTable(), std::move(localVocab)};
}

// If one of the sorted ranges `left` and `right` is much larger than the other
// (see `GALLOP_THRESHOLD`), join them via the galloping join, call
// `addRow(itLeft, itRight)` for each pair of matching elements, and return
// `true`. Otherwise do nothing and return `false`. The inputs must not contain
// UNDEF values.
template <typename Left, typename Right, typename LessThan, typename AddRow,
          typename CheckCancellation>
bool gallopingJoinIfUnbalanced(const Left& left, const Right& right,
                               const LessThan& lessThan, const AddRow& addRow,
                               const CheckCancellation& checkCancellation) {
  size_t sizeLeft = ql::ranges::size(left);
  size_t sizeRight = ql::ranges::size(right);
  if (sizeLeft / sizeRight > GALLOP_THRESHOLD) {
    // The first argument to the galloping join will always be the smaller
    // input, so we need to switch the rows when adding them.
    auto inverseAddRow = [&addRow](const auto& itRight, const auto& itLeft) {
      addRow(itLeft, itRight);
    };
    ad_utility::gallopingJoin(right, left, lessThan, inverseAddRow, {},
                              checkCancellation);
    return true;
  }
  if (sizeRight / sizeLeft > GALLOP_THRESHOLD) {
    ad_utility::gallopingJoin(left, right, lessThan, addRow, {},
                              checkCancellation);
    return true;
  }
  return false;
}
}  // namespace

// ______________________________________________________________________________
void JoinImpl::join(const IdTableView<0>& a, const IdTableView<0>& b,
                    IdTable* result) const {
  AD_LOG_DEBUG << "Performing join between two tables.\n";
  AD_LOG_DEBUG << "A: width = " << a.numColumns() << ", size = " << a.size()
               << "\n";
  AD_LOG_DEBUG << "B: width = " << b.numColumns() << ", size = " << b.size()
               << "\n";

  // Check trivial case.
  if (a.empty() || b.empty()) {
    return;
  }
  if (joinColumns_.size() > 1) {
    multiColumnJoin(a, b, result);
    return;
  }
  checkCancellation();
  ad_utility::JoinColumnMapping joinColumnData = getJoinColumnMapping();
  auto [leftJoinCol, rightJoinCol] = joinColumns_.at(0);
  auto joinColumnL = a.getColumn(leftJoinCol);
  auto joinColumnR = b.getColumn(rightJoinCol);

  auto aPermuted = a.asColumnSubsetView(joinColumnData.permutationLeft());
  auto bPermuted = b.asColumnSubsetView(joinColumnData.permutationRight());

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      1, aPermuted, bPermuted, std::move(*result), cancellationHandle(),
      keepJoinColumn_);
  auto addRow = [beginLeft = joinColumnL.begin(),
                 beginRight = joinColumnR.begin(),
                 &rowAdder](const auto& itLeft, const auto& itRight) {
    rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
  };

  // The UNDEF values are right at the start, so this calculation works.
  size_t numUndefA =
      ql::ranges::upper_bound(joinColumnL, ValueId::makeUndefined()) -
      joinColumnL.begin();
  size_t numUndefB =
      ql::ranges::upper_bound(joinColumnR, ValueId::makeUndefined()) -
      joinColumnR.begin();
  std::pair undefRangeA{joinColumnL.begin(), joinColumnL.begin() + numUndefA};
  std::pair undefRangeB{joinColumnR.begin(), joinColumnR.begin() + numUndefB};

  auto cancellationCallback = cancellationChecker();

  // Determine whether we should use the galloping join optimization.
  bool usedGallopingJoin =
      numUndefA == 0 && numUndefB == 0 &&
      gallopingJoinIfUnbalanced(joinColumnL, joinColumnR, ql::ranges::less{},
                                addRow, cancellationCallback);
  if (!usedGallopingJoin) {
    auto findSmallerUndefRangeLeft = [undefRangeA](auto&&...) {
      return ad_utility::IteratorRange{undefRangeA.first, undefRangeA.second};
    };
    auto findSmallerUndefRangeRight = [undefRangeB](auto&&...) {
      return ad_utility::IteratorRange{undefRangeB.first, undefRangeB.second};
    };

    auto numOutOfOrder = [&]() {
      if (numUndefB == 0 && numUndefA == 0) {
        return ad_utility::zipperJoinWithUndef(
            joinColumnL, joinColumnR, ql::ranges::less{}, addRow,
            ad_utility::noop, ad_utility::noop, {}, cancellationCallback);

      } else {
        return ad_utility::zipperJoinWithUndef(
            joinColumnL, joinColumnR, ql::ranges::less{}, addRow,
            findSmallerUndefRangeLeft, findSmallerUndefRangeRight, {},
            cancellationCallback);
      }
    }();
    AD_CORRECTNESS_CHECK(numOutOfOrder == 0);
  }
  *result = std::move(rowAdder).resultTable();
  // The column order in the result is now
  // [joinColumns, non-join-columns-a, non-join-columns-b] (which makes the
  // algorithms above easier), be the order that is expected by the rest of
  // the code is [columns-a, non-join-columns-b]. Permute the columns to fix
  // the order.
  result->setColumnSubset(joinColumnData.permutationResult());

  AD_LOG_DEBUG << "Join done.\n";
  AD_LOG_DEBUG << "Result: width = " << result->numColumns()
               << ", size = " << result->size() << "\n";
}

// _____________________________________________________________________________
void JoinImpl::multiColumnJoin(const IdTableView<0>& a, const IdTableView<0>& b,
                               IdTable* result) const {
  AD_CORRECTNESS_CHECK(keepJoinColumn_);
  checkCancellation();
  ad_utility::JoinColumnMapping joinColumnData = getJoinColumnMapping();

  IdTableView<0> leftJoinColumns =
      a.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> rightJoinColumns =
      b.asColumnSubsetView(joinColumnData.jcsRight());

  auto leftPermuted = a.asColumnSubsetView(joinColumnData.permutationLeft());
  auto rightPermuted = b.asColumnSubsetView(joinColumnData.permutationRight());

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      joinColumns_.size(), leftPermuted, rightPermuted, std::move(*result),
      cancellationHandle());
  auto addRow = [&rowAdder, beginLeft = leftJoinColumns.begin(),
                 beginRight = rightJoinColumns.begin()](const auto& itLeft,
                                                        const auto& itRight) {
    rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
  };

  // `hasNoUndef` is true iff there are no UNDEF values in the join columns, in
  // which case we can use a simpler and cheaper join algorithm.
  auto hasNoUndef = [](const IdTableView<0>& joinColumns) {
    return ql::ranges::none_of(
        ad_utility::integerRange(joinColumns.numColumns()), [&](size_t col) {
          return ql::ranges::any_of(joinColumns.getColumn(col),
                                    Id::isUndefinedL);
        });
  };
  bool isCheap = hasNoUndef(leftJoinColumns) && hasNoUndef(rightJoinColumns);

  auto cancellationCallback = cancellationChecker();
  auto lessThan = ql::ranges::lexicographical_compare;

  size_t numOutOfOrder = 0;
  if (isCheap &&
      gallopingJoinIfUnbalanced(leftJoinColumns, rightJoinColumns, lessThan,
                                addRow, cancellationCallback)) {
    // The galloping join has already computed the (sorted) result.
  } else if (isCheap) {
    numOutOfOrder = ad_utility::zipperJoinWithUndef(
        leftJoinColumns, rightJoinColumns, lessThan, addRow, ad_utility::noop,
        ad_utility::noop, ad_utility::noop, cancellationCallback);
  } else {
    numOutOfOrder = ad_utility::zipperJoinWithUndef(
        leftJoinColumns, rightJoinColumns, lessThan, addRow,
        ad_utility::findSmallerUndefRanges, ad_utility::findSmallerUndefRanges,
        ad_utility::noop, cancellationCallback);
  }
  *result = std::move(rowAdder).resultTable();
  // If there were UNDEF values in the input, the result might be out of
  // order. Sort it, because this operation promises a sorted result in its
  // `resultSortedOn()` member function.
  if (numOutOfOrder > 0) {
    std::vector<ColumnIndex> cols;
    for (size_t i = 0; i < joinColumns_.size(); ++i) {
      cols.push_back(i);
    }
    checkCancellation();
    IdTableUtils::sort(*result, cols, sortConfig());
  }

  // The result that `zipperJoinWithUndef` produces has a different order of
  // columns than expected, permute them. See the documentation of
  // `JoinColumnMapping` for details.
  result->setColumnSubset(joinColumnData.permutationResult());
  checkCancellation();
}

namespace {
// Run the zipper join on blocks for `NumJoinCols` join columns. For a single
// join column, UNDEF values are supported. For more join columns, the caller
// has to make sure that the join columns contain no UNDEF values (see
// `JoinImpl::lazyJoinAlgorithmsAreApplicable`).
template <size_t NumJoinCols, typename LeftBlocks, typename RightBlocks>
void zipperJoinForBlocks(LeftBlocks&& leftBlocks, RightBlocks&& rightBlocks,
                         ad_utility::AddCombinedRowToIdTable& rowAdder) {
  if constexpr (NumJoinCols == 1) {
    // Note: The `zipperJoinForBlocksWithPotentialUndef` automatically
    // switches to a more efficient implementation if there are no UNDEF
    // values in any of the inputs.
    ad_utility::zipperJoinForBlocksWithPotentialUndef(
        AD_FWD(leftBlocks), AD_FWD(rightBlocks), std::less{}, rowAdder);
  } else {
    ad_utility::zipperJoinForBlocksWithoutUndef(
        AD_FWD(leftBlocks), AD_FWD(rightBlocks), std::less{}, rowAdder);
  }
}

// Join the (lazy or fully materialized) results `a` and `b` on the first
// `NumJoinCols` columns after applying the permutations from `joinColMap`.
template <size_t NumJoinCols>
void zipperJoinResults(const Result& a, const Result& b,
                       const ad_utility::JoinColumnMapping& joinColMap,
                       ad_utility::AddCombinedRowToIdTable& rowAdder) {
  auto leftRange = resultToView<NumJoinCols>(a, joinColMap.permutationLeft());
  auto rightRange = resultToView<NumJoinCols>(b, joinColMap.permutationRight());
  std::visit(
      [&rowAdder](auto& leftBlocks, auto& rightBlocks) {
        zipperJoinForBlocks<NumJoinCols>(leftBlocks, rightBlocks, rowAdder);
      },
      leftRange, rightRange);
}
}  // namespace

// ______________________________________________________________________________
Result JoinImpl::lazyJoin(std::shared_ptr<const Result> a,
                          std::shared_ptr<const Result> b,
                          bool requestLaziness) const {
  // If both inputs are fully materialized, we can join them more
  // efficiently.
  AD_CONTRACT_CHECK(!a->isFullyMaterialized() || !b->isFullyMaterialized());
  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResultFromAction(
      requestLaziness,
      [this, a = std::move(a), b = std::move(b),
       joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto rowAdder = makeRowAdder(std::move(yieldTable));
        callWithNumJoinColumns([&](auto numJoinCols) {
          zipperJoinResults<numJoinCols>(*a, *b, joinColMap, rowAdder);
        });
        return toIdTableVocabPair(std::move(rowAdder));
      },
      resultSortedOn(), std::move(resultPermutation));
}

// ______________________________________________________________________________
template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
void JoinImpl::hashJoinImpl(const IdTable& dynA, ColumnIndex jc1,
                            const IdTable& dynB, ColumnIndex jc2,
                            IdTable* dynRes) {
  const IdTableView<L_WIDTH> a = dynA.asStaticView<L_WIDTH>();
  const IdTableView<R_WIDTH> b = dynB.asStaticView<R_WIDTH>();

  AD_LOG_DEBUG << "Performing hashJoin between two tables.\n";
  AD_LOG_DEBUG << "A: width = " << a.numColumns() << ", size = " << a.size()
               << "\n";
  AD_LOG_DEBUG << "B: width = " << b.numColumns() << ", size = " << b.size()
               << "\n";

  // Check trivial case.
  if (a.empty() || b.empty()) {
    return;
  }

  IdTableStatic<OUT_WIDTH> result = std::move(*dynRes).toStatic<OUT_WIDTH>();

  // Puts the rows of the given table into a hash map, with the value of
  // the join column of a row as the key, and returns the hash map.
  auto idTableToHashMap = [](const auto& table, const ColumnIndex jc) {
    // This declaration works, because generic lambdas are just syntactic sugar
    // for templates.
    using Table = std::decay_t<decltype(table)>;
    ad_utility::HashMap<Id, std::vector<typename Table::row_type>> map;
    for (const auto& row : table) {
      map[row[jc]].push_back(row);
    }
    return map;
  };

  /*
   * @brief Joins the two tables, putting the result in result. Creates a cross
   *  product for matching rows by putting the smaller IdTable in a hash map and
   *  using that, to faster find the matching rows.
   *
   * @tparam leftIsLarger If the left table in the join operation has more
   *  rows, or the right. True, if he has. False, if he hasn't.
   * @tparam LargerTableType, SmallerTableType The types of the tables given.
   *  Can be easily done with type interference, so no need to write them out.
   *
   * @param largerTable, smallerTable The two tables of the join operation.
   * @param largerTableJoinColumn, smallerTableJoinColumn The join columns
   *  of the tables
   */
  auto performHashJoin = ad_utility::ApplyAsValueIdentity{
      [&idTableToHashMap, &result](auto leftIsLarger, const auto& largerTable,
                                   const ColumnIndex largerTableJoinColumn,
                                   const auto& smallerTable,
                                   const ColumnIndex smallerTableJoinColumn) {
        // Put the smaller table into the hash table.
        auto map = idTableToHashMap(smallerTable, smallerTableJoinColumn);

        // Create cross product by going through the larger table.
        for (size_t i = 0; i < largerTable.size(); i++) {
          // Skip, if there is no matching entry for the join column.
          auto entry = map.find(largerTable(i, largerTableJoinColumn));
          if (entry == map.end()) {
            continue;
          }

          for (const auto& row : entry->second) {
            // Based on which table was larger, the arguments of
            // addCombinedRowToIdTable are different.
            // However this information is known at compile time, so the other
            // branch gets discarded at compile time, which makes this
            // condition have constant runtime.
            if constexpr (leftIsLarger) {
              addCombinedRowToIdTable(largerTable[i], row,
                                      smallerTableJoinColumn, &result);
            } else {
              addCombinedRowToIdTable(row, largerTable[i],
                                      largerTableJoinColumn, &result);
            }
          }
        }
      }};

  // Cannot just switch a and b around because the order of
  // items in the result tuples is important.
  // Procceding with the actual hash join depended on which IdTableView
  // is bigger.
  if (a.size() >= b.size()) {
    performHashJoin.template operator()<true>(a, jc1, b, jc2);
  } else {
    performHashJoin.template operator()<false>(b, jc2, a, jc1);
  }
  *dynRes = std::move(result).toDynamic();

  AD_LOG_DEBUG << "HashJoin done.\n";
  AD_LOG_DEBUG << "Result: width = " << dynRes->numColumns()
               << ", size = " << dynRes->size() << "\n";
}

// ______________________________________________________________________________
void JoinImpl::hashJoin(const IdTable& dynA, ColumnIndex jc1,
                        const IdTable& dynB, ColumnIndex jc2, IdTable* dynRes) {
  ad_utility::callFixedSizeVi(
      (std::array{dynA.numColumns(), dynB.numColumns(), dynRes->numColumns()}),
      [&](auto l, auto r, auto o) {
        return JoinImpl::hashJoinImpl<l, r, o>(dynA, jc1, dynB, jc2, dynRes);
      });
}

// ___________________________________________________________________________
template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
void JoinImpl::addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                       ColumnIndex jcRowB,
                                       IdTableStatic<TABLE_WIDTH>* table) {
  // Add a new, empty row.
  const size_t backIndex = table->size();
  table->emplace_back();

  // Copy the entire rowA in the table.
  for (auto h = ColumnIndex{0}; h < rowA.numColumns(); h++) {
    (*table)(backIndex, h) = rowA[h];
  }

  // Copy rowB columns before the join column.
  for (auto h = ColumnIndex{0}; h < jcRowB; h++) {
    (*table)(backIndex, h + rowA.numColumns()) = rowB[h];
  }

  // Copy rowB columns after the join column.
  for (auto h = jcRowB + 1; h < rowB.numColumns(); h++) {
    (*table)(backIndex, h + rowA.numColumns() - 1) = rowB[h];
  }
}

// ______________________________________________________________________________________________________
template <size_t NumJoinCols>
Result JoinImpl::computeResultForTwoIndexScans(bool requestLaziness) const {
  return createResultFromAction(
      requestLaziness,
      [this](std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto leftScan =
            std::dynamic_pointer_cast<IndexScan>(left_->getRootOperation());
        auto rightScan =
            std::dynamic_pointer_cast<IndexScan>(right_->getRootOperation());
        AD_CORRECTNESS_CHECK(leftScan && rightScan);
        // The join columns already are the first columns in both inputs, so
        // we don't have to permute the inputs and results for the
        // `AddCombinedRowToIdTable` class to work correctly.
        AD_CORRECTNESS_CHECK(joinColumnsArePrefix(0) &&
                             joinColumnsArePrefix(1));
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        ad_utility::Timer timer{
            ad_utility::timer::Timer::InitialStatus::Started};
        auto [leftBlocksInternal, rightBlocksInternal] =
            IndexScan::lazyScanForJoinOfTwoScans(*leftScan, *rightScan,
                                                 NumJoinCols);
        runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());

        // If requestLaziness, we don't need to serialize json for every update
        // of the child. If we serialize it whenever the join operation yields a
        // table that's frequent enough and reduces the overhead.
        auto leftBlocks = convertGeneratorFromScan<NumJoinCols>(
            std::move(leftBlocksInternal), *leftScan);
        auto rightBlocks = convertGeneratorFromScan<NumJoinCols>(
            std::move(rightBlocksInternal), *rightScan);

        ad_utility::zipperJoinForBlocksWithoutUndef(leftBlocks, rightBlocks,
                                                    std::less{}, rowAdder);
        setScanStatusToLazilyCompleted(*leftScan, *rightScan);
        return toIdTableVocabPair(std::move(rowAdder));
      },
      resultSortedOn(), {});
}

// ______________________________________________________________________________________________________
template <size_t NumJoinCols, bool idTableIsRightInput>
Result JoinImpl::computeResultForIndexScanAndIdTable(
    bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
    std::shared_ptr<IndexScan> scan) const {
  constexpr size_t scanSide = idTableIsRightInput ? 0 : 1;
  AD_CORRECTNESS_CHECK(joinColumnsArePrefix(scanSide));
  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResultFromAction(
      requestLaziness,
      [this, scan = std::move(scan),
       resultWithIdTable = std::move(resultWithIdTable),
       joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        const IdTableView<0>& idTable = resultWithIdTable->idTableView();
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        auto permutationIdTable =
            ad_utility::IdTableAndFirstCols<NumJoinCols, IdTableView<0>>{
                idTable.asColumnSubsetView(idTableIsRightInput
                                               ? joinColMap.permutationRight()
                                               : joinColMap.permutationLeft()),
                resultWithIdTable->getCopyOfLocalVocab()};

        ad_utility::Timer timer{
            ad_utility::timer::Timer::InitialStatus::Started};
        // The first join column of the `idTable`, by which it is sorted.
        auto firstJoinColumn =
            idTable.getColumn(joinColumns_.at(0).at(1 - scanSide));
        bool idTableHasUndef =
            !idTable.empty() && firstJoinColumn.front().isUndefined();
        std::optional<std::shared_ptr<const Result>> indexScanResult =
            std::nullopt;
        auto rightBlocks = [&scan, idTableHasUndef, &firstJoinColumn,
                            &indexScanResult]() {
          if (idTableHasUndef) {
            indexScanResult =
                scan->getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
            AD_CORRECTNESS_CHECK(
                !indexScanResult.value()->isFullyMaterialized());
            return convertGenerator<Result::LazyResult, NumJoinCols>(
                indexScanResult.value()->idTables());
          } else {
            // Note: For multiple join columns, the blocks are only filtered
            // by the first join column. This is correct, as the `idTable` is
            // sorted by its first join column, but might yield some blocks
            // that don't contain matches.
            auto rightBlocksInternal =
                scan->lazyScanForJoinOfColumnWithScan(firstJoinColumn);
            return convertGeneratorFromScan<NumJoinCols>(
                std::move(rightBlocksInternal), *scan);
          }
        }();

        runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());
        auto doJoin = [&rowAdder](auto& left, auto& right) mutable {
          zipperJoinForBlocks<NumJoinCols>(left, right, rowAdder);
        };
        auto blockForIdTable = std::array{std::move(permutationIdTable)};
        if constexpr (idTableIsRightInput) {
          doJoin(rightBlocks, blockForIdTable);
        } else {
          doJoin(blockForIdTable, rightBlocks);
        }
        setScanStatusToLazilyCompleted(*scan);

        return toIdTableVocabPair(std::move(rowAdder));
      },
      resultSortedOn(), std::move(resultPermutation));
}

// ______________________________________________________________________________________________________
template <size_t NumJoinCols>
Result JoinImpl::computeResultForIndexScanAndLazyOperation(
    bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
    std::shared_ptr<IndexScan> scan) const {
  AD_CORRECTNESS_CHECK(joinColumnsArePrefix(1));

  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResultFromAction(
      requestLaziness,
      [this, scan = std::move(scan),
       resultWithIdTable = std::move(resultWithIdTable),
       joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        // Note: For multiple join columns, the blocks are only prefiltered by
        // the first join column, which is correct, but might yield some blocks
        // that don't contain matches.
        auto [joinSide, indexScanSide] = scan->prefilterTables(
            resultWithIdTable->idTables(), joinColumns_.at(0).at(0));

        zipperJoinForBlocks<NumJoinCols>(
            convertGenerator<Result::LazyResult, NumJoinCols>(
                std::move(joinSide), joinColMap.permutationLeft()),
            convertGenerator<Result::LazyResult, NumJoinCols>(
                std::move(indexScanSide), joinColMap.permutationRight()),
            rowAdder);
        setScanStatusToLazilyCompleted(*scan);

        return toIdTableVocabPair(std::move(rowAdder));
      },
      resultSortedOn(), std::move(resultPermutation));
}
// _____________________________________________________________________________
Result JoinImpl::computeResultForTwoMaterializedInputs(
    std::shared_ptr<const Result> leftRes,
    std::shared_ptr<const Result> rightRes) const {
  IdTable idTable{getResultWidth(), allocator()};
  join(leftRes->idTableView(), rightRes->idTableView(), &idTable);
  checkCancellation();

  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*leftRes, *rightRes)};
}

// _____________________________________________________________________________
Result JoinImpl::createEmptyResult() const {
  return {IdTable{getResultWidth(), allocator()}, resultSortedOn(),
          LocalVocab{}};
}

// _____________________________________________________________________________
ad_utility::JoinColumnMapping JoinImpl::getJoinColumnMapping() const {
  return ad_utility::JoinColumnMapping{joinColumns_, left_->getResultWidth(),
                                       right_->getResultWidth(),
                                       keepJoinColumn_};
}

// _____________________________________________________________________________
ad_utility::AddCombinedRowToIdTable JoinImpl::makeRowAdder(
    std::function<void(IdTable&, LocalVocab&)> callback) const {
  return ad_utility::AddCombinedRowToIdTable{
      joinColumns_.size(),  IdTable{getResultWidth(), allocator()},
      cancellationHandle(), keepJoinColumn_,
      CHUNK_SIZE,           std::move(callback)};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> JoinImpl::cloneImpl() const {
  auto copy = std::make_unique<JoinImpl>(*this);
  copy->left_ = left_->clone();
  copy->right_ = right_->clone();
  return copy;
}

// _____________________________________________________________________________
bool JoinImpl::joinColumnsArePrefix(size_t side) const {
  return ql::ranges::equal(joinColumnsOfSide(side),
                           ad_utility::integerRange(joinColumns_.size()));
}

// _____________________________________________________________________________
bool JoinImpl::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // For the join columns we don't union the elements, we intersect them so we
  // can have a more efficient implementation.
  if (ad_utility::contains(joinVars_, variable)) {
    return doesJoinProduceGuaranteedGraphValuesOrUndef(left_, right_, variable);
  }
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
JoinImpl::makeTreeWithStrippedColumns(
    const std::set<Variable>& variables) const {
  // The join columns are required for the join, so we must not strip them
  // from the children.
  std::set<Variable> childVariables = variables;
  childVariables.insert(joinVars_.begin(), joinVars_.end());

  auto left =
      QueryExecutionTree::makeTreeWithStrippedColumns(left_, childVariables);
  auto right =
      QueryExecutionTree::makeTreeWithStrippedColumns(right_, childVariables);
  auto joinColumns = computeJoinColumns(*left, *right, joinVars_);
  // The join columns can only be removed from the result if there is a single
  // join column.
  bool keepJoinColumns =
      joinVars_.size() > 1 || ad_utility::contains(variables, joinVars_.at(0));
  return ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), std::move(left), std::move(right),
      std::move(joinColumns), keepJoinColumns);
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
JoinImpl::makeTreeWithBindColumn(const parsedQuery::Bind& bind) const {
  return pushDownBindToAnyChild(bind, {left_, right_});
}

// _____________________________________________________________________________
std::unique_ptr<Operation> JoinImpl::cloneWithNewChildren(
    std::vector<std::shared_ptr<QueryExecutionTree>> children) const {
  auto& left = children.at(0);
  auto& right = children.at(1);
  auto joinColumns = computeJoinColumns(*left, *right, joinVars_);
  // NOTE: This deliberately creates a `Join`, as `JoinImpl` is only used as
  // the implementation of `Join`.
  return std::make_unique<Join>(_executionContext, std::move(left),
                                std::move(right), std::move(joinColumns),
                                keepJoinColumn_);
}
