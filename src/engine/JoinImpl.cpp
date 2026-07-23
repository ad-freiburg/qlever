// Copyright 2015 - 2026 The QLever Authors, in particular:

// 2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// 2018-2026 Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de), UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026 Mark Veser (mark.veser87@gmail.com)

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/JoinImpl.h"

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
#include "engine/OperationBindPushDownImpl.h"
#include "engine/Service.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
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
                   ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
                   bool keepJoinColumn,
                   bool allowSwappingChildrenOnlyForTesting)
    : Operation(qec), keepJoinColumn_{keepJoinColumn} {
  AD_CONTRACT_CHECK(t1 && t2);
  // Currently all join algorithms require both inputs to be sorted, so we
  // enforce the sorting here.
  t1 = QueryExecutionTree::createSortedTree(std::move(t1), {t1JoinCol});
  t2 = QueryExecutionTree::createSortedTree(std::move(t2), {t2JoinCol});

  // Make the order of the two subtrees deterministic. That way, queries that
  // are identical except for the order of the join operands, are easier to
  // identify.
  auto swapChildren = [&]() {
    // This can be disabled by tests to fix the order of the subtrees.
    if (allowSwappingChildrenOnlyForTesting) {
      std::swap(t1, t2);
      std::swap(t1JoinCol, t2JoinCol);
    }
  };
  if (t1->getCacheKey() > t2->getCacheKey()) {
    swapChildren();
  }
  // If one of the inputs is a SCAN and the other one is not, always make the
  // SCAN the right child (which also gives a deterministic order of the
  // subtrees). This simplifies several branches in the `computeResult` method.
  if (std::dynamic_pointer_cast<IndexScan>(t1->getRootOperation()) &&
      !std::dynamic_pointer_cast<IndexScan>(t2->getRootOperation())) {
    swapChildren();
  }
  left_ = std::move(t1);
  leftJoinCol_ = t1JoinCol;
  right_ = std::move(t2);
  rightJoinCol_ = t2JoinCol;
  sizeEstimate_ = 0;
  sizeEstimateComputed_ = false;
  multiplicities_.clear();
  auto findJoinVar = [](const QueryExecutionTree& tree,
                        ColumnIndex joinCol) -> Variable {
    return tree.getVariableAndInfoByColumnIndex(joinCol).first;
  };
  joinVar_ = findJoinVar(*left_, leftJoinCol_);
  AD_CONTRACT_CHECK(joinVar_ == findJoinVar(*right_, rightJoinCol_));
}

// _____________________________________________________________________________
string JoinImpl::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "JOIN\n"
     << left_->getCacheKey() << " join-column: [" << leftJoinCol_ << "]\n";
  os << "|X|\n"
     << right_->getCacheKey() << " join-column: [" << rightJoinCol_ << "]";
  os << "keep join Col " << keepJoinColumn_;
  return std::move(os).str();
}

// _____________________________________________________________________________
string JoinImpl::getDescriptor() const { return "Join on " + joinVar_.name(); }

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

  auto leftIndexScan =
      std::dynamic_pointer_cast<IndexScan>(left_->getRootOperation());
  if (leftIndexScan &&
      std::dynamic_pointer_cast<IndexScan>(right_->getRootOperation())) {
    if (rightResIfCached && !leftResIfCached) {
      AD_CORRECTNESS_CHECK(rightResIfCached->isFullyMaterialized());
      return computeResultForIndexScanAndIdTable<true>(
          requestLaziness, std::move(rightResIfCached), leftIndexScan);

    } else if (!leftResIfCached) {
      return computeResultForTwoIndexScans(requestLaziness);
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
  auto rightIndexScan =
      std::dynamic_pointer_cast<IndexScan>(right_->getRootOperation());
  if (rightIndexScan && !rightResIfCached) {
    if (leftRes->isFullyMaterialized()) {
      return computeResultForIndexScanAndIdTable<false>(
          requestLaziness, std::move(leftRes), rightIndexScan);
    }
    return computeResultForIndexScanAndLazyOperation(
        requestLaziness, std::move(leftRes), rightIndexScan);
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
VariableToColumnMap JoinImpl::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      left_->getVariableColumns(), right_->getVariableColumns(),
      {{leftJoinCol_, rightJoinCol_}}, BinOpType::Join, left_->getResultWidth(),
      keepJoinColumn_);
}

// _____________________________________________________________________________
size_t JoinImpl::getResultWidth() const {
  auto sumOfChildWidths = left_->getResultWidth() + right_->getResultWidth();
  AD_CORRECTNESS_CHECK(sumOfChildWidths >= 2);
  return sumOfChildWidths - 1 - static_cast<size_t>(!keepJoinColumn_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> JoinImpl::resultSortedOn() const {
  if (keepJoinColumn_) {
    return {leftJoinCol_};
  } else {
    return {};
  }
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
  size_t costJoin = left_->getSizeEstimate() + right_->getSizeEstimate();

  // TODO<joka921> once the `getCostEstimate` functions are `const`,
  // the argument can also be `const auto`
  auto costOfSubtree = [](auto& subtree) { return subtree->getCostEstimate(); };

  return getSizeEstimateBeforeLimit() + costJoin + costOfSubtree(left_) +
         costOfSubtree(right_);
}

// _____________________________________________________________________________
void JoinImpl::computeSizeEstimateAndMultiplicities() {
  multiplicities_.clear();
  if (left_->getSizeEstimate() == 0 || right_->getSizeEstimate() == 0) {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      multiplicities_.emplace_back(1);
    }
    return;
  }

  size_t nofDistinctLeft = std::max(
      size_t(1), static_cast<size_t>(left_->getSizeEstimate() /
                                     left_->getMultiplicity(leftJoinCol_)));
  size_t nofDistinctRight = std::max(
      size_t(1), static_cast<size_t>(right_->getSizeEstimate() /
                                     right_->getMultiplicity(rightJoinCol_)));

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

  double jcMultiplicityInResult = left_->getMultiplicity(leftJoinCol_) *
                                  right_->getMultiplicity(rightJoinCol_);
  sizeEstimate_ = std::max(
      size_t(1), static_cast<size_t>(corrFactor * jcMultiplicityInResult *
                                     nofDistinctInResult));

  AD_LOG_TRACE << "Estimated size as: " << sizeEstimate_ << " := " << corrFactor
               << " * " << jcMultiplicityInResult << " * "
               << nofDistinctInResult << std::endl;

  for (auto i = ColumnIndex{0}; i < left_->getResultWidth(); ++i) {
    double oldMult = left_->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * right_->getMultiplicity(rightJoinCol_) * corrFactor);
    if (i != leftJoinCol_ && nofDistinctLeft != nofDistinctInResult) {
      double oldDist = left_->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeLeft);
      m = (sizeEstimate_ / corrFactor) / newDist;
    }
    if (i != leftJoinCol_ || keepJoinColumn_) {
      multiplicities_.emplace_back(m);
    }
  }
  for (auto i = ColumnIndex{0}; i < right_->getResultWidth(); ++i) {
    if (i == rightJoinCol_) {
      continue;
    }
    double oldMult = right_->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * left_->getMultiplicity(leftJoinCol_) * corrFactor);
    if (i != rightJoinCol_ && nofDistinctRight != nofDistinctInResult) {
      double oldDist = right_->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeRight);
      m = (sizeEstimate_ / corrFactor) / newDist;
    }
    multiplicities_.emplace_back(m);
  }
  assert(multiplicities_.size() == getResultWidth());
}

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
  checkCancellation();
  ad_utility::JoinColumnMapping joinColumnData = getJoinColumnMapping();
  auto joinColumnL = a.getColumn(leftJoinCol_);
  auto joinColumnR = b.getColumn(rightJoinCol_);

  auto aPermuted = a.asColumnSubsetView(joinColumnData.permutationLeft());
  auto bPermuted = b.asColumnSubsetView(joinColumnData.permutationRight());

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      1, aPermuted, bPermuted, std::move(*result), cancellationHandle_,
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

  auto cancellationCallback = [this]() { checkCancellation(); };

  // Determine whether we should use the galloping join optimization.
  if (a.size() / b.size() > GALLOP_THRESHOLD && numUndefA == 0 &&
      numUndefB == 0) {
    // The first argument to the galloping join will always be the smaller
    // input, so we need to switch the rows when adding them.
    auto inverseAddRow = [&addRow](const auto& rowA, const auto& rowB) {
      addRow(rowB, rowA);
    };
    ad_utility::gallopingJoin(joinColumnR, joinColumnL, ql::ranges::less{},
                              inverseAddRow, {}, cancellationCallback);
  } else if (b.size() / a.size() > GALLOP_THRESHOLD && numUndefA == 0 &&
             numUndefB == 0) {
    ad_utility::gallopingJoin(joinColumnL, joinColumnR, ql::ranges::less{},
                              addRow, {}, cancellationCallback);
  } else {
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
        auto leftRange = resultToView(*a, joinColMap.permutationLeft());
        auto rightRange = resultToView(*b, joinColMap.permutationRight());
        std::visit(
            [&rowAdder](auto& leftBlocks, auto& rightBlocks) {
              ad_utility::zipperJoinForBlocksWithPotentialUndef(
                  leftBlocks, rightBlocks, std::less{}, rowAdder);
            },
            leftRange, rightRange);
        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
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
Result JoinImpl::computeResultForTwoIndexScans(bool requestLaziness) const {
  // The block-skipping lazy scans below join on the primary (physical) sort
  // column of each scan, which must be the join column. The columns are then
  // reordered so that the join column comes first (via the `JoinColumnMapping`),
  // and the result is reordered back into the expected layout via
  // `permutationResult()`. This works for any join column, not just column 0.
  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResultFromAction(
      requestLaziness,
      [this, joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto leftScan =
            std::dynamic_pointer_cast<IndexScan>(left_->getRootOperation());
        auto rightScan =
            std::dynamic_pointer_cast<IndexScan>(right_->getRootOperation());
        AD_CORRECTNESS_CHECK(leftScan && rightScan);
        // The join has to be on the primary (physical) sort column of both
        // scans for the block-skipping to be correct.
        AD_CORRECTNESS_CHECK(leftScan->resultSortedOn().front() == leftJoinCol_);
        AD_CORRECTNESS_CHECK(rightScan->resultSortedOn().front() ==
                             rightJoinCol_);
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        ad_utility::Timer timer{
            ad_utility::timer::Timer::InitialStatus::Started};
        auto [leftBlocksInternal, rightBlocksInternal] =
            IndexScan::lazyScanForJoinOfTwoScans(*leftScan, *rightScan);
        runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());

        // If requestLaziness, we don't need to serialize json for every update
        // of the child. If we serialize it whenever the join operation yields a
        // table that's frequent enough and reduces the overhead.
        auto leftBlocks = convertGeneratorFromScan(
            std::move(leftBlocksInternal), *leftScan, joinColMap.permutationLeft());
        auto rightBlocks =
            convertGeneratorFromScan(std::move(rightBlocksInternal), *rightScan,
                                     joinColMap.permutationRight());

        ad_utility::zipperJoinForBlocksWithoutUndef(leftBlocks, rightBlocks,
                                                    std::less{}, rowAdder);
        setScanStatusToLazilyCompleted(*leftScan, *rightScan);
        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
      },
      resultSortedOn(), std::move(resultPermutation));
}

// ______________________________________________________________________________________________________
template <bool idTableIsRightInput>
Result JoinImpl::computeResultForIndexScanAndIdTable(
    bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
    std::shared_ptr<IndexScan> scan) const {
  // The scan is the left input iff the `idTable` is the right input. The
  // block-skipping requires that the scan is sorted primarily on the join
  // column. The columns are then reordered so that the join column comes first
  // (via the `JoinColumnMapping`), and the result is reordered back into the
  // expected layout via `permutationResult()`. This works for any join column,
  // not just column 0.
  ColumnIndex scanJoinCol = idTableIsRightInput ? leftJoinCol_ : rightJoinCol_;
  AD_CORRECTNESS_CHECK(scan->resultSortedOn().front() == scanJoinCol);
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

        // The permutation to apply to the scan side so that the join column
        // comes first.
        const auto& scanPermutation = idTableIsRightInput
                                          ? joinColMap.permutationLeft()
                                          : joinColMap.permutationRight();

        auto permutationIdTable =
            ad_utility::IdTableAndFirstCols<1, IdTableView<0>>{
                idTable.asColumnSubsetView(idTableIsRightInput
                                               ? joinColMap.permutationRight()
                                               : joinColMap.permutationLeft()),
                resultWithIdTable->getCopyOfLocalVocab()};

        ad_utility::Timer timer{
            ad_utility::timer::Timer::InitialStatus::Started};
        bool idTableHasUndef =
            !idTable.empty() &&
            idTable.at(0, idTableIsRightInput ? rightJoinCol_ : leftJoinCol_)
                .isUndefined();
        std::optional<std::shared_ptr<const Result>> indexScanResult =
            std::nullopt;
        auto rightBlocks = [&scan, idTableHasUndef, &permutationIdTable,
                            &indexScanResult, &scanPermutation]() {
          if (idTableHasUndef) {
            indexScanResult =
                scan->getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
            AD_CORRECTNESS_CHECK(
                !indexScanResult.value()->isFullyMaterialized());
            return convertGenerator(indexScanResult.value()->idTables(),
                                    scanPermutation);
          } else {
            auto rightBlocksInternal =
                scan->lazyScanForJoinOfColumnWithScan(permutationIdTable.col());
            return convertGeneratorFromScan(std::move(rightBlocksInternal),
                                            *scan, scanPermutation);
          }
        }();

        runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());
        auto doJoin = [&rowAdder](auto& left, auto& right) mutable {
          // Note: The `zipperJoinForBlocksWithPotentialUndef` automatically
          // switches to a more efficient implementation if there are no UNDEF
          // values in any of the inputs.
          ad_utility::zipperJoinForBlocksWithPotentialUndef(
              left, right, std::less{}, rowAdder);
        };
        auto blockForIdTable = std::array{std::move(permutationIdTable)};
        if constexpr (idTableIsRightInput) {
          doJoin(rightBlocks, blockForIdTable);
        } else {
          doJoin(blockForIdTable, rightBlocks);
        }
        setScanStatusToLazilyCompleted(*scan);

        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
      },
      resultSortedOn(), std::move(resultPermutation));
}

// ______________________________________________________________________________________________________
Result JoinImpl::computeResultForIndexScanAndLazyOperation(
    bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
    std::shared_ptr<IndexScan> scan) const {
  // The scan is the right input. The block-skipping requires that the scan is
  // sorted primarily on the join column. The scan side is reordered so that the
  // join column comes first via `joinColMap.permutationRight()` (see below).
  AD_CORRECTNESS_CHECK(scan->resultSortedOn().front() == rightJoinCol_);

  ad_utility::JoinColumnMapping joinColMap = getJoinColumnMapping();
  auto resultPermutation = joinColMap.permutationResult();
  return createResultFromAction(
      requestLaziness,
      [this, scan = std::move(scan),
       resultWithIdTable = std::move(resultWithIdTable),
       joinColMap = std::move(joinColMap)](
          std::function<void(IdTable&, LocalVocab&)> yieldTable) {
        auto rowAdder = makeRowAdder(std::move(yieldTable));

        auto [joinSide, indexScanSide] =
            scan->prefilterTables(resultWithIdTable->idTables(), leftJoinCol_);

        // Note: The `zipperJoinForBlocksWithPotentialUndef` automatically
        // switches to a more efficient implementation if there are no UNDEF
        // values in any of the inputs.
        zipperJoinForBlocksWithPotentialUndef(
            convertGenerator(std::move(joinSide), joinColMap.permutationLeft()),
            convertGenerator(std::move(indexScanSide),
                             joinColMap.permutationRight()),
            std::less{}, rowAdder);
        setScanStatusToLazilyCompleted(*scan);

        auto localVocab = std::move(rowAdder.localVocab());
        return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                        std::move(localVocab)};
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
  return ad_utility::JoinColumnMapping{{{leftJoinCol_, rightJoinCol_}},
                                       left_->getResultWidth(),
                                       right_->getResultWidth(),
                                       keepJoinColumn_};
}

// _____________________________________________________________________________
ad_utility::AddCombinedRowToIdTable JoinImpl::makeRowAdder(
    std::function<void(IdTable&, LocalVocab&)> callback) const {
  return ad_utility::AddCombinedRowToIdTable{
      1,
      IdTable{getResultWidth(), allocator()},
      cancellationHandle_,
      keepJoinColumn_,
      CHUNK_SIZE,
      std::move(callback)};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> JoinImpl::cloneImpl() const {
  auto copy = std::make_unique<JoinImpl>(*this);
  copy->left_ = left_->clone();
  copy->right_ = right_->clone();
  return copy;
}

// _____________________________________________________________________________
bool JoinImpl::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // For the join column we don't union the elements, we intersect them so we
  // can have a more efficient implementation.
  if (variable == joinVar_) {
    return doesJoinProduceGuaranteedGraphValuesOrUndef(left_, right_, variable);
  }
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
JoinImpl::makeTreeWithStrippedColumns(
    const std::set<Variable>& variables) const {
  std::set<Variable> newVariables;
  const auto* vars = &variables;
  if (!ad_utility::contains(variables, joinVar_)) {
    newVariables = variables;
    newVariables.insert(joinVar_);
    vars = &newVariables;
  }

  // TODO<joka921> Code duplication including a former copy-paste bug.
  auto left = QueryExecutionTree::makeTreeWithStrippedColumns(left_, *vars);
  auto right = QueryExecutionTree::makeTreeWithStrippedColumns(right_, *vars);
  auto leftCol = left->getVariableColumn(joinVar_);
  auto rightCol = right->getVariableColumn(joinVar_);
  return ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), std::move(left), std::move(right), leftCol,
      rightCol, ad_utility::contains(variables, joinVar_));
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
JoinImpl::makeTreeWithBindColumn(const parsedQuery::Bind& bind) const {
  return pushDownBindToAnyChild(
      bind, {left_, right_},
      [this](std::vector<std::shared_ptr<QueryExecutionTree>> newChildren) {
        auto& left = newChildren.at(0);
        auto& right = newChildren.at(1);
        auto leftCol = left->getVariableColumn(joinVar_);
        auto rightCol = right->getVariableColumn(joinVar_);
        return ad_utility::makeExecutionTree<Join>(
            getExecutionContext(), std::move(left), std::move(right), leftCol,
            rightCol);
      });
}
