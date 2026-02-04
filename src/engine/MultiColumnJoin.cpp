// Copyright 2018 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Florian Kramer [2018 - 2020]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/MultiColumnJoin.h"

#include <numeric>

#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/IndexScan.h"
#include "engine/JoinHelpers.h"
#include "engine/JoinWithIndexScanHelpers.h"
#include "global/RuntimeParameters.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
MultiColumnJoin::MultiColumnJoin(QueryExecutionContext* qec,
                                 std::shared_ptr<QueryExecutionTree> t1,
                                 std::shared_ptr<QueryExecutionTree> t2,
                                 bool allowSwappingChildrenOnlyForTesting)
    : Operation{qec} {
  // Make sure subtrees are ordered so that identical queries can be identified.
  if (allowSwappingChildrenOnlyForTesting &&
      t1->getCacheKey() > t2->getCacheKey()) {
    std::swap(t1, t2);
  }
  std::tie(_left, _right, _joinColumns) =
      QueryExecutionTree::getSortedSubtreesAndJoinColumns(std::move(t1),
                                                          std::move(t2));
}

// _____________________________________________________________________________
string MultiColumnJoin::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "MULTI_COLUMN_JOIN\n" << _left->getCacheKey() << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][0] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  os << "|X|\n" << _right->getCacheKey() << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][1] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]";
  return std::move(os).str();
}

// _____________________________________________________________________________
string MultiColumnJoin::getDescriptor() const {
  std::string joinVars = "";
  for (auto jc : _joinColumns) {
    joinVars +=
        _left->getVariableAndInfoByColumnIndex(jc[0]).first.name() + " ";
  }
  return "MultiColumnJoin on " + joinVars;
}

// _____________________________________________________________________________
Result MultiColumnJoin::computeResultForTwoIndexScans(
    bool requestLaziness, IndexScan& leftScan, IndexScan& rightScan) const {
  using namespace qlever::joinWithIndexScanHelpers;

  ad_utility::Timer timer{ad_utility::timer::Timer::InitialStatus::Started};

  // Get filtered blocks for both sides
  auto blocks =
      getBlocksForJoinOfTwoScans(leftScan, rightScan, _joinColumns.size());

  runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());

  // Create result generator
  // Wrap generators in shared_ptr to allow const lambda capture
  auto leftBlocksPtr =
      std::make_shared<CompressedRelationReader::IdTableGeneratorInputRange>(
          std::move(blocks[0]));
  auto rightBlocksPtr =
      std::make_shared<CompressedRelationReader::IdTableGeneratorInputRange>(
          std::move(blocks[1]));

  auto action = [this, leftBlocksPtr, rightBlocksPtr, &leftScan, &rightScan](
                    std::function<void(IdTable&, LocalVocab&)> yieldTable) {
    auto rowAdder = ad_utility::AddCombinedRowToIdTable{
        _joinColumns.size(),
        IdTable{getResultWidth(), allocator()},
        cancellationHandle_,
        true,  // keepJoinColumns (for multi-column joins, we always keep them)
        qlever::joinHelpers::CHUNK_SIZE,
        std::move(yieldTable)};

    auto leftConverted = convertGenerator(std::move(*leftBlocksPtr), leftScan);
    auto rightConverted =
        convertGenerator(std::move(*rightBlocksPtr), rightScan);

    ad_utility::zipperJoinForBlocksWithPotentialUndef(
        leftConverted, rightConverted, std::less{}, rowAdder, {}, {});

    setScanStatusToLazilyCompleted(leftScan, rightScan);

    auto localVocab = std::move(rowAdder.localVocab());
    return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                    std::move(localVocab)};
  };

  if (requestLaziness) {
    return {qlever::joinHelpers::runLazyJoinAndConvertToGenerator(
                std::move(action), {}),
            resultSortedOn()};
  } else {
    auto [idTable, localVocab] = action(ad_utility::noop);
    return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
  }
}

// _____________________________________________________________________________
template <bool idTableIsRightInput>
Result MultiColumnJoin::computeResultForIndexScanAndIdTable(
    bool requestLaziness, std::shared_ptr<const Result> resultWithIdTable,
    std::shared_ptr<IndexScan> scan) const {
  using namespace qlever::joinWithIndexScanHelpers;

  AD_CORRECTNESS_CHECK(resultWithIdTable->isFullyMaterialized());

  ad_utility::Timer timer{ad_utility::timer::Timer::InitialStatus::Started};

  const IdTable& idTable = resultWithIdTable->idTable();

  // Check if IdTable has UNDEF in join columns
  bool idTableHasUndef = false;
  for (const auto& [leftCol, rightCol] : _joinColumns) {
    auto col = idTableIsRightInput ? rightCol : leftCol;
    if (!idTable.empty() && idTable.at(0, col).isUndefined()) {
      idTableHasUndef = true;
      break;
    }
  }

  // Get prefiltered blocks from the IndexScan
  CompressedRelationReader::IdTableGeneratorInputRange scanBlocks;
  if (!idTableHasUndef) {
    scanBlocks = getBlocksForJoinOfColumnsWithScan(idTable, _joinColumns, *scan,
                                                   idTableIsRightInput ? 1 : 0);
  } else {
    // Cannot prefilter with UNDEF, scan everything
    scanBlocks = scan->getLazyScan(std::nullopt);
    auto metaBlocks = scan->getMetadataForScan();
    if (metaBlocks.has_value()) {
      scanBlocks.details().numBlocksAll_ =
          metaBlocks.value().sizeBlockMetadata_;
    }
  }

  runtimeInfo().addDetail("time-for-filtering-blocks", timer.msecs());

  // Wrap generator in shared_ptr
  auto scanBlocksPtr =
      std::make_shared<CompressedRelationReader::IdTableGeneratorInputRange>(
          std::move(scanBlocks));

  auto action = [this, resultWithIdTable = std::move(resultWithIdTable),
                 scanBlocksPtr,
                 scan](std::function<void(IdTable&, LocalVocab&)> yieldTable) {
    auto rowAdder = ad_utility::AddCombinedRowToIdTable{
        _joinColumns.size(),
        IdTable{getResultWidth(), allocator()},
        cancellationHandle_,
        true,  // keepJoinColumns (for multi-column joins, we always keep them)
        qlever::joinHelpers::CHUNK_SIZE,
        std::move(yieldTable)};

    // Create view of idTable
    const IdTable& table = resultWithIdTable->idTable();
    std::vector<ColumnIndex> identityPerm(table.numColumns());
    std::iota(identityPerm.begin(), identityPerm.end(), 0);
    auto idTableBlock = std::array{ad_utility::IdTableAndFirstCol{
        table.asColumnSubsetView(identityPerm),
        resultWithIdTable->getCopyOfLocalVocab()}};
    auto scanConverted = convertGenerator(std::move(*scanBlocksPtr), *scan);

    if constexpr (idTableIsRightInput) {
      ad_utility::zipperJoinForBlocksWithPotentialUndef(
          scanConverted, idTableBlock, std::less{}, rowAdder, {}, {});
    } else {
      ad_utility::zipperJoinForBlocksWithPotentialUndef(
          idTableBlock, scanConverted, std::less{}, rowAdder, {}, {});
    }

    setScanStatusToLazilyCompleted(*scan);

    auto localVocab = std::move(rowAdder.localVocab());
    return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                    std::move(localVocab)};
  };

  if (requestLaziness) {
    return {qlever::joinHelpers::runLazyJoinAndConvertToGenerator(
                std::move(action), {}),
            resultSortedOn()};
  } else {
    auto [idTable, localVocab] = action(ad_utility::noop);
    return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
  }
}

// Explicit template instantiation
template Result MultiColumnJoin::computeResultForIndexScanAndIdTable<false>(
    bool, std::shared_ptr<const Result>, std::shared_ptr<IndexScan>) const;
template Result MultiColumnJoin::computeResultForIndexScanAndIdTable<true>(
    bool, std::shared_ptr<const Result>, std::shared_ptr<IndexScan>) const;

// _____________________________________________________________________________
Result MultiColumnJoin::computeResultForIndexScanAndLazyOperation(
    bool requestLaziness, std::shared_ptr<const Result> lazyResult,
    std::shared_ptr<IndexScan> scan) const {
  // For lazy input with IndexScan, we cannot use prefiltering efficiently
  // TODO: Implement proper lazy prefiltering similar to Join
  // For now, signal to fall back to regular path by returning empty result
  (void)requestLaziness;
  (void)lazyResult;
  (void)scan;

  // Return empty result to signal fallback to regular computation path
  return {IdTable{getResultWidth(), allocator()}, resultSortedOn(),
          LocalVocab{}};
}

// _____________________________________________________________________________
Result MultiColumnJoin::computeResult([[maybe_unused]] bool requestLaziness) {
  AD_LOG_DEBUG << "MultiColumnJoin result computation..." << endl;

  // Try prefiltering with IndexScans
  auto leftIndexScan =
      std::dynamic_pointer_cast<IndexScan>(_left->getRootOperation());
  auto rightIndexScan =
      std::dynamic_pointer_cast<IndexScan>(_right->getRootOperation());

  // Case 1: Both children are IndexScans
  if (leftIndexScan && rightIndexScan) {
    return computeResultForTwoIndexScans(requestLaziness, *leftIndexScan,
                                         *rightIndexScan);
  }

  // Case 2: One child is IndexScan, try to use prefiltering
  if (leftIndexScan || rightIndexScan) {
    bool leftIsSmall =
        _left->getRootOperation()->getSizeEstimate() <
        getRuntimeParameter<
            &RuntimeParameters::lazyIndexScanMaxSizeMaterialization_>();
    bool rightIsSmall =
        _right->getRootOperation()->getSizeEstimate() <
        getRuntimeParameter<
            &RuntimeParameters::lazyIndexScanMaxSizeMaterialization_>();

    auto leftResIfCached = _left->getRootOperation()->getResult(
        false, leftIsSmall ? ComputationMode::FULLY_MATERIALIZED
                           : ComputationMode::ONLY_IF_CACHED);
    auto rightResIfCached = _right->getRootOperation()->getResult(
        false, rightIsSmall ? ComputationMode::FULLY_MATERIALIZED
                            : ComputationMode::ONLY_IF_CACHED);

    if (leftIndexScan && rightResIfCached &&
        rightResIfCached->isFullyMaterialized()) {
      return computeResultForIndexScanAndIdTable<true>(
          requestLaziness, std::move(rightResIfCached), leftIndexScan);
    }

    if (rightIndexScan && leftResIfCached &&
        leftResIfCached->isFullyMaterialized()) {
      return computeResultForIndexScanAndIdTable<false>(
          requestLaziness, std::move(leftResIfCached), rightIndexScan);
    }

    // Try getting the full results
    auto leftResult =
        leftResIfCached ? leftResIfCached : _left->getResult(true);
    auto rightResult =
        rightResIfCached ? rightResIfCached : _right->getResult(true);

    if (leftIndexScan && rightResult->isFullyMaterialized()) {
      return computeResultForIndexScanAndIdTable<true>(
          requestLaziness, std::move(rightResult), leftIndexScan);
    }

    if (rightIndexScan && leftResult->isFullyMaterialized()) {
      return computeResultForIndexScanAndIdTable<false>(
          requestLaziness, std::move(leftResult), rightIndexScan);
    }

    // Handle lazy cases
    if (leftIndexScan && !rightResult->isFullyMaterialized()) {
      return computeResultForIndexScanAndLazyOperation(
          requestLaziness, std::move(rightResult), leftIndexScan);
    }

    if (rightIndexScan && !leftResult->isFullyMaterialized()) {
      return computeResultForIndexScanAndLazyOperation(
          requestLaziness, std::move(leftResult), rightIndexScan);
    }
  }

  // Regular path: no IndexScan optimization
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  AD_CONTRACT_CHECK(idTable.numColumns() >= _joinColumns.size());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  checkCancellation();

  AD_LOG_DEBUG << "MultiColumnJoin subresult computation done." << std::endl;

  AD_LOG_DEBUG << "Computing a multi column join between results of size "
               << leftResult->idTable().size() << " and "
               << rightResult->idTable().size() << endl;

  computeMultiColumnJoin(leftResult->idTable(), rightResult->idTable(),
                         _joinColumns, &idTable);

  checkCancellation();

  AD_LOG_DEBUG << "MultiColumnJoin result computation done" << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*leftResult, *rightResult)};
}

// _____________________________________________________________________________
VariableToColumnMap MultiColumnJoin::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      _left->getVariableColumns(), _right->getVariableColumns(), _joinColumns,
      BinOpType::Join, _left->getResultWidth());
}

// _____________________________________________________________________________
size_t MultiColumnJoin::getResultWidth() const {
  size_t res =
      _left->getResultWidth() + _right->getResultWidth() - _joinColumns.size();
  AD_CONTRACT_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
std::vector<ColumnIndex> MultiColumnJoin::resultSortedOn() const {
  std::vector<ColumnIndex> sortedOn;
  // The result is sorted on all join columns from the left subtree.
  for (const auto& a : _joinColumns) {
    sortedOn.push_back(a[0]);
  }
  return sortedOn;
}

// _____________________________________________________________________________
float MultiColumnJoin::getMultiplicity(size_t col) {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _multiplicities[col];
}

// _____________________________________________________________________________
uint64_t MultiColumnJoin::getSizeEstimateBeforeLimit() {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t MultiColumnJoin::getCostEstimate() {
  size_t costEstimate = getSizeEstimateBeforeLimit() +
                        _left->getSizeEstimate() + _right->getSizeEstimate();
  // This join is slower than a normal join, due to
  // its increased complexity
  costEstimate *= 2;
  // Make the join 7% more expensive per join column
  costEstimate *= (1 + (_joinColumns.size() - 1) * 0.07);
  return _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
}

// _____________________________________________________________________________
void MultiColumnJoin::computeSizeEstimateAndMultiplicities() {
  // The number of distinct entries in the result is at most the minimum of
  // the numbers of distinct entries in all join columns.
  // The multiplicity in the result is approximated by the product of the
  // maximum of the multiplicities of each side.

  // compute the minimum number of distinct elements in the join columns
  size_t numDistinctLeft = std::numeric_limits<size_t>::max();
  size_t numDistinctRight = std::numeric_limits<size_t>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    size_t dl = std::max(1.0f, _left->getSizeEstimate() /
                                   _left->getMultiplicity(_joinColumns[i][0]));
    size_t dr = std::max(1.0f, _right->getSizeEstimate() /
                                   _right->getMultiplicity(_joinColumns[i][1]));
    numDistinctLeft = std::min(numDistinctLeft, dl);
    numDistinctRight = std::min(numDistinctRight, dr);
  }
  size_t numDistinctResult = std::min(numDistinctLeft, numDistinctRight);

  // compute an estimate for the results multiplicity
  float multLeft = std::numeric_limits<float>::max();
  float multRight = std::numeric_limits<float>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    multLeft = std::min(multLeft, _left->getMultiplicity(_joinColumns[i][0]));
    multRight =
        std::min(multRight, _right->getMultiplicity(_joinColumns[i][1]));
  }
  float multResult = multLeft * multRight;

  _sizeEstimate = multResult * numDistinctResult;
  // Don't estimate 0 since then some parent operations
  // (in particular joins) using isKnownEmpty() will
  // will assume the size to be exactly zero
  _sizeEstimate += 1;

  // compute estimates for the multiplicities of the result columns
  _multiplicities.clear();

  for (size_t i = 0; i < _left->getResultWidth(); i++) {
    float mult = _left->getMultiplicity(i) * (multResult / multLeft);
    _multiplicities.push_back(mult);
  }

  for (size_t i = 0; i < _right->getResultWidth(); i++) {
    bool isJcl = false;
    for (size_t j = 0; j < _joinColumns.size(); j++) {
      if (_joinColumns[j][1] == i) {
        isJcl = true;
        break;
      }
    }
    if (isJcl) {
      continue;
    }
    float mult = _right->getMultiplicity(i) * (multResult / multRight);
    _multiplicities.push_back(mult);
  }
  _multiplicitiesComputed = true;
}

// _______________________________________________________________________
void MultiColumnJoin::computeMultiColumnJoin(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
    IdTable* result) {
  // check for trivial cases
  if (left.empty() || right.empty()) {
    return;
  }

  ad_utility::JoinColumnMapping joinColumnData{joinColumns, left.numColumns(),
                                               right.numColumns()};

  IdTableView<0> leftJoinColumns =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> rightJoinColumns =
      right.asColumnSubsetView(joinColumnData.jcsRight());

  auto leftPermuted = left.asColumnSubsetView(joinColumnData.permutationLeft());
  auto rightPermuted =
      right.asColumnSubsetView(joinColumnData.permutationRight());

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      joinColumns.size(), leftPermuted, rightPermuted, std::move(*result),
      cancellationHandle_);
  auto addRow = [&rowAdder, beginLeft = leftJoinColumns.begin(),
                 beginRight = rightJoinColumns.begin()](const auto& itLeft,
                                                        const auto& itRight) {
    rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
  };

  // Compute `isCheap`, which is true iff there are no UNDEF values in the join
  // columns (in which case we can use a simpler and cheaper join algorithm).
  //
  // TODO<joka921> This is the most common case. There are many other cases
  // where the generic `zipperJoinWithUndef` can be optimized. We will those
  // for a later PR.
  bool isCheap = ql::ranges::none_of(joinColumns, [&](const auto& jcs) {
    auto [leftCol, rightCol] = jcs;
    return (ql::ranges::any_of(right.getColumn(rightCol), &Id::isUndefined)) ||
           (ql::ranges::any_of(left.getColumn(leftCol), &Id::isUndefined));
  });

  auto checkCancellationLambda = [this] { checkCancellation(); };

  const size_t numOutOfOrder = [&]() {
    if (isCheap) {
      return ad_utility::zipperJoinWithUndef(
          leftJoinColumns, rightJoinColumns,
          ql::ranges::lexicographical_compare, addRow, ad_utility::noop,
          ad_utility::noop, ad_utility::noop, checkCancellationLambda);
    } else {
      return ad_utility::zipperJoinWithUndef(
          leftJoinColumns, rightJoinColumns,
          ql::ranges::lexicographical_compare, addRow,
          ad_utility::findSmallerUndefRanges,
          ad_utility::findSmallerUndefRanges, ad_utility::noop,
          checkCancellationLambda);
    }
  }();
  *result = std::move(rowAdder).resultTable();
  // If there were UNDEF values in the input, the result might be out of
  // order. Sort it, because this operation promises a sorted result in its
  // `resultSortedOn()` member function.
  // TODO<joka921> We only have to do this if the sorting is required (merge the
  // other PR first).
  if (numOutOfOrder > 0) {
    std::vector<ColumnIndex> cols;
    for (size_t i = 0; i < joinColumns.size(); ++i) {
      cols.push_back(i);
    }
    checkCancellation();
    Engine::sort(*result, cols);
  }

  // The result that `zipperJoinWithUndef` produces has a different order of
  // columns than expected, permute them. See the documentation of
  // `JoinColumnMapping` for details.
  result->setColumnSubset(joinColumnData.permutationResult());
  checkCancellation();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> MultiColumnJoin::cloneImpl() const {
  auto copy = std::make_unique<MultiColumnJoin>(*this);
  copy->_left = _left->clone();
  copy->_right = _right->clone();
  return copy;
}

// _____________________________________________________________________________
bool MultiColumnJoin::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // For the join columns we don't union the elements, we intersect them so we
  // can have a more efficient implementation.
  if (_left->getVariableColumnOrNullopt(variable).has_value() &&
      _right->getVariableColumnOrNullopt(variable).has_value()) {
    using namespace qlever::joinHelpers;
    return doesJoinProduceGuaranteedGraphValuesOrUndef(_left, _right, variable);
  }
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}
