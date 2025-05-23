// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "engine/OptionalJoin.h"

#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/JoinHelpers.h"
#include "engine/Service.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using namespace qlever::joinHelpers;

using std::endl;
using std::string;

// _____________________________________________________________________________
OptionalJoin::OptionalJoin(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> t1,
                           std::shared_ptr<QueryExecutionTree> t2)
    : Operation(qec),
      _left{std::move(t1)},
      _right{std::move(t2)},
      _joinColumns(QueryExecutionTree::getJoinColumns(*_left, *_right)) {
  AD_CORRECTNESS_CHECK(!_joinColumns.empty());

  // If `_right` contains no UNDEF in the join columns and at most one column in
  // `_left` contains UNDEF values, and that column is the last join column,
  // then a cheaper implementation can be used. The following code determines
  // whether only one join column in `_left` can contain UNDEF values and makes
  // this join column the last one.
  bool rightHasUndefColumn = false;
  size_t numUndefColumnsLeft = 0;
  ColumnIndex undefColumnLeftIndex = 0;
  std::vector<bool> leftUndefJoinCols;
  for (size_t i = 0; i < _joinColumns.size(); ++i) {
    auto [leftCol, rightCol] = _joinColumns.at(i);
    auto leftIt = _left->getVariableAndInfoByColumnIndex(leftCol);
    auto rightIt = _right->getVariableAndInfoByColumnIndex(rightCol);
    if (leftIt.second.mightContainUndef_ ==
        ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined) {
      ++numUndefColumnsLeft;
      undefColumnLeftIndex = i;
    }
    if (rightIt.second.mightContainUndef_ ==
        ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined) {
      rightHasUndefColumn = true;
    }
  }
  if (!rightHasUndefColumn && numUndefColumnsLeft == 0) {
    implementation_ = Implementation::NoUndef;
  } else if (!rightHasUndefColumn && numUndefColumnsLeft == 1) {
    std::swap(_joinColumns.at(undefColumnLeftIndex), _joinColumns.back());
    implementation_ = Implementation::OnlyUndefInLastJoinColumnOfLeft;
  }

  // The inputs must be sorted by the join columns.
  std::tie(_left, _right) = QueryExecutionTree::createSortedTrees(
      std::move(_left), std::move(_right), _joinColumns);
}

// _____________________________________________________________________________
string OptionalJoin::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "OPTIONAL_JOIN\n" << _left->getCacheKey() << " ";
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
string OptionalJoin::getDescriptor() const {
  std::string joinVars;
  for (auto [leftCol, rightCol] : _joinColumns) {
    (void)rightCol;
    const std::string& varName =
        _left->getVariableAndInfoByColumnIndex(leftCol).first.name();
    joinVars += varName + " ";
  }
  return "OptionalJoin on " + joinVars;
}

// _____________________________________________________________________________
Result OptionalJoin::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "OptionalJoin result computation..." << endl;

  // If the right of the RootOperations is a Service, precompute the result of
  // its sibling.
  Service::precomputeSiblingResult(_left->getRootOperation(),
                                   _right->getRootOperation(), true,
                                   requestLaziness);

  IdTable idTable{getResultWidth(), getExecutionContext()->getAllocator()};

  AD_CONTRACT_CHECK(idTable.numColumns() >= _joinColumns.size());
  // The optional join implementation does only work if there's just a single
  // join column. This might be extended in the future.
  bool lazyJoinIsSupported = _joinColumns.size() == 1;

  auto leftResult = _left->getResult(lazyJoinIsSupported);
  auto rightResult = _right->getResult(lazyJoinIsSupported);

  checkCancellation();

  LOG(DEBUG) << "OptionalJoin subresult computation done." << std::endl;

  if (!leftResult->isFullyMaterialized() ||
      !rightResult->isFullyMaterialized()) {
    return lazyOptionalJoin(std::move(leftResult), std::move(rightResult),
                            requestLaziness);
  }

  LOG(DEBUG) << "Computing optional join between results of size "
             << leftResult->idTable().size() << " and "
             << rightResult->idTable().size() << endl;

  optionalJoin(leftResult->idTable(), rightResult->idTable(), _joinColumns,
               &idTable, implementation_);

  checkCancellation();

  LOG(DEBUG) << "OptionalJoin result computation done." << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*leftResult, *rightResult)};
}

// _____________________________________________________________________________
VariableToColumnMap OptionalJoin::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      _left->getVariableColumns(), _right->getVariableColumns(), _joinColumns,
      BinOpType::OptionalJoin, _left->getResultWidth());
}

// _____________________________________________________________________________
size_t OptionalJoin::getResultWidth() const {
  size_t res =
      _left->getResultWidth() + _right->getResultWidth() - _joinColumns.size();
  AD_CONTRACT_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
vector<ColumnIndex> OptionalJoin::resultSortedOn() const {
  std::vector<ColumnIndex> sortedOn;
  // The result is sorted on all join columns from the left subtree.
  for (const auto& [joinColumnLeft, joinColumnRight] : _joinColumns) {
    (void)joinColumnRight;
    sortedOn.push_back(joinColumnLeft);
  }
  return sortedOn;
}

// _____________________________________________________________________________
float OptionalJoin::getMultiplicity(size_t col) {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _multiplicities[col];
}

// _____________________________________________________________________________
uint64_t OptionalJoin::getSizeEstimateBeforeLimit() {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t OptionalJoin::getCostEstimate() {
  if (!_costEstimate.has_value()) {
    size_t costEstimate = getSizeEstimateBeforeLimit() +
                          _left->getSizeEstimate() + _right->getSizeEstimate();
    // The optional join is about 3-7 times slower than a normal join, due to
    // its increased complexity
    costEstimate *= 4;
    // Make the join 7% more expensive per join column
    costEstimate *= (1 + (_joinColumns.size() - 1) * 0.07);

    _costEstimate =
        _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
  }
  return _costEstimate.value();
}

// _____________________________________________________________________________
void OptionalJoin::computeSizeEstimateAndMultiplicities() {
  // The number of distinct entries in the result is at most the minimum of
  // the numbers of distinc entries in all join columns.
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
  // The number of distinct is at least the number of distinct in a non optional
  // column, if the other one is optional.
  numDistinctRight = std::max(numDistinctRight, numDistinctResult);

  // compute an estimate for the results multiplicity
  float multLeft = std::numeric_limits<float>::max();
  float multRight = std::numeric_limits<float>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    multLeft = std::min(multLeft, _left->getMultiplicity(_joinColumns[i][0]));
    multRight =
        std::min(multRight, _right->getMultiplicity(_joinColumns[i][1]));
  }
  float multResult = multLeft * multRight;

  _sizeEstimate = _left->getSizeEstimate() * multResult;

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

// ______________________________________________________________
auto OptionalJoin::computeImplementationFromIdTables(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns)
    -> Implementation {
  auto implementation = Implementation::NoUndef;
  auto anyIsUndefined = [](auto column) {
    return ql::ranges::any_of(column, &Id::isUndefined);
  };
  for (size_t i = 0; i < joinColumns.size(); ++i) {
    auto [leftCol, rightCol] = joinColumns.at(i);
    if (anyIsUndefined(right.getColumn(rightCol))) {
      return Implementation::GeneralCase;
    }
    if (anyIsUndefined(left.getColumn(leftCol))) {
      if (i == joinColumns.size() - 1) {
        implementation = Implementation::OnlyUndefInLastJoinColumnOfLeft;
      } else {
        return Implementation::GeneralCase;
      }
    }
  }
  return implementation;
}

// ______________________________________________________________
void OptionalJoin::optionalJoin(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns, IdTable* result,
    Implementation implementation) {
  // check for trivial cases
  if (left.empty()) {
    return;
  }

  // If we cannot determine statically whether a cheaper implementation can
  // be chosen, we try to determine this dynamically by checking all the join
  // columns for UNDEF values.
  if (implementation == Implementation::GeneralCase) {
    implementation =
        computeImplementationFromIdTables(left, right, joinColumns);
  }

  ad_utility::JoinColumnMapping joinColumnData{joinColumns, left.numColumns(),
                                               right.numColumns()};

  IdTableView<0> joinColumnsLeft =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> joinColumnsRight =
      right.asColumnSubsetView(joinColumnData.jcsRight());

  checkCancellation();

  auto leftPermuted = left.asColumnSubsetView(joinColumnData.permutationLeft());
  auto rightPermuted =
      right.asColumnSubsetView(joinColumnData.permutationRight());

  auto lessThanBoth = ql::ranges::lexicographical_compare;

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      joinColumns.size(), leftPermuted, rightPermuted, std::move(*result),
      cancellationHandle_);
  auto addRow = [&rowAdder, beginLeft = joinColumnsLeft.begin(),
                 beginRight = joinColumnsRight.begin()](const auto& itLeft,
                                                        const auto& itRight) {
    rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
  };

  auto addOptionalRow = [&rowAdder,
                         begin = joinColumnsLeft.begin()](const auto& itLeft) {
    rowAdder.addOptionalRow(itLeft - begin);
  };

  auto findUndefDispatch = [](const auto& row, auto begin, auto end,
                              bool& outOfOrder) {
    return ad_utility::findSmallerUndefRanges(row, begin, end, outOfOrder);
  };

  const size_t numOutOfOrder = [&]() {
    auto checkCancellationLambda = [this] { checkCancellation(); };
    if (implementation == Implementation::OnlyUndefInLastJoinColumnOfLeft) {
      ad_utility::specialOptionalJoin(joinColumnsLeft, joinColumnsRight, addRow,
                                      addOptionalRow, checkCancellationLambda);
      return 0UL;
    } else if (implementation == Implementation::NoUndef) {
      if (right.size() / left.size() > GALLOP_THRESHOLD) {
        ad_utility::gallopingJoin(joinColumnsLeft, joinColumnsRight,
                                  lessThanBoth, addRow, addOptionalRow,
                                  checkCancellationLambda);
      } else {
        auto shouldBeZero = ad_utility::zipperJoinWithUndef(
            joinColumnsLeft, joinColumnsRight, lessThanBoth, addRow,
            ad_utility::noop, ad_utility::noop, addOptionalRow,
            checkCancellationLambda);
        AD_CORRECTNESS_CHECK(shouldBeZero == 0UL);
      }
      return 0UL;
    } else {
      return ad_utility::zipperJoinWithUndef(
          joinColumnsLeft, joinColumnsRight, lessThanBoth, addRow,
          findUndefDispatch, findUndefDispatch, addOptionalRow,
          checkCancellationLambda);
    }
  }();
  // The column order in the result is now
  // [joinColumns, non-join-columns-a, non-join-columns-b] (which makes the
  // algorithms above easier), be the order that is expected by the rest of the
  // code is [columns-a, non-join-columns-b]. Permute the columns to fix the
  // order.
  *result = std::move(rowAdder).resultTable();

  // TODO<joka921> We have two sorted ranges, a simple merge would suffice
  // (possibly even in-place), or we could even lazily pass them on to the
  // upstream operation.
  // Note: the merging only works if we don't have the arbitrary out of order
  // case.
  // TODO<joka921> We only have to do this if the sorting is required.
  if (numOutOfOrder > 0) {
    std::vector<ColumnIndex> cols;
    for (size_t i = 0; i < joinColumns.size(); ++i) {
      cols.push_back(i);
    }
    checkCancellation();
    Engine::sort(*result, cols);
  }
  result->setColumnSubset(joinColumnData.permutationResult());
  checkCancellation();
}

// _____________________________________________________________________________
Result OptionalJoin::lazyOptionalJoin(std::shared_ptr<const Result> left,
                                      std::shared_ptr<const Result> right,
                                      bool requestLaziness) {
  // If both inputs are fully materialized, we can join them more
  // efficiently.
  AD_CONTRACT_CHECK(!left->isFullyMaterialized() ||
                    !right->isFullyMaterialized());
  // Currently only supports a single join column.
  AD_CORRECTNESS_CHECK(_joinColumns.size() == 1);
  ad_utility::JoinColumnMapping joinColMap{
      _joinColumns, _left->getResultWidth(), _right->getResultWidth()};

  auto resultPermutation = joinColMap.permutationResult();

  auto action = [this, left = std::move(left), right = std::move(right),
                 joinColMap = std::move(joinColMap)](
                    std::function<void(IdTable&, LocalVocab&)> yieldTable) {
    ad_utility::AddCombinedRowToIdTable rowAdder{
        _joinColumns.size(), IdTable{getResultWidth(), allocator()},
        cancellationHandle_, CHUNK_SIZE, std::move(yieldTable)};
    auto leftRange = resultToView(*left, joinColMap.permutationLeft());
    auto rightRange = resultToView(*right, joinColMap.permutationRight());
    std::visit(
        [&rowAdder](auto& leftBlocks, auto& rightBlocks) {
          ad_utility::zipperJoinForBlocksWithPotentialUndef(
              leftBlocks, rightBlocks, std::less{}, rowAdder, {}, {},
              std::true_type{});
        },
        leftRange, rightRange);
    auto localVocab = std::move(rowAdder.localVocab());
    return Result::IdTableVocabPair{std::move(rowAdder).resultTable(),
                                    std::move(localVocab)};
  };

  if (requestLaziness) {
    return {runLazyJoinAndConvertToGenerator(std::move(action),
                                             std::move(resultPermutation)),
            resultSortedOn()};
  } else {
    auto [idTable, localVocab] = action(ad_utility::noop);
    applyPermutation(idTable, resultPermutation);
    return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
  }
}

// _____________________________________________________________________________
std::unique_ptr<Operation> OptionalJoin::cloneImpl() const {
  auto copy = std::make_unique<OptionalJoin>(*this);
  copy->_left = _left->clone();
  copy->_right = _right->clone();
  return copy;
}
