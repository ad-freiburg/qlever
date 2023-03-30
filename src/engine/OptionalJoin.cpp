// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "engine/OptionalJoin.h"

#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "util/JoinAlgorithms.h"

using std::string;

// _____________________________________________________________________________
OptionalJoin::OptionalJoin(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> t1,
                           std::shared_ptr<QueryExecutionTree> t2,
                           const std::vector<std::array<ColumnIndex, 2>>& jcs)
    : Operation(qec),
      _left{std::move(t1)},
      _right{std::move(t2)},
      _joinColumns(jcs),
      _multiplicitiesComputed(false) {
  AD_CONTRACT_CHECK(jcs.size() > 0);
}

// _____________________________________________________________________________
string OptionalJoin::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "OPTIONAL_JOIN\n" << _left->asString(indent) << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][0] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "|X|\n" << _right->asString(indent) << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][1] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]";
  return std::move(os).str();
}

// _____________________________________________________________________________
string OptionalJoin::getDescriptor() const {
  std::string joinVars = "";
  for (auto p : _left->getVariableColumns()) {
    for (auto jc : _joinColumns) {
      // If the left join column matches the index of a variable in the left
      // subresult.
      if (jc[0] == p.second) {
        joinVars += p.first.name() + " ";
      }
    }
  }
  return "OptionalJoin on " + joinVars;
}

// _____________________________________________________________________________
ResultTable OptionalJoin::computeResult() {
  LOG(DEBUG) << "OptionalJoin result computation..." << endl;

  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  AD_CONTRACT_CHECK(idTable.numColumns() >= _joinColumns.size());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  LOG(DEBUG) << "OptionalJoin subresult computation done." << std::endl;

  LOG(DEBUG) << "Computing optional join between results of size "
             << leftResult->size() << " and " << rightResult->size() << endl;

  int leftWidth = leftResult->idTable().numColumns();
  int rightWidth = rightResult->idTable().numColumns();
  int resWidth = idTable.numColumns();
  CALL_FIXED_SIZE((std::array{leftWidth, rightWidth, resWidth}),
                  &OptionalJoin::optionalJoin, leftResult->idTable(),
                  rightResult->idTable(), _joinColumns, &idTable);

  LOG(DEBUG) << "OptionalJoin result computation done." << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          ResultTable::getSharedLocalVocabFromNonEmptyOf(*leftResult,
                                                         *rightResult)};
}

// _____________________________________________________________________________
VariableToColumnMap OptionalJoin::computeVariableToColumnMap() const {
  VariableToColumnMap retVal(_left->getVariableColumns());
  size_t leftSize = _left->getResultWidth();
  for (const auto& [variable, columnIndexRight] :
       _right->getVariableColumns()) {
    size_t columnIndex = leftSize + columnIndexRight;
    bool isJoinColumn = false;
    // Reduce the index for every column of _right that is beeing joined on,
    // and the index of which is smaller than the index of it.
    for (const std::array<ColumnIndex, 2>& a : _joinColumns) {
      if (a[1] < columnIndexRight) {
        columnIndex--;
      } else if (a[1] == columnIndexRight) {
        isJoinColumn = true;
        break;
      }
    }
    if (!isJoinColumn) {
      retVal[variable] = columnIndex;
    }
  }
  return retVal;
}

// _____________________________________________________________________________
size_t OptionalJoin::getResultWidth() const {
  size_t res =
      _left->getResultWidth() + _right->getResultWidth() - _joinColumns.size();
  AD_CONTRACT_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
vector<size_t> OptionalJoin::resultSortedOn() const {
  std::vector<size_t> sortedOn;
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
size_t OptionalJoin::getSizeEstimate() {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t OptionalJoin::getCostEstimate() {
  size_t costEstimate =
      getSizeEstimate() + _left->getSizeEstimate() + _right->getSizeEstimate();
  // The optional join is about 3-7 times slower than a normal join, due to
  // its increased complexity
  costEstimate *= 4;
  // Make the join 7% more expensive per join column
  costEstimate *= (1 + (_joinColumns.size() - 1) * 0.07);
  return _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
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
  // The number of distinct is at leat the number of distinct in a non optional
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

template <int OUT_WIDTH>
void OptionalJoin::createOptionalResult(const auto& row,
                                        IdTableStatic<OUT_WIDTH>* res) {
  res->emplace_back();
  size_t rIdx = res->size() - 1;
  // Fill the columns of b with ID_NO_VALUE and the rest with a
  for (size_t col = 0; col < row.numColumns(); col++) {
    (*res)(rIdx, col) = row[col];
  }
  for (size_t col = row.numColumns(); col < res->numColumns(); col++) {
    (*res)(rIdx, col) = ID_NO_VALUE;
  }
}

// ______________________________________________________________
template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void OptionalJoin::optionalJoin(
    const IdTable& dynA, const IdTable& dynB,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
    IdTable* result) {
  // check for trivial cases
  if (dynA.empty()) {
    return;
  }
  IdTableView<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableView<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();

  auto joinColumnData = ad_utility::prepareJoinColumns(
      joinColumns, a.numColumns(), b.numColumns());

  auto dynASubset = dynA.asColumnSubsetView(joinColumnData.jcsA_);
  auto dynBSubset = dynB.asColumnSubsetView(joinColumnData.jcsB_);

  auto dynAPermuted = dynA.asColumnSubsetView(joinColumnData.colsCompleteA_);
  auto dynBPermuted = dynB.asColumnSubsetView(joinColumnData.colsCompleteB_);

  auto lessThanBoth = std::ranges::lexicographical_compare;

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      joinColumns.size(), dynAPermuted, dynBPermuted, result);
  auto addRow = [&rowAdder](const auto& rowA, const auto& rowB) {
    rowAdder.addRow(rowA.rowIndex(), rowB.rowIndex());
  };

  auto addOptionalRow = [&rowAdder](const auto& rowA) {
    rowAdder.addOptionalRow(rowA.rowIndex());
  };

  auto findUndefDispatch = [](const auto& row, auto begin, auto end,
                              bool& outOfOrder) {
    return ad_utility::findSmallerUndefRanges(row, begin, end, outOfOrder);
  };
  auto numOutOfOrder = ad_utility::zipperJoinWithUndef(
      dynASubset, dynBSubset, lessThanBoth, addRow, findUndefDispatch,
      findUndefDispatch, addOptionalRow);

  // The column order in the result is now
  // [joinColumns, non-join-columns-a, non-join-columns-b] (which makes the
  // algorithms above easier), be the order that is expected by the rest of the
  // code is [columns-a, non-join-columns-b]. Permute the columns to fix the
  // order.
  rowAdder.flush();

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
    Engine::sort(*result, cols);
  }
  result->permuteColumns(joinColumnData.permutation_);
}
