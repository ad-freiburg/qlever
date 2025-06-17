// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "Minus.h"

#include "engine/CallFixedSize.h"
#include "engine/Service.h"
#include "util/Exception.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
Minus::Minus(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> left,
             std::shared_ptr<QueryExecutionTree> right)
    : Operation{qec} {
  std::tie(_left, _right, _matchedColumns) =
      QueryExecutionTree::getSortedSubtreesAndJoinColumns(std::move(left),
                                                          std::move(right));
}

// _____________________________________________________________________________
string Minus::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "MINUS\n" << _left->getCacheKey() << "\n";
  os << _right->getCacheKey() << " ";
  return std::move(os).str();
}

// _____________________________________________________________________________
string Minus::getDescriptor() const { return "Minus"; }

// _____________________________________________________________________________
Result Minus::computeResult([[maybe_unused]] bool requestLaziness) {
  LOG(DEBUG) << "Minus result computation..." << endl;

  // If the right of the RootOperations is a Service, precompute the result of
  // its sibling.
  Service::precomputeSiblingResult(_left->getRootOperation(),
                                   _right->getRootOperation(), true,
                                   requestLaziness);

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  LOG(DEBUG) << "Minus subresult computation done" << std::endl;

  LOG(DEBUG) << "Computing minus of results of size "
             << leftResult->idTable().size() << " and "
             << rightResult->idTable().size() << endl;

  IdTable idTable = computeMinus(leftResult->idTable(), rightResult->idTable(),
                                 _matchedColumns);

  LOG(DEBUG) << "Minus result computation done" << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          Result::getMergedLocalVocab(*leftResult, *rightResult)};
}

// _____________________________________________________________________________
VariableToColumnMap Minus::computeVariableToColumnMap() const {
  return _left->getVariableColumns();
}

// _____________________________________________________________________________
size_t Minus::getResultWidth() const { return _left->getResultWidth(); }

// _____________________________________________________________________________
vector<ColumnIndex> Minus::resultSortedOn() const {
  return _left->resultSortedOn();
}

// _____________________________________________________________________________
float Minus::getMultiplicity(size_t col) {
  // This is an upper bound on the multiplicity as an arbitrary number
  // of rows might be deleted in this operation.
  return _left->getMultiplicity(col);
}

// _____________________________________________________________________________
uint64_t Minus::getSizeEstimateBeforeLimit() {
  // This is an upper bound on the size as an arbitrary number
  // of rows might be deleted in this operation.
  return _left->getSizeEstimate();
}

// _____________________________________________________________________________
size_t Minus::getCostEstimate() {
  size_t costEstimate = _left->getSizeEstimate() + _right->getSizeEstimate();
  return _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
}

// _____________________________________________________________________________
IdTable Minus::computeMinus(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns) const {
  if (left.empty()) {
    return IdTable{getResultWidth(), getExecutionContext()->getAllocator()};
  }

  if (right.empty() || joinColumns.empty()) {
    return left.clone();
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

  std::vector<size_t> nonMatchingIndices;

  auto outOfOrder = ad_utility::zipperJoinWithUndef(
      joinColumnsLeft, joinColumnsRight, ql::ranges::lexicographical_compare,
      [](const auto&, const auto&) {}, ad_utility::findSmallerUndefRanges,
      ad_utility::findSmallerUndefRanges,
      [&nonMatchingIndices, &joinColumnsLeft](const auto& matching) {
        nonMatchingIndices.push_back(
            ql::ranges::distance(joinColumnsLeft.begin(), matching));
      });
  AD_CORRECTNESS_CHECK(outOfOrder == 0);

  IdTable result{getResultWidth(), getExecutionContext()->getAllocator()};
  AD_CORRECTNESS_CHECK(result.numColumns() == left.numColumns());
  result.resize(nonMatchingIndices.size());

  for (ColumnIndex col = 0; col < result.numColumns(); col++) {
    ql::ranges::transform(
        nonMatchingIndices, result.getColumn(col).begin(),
        [inputCol = left.getColumn(col)](size_t row) { return inputCol[row]; });
  }

  return result;
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Minus::cloneImpl() const {
  auto copy = std::make_unique<Minus>(*this);
  copy->_left = _left->clone();
  copy->_right = _right->clone();
  return copy;
}

// _____________________________________________________________________________
bool Minus::columnOriginatesFromGraphOrUndef(const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  return _left->getRootOperation()->columnOriginatesFromGraphOrUndef(variable);
}
