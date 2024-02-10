// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "Minus.h"

#include "engine/CallFixedSize.h"
#include "util/Exception.h"

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
ResultTable Minus::computeResult() {
  LOG(DEBUG) << "Minus result computation..." << endl;

  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  LOG(DEBUG) << "Minus subresult computation done" << std::endl;

  LOG(DEBUG) << "Computing minus of results of size " << leftResult->size()
             << " and " << rightResult->size() << endl;

  int leftWidth = leftResult->idTable().numColumns();
  int rightWidth = rightResult->idTable().numColumns();
  CALL_FIXED_SIZE((std::array{leftWidth, rightWidth}), &Minus::computeMinus,
                  this, leftResult->idTable(), rightResult->idTable(),
                  _matchedColumns, &idTable);

  LOG(DEBUG) << "Minus result computation done" << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          ResultTable::getSharedLocalVocabFromNonEmptyOf(*leftResult,
                                                         *rightResult)};
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
template <int A_WIDTH, int B_WIDTH>
void Minus::computeMinus(
    const IdTable& dynA, const IdTable& dynB,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
    IdTable* dynResult) const {
  // Substract dynB from dynA. The result should be all result mappings mu
  // for which all result mappings mu' in dynB are not compatible (one value
  // for a variable defined in both differs) or the domain of mu and mu' are
  // disjoint (mu' defines no solution for any variables for which mu defines a
  // solution).

  // The output is always the same size as the left input
  constexpr int OUT_WIDTH = A_WIDTH;

  // check for trivial cases
  if (dynA.size() == 0) {
    return;
  }

  if (dynB.size() == 0 || joinColumns.empty()) {
    // B is the empty set of solution mappings, so the result is A
    // Copy a into the result, allowing for optimizations for small width by
    // using the templated width types.
    *dynResult = dynA.clone();
    return;
  }

  IdTableView<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableView<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = std::move(*dynResult).toStatic<OUT_WIDTH>();

  std::vector<size_t> rightToLeftCols(b.numColumns(),
                                      std::numeric_limits<size_t>::max());
  for (const auto& jc : joinColumns) {
    rightToLeftCols[jc[1]] = jc[0];
  }

  /**
   * @brief A function to copy a row from a to the end of result.
   * @param ia The index of the row in a.
   */
  auto writeResult = [&result, &a](size_t ia) { result.push_back(a[ia]); };

  size_t ia = 0, ib = 0;
  while (ia < a.size() && ib < b.size()) {
    // Join columns 0 are the primary sort columns
    while (a(ia, joinColumns[0][0]) < b(ib, joinColumns[0][1])) {
      // Write a result
      writeResult(ia);
      ia++;
      checkCancellation();
      if (ia >= a.size()) {
        goto finish;
      }
    }
    while (b(ib, joinColumns[0][1]) < a(ia, joinColumns[0][0])) {
      ib++;
      checkCancellation();
      if (ib >= b.size()) {
        goto finish;
      }
    }

    while (b(ib, joinColumns[0][1]) == a(ia, joinColumns[0][0])) {
      // check if the rest of the join columns also match
      RowComparison rowEq = isRowEqSkipFirst(a, b, ia, ib, joinColumns);
      switch (rowEq) {
        case RowComparison::EQUAL: {
          ia++;
          if (ia >= a.size()) {
            goto finish;
          }
        } break;
        case RowComparison::LEFT_SMALLER: {
          // ib does not discard ia, and there can not be another ib that
          // would discard ia.
          writeResult(ia);
          ia++;
          if (ia >= a.size()) {
            goto finish;
          }
        } break;
        case RowComparison::RIGHT_SMALLER: {
          ib++;
          if (ib >= b.size()) {
            goto finish;
          }
        } break;
        default:
          AD_FAIL();
      }
      checkCancellation();
    }
  }
finish:
  result.reserve(result.size() + (a.size() - ia));
  while (ia < a.size()) {
    writeResult(ia);
    ia++;
  }
  *dynResult = std::move(result).toDynamic();
}

template <int A_WIDTH, int B_WIDTH>
Minus::RowComparison Minus::isRowEqSkipFirst(
    const IdTableView<A_WIDTH>& a, const IdTableView<B_WIDTH>& b, size_t ia,
    size_t ib, const std::vector<std::array<ColumnIndex, 2>>& joinColumns) {
  for (size_t i = 1; i < joinColumns.size(); ++i) {
    Id va{a(ia, joinColumns[i][0])};
    Id vb{b(ib, joinColumns[i][1])};
    if (va < vb) {
      return RowComparison::LEFT_SMALLER;
    }
    if (va > vb) {
      return RowComparison::RIGHT_SMALLER;
    }
  }
  return RowComparison::EQUAL;
}
