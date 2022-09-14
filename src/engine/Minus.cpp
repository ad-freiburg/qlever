// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "Minus.h"

#include "../util/Exception.h"
#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
Minus::Minus(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> left,
             std::shared_ptr<QueryExecutionTree> right,
             std::vector<array<size_t, 2>> matchedColumns)
    : Operation{qec},
      _left{std::move(left)},
      _right{std::move(right)},
      _matchedColumns{std::move(matchedColumns)} {
  // Check that the invariant (inputs are sorted on the matched columns) holds.
  auto l = _left->resultSortedOn();
  auto r = _right->resultSortedOn();
  AD_CHECK(_matchedColumns.size() <= l.size());
  AD_CHECK(_matchedColumns.size() <= r.size());
  for (size_t i = 0; i < _matchedColumns.size(); ++i) {
    AD_CHECK(_matchedColumns[i][0] == l[i]);
    AD_CHECK(_matchedColumns[i][1] == r[i]);
  }
}

// _____________________________________________________________________________
string Minus::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "MINUS\n" << _left->asString(indent) << "\n";
  os << _right->asString(indent) << " ";
  return std::move(os).str();
}

// _____________________________________________________________________________
string Minus::getDescriptor() const { return "Minus"; }

// _____________________________________________________________________________
void Minus::computeResult(ResultTable* result) {
  AD_CHECK(result);
  LOG(DEBUG) << "Minus result computation..." << endl;

  result->_sortedBy = resultSortedOn();
  result->_idTable.setCols(getResultWidth());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  LOG(DEBUG) << "Minus subresult computation done." << std::endl;

  // We have the same output columns as the left input, so we also
  // have the same output column types.
  result->_resultTypes = leftResult->_resultTypes;

  LOG(DEBUG) << "Computing minus of results of size " << leftResult->size()
             << " and " << rightResult->size() << endl;

  int leftWidth = leftResult->_idTable.cols();
  int rightWidth = rightResult->_idTable.cols();
  CALL_FIXED_SIZE_2(leftWidth, rightWidth, computeMinus, leftResult->_idTable,
                    rightResult->_idTable, _matchedColumns, &result->_idTable);
  LOG(DEBUG) << "Minus result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> Minus::getVariableColumns() const {
  return _left->getVariableColumns();
}

// _____________________________________________________________________________
size_t Minus::getResultWidth() const { return _left->getResultWidth(); }

// _____________________________________________________________________________
vector<size_t> Minus::resultSortedOn() const { return _left->resultSortedOn(); }

// _____________________________________________________________________________
float Minus::getMultiplicity(size_t col) {
  // This is an upper bound on the multiplicity as an arbitrary number
  // of rows might be deleted in this operation.
  return _left->getMultiplicity(col);
}

// _____________________________________________________________________________
size_t Minus::getSizeEstimate() {
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
void Minus::computeMinus(const IdTable& dynA, const IdTable& dynB,
                         const vector<array<ColumnIndex, 2>>& joinColumns,
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
    *dynResult = dynA;
    return;
  }

  IdTableView<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableView<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();

  std::vector<size_t> rightToLeftCols(b.cols(),
                                      std::numeric_limits<size_t>::max());
  for (const auto& jc : joinColumns) {
    rightToLeftCols[jc[1]] = jc[0];
  }

  /**
   * @brief A function to copy a row from a to the end of result.
   * @param ia The index of the row in a.
   */
  auto writeResult = [&result, &a](size_t ia) {
    result.template push_back(a[ia]);
  };

  auto checkTimeout = checkTimeoutAfterNCallsFactory();

  size_t ia = 0, ib = 0;
  while (ia < a.size() && ib < b.size()) {
    // Join columns 0 are the primary sort columns
    while (a(ia, joinColumns[0][0]) < b(ib, joinColumns[0][1])) {
      // Write a result
      writeResult(ia);
      ia++;
      checkTimeout();
      if (ia >= a.size()) {
        goto finish;
      }
    }
    while (b(ib, joinColumns[0][1]) < a(ia, joinColumns[0][0])) {
      ib++;
      checkTimeout();
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
      checkTimeout();
    }
  }
finish:
  result.reserve(result.size() + (a.size() - ia));
  while (ia < a.size()) {
    writeResult(ia);
    ia++;
  }
  *dynResult = result.moveToDynamic();
}

template <int A_WIDTH, int B_WIDTH>
Minus::RowComparison Minus::isRowEqSkipFirst(
    const IdTableView<A_WIDTH>& a, const IdTableView<B_WIDTH>& b, size_t ia,
    size_t ib, const vector<array<size_t, 2>>& joinColumns) {
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
