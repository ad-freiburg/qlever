// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "Minus.h"

#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
Minus::Minus(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> left,
             std::shared_ptr<QueryExecutionTree> right,
             const std::vector<array<size_t, 2>>& matchedColumns)
    : Operation(qec),
      _left(left),
      _right(right),
      _multiplicitiesComputed(false),
      _matchedColumns(matchedColumns) {}

// _____________________________________________________________________________
string Minus::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "MINUS\n" << _left->asString(indent) << "\n";
  os << _right->asString(indent) << " ";
  return os.str();
}

// _____________________________________________________________________________
string Minus::getDescriptor() const { return "Minus"; }

// _____________________________________________________________________________
void Minus::computeResult(ResultTable* result) {
  AD_CHECK(result);
  LOG(DEBUG) << "Minus result computation..." << endl;

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  result->_sortedBy = resultSortedOn();
  result->_data.setCols(getResultWidth());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  runtimeInfo.addChild(_left->getRootOperation()->getRuntimeInfo());
  runtimeInfo.addChild(_right->getRootOperation()->getRuntimeInfo());

  LOG(DEBUG) << "Minus subresult computation done." << std::endl;

  // compute the result types
  result->_resultTypes.reserve(result->_data.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftResult->_resultTypes.begin(),
                              leftResult->_resultTypes.end());

  LOG(DEBUG) << "Computing minus of results of size " << leftResult->size()
             << " and " << rightResult->size() << endl;

  int leftWidth = leftResult->_data.cols();
  int rightWidth = rightResult->_data.cols();
  int resWidth = result->_data.cols();
  CALL_FIXED_SIZE_3(leftWidth, rightWidth, resWidth, computeMinus,
                    leftResult->_data, rightResult->_data, _matchedColumns,
                    &result->_data);
  LOG(DEBUG) << "Minus result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> Minus::getVariableColumns() const {
  return _left->getVariableColumns();
}

// _____________________________________________________________________________
size_t Minus::getResultWidth() const { return _left->getResultWidth(); }

// _____________________________________________________________________________
vector<size_t> Minus::resultSortedOn() const {
  std::vector<size_t> sortedOn;
  // The result is sorted on all join columns from the left subtree.
  for (const auto& a : _matchedColumns) {
    sortedOn.push_back(a[0]);
  }
  return sortedOn;
}

// _____________________________________________________________________________
float Minus::getMultiplicity(size_t col) {
  // This is an upper bound on the multiplicity as an arbitrary number
  // of columns might be deleted in this operation.
  return _left->getMultiplicity(col);
}

// _____________________________________________________________________________
size_t Minus::getSizeEstimate() {
  // This is an upper bound on the multiplicity as an arbitrary number
  // of columns might be deleted in this operation.
  return _left->getSizeEstimate();
}

// _____________________________________________________________________________
size_t Minus::getCostEstimate() {
  size_t costEstimate = _left->getSizeEstimate() + _right->getSizeEstimate();
  return _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
}

// _____________________________________________________________________________
template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void Minus::computeMinus(const IdTable& dynA, const IdTable& dynB,
                         const vector<array<Id, 2>>& joinColumns,
                         IdTable* dynResult) {
  // Substract dynB from dynA. The result should be all result mappings mu
  // for which all result mappings mu' in dynB are not compatible (one value
  // differs) or the domain of mu and mu' are disjoint (mu' defines no
  // solution for any variables for which mu defines a solution).

  // check for trivial cases
  if (dynA.size() == 0) {
    return;
  }

  if (dynA.size() == 0) {
    if constexpr (A_WIDTH == OUT_WIDTH) {
      IdTableStatic<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
      IdTableStatic<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
      IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();
      result = a;
      *dynResult = result.moveToDynamic();
      return;
    } else {
      // This case should never happen.
      AD_CHECK(false);
    }
    *dynResult = dynA;
    return;
  }

  IdTableStatic<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableStatic<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();

  std::vector<size_t> rightToLeftCols(b.cols(),
                                      std::numeric_limits<size_t>::max());
  for (const auto& jc : joinColumns) {
    rightToLeftCols[jc[1]] = jc[0];
  }

  size_t ia = 0, ib = 0;
  while (ia < a.size() && ib < b.size()) {
    // Join columns 0 are the primary sort columns
    while (a(ia, joinColumns[0][0]) < b(ib, joinColumns[0][1])) {
      // Write a result
      result.emplace_back();
      size_t backIdx = result.size() - 1;
      for (size_t col = 0; col < a.cols(); col++) {
        result(backIdx, col) = a(ia, col);
      }

      ia++;
      if (ia >= a.size()) {
        goto finish;
      }
    }
    while (b(ib, joinColumns[0][1]) < a(ia, joinColumns[0][0])) {
      ib++;
      if (ib >= b.size()) {
        goto finish;
      }
    }

    // check if the rest of the join columns also match
    RowComparison rowEq = isRowEq(a, b, &ia, &ib, joinColumns);
    if (rowEq == RowComparison::DISJOINT_DOMAINS) {
      // dom(a) and dom(b) are disjoint. There might still be another entry in
      // b that discards a though.

      // Look for an ib that discards the current ia.
      size_t nia = ia;
      size_t nib = ib + 1;
      bool wasDiscarded = true;
      while (nib < b.size()) {
        RowComparison r = isRowEq(a, b, &nia, &nib, joinColumns);
        switch (r) {
          case RowComparison::NOT_EQUAL_LEFT_SMALLER:
            // An ib that discard the current ia does not exists
            wasDiscarded = false;
            break;
          case RowComparison::EQUAL:
            // An ib that discard the current ia exists
            break;
            break;
          case RowComparison::DISJOINT_DOMAINS:
            // The domains are still disjoint, keep looking
            nib++;
            break;
          case RowComparison::NOT_EQUAL_RIGHT_SMALLER:
            // keep looking for an ib
            break;
        }
      }
      if (!wasDiscarded) {
        result.emplace_back();
        size_t backIdx = result.size() - 1;
        for (size_t col = 0; col < a.cols(); col++) {
          result(backIdx, col) = a(ia, col);
        }
      } else {
        // ia was discarded. The current ib might not be disjoint with a later
        // ia though, so we can't advance ib.
      }
      // process the next ia
      ia++;
    } else if (rowEq == RowComparison::NOT_EQUAL_LEFT_SMALLER) {
      // ib does not discard ia, and there can not be another ib that
      // would discard ia.
      result.emplace_back();
      size_t backIdx = result.size() - 1;
      for (size_t col = 0; col < a.cols(); col++) {
        result(backIdx, col) = a(ia, col);
      }
    }
  }
finish:
  *dynResult = result.moveToDynamic();
}

template <int A_WIDTH, int B_WIDTH>
Minus::RowComparison Minus::isRowEq(
    const IdTableStatic<A_WIDTH>& a, const IdTableStatic<B_WIDTH>& b,
    size_t* ia, size_t* ib, const vector<array<size_t, 2>>& matchedColumns) {
  size_t numUndefined = 0;
  for (size_t i = 0; i < matchedColumns.size(); ++i) {
    const array<Id, 2>& joinColumn = matchedColumns[i];
    Id va = a(*ia, joinColumn[i]);
    Id vb = b(*ib, joinColumn[i]);
    if (vb == ID_NO_VALUE || va == ID_NO_VALUE) {
      // count the number of entries for which b is undefined.
      numUndefined++;
    } else {
      if (va < vb) {
        (*ia)++;
        return RowComparison::NOT_EQUAL_LEFT_SMALLER;
      }
      if (vb < va) {
        (*ib)++;
        return RowComparison::NOT_EQUAL_RIGHT_SMALLER;
      }
    }
  }
  if (numUndefined == matchedColumns.size()) {
    return RowComparison::DISJOINT_DOMAINS;
  }
  return RowComparison::EQUAL;
}
