// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "Minus.h"

#include <sparsehash/sparse_hash_set>

#include "../util/Exception.h"
#include "../util/HashSet.h"
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
template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void Minus::computeMinus(const IdTable& dynA, const IdTable& dynB,
                         const vector<array<Id, 2>>& joinColumns,
                         IdTable* dynResult) {
  // Substract dynB from dynA. The result should be all result mappings mu
  // for which all result mappings mu' in dynB are not compatible (one value
  // for a variable defined in both differs) or the domain of mu and mu' are
  // disjoint (mu' defines no solution for any variables for which mu defines a
  // solution).

  // check for trivial cases
  if (dynA.size() == 0) {
    return;
  }

  if (dynB.size() == 0) {
    // B is the empty set of solution mappings, so the result is A
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

  struct RowHash {
    size_t operator()(const std::vector<Id>& row) const {
      size_t hash = 0;
      for (size_t i = 0; i < row.size(); ++i) {
        hash ^= std::hash<Id>()(row[i]);
      }
      return 0;
    };
  };

  // Columns in a for which the matching column in b contains an entry with a
  // unbound variable
  ad_utility::HashSet<size_t> cols_containing_unbound;
  google::dense_hash_set<std::vector<Id>, RowHash> ids_to_remove(b.size());
  ids_to_remove.set_empty_key({});
  // Based upon the default deleted key for ids
  ids_to_remove.set_deleted_key({std::numeric_limits<Id>::max() - 1});
  std::vector<Id> entry_to_remove(joinColumns.size());
  for (size_t ib = 0; ib < b.size(); ib++) {
    for (size_t i = 0; i < joinColumns.size(); ++i) {
      Id id = b(ib, joinColumns[i][1]);
      if (id == ID_NO_VALUE) {
        cols_containing_unbound.insert(joinColumns[i][0]);
      }
      entry_to_remove[i] = id;
    }
    ids_to_remove.insert(entry_to_remove);
  }
  std::vector<size_t> v_cols_containing_unbound(cols_containing_unbound.begin(),
                                                cols_containing_unbound.end());

  // Build a list of bitmasks of combinations of join columns in a which contain
  // no value in b
  std::vector<uint64_t> no_value_masks;
  if (joinColumns.size() > 62) {
    AD_THROW(ad_semsearch::Exception::OTHER,
             "Minus only supports up to 62 columns in its input.");
  }
  // Iterate all possible states of columns in b being unbound
  for (uint64_t i = (1 << (cols_containing_unbound.size() + 1)); i > 0; i--) {
    // Build a bitmaks of columns in a which correspond to join colunmns in b
    // being unbound.
    uint64_t mask = ~uint64_t(0);
    for (size_t j = 0; j < cols_containing_unbound.size(); j++) {
      if (((i - 1) & (1 << j)) == 0) {
        // Set the bit for the column to 0
        mask &= ~(1 << v_cols_containing_unbound[j]);
      }
    }
    no_value_masks.push_back(mask);
  }
  if (no_value_masks.empty()) {
    no_value_masks.push_back(~uint64_t(0));
  }

  for (size_t ia = 0; ia < a.size(); ia++) {
    bool remove = false;
    // iterate every combination of ignoring variables
    for (uint64_t mask : no_value_masks) {
      for (size_t i = 0; i < joinColumns.size(); ++i) {
        if ((mask & (1 << joinColumns[i][0])) > 0) {
          entry_to_remove[i] = a(ia, joinColumns[i][0]);
        } else {
          entry_to_remove[i] = ID_NO_VALUE;
        }
      }
      if (ids_to_remove.count(entry_to_remove) != 0) {
        remove = true;
        break;
      }
    }
    if (!remove) {
      result.emplace_back();
      size_t backIdx = result.size() - 1;
      for (size_t col = 0; col < a.cols(); col++) {
        result(backIdx, col) = a(ia, col);
      }
    }
  }
  *dynResult = result.moveToDynamic();
}
