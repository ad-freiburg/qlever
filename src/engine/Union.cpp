// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#include "Union.h"

const size_t Union::NO_COLUMN = std::numeric_limits<size_t>::max();

Union::Union(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
             std::shared_ptr<QueryExecutionTree> t2)
    : Operation(qec) {
  _subtrees[0] = t1;
  _subtrees[1] = t2;

  // compute the column origins
  ad_utility::HashMap<string, size_t> variableColumns = getVariableColumns();
  _columnOrigins.resize(variableColumns.size(), {NO_COLUMN, NO_COLUMN});
  for (auto it : variableColumns) {
    // look for the corresponding column in t1
    auto it1 = t1->getVariableColumnMap().find(it.first);
    if (it1 != t1->getVariableColumnMap().end()) {
      _columnOrigins[it.second][0] = it1->second;
    } else {
      _columnOrigins[it.second][0] = NO_COLUMN;
    }

    // look for the corresponding column in t2
    auto it2 = t2->getVariableColumnMap().find(it.first);
    if (it2 != t2->getVariableColumnMap().end()) {
      _columnOrigins[it.second][1] = it2->second;
    } else {
      _columnOrigins[it.second][1] = NO_COLUMN;
    }
  }
}

string Union::asString(size_t indent) const {
  std::ostringstream os;
  os << _subtrees[0]->asString(indent) << "\n";
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "UNION\n";
  os << _subtrees[1]->asString(indent) << "\n";
  return os.str();
}

size_t Union::getResultWidth() const {
  // The width depends on the number of unique variables (as the columns of
  // two variables from the left and right subtree will end up in the same
  // result column if they have the same name).
  return _columnOrigins.size();
}

vector<size_t> Union::resultSortedOn() const { return {}; }

ad_utility::HashMap<string, size_t> Union::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> variableColumns(
      _subtrees[0]->getVariableColumnMap());

  size_t column = variableColumns.size();
  for (auto it : _subtrees[1]->getVariableColumnMap()) {
    if (variableColumns.find(it.first) == variableColumns.end()) {
      variableColumns[it.first] = column;
      column++;
    }
  }
  return variableColumns;
}

void Union::setTextLimit(size_t limit) {
  _subtrees[0]->setTextLimit(limit);
  _subtrees[1]->setTextLimit(limit);
}

bool Union::knownEmptyResult() {
  return _subtrees[0]->knownEmptyResult() && _subtrees[1]->knownEmptyResult();
}

float Union::getMultiplicity(size_t col) {
  if (col >= _columnOrigins.size()) {
    return 1;
  }
  if (_columnOrigins[col][0] != NO_COLUMN &&
      _columnOrigins[col][1] != NO_COLUMN) {
    return (_subtrees[0]->getMultiplicity(_columnOrigins[col][0]) +
            _subtrees[1]->getMultiplicity(_columnOrigins[col][1])) /
           2;
  } else if (_columnOrigins[col][0] != NO_COLUMN) {
    // Compute the number of distinct elements in the input, add one new element
    // for the unbound variables, then divide it by the number of elements in
    // the result. This is slightly off if the subresult already contained
    // an unbound result, but the error is small in most common cases.
    double numDistinct = _subtrees[0]->getSizeEstimate() /
                         static_cast<double>(_subtrees[0]->getMultiplicity(
                             _columnOrigins[col][0]));
    numDistinct += 1;
    return getSizeEstimate() / numDistinct;
  } else if (_columnOrigins[col][1] != NO_COLUMN) {
    // Compute the number of distinct elements in the input, add one new element
    // for the unbound variables, then divide it by the number of elements in
    // the result. This is slightly off if the subresult already contained
    // an unbound result, but the error is small in most common cases.
    double numDistinct = _subtrees[1]->getSizeEstimate() /
                         static_cast<double>(_subtrees[1]->getMultiplicity(
                             _columnOrigins[col][1]));
    numDistinct += 1;
    return getSizeEstimate() / numDistinct;
  }
  return 1;
}

size_t Union::getSizeEstimate() {
  return _subtrees[0]->getSizeEstimate() + _subtrees[1]->getSizeEstimate();
}

size_t Union::getCostEstimate() {
  return _subtrees[0]->getCostEstimate() + _subtrees[1]->getCostEstimate() +
         getSizeEstimate();
}

void Union::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Union result computation..." << std::endl;
  shared_ptr<const ResultTable> subRes1 = _subtrees[0]->getResult();
  shared_ptr<const ResultTable> subRes2 = _subtrees[1]->getResult();
  LOG(DEBUG) << "Union subresult computation done." << std::endl;

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.setDescriptor("Union");
  runtimeInfo.addChild(_subtrees[0]->getRootOperation()->getRuntimeInfo());
  runtimeInfo.addChild(_subtrees[1]->getRootOperation()->getRuntimeInfo());

  result->_sortedBy = resultSortedOn();
  for (const std::array<size_t, 2>& o : _columnOrigins) {
    if (o[0] != NO_COLUMN) {
      result->_resultTypes.push_back(subRes1->getResultType(o[0]));
    } else if (o[1] != NO_COLUMN) {
      result->_resultTypes.push_back(subRes1->getResultType(o[1]));
    } else {
      result->_resultTypes.push_back(ResultTable::ResultType::KB);
    }
  }
  result->_nofColumns = getResultWidth();
  computeUnion(result, subRes1, subRes2, _columnOrigins);

  LOG(DEBUG) << "Union result computation done." << std::endl;
}

void Union::computeUnion(
    ResultTable* result, shared_ptr<const ResultTable> subRes1,
    shared_ptr<const ResultTable> subRes2,
    const std::vector<std::array<size_t, 2>>& columnOrigins) {
  if (result->_nofColumns == 1) {
    result->_fixedSizeData = new vector<array<Id, 1>>();
    computeUnion(static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData),
                 subRes1, subRes2, columnOrigins);
  } else if (result->_nofColumns == 2) {
    result->_fixedSizeData = new vector<array<Id, 2>>();
    computeUnion(static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
                 subRes1, subRes2, columnOrigins);
  } else if (result->_nofColumns == 3) {
    result->_fixedSizeData = new vector<array<Id, 3>>();
    computeUnion(static_cast<vector<array<Id, 3>>*>(result->_fixedSizeData),
                 subRes1, subRes2, columnOrigins);
  } else if (result->_nofColumns == 4) {
    result->_fixedSizeData = new vector<array<Id, 4>>();
    computeUnion(static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData),
                 subRes1, subRes2, columnOrigins);
  } else if (result->_nofColumns == 5) {
    result->_fixedSizeData = new vector<array<Id, 5>>();
    computeUnion(static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData),
                 subRes1, subRes2, columnOrigins);
  } else {
    computeUnion(&result->_varSizeData, subRes1, subRes2, columnOrigins);
  }
}

template <typename Res>
void Union::computeUnion(
    vector<Res>* res, shared_ptr<const ResultTable> subRes1,
    shared_ptr<const ResultTable> subRes2,
    const std::vector<std::array<size_t, 2>>& columnOrigins) {
  if (subRes1->_nofColumns == 1) {
    computeUnion(res,
                 static_cast<vector<array<Id, 1>>*>(subRes1->_fixedSizeData),
                 subRes2, columnOrigins);
  } else if (subRes1->_nofColumns == 2) {
    computeUnion(res,
                 static_cast<vector<array<Id, 2>>*>(subRes1->_fixedSizeData),
                 subRes2, columnOrigins);
  } else if (subRes1->_nofColumns == 3) {
    computeUnion(res,
                 static_cast<vector<array<Id, 3>>*>(subRes1->_fixedSizeData),
                 subRes2, columnOrigins);
  } else if (subRes1->_nofColumns == 4) {
    computeUnion(res,
                 static_cast<vector<array<Id, 4>>*>(subRes1->_fixedSizeData),
                 subRes2, columnOrigins);
  } else if (subRes1->_nofColumns == 5) {
    computeUnion(res,
                 static_cast<vector<array<Id, 5>>*>(subRes1->_fixedSizeData),
                 subRes2, columnOrigins);
  } else {
    computeUnion(res, &subRes1->_varSizeData, subRes2, columnOrigins);
  }
}

template <typename Res, typename L>
void Union::computeUnion(
    vector<Res>* res, const vector<L>* left,
    shared_ptr<const ResultTable> subRes2,
    const std::vector<std::array<size_t, 2>>& columnOrigins) {
  if (subRes2->_nofColumns == 1) {
    computeUnion(res, left,
                 static_cast<vector<array<Id, 1>>*>(subRes2->_fixedSizeData),
                 columnOrigins);
  } else if (subRes2->_nofColumns == 2) {
    computeUnion(res, left,
                 static_cast<vector<array<Id, 2>>*>(subRes2->_fixedSizeData),
                 columnOrigins);
  } else if (subRes2->_nofColumns == 3) {
    computeUnion(res, left,
                 static_cast<vector<array<Id, 3>>*>(subRes2->_fixedSizeData),
                 columnOrigins);
  } else if (subRes2->_nofColumns == 4) {
    computeUnion(res, left,
                 static_cast<vector<array<Id, 4>>*>(subRes2->_fixedSizeData),
                 columnOrigins);
  } else if (subRes2->_nofColumns == 5) {
    computeUnion(res, left,
                 static_cast<vector<array<Id, 5>>*>(subRes2->_fixedSizeData),
                 columnOrigins);
  } else {
    computeUnion(res, left, &subRes2->_varSizeData, columnOrigins);
  }
}

/**
 * @brief This struct creates a result row of the correct size
 */
template <typename R>
struct newResultRow {
  static R create(unsigned int resultSize) {
    (void)resultSize;
    return R();
  }
};

/**
 * @brief This struct creates a result row of the correct size, resizing
 *        the vector as requried.
 */
template <>
struct newResultRow<std::vector<Id>> {
  static std::vector<Id> create(unsigned int resultSize) {
    return vector<Id>(resultSize);
  }
};

template <typename Res, typename L, typename R>
void Union::computeUnion(
    vector<Res>* res, const vector<L>* left, const vector<R>* right,
    const std::vector<std::array<size_t, 2>>& columnOrigins) {
  res->reserve(left->size() + right->size());
  if (left->size() > 0) {
    if ((*left)[0].size() == columnOrigins.size()) {
      // Left and right have the same columns, we can simply copy the entries.
      // As the variableColumnMap of left was simply copied over to create
      // this operations variableColumnMap the order of the columns will
      // be the same.
      if constexpr (std::is_same<Res, L>::value) {
        // This is only true iff the columns match, but the compiler
        // would complain if Res != L.
        res->insert(res->end(), left->begin(), left->end());
      } else {
        // This should never occur
        AD_THROW(ad_semsearch::Exception::OTHER,
                 "Error in " __FILE__
                 ": Types Res and L differ but their size is the same.");
      }
    } else {
      for (const L& l : *left) {
        Res row = newResultRow<Res>::create(columnOrigins.size());
        for (size_t i = 0; i < columnOrigins.size(); i++) {
          const std::array<size_t, 2>& co = columnOrigins[i];
          if (co[0] != Union::NO_COLUMN) {
            row[i] = l[co[0]];
          } else {
            row[i] = ID_NO_VALUE;
          }
        }
        res->push_back(row);
      }
    }
  }

  if (right->size() > 0) {
    bool columnsMatch = (*right)[0].size() == columnOrigins.size();
    // check if the order of the columns matches
    for (size_t i = 0; columnsMatch && i < columnOrigins.size(); i++) {
      const std::array<size_t, 2>& co = columnOrigins[i];
      if (co[1] != i) {
        columnsMatch = false;
      }
    }
    if (columnsMatch) {
      // The columns of the right subtree and the result match, we can
      // just copy the entries.
      if constexpr (std::is_same<Res, R>::value) {
        // This is only true iff the columns match, but the compiler
        // would complain if Res != R.
        res->insert(res->end(), right->begin(), right->end());
      } else {
        // This should never occur
        AD_THROW(ad_semsearch::Exception::OTHER,
                 "Error in " __FILE__
                 ": Types Res and R differ but their size is the same.");
      }
    } else {
      for (const R& r : *right) {
        Res row = newResultRow<Res>::create(columnOrigins.size());
        for (size_t i = 0; i < columnOrigins.size(); i++) {
          const std::array<size_t, 2>& co = columnOrigins[i];
          if (co[1] != Union::NO_COLUMN) {
            row[i] = r[co[1]];
          } else {
            row[i] = ID_NO_VALUE;
          }
        }
        res->push_back(row);
      }
    }
  }
}
