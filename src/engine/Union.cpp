// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#include "Union.h"
#include "CallFixedSize.h"

const size_t Union::NO_COLUMN = std::numeric_limits<size_t>::max();

Union::Union(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
             std::shared_ptr<QueryExecutionTree> t2)
    : Operation(qec) {
  _subtrees[0] = t1;
  _subtrees[1] = t2;

  // compute the column origins
  ad_utility::HashMap<string, size_t> variableColumns = getVariableColumns();
  _columnOrigins.resize(variableColumns.size(), {NO_COLUMN, NO_COLUMN});
  const auto& t1VarCols = t1->getVariableColumns();
  const auto& t2VarCols = t2->getVariableColumns();
  for (auto it : variableColumns) {
    // look for the corresponding column in t1
    auto it1 = t1VarCols.find(it.first);
    if (it1 != t1VarCols.end()) {
      _columnOrigins[it.second][0] = it1->second;
    } else {
      _columnOrigins[it.second][0] = NO_COLUMN;
    }

    // look for the corresponding column in t2
    const auto it2 = t2VarCols.find(it.first);
    if (it2 != t2VarCols.end()) {
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

string Union::getDescriptor() const { return "Union"; }

size_t Union::getResultWidth() const {
  // The width depends on the number of unique variables (as the columns of
  // two variables from the left and right subtree will end up in the same
  // result column if they have the same name).
  return _columnOrigins.size();
}

vector<size_t> Union::resultSortedOn() const { return {}; }

ad_utility::HashMap<string, size_t> Union::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> variableColumns(
      _subtrees[0]->getVariableColumns());

  size_t column = variableColumns.size();
  for (auto it : _subtrees[1]->getVariableColumns()) {
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
  result->_data.setCols(getResultWidth());
  int leftWidth = subRes1->_data.cols();
  int rightWidth = subRes2->_data.cols();
  int outWidth = result->_data.cols();
  CALL_FIXED_SIZE_3(leftWidth, rightWidth, outWidth, computeUnion,
                    &result->_data, subRes1->_data, subRes2->_data,
                    _columnOrigins);

  LOG(DEBUG) << "Union result computation done." << std::endl;
}

template <int LEFT_WIDTH, int RIGHT_WIDTH, int OUT_WIDTH>
void Union::computeUnion(
    IdTable* dynRes, const IdTable& dynLeft, const IdTable& dynRight,
    const std::vector<std::array<size_t, 2>>& columnOrigins) {
  const IdTableStatic<LEFT_WIDTH> left = dynLeft.asStaticView<LEFT_WIDTH>();
  const IdTableStatic<RIGHT_WIDTH> right = dynRight.asStaticView<RIGHT_WIDTH>();
  IdTableStatic<OUT_WIDTH> res = dynRes->moveToStatic<OUT_WIDTH>();

  res.reserve(left.size() + right.size());
  if (left.size() > 0) {
    bool columnsMatch = left.cols() == columnOrigins.size();
    // check if the order of the columns matches
    for (size_t i = 0; columnsMatch && i < columnOrigins.size(); i++) {
      const std::array<size_t, 2>& co = columnOrigins[i];
      if (co[0] != i) {
        columnsMatch = false;
      }
    }
    if (columnsMatch) {
      // Left and right have the same columns, we can simply copy the entries.
      // As the variableColumnMap of left was simply copied over to create
      // this operations variableColumnMap the order of the columns will
      // be the same.
      // This if clause is only here to avoid creating the call to insert when
      // it would not be possible to call the function due to not matching
      // columns.
      if constexpr (LEFT_WIDTH == OUT_WIDTH) {
        res.insert(res.end(), left.begin(), left.end());
      }
    } else {
      for (const auto& l : left) {
        res.emplace_back();
        size_t backIdx = res.size() - 1;
        for (size_t i = 0; i < columnOrigins.size(); i++) {
          const std::array<size_t, 2>& co = columnOrigins[i];
          if (co[0] != Union::NO_COLUMN) {
            res(backIdx, i) = l[co[0]];
          } else {
            res(backIdx, i) = ID_NO_VALUE;
          }
        }
      }
    }
  }

  if (right.size() > 0) {
    bool columnsMatch = right.cols() == columnOrigins.size();
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
      if constexpr (RIGHT_WIDTH == OUT_WIDTH) {
        res.insert(res.end(), right.begin(), right.end());
      }
    } else {
      for (const auto& r : right) {
        res.emplace_back();
        size_t backIdx = res.size() - 1;
        for (size_t i = 0; i < columnOrigins.size(); i++) {
          const std::array<size_t, 2>& co = columnOrigins[i];
          if (co[1] != Union::NO_COLUMN) {
            res(backIdx, i) = r[co[1]];
          } else {
            res(backIdx, i) = ID_NO_VALUE;
          }
        }
      }
    }
  }
  *dynRes = res.moveToDynamic();
}
