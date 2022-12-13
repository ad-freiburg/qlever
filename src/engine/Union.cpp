// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018     Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2022-    Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#include "Union.h"

#include "engine/CallFixedSize.h"
#include "util/TransparentFunctors.h"

const size_t Union::NO_COLUMN = std::numeric_limits<size_t>::max();

Union::Union(QueryExecutionContext* qec,
             const std::shared_ptr<QueryExecutionTree>& t1,
             const std::shared_ptr<QueryExecutionTree>& t2)
    : Operation(qec) {
  AD_CHECK(t1 && t2);
  _subtrees[0] = t1;
  _subtrees[1] = t2;

  // compute the column origins
  VariableToColumnMap variableColumns = getInternallyVisibleVariableColumns();
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

string Union::asStringImpl(size_t indent) const {
  std::ostringstream os;
  os << _subtrees[0]->asString(indent) << "\n";
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "UNION\n";
  os << _subtrees[1]->asString(indent) << "\n";
  return std::move(os).str();
}

string Union::getDescriptor() const { return "Union"; }

size_t Union::getResultWidth() const {
  // The width depends on the number of unique variables (as the columns of
  // two variables from the left and right subtree will end up in the same
  // result column if they have the same name).
  return _columnOrigins.size();
}

vector<size_t> Union::resultSortedOn() const { return {}; }

// _____________________________________________________________________________
VariableToColumnMap Union::computeVariableToColumnMap() const {
  using VarAndIndex = std::pair<Variable, size_t>;

  VariableToColumnMap variableColumns;
  size_t column = 0;

  auto addVariableColumnIfNotExists =
      [&variableColumns, &column](const VarAndIndex& varAndIndex) {
        if (!variableColumns.contains(varAndIndex.first)) {
          variableColumns[varAndIndex.first] = column;
          column++;
        }
      };

  auto addVariablesForSubtree =
      [&addVariableColumnIfNotExists](const auto& subtree) {
        std::ranges::for_each(
            copySortedByColumnIndex(subtree->getVariableColumns()),
            addVariableColumnIfNotExists);
      };

  std::ranges::for_each(_subtrees, addVariablesForSubtree);
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

  result->_sortedBy = resultSortedOn();
  for (const std::array<size_t, 2>& o : _columnOrigins) {
    if (o[0] != NO_COLUMN) {
      result->_resultTypes.push_back(subRes1->getResultType(o[0]));
    } else if (o[1] != NO_COLUMN) {
      result->_resultTypes.push_back(subRes2->getResultType(o[1]));
    } else {
      result->_resultTypes.push_back(ResultTable::ResultType::KB);
    }
  }
  result->_idTable.setNumColumns(getResultWidth());
  int leftWidth = subRes1->_idTable.numColumns();
  int rightWidth = subRes2->_idTable.numColumns();
  int outWidth = result->_idTable.numColumns();

  CALL_FIXED_SIZE((std::array{leftWidth, rightWidth, outWidth}),
                  &Union::computeUnion, this, &result->_idTable,
                  subRes1->_idTable, subRes2->_idTable, _columnOrigins);
  LOG(DEBUG) << "Union result computation done." << std::endl;
}

template <int LEFT_WIDTH, int RIGHT_WIDTH, int OUT_WIDTH>
void Union::computeUnion(
    IdTable* dynRes, const IdTable& dynLeft, const IdTable& dynRight,
    const std::vector<std::array<size_t, 2>>& columnOrigins) {
  const IdTableView<LEFT_WIDTH> left = dynLeft.asStaticView<LEFT_WIDTH>();
  const IdTableView<RIGHT_WIDTH> right = dynRight.asStaticView<RIGHT_WIDTH>();
  IdTableStatic<OUT_WIDTH> res = std::move(*dynRes).toStatic<OUT_WIDTH>();

  res.reserve(left.size() + right.size());

  // TODO<joka921> insert global constant here
  const size_t chunkSize =
      100000 /
      res.numColumns();  // after this many elements, check for timeouts
  auto checkAfterChunkSize = checkTimeoutAfterNCallsFactory(chunkSize);

  // Append the contents of inputTable to the result. This requires previous
  // checks that the columns are compatible. After copying chunkSize elements,
  // we check for a timeout. It is only called when the number of columns
  // matches at compile time, hence the [[maybe_unused]]
  [[maybe_unused]] auto appendToResultChunked = [&res, chunkSize,
                                                 this](const auto& inputTable) {
    // always copy chunkSize results at once and then check for a timeout
    size_t numChunks = inputTable.size() / chunkSize;
    for (size_t i = 0; i < numChunks; ++i) {
      res.insertAtEnd(inputTable.begin() + i * chunkSize,
                      inputTable.begin() + (i + 1) * chunkSize);
      checkTimeout();
    }
    res.insertAtEnd(inputTable.begin() + numChunks * chunkSize,
                    inputTable.end());
  };

  auto columnsMatch = [&columnOrigins](const auto& inputTable,
                                       const auto& getter) {
    bool allColumnsAreUsed = inputTable.numColumns() == columnOrigins.size();
    bool columnsAreSorted = std::ranges::is_sorted(columnOrigins, {}, getter);
    bool noGapsInColumns =
        getter(columnOrigins.back()) == inputTable.numColumns() - 1;
    return allColumnsAreUsed && columnsAreSorted && noGapsInColumns;
  };

  // Append the result of one the children (`left` or `right`), passed via the
  // `inputTable` argument to the final result of this `UNION` operation. The
  // `WIDTH` must be the width of the input table (`LEFT_WIDTH` or
  // `RIGHT_WIDTH`) and  `getter` must be a function that extracts the correct
  // column index for the `inputTable` from an element of the `columnOrigins`.
  auto appendResult = [&columnOrigins, &res, &appendToResultChunked,
                       &checkAfterChunkSize, &columnsMatch]<size_t WIDTH>(
                          const auto& inputTable, const auto& getter) {
    (void)appendToResultChunked;
    if (columnsMatch(inputTable, getter)) {
      // Left and right have the same columns, we can simply copy the entries.
      // This if clause is only here to avoid creating the call to insert when
      // it would not be possible to call the function due to not matching
      // columns.
      AD_CHECK(WIDTH == OUT_WIDTH);
      if constexpr (WIDTH == OUT_WIDTH) {
        appendToResultChunked(inputTable);
      }
    } else {
      for (const auto& row : inputTable) {
        checkAfterChunkSize();
        res.emplace_back();
        size_t backIdx = res.size() - 1;
        for (size_t i = 0; i < columnOrigins.size(); i++) {
          const auto column = getter(columnOrigins[i]);
          res(backIdx, i) = column != NO_COLUMN ? row[column] : ID_NO_VALUE;
        }
      }
    }
  };

  if (left.size() > 0) {
    appendResult.template operator()<LEFT_WIDTH>(left, ad_utility::first);
  }

  if (right.size() > 0) {
    appendResult.template operator()<RIGHT_WIDTH>(right, ad_utility::second);
  }
  *dynRes = std::move(res).toDynamic();
}
