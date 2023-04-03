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
  AD_CONTRACT_CHECK(t1 && t2);
  _subtrees[0] = t1;
  _subtrees[1] = t2;

  // compute the column origins
  const VariableToColumnMap& variableColumns =
      getInternallyVisibleVariableColumns();
  _columnOrigins.resize(variableColumns.size(), {NO_COLUMN, NO_COLUMN});
  const auto& t1VarCols = t1->getVariableColumns();
  const auto& t2VarCols = t2->getVariableColumns();
  for (auto it : variableColumns) {
    // look for the corresponding column in t1
    auto it1 = t1VarCols.find(it.first);
    if (it1 != t1VarCols.end()) {
      _columnOrigins[it.second.columnIndex_][0] = it1->second.columnIndex_;
    } else {
      _columnOrigins[it.second.columnIndex_][0] = NO_COLUMN;
    }

    // look for the corresponding column in t2
    const auto it2 = t2VarCols.find(it.first);
    if (it2 != t2VarCols.end()) {
      _columnOrigins[it.second.columnIndex_][1] = it2->second.columnIndex_;
    } else {
      _columnOrigins[it.second.columnIndex_][1] = NO_COLUMN;
    }
  }
  AD_CORRECTNESS_CHECK(std::ranges::all_of(_columnOrigins, [](const auto& el) {
    return el[0] != NO_COLUMN || el[1] != NO_COLUMN;
  }));
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
  using VarAndTypeInfo = std::pair<Variable, ColumnIndexAndTypeInfo>;

  VariableToColumnMap variableColumns;

  // A variable is only guaranteed to always be bound if it exists in all the
  // subtrees and if it is guaranteed to be bound in all the subrees.
  auto mightContainUndef = [this](const Variable& var) {
    return std::ranges::any_of(
        _subtrees, [&](const shared_ptr<QueryExecutionTree>& subtree) {
          const auto& varCols = subtree->getVariableColumns();
          return !varCols.contains(var) ||
                 (varCols.at(var).mightContainUndef_ ==
                  ColumnIndexAndTypeInfo::PossiblyUndefined);
        });
  };

  // Note: it is tempting to declare `nextColumnIndex` inside the lambda
  // `addVariableColumnIfNotExists`, but that doesn't work because
  // `std::ranges::for_each` takes the lambda by value and creates a new
  // variable at every invocation.
  size_t nextColumnIndex = 0;
  auto addVariableColumnIfNotExists =
      [&mightContainUndef, &variableColumns,
       &nextColumnIndex](const VarAndTypeInfo& varAndIndex) mutable {
        const auto& variable = varAndIndex.first;
        if (!variableColumns.contains(variable)) {
          using enum ColumnIndexAndTypeInfo::UndefStatus;
          variableColumns[variable] = ColumnIndexAndTypeInfo{
              nextColumnIndex,
              mightContainUndef(variable) ? PossiblyUndefined : AlwaysDefined};
          nextColumnIndex++;
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
    return getSizeEstimateBeforeLimit() / numDistinct;
  } else if (_columnOrigins[col][1] != NO_COLUMN) {
    // Compute the number of distinct elements in the input, add one new element
    // for the unbound variables, then divide it by the number of elements in
    // the result. This is slightly off if the subresult already contained
    // an unbound result, but the error is small in most common cases.
    double numDistinct = _subtrees[1]->getSizeEstimate() /
                         static_cast<double>(_subtrees[1]->getMultiplicity(
                             _columnOrigins[col][1]));
    numDistinct += 1;
    return getSizeEstimateBeforeLimit() / numDistinct;
  }
  return 1;
}

size_t Union::getSizeEstimateBeforeLimit() {
  return _subtrees[0]->getSizeEstimate() + _subtrees[1]->getSizeEstimate();
}

size_t Union::getCostEstimate() {
  return _subtrees[0]->getCostEstimate() + _subtrees[1]->getCostEstimate() +
         getSizeEstimateBeforeLimit();
}

ResultTable Union::computeResult() {
  LOG(DEBUG) << "Union result computation..." << std::endl;
  shared_ptr<const ResultTable> subRes1 = _subtrees[0]->getResult();
  shared_ptr<const ResultTable> subRes2 = _subtrees[1]->getResult();
  LOG(DEBUG) << "Union subresult computation done." << std::endl;

  IdTable idTable{getExecutionContext()->getAllocator()};

  idTable.setNumColumns(getResultWidth());
  Union::computeUnion(&idTable, subRes1->idTable(), subRes2->idTable(),
                      _columnOrigins);

  LOG(DEBUG) << "Union result computation done" << std::endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return ResultTable{
      std::move(idTable), resultSortedOn(),
      ResultTable::getSharedLocalVocabFromNonEmptyOf(*subRes1, *subRes2)};
}

void Union::computeUnion(
    IdTable* resPtr, const IdTable& left, const IdTable& right,
    const std::vector<std::array<size_t, 2>>& columnOrigins) {
  IdTable& res = *resPtr;
  res.resize(left.size() + right.size());

  static constexpr size_t chunkSize = 1'000'000;

  // A drop-in replacement for `std::copy` that performs the copying in chunks
  // of `chunkSize` and checks the timeout after each chunk.
  auto copyChunked = [this](auto beg, auto end, auto target) {
    size_t total = end - beg;
    for (size_t i = 0; i < total; i += chunkSize) {
      checkTimeout();
      size_t actualEnd = std::min(i + chunkSize, total);
      std::copy(beg + i, beg + actualEnd, target + i);
    }
  };

  // A similar timeout-checking replacement for `std::fill`.
  auto fillChunked = [this](auto beg, auto end, const auto& value) {
    size_t total = end - beg;
    for (size_t i = 0; i < total; i += chunkSize) {
      checkTimeout();
      size_t actualEnd = std::min(i + chunkSize, total);
      std::fill(beg + i, beg + actualEnd, value);
    }
  };

  // Write the column with the `inputColumnIndex` from the `inputTable` into the
  // `targetColumn`. Always copy the complete input column and start at position
  // `offset` in the target column. If the `inputColumnIndex` is `NO_COLUMN`,
  // then the corresponding range in the `targetColumn` will be filled with
  // UNDEF.
  auto writeColumn = [&copyChunked, &fillChunked](
                         const auto& inputTable, auto& targetColumn,
                         size_t inputColumnIndex, size_t offset) {
    if (inputColumnIndex != NO_COLUMN) {
      decltype(auto) input = inputTable.getColumn(inputColumnIndex);
      copyChunked(input.begin(), input.end(), targetColumn.begin() + offset);
    } else {
      fillChunked(targetColumn.begin() + offset,
                  targetColumn.begin() + offset + inputTable.size(),
                  Id::makeUndefined());
    }
  };

  // Write all the columns.
  AD_CORRECTNESS_CHECK(columnOrigins.size() == res.numColumns());
  for (size_t targetColIdx = 0; targetColIdx < columnOrigins.size();
       ++targetColIdx) {
    auto [leftCol, rightCol] = columnOrigins.at(targetColIdx);
    decltype(auto) targetColumn = res.getColumn(targetColIdx);
    writeColumn(left, targetColumn, leftCol, 0u);
    writeColumn(right, targetColumn, rightCol, left.size());
  }
}
