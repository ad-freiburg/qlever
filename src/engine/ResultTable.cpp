// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/ResultTable.h"

#include "engine/LocalVocab.h"
#include "util/Exception.h"

// _____________________________________________________________________________
string ResultTable::asDebugString() const {
  std::ostringstream os;
  os << "First (up to) 5 rows of result with size:\n";
  for (size_t i = 0; i < std::min<size_t>(5, idTable().size()); ++i) {
    for (size_t j = 0; j < idTable().numColumns(); ++j) {
      os << idTable()(i, j) << '\t';
    }
    os << '\n';
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
auto ResultTable::getMergedLocalVocab(const ResultTable& resultTable1,
                                      const ResultTable& resultTable2)
    -> SharedLocalVocabWrapper {
  return getMergedLocalVocab(
      std::array{std::cref(resultTable1), std::cref(resultTable2)});
}

// _____________________________________________________________________________
LocalVocab ResultTable::getCopyOfLocalVocab() const {
  return localVocab().clone();
}

// _____________________________________________________________________________
ResultTable::ResultTable(IdTable idTable, vector<ColumnIndex> sortedBy,
                         SharedLocalVocabWrapper localVocab)
    : _idTable{std::move(idTable)},
      _sortedBy{std::move(sortedBy)},
      localVocab_{std::move(localVocab.localVocab_)} {
  AD_CONTRACT_CHECK(localVocab_ != nullptr);
  AD_CONTRACT_CHECK(std::ranges::all_of(_sortedBy, [this](size_t numCols) {
    return numCols < this->idTable().numColumns();
  }));

  [[maybe_unused]] auto compareRowsByJoinColumns = [this](const auto& row1,
                                                          const auto& row2) {
    for (size_t col : this->sortedBy()) {
      if (row1[col] != row2[col]) {
        return row1[col] < row2[col];
      }
    }
    return false;
  };
  AD_EXPENSIVE_CHECK(
      std::ranges::is_sorted(this->idTable(), compareRowsByJoinColumns));
}

// _____________________________________________________________________________
ResultTable::ResultTable(IdTable idTable, vector<ColumnIndex> sortedBy,
                         LocalVocab&& localVocab)
    : ResultTable(std::move(idTable), std::move(sortedBy),
                  SharedLocalVocabWrapper{std::move(localVocab)}) {}

// _____________________________________________________________________________
void ResultTable::applyLimitOffset(const LimitOffsetClause& limitOffset) {
  // Apply the OFFSET clause. If the offset is `0` or the offset is larger
  // than the size of the `IdTable`, then this has no effect and runtime
  // `O(1)` (see the docs for `std::shift_left`).
  std::ranges::for_each(
      _idTable.getColumns(),
      [offset = limitOffset.actualOffset(_idTable.numRows()),
       upperBound =
           limitOffset.upperBound(_idTable.numRows())](std::span<Id> column) {
        std::shift_left(column.begin(), column.begin() + upperBound, offset);
      });
  // Resize the `IdTable` if necessary.
  size_t targetSize = limitOffset.actualSize(_idTable.numRows());
  AD_CORRECTNESS_CHECK(targetSize <= _idTable.numRows());
  _idTable.resize(targetSize);
  _idTable.shrinkToFit();
}

// _____________________________________________________________________________
auto ResultTable::getOrComputeDatatypeCountsPerColumn()
    -> const DatatypeCountsPerColumn& {
  if (datatypeCountsPerColumn_.has_value()) {
    return datatypeCountsPerColumn_.value();
  }
  auto& types = datatypeCountsPerColumn_.emplace();
  types.resize(_idTable.numColumns());
  for (size_t i = 0; i < _idTable.numColumns(); ++i) {
    const auto& col = _idTable.getColumn(i);
    auto& datatypes = types.at(i);
    for (Id id : col) {
      ++datatypes[static_cast<size_t>(id.getDatatype())];
    }
  }
  return types;
}

// _____________________________________________________________
bool ResultTable::checkDefinedness(const VariableToColumnMap& varColMap) {
  const auto& datatypesPerColumn = getOrComputeDatatypeCountsPerColumn();
  return std::ranges::all_of(varColMap, [&](const auto& varAndCol) {
    const auto& [columnIndex, mightContainUndef] = varAndCol.second;
    bool hasUndefined = datatypesPerColumn.at(columnIndex)
                            .at(static_cast<size_t>(Datatype::Undefined)) != 0;
    return mightContainUndef == ColumnIndexAndTypeInfo::PossiblyUndefined ||
           !hasUndefined;
  });
}
