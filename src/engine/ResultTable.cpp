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
auto ResultTable::getSharedLocalVocabFromNonEmptyOf(
    const ResultTable& resultTable1, const ResultTable& resultTable2)
    -> SharedLocalVocabWrapper {
  const auto& localVocab1 = resultTable1.localVocab_;
  const auto& localVocab2 = resultTable2.localVocab_;
  if (!localVocab1->empty() && !localVocab2->empty()) {
    throw std::runtime_error(
        "Merging of two non-empty local vocabularies is currently not "
        "supported, please contact the developers");
  }
  return SharedLocalVocabWrapper{localVocab2->empty() ? localVocab1
                                                      : localVocab2};
}

// _____________________________________________________________________________
LocalVocab ResultTable::getCopyOfLocalVocab() const {
  return localVocab().clone();
}

// _____________________________________________________________________________
ResultTable::ResultTable(IdTable idTable, vector<size_t> sortedBy,
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
ResultTable::ResultTable(IdTable idTable, vector<size_t> sortedBy,
                         LocalVocab&& localVocab)
    : ResultTable(std::move(idTable), std::move(sortedBy),
                  SharedLocalVocabWrapper{std::move(localVocab)}) {}

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
