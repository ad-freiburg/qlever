// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/Result.h"

#include "engine/LocalVocab.h"
#include "util/Exception.h"
#include "util/Log.h"

// _____________________________________________________________________________
string Result::asDebugString() const {
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
auto Result::getMergedLocalVocab(const Result& resultTable1,
                                 const Result& resultTable2)
    -> SharedLocalVocabWrapper {
  return getMergedLocalVocab(
      std::array{std::cref(resultTable1), std::cref(resultTable2)});
}

// _____________________________________________________________________________
LocalVocab Result::getCopyOfLocalVocab() const { return localVocab().clone(); }

// _____________________________________________________________________________
Result::Result(TableType idTable, std::vector<ColumnIndex> sortedBy,
               SharedLocalVocabWrapper localVocab)
    : _idTable{std::move(idTable)},
      _sortedBy{std::move(sortedBy)},
      localVocab_{std::move(localVocab.localVocab_)} {
  AD_CONTRACT_CHECK(localVocab_ != nullptr);
  // TODO<RobinTF> move checks into generators if possible
  AD_CONTRACT_CHECK(!isDataEvaluated() ||
                    std::ranges::all_of(_sortedBy, [this](size_t numCols) {
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
      !isDataEvaluated() ||
      std::ranges::is_sorted(this->idTable(), compareRowsByJoinColumns));
}

// _____________________________________________________________________________
Result::Result(TableType idTable, std::vector<ColumnIndex> sortedBy,
               LocalVocab&& localVocab)
    : Result(std::move(idTable), std::move(sortedBy),
             SharedLocalVocabWrapper{std::move(localVocab)}) {}

// _____________________________________________________________________________
void modifyIdTable(IdTable& idTable, const LimitOffsetClause& limitOffset) {
  std::ranges::for_each(
      idTable.getColumns(),
      [offset = limitOffset.actualOffset(idTable.numRows()),
       upperBound =
           limitOffset.upperBound(idTable.numRows())](std::span<Id> column) {
        std::shift_left(column.begin(), column.begin() + upperBound, offset);
      });
  // Resize the `IdTable` if necessary.
  size_t targetSize = limitOffset.actualSize(idTable.numRows());
  AD_CORRECTNESS_CHECK(targetSize <= idTable.numRows());
  idTable.resize(targetSize);
  idTable.shrinkToFit();
}

// TODO<RobinTF> add unit tests for this
// _____________________________________________________________________________
void Result::applyLimitOffset(const LimitOffsetClause& limitOffset) {
  // Apply the OFFSET clause. If the offset is `0` or the offset is larger
  // than the size of the `IdTable`, then this has no effect and runtime
  // `O(1)` (see the docs for `std::shift_left`).
  // TODO<RobinTF> make limit its own dedicated operation to avoid this
  // modification here
  AD_CONTRACT_CHECK(
      !std::holds_alternative<cppcoro::generator<const IdTable>>(_idTable));
  using Gen = ad_utility::ReusableGenerator<IdTable>;
  if (std::holds_alternative<IdTable>(_idTable)) {
    modifyIdTable(std::get<IdTable>(_idTable), limitOffset);
  } else if (std::holds_alternative<Gen>(_idTable)) {
    auto generator =
        [](cppcoro::generator<IdTable> original,
           LimitOffsetClause limitOffset) -> cppcoro::generator<IdTable> {
      if (limitOffset._limit.value_or(1) == 0) {
        co_return;
      }
      for (auto&& idTable : original) {
        modifyIdTable(idTable, limitOffset);
        uint64_t offsetDelta = limitOffset.actualOffset(idTable.numRows());
        limitOffset._offset -= offsetDelta;
        if (limitOffset._limit.has_value()) {
          limitOffset._limit.value() -=
              limitOffset.actualSize(idTable.numRows() - offsetDelta);
        }
        if (limitOffset._offset == 0) {
          co_yield std::move(idTable);
        }
        if (limitOffset._limit.value_or(1) == 0) {
          break;
        }
      }
    }(std::move(std::get<Gen>(_idTable)).extractGenerator(), limitOffset);
    _idTable.emplace<Gen>(std::move(generator));
  } else {
    AD_FAIL();
  }
}

// _____________________________________________________________________________
auto Result::getOrComputeDatatypeCountsPerColumn()
    -> const DatatypeCountsPerColumn& {
  if (datatypeCountsPerColumn_.has_value()) {
    return datatypeCountsPerColumn_.value();
  }
  auto& idTable = std::get<IdTable>(_idTable);
  auto& types = datatypeCountsPerColumn_.emplace();
  types.resize(idTable.numColumns());
  for (size_t i = 0; i < idTable.numColumns(); ++i) {
    const auto& col = idTable.getColumn(i);
    auto& datatypes = types.at(i);
    for (Id id : col) {
      ++datatypes[static_cast<size_t>(id.getDatatype())];
    }
  }
  return types;
}

// _____________________________________________________________
bool Result::checkDefinedness(const VariableToColumnMap& varColMap) {
  AD_CONTRACT_CHECK(isDataEvaluated());
  const auto& datatypesPerColumn = getOrComputeDatatypeCountsPerColumn();
  return std::ranges::all_of(varColMap, [&](const auto& varAndCol) {
    const auto& [columnIndex, mightContainUndef] = varAndCol.second;
    bool hasUndefined = datatypesPerColumn.at(columnIndex)
                            .at(static_cast<size_t>(Datatype::Undefined)) != 0;
    return mightContainUndef == ColumnIndexAndTypeInfo::PossiblyUndefined ||
           !hasUndefined;
  });
}

// _____________________________________________________________________________
const IdTable& Result::idTable() const {
  AD_CONTRACT_CHECK(isDataEvaluated());
  return std::get<IdTable>(_idTable);
}

// _____________________________________________________________________________
cppcoro::generator<const IdTable> Result::idTables() const {
  AD_CONTRACT_CHECK(!isDataEvaluated());
  return std::visit(
      [](auto& generator) -> cppcoro::generator<const IdTable> {
        if constexpr (!std::is_same_v<decltype(generator), IdTable&>) {
          for (auto&& idTable : generator) {
            co_yield idTable;
          }
        } else {
          // Type of variant here should never be `IdTable`
          AD_FAIL();
        }
      },
      _idTable);
}

// _____________________________________________________________________________
bool Result::isDataEvaluated() const {
  return std::holds_alternative<IdTable>(_idTable);
}

// _____________________________________________________________________________
void Result::logResultSize() const {
  if (isDataEvaluated()) {
    LOG(INFO) << "Result has size " << idTable().size() << " x "
              << idTable().numColumns() << std::endl;
  } else {
    LOG(INFO) << "Result has unknown size (not computed yet)" << std::endl;
  }
}

// _____________________________________________________________________________
Result Result::createResultWithFallback(std::shared_ptr<const Result> original,
                                        std::function<Result()> fallback) {
  AD_CONTRACT_CHECK(!original->isDataEvaluated());
  auto generator = [](std::shared_ptr<const Result> sharedResult,
                      std::function<Result()> fallback)
      -> cppcoro::generator<const IdTable> {
    size_t index = 0;
    try {
      for (auto&& idTable : sharedResult->idTables()) {
        co_yield idTable;
        index++;
      }
      co_return;
    } catch (const ad_utility::IteratorExpired&) {
      // co_yield is not allowed here, so simply ignore this and allow control
      // flow to take over
    } catch (...) {
      throw;
    }
    Result freshResult = fallback();
    // If data is evaluated this means that this process is not deterministic
    // or that there's a wrong callback used here.
    AD_CORRECTNESS_CHECK(!freshResult.isDataEvaluated());
    for (auto&& idTable : freshResult.idTables()) {
      if (index > 0) {
        index--;
        continue;
      }
      co_yield idTable;
    }
  };
  return Result{generator(std::move(original), std::move(fallback)),
                original->_sortedBy,
                SharedLocalVocabWrapper{original->localVocab_}};
}
