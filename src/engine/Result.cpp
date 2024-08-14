// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/Result.h"

#include "engine/LocalVocab.h"
#include "util/CacheableGenerator.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"

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
auto Result::getMergedLocalVocab(const Result& result1, const Result& result2)
    -> SharedLocalVocabWrapper {
  return getMergedLocalVocab(
      std::array{std::cref(result1), std::cref(result2)});
}

// _____________________________________________________________________________
LocalVocab Result::getCopyOfLocalVocab() const { return localVocab().clone(); }

// _____________________________________________________________________________
Result::Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
               SharedLocalVocabWrapper localVocab)
    : data_{std::move(idTable)},
      sortedBy_{std::move(sortedBy)},
      localVocab_{std::move(localVocab.localVocab_)} {
  AD_CONTRACT_CHECK(localVocab_ != nullptr);
  validateIdTable(this->idTable(), sortedBy_);
}

// _____________________________________________________________________________
Result::Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
               LocalVocab&& localVocab)
    : Result{std::move(idTable), std::move(sortedBy),
             SharedLocalVocabWrapper{std::move(localVocab)}} {}

// _____________________________________________________________________________
Result::Result(cppcoro::generator<IdTable> idTables,
               std::vector<ColumnIndex> sortedBy,
               SharedLocalVocabWrapper localVocab)
    : data_{[](auto idTables,
               auto sortedBy) mutable -> cppcoro::generator<IdTable> {
        for (IdTable& idTable : idTables) {
          validateIdTable(idTable, sortedBy);
          co_yield std::move(idTable);
        }
      }(std::move(idTables), sortedBy)},
      sortedBy_{std::move(sortedBy)},
      localVocab_{std::move(localVocab.localVocab_)} {
  AD_CONTRACT_CHECK(localVocab_ != nullptr);
}

// _____________________________________________________________________________
Result::Result(cppcoro::generator<IdTable> idTables,
               std::vector<ColumnIndex> sortedBy, LocalVocab&& localVocab)
    : Result{std::move(idTables), std::move(sortedBy),
             SharedLocalVocabWrapper{std::move(localVocab)}} {}

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

// _____________________________________________________________________________
void Result::applyLimitOffset(
    const LimitOffsetClause& limitOffset,
    std::function<void(std::chrono::milliseconds)> limitTimeCallback) {
  // Apply the OFFSET clause. If the offset is `0` or the offset is larger
  // than the size of the `IdTable`, then this has no effect and runtime
  // `O(1)` (see the docs for `std::shift_left`).
  AD_CONTRACT_CHECK(limitTimeCallback);
  if (isDataEvaluated()) {
    ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
    modifyIdTable(std::get<IdTable>(data_), limitOffset);
    limitTimeCallback(limitTimer.msecs());
  } else {
    auto generator =
        [](cppcoro::generator<IdTable> original, LimitOffsetClause limitOffset,
           std::function<void(std::chrono::milliseconds)> limitTimeCallback)
        -> cppcoro::generator<IdTable> {
      if (limitOffset._limit.value_or(1) == 0) {
        co_return;
      }
      for (auto&& idTable : original) {
        ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
        size_t originalSize = idTable.numRows();
        modifyIdTable(idTable, limitOffset);
        uint64_t offsetDelta = limitOffset.actualOffset(originalSize);
        limitOffset._offset -= offsetDelta;
        if (limitOffset._limit.has_value()) {
          limitOffset._limit.value() -=
              limitOffset.actualSize(originalSize - offsetDelta);
        }
        limitTimeCallback(limitTimer.msecs());
        if (limitOffset._offset == 0) {
          co_yield std::move(idTable);
        }
        if (limitOffset._limit.value_or(1) == 0) {
          break;
        }
      }
    }(std::move(idTables()), limitOffset, std::move(limitTimeCallback));
    data_ = std::move(generator);
  }
}

// _____________________________________________________________________________
void Result::enforceLimitOffset(const LimitOffsetClause& limitOffset) {
  if (isDataEvaluated()) {
    uint64_t numRows = idTable().numRows();
    auto limit = limitOffset._limit;
    AD_CONTRACT_CHECK(!limit.has_value() || numRows <= limit.value());
  } else {
    auto generator =
        [](cppcoro::generator<IdTable> original,
           LimitOffsetClause limitOffset) -> cppcoro::generator<IdTable> {
      auto limit = limitOffset._limit;
      uint64_t elementCount = 0;
      for (auto&& idTable : original) {
        elementCount += idTable.numRows();
        AD_CONTRACT_CHECK(!limit.has_value() || elementCount <= limit.value());
        co_yield std::move(idTable);
      }
      AD_CONTRACT_CHECK(!limit.has_value() || elementCount <= limit.value());
    }(std::move(idTables()), limitOffset);
    data_ = std::move(generator);
  }
}

// _____________________________________________________________________________
auto Result::computeDatatypeCountsPerColumn(IdTable& idTable)
    -> DatatypeCountsPerColumn {
  DatatypeCountsPerColumn types;
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
void Result::checkDefinedness(const VariableToColumnMap& varColMap) {
  auto performCheck = [](const auto& map, IdTable& idTable) {
    DatatypeCountsPerColumn datatypeCountsPerColumn =
        computeDatatypeCountsPerColumn(idTable);
    return std::ranges::all_of(map, [&](const auto& varAndCol) {
      const auto& [columnIndex, mightContainUndef] = varAndCol.second;
      bool hasUndefined =
          datatypeCountsPerColumn.at(columnIndex)
              .at(static_cast<size_t>(Datatype::Undefined)) != 0;
      return mightContainUndef == ColumnIndexAndTypeInfo::PossiblyUndefined ||
             !hasUndefined;
    });
  };
  if (isDataEvaluated()) {
    AD_EXPENSIVE_CHECK(performCheck(varColMap, std::get<IdTable>(data_)));
  } else {
    auto generator = [](cppcoro::generator<IdTable> original,
                        VariableToColumnMap varColMap,
                        auto performCheck) -> cppcoro::generator<IdTable> {
      bool first = true;
      for (auto&& idTable : original) {
        if (first) {
          first = false;
          // No need to check subsequent idTables assuming the datatypes
          // don't change mid result.
          AD_EXPENSIVE_CHECK(performCheck(varColMap, idTable));
        }
        co_yield std::move(idTable);
      }
    }(std::move(idTables()), varColMap, std::move(performCheck));
    data_ = std::move(generator);
  }
}

// _____________________________________________________________________________
void Result::runOnNewChunkComputed(
    std::function<void(const IdTable&, std::chrono::milliseconds)> function) {
  AD_CONTRACT_CHECK(!isDataEvaluated());
  auto generator =
      [](cppcoro::generator<IdTable> original,
         std::function<void(const IdTable&, std::chrono::milliseconds)>
             function) -> cppcoro::generator<IdTable> {
    ad_utility::timer::Timer timer{ad_utility::timer::Timer::Started};
    for (auto&& idTable : original) {
      function(idTable, timer.msecs());
      co_yield std::move(idTable);
      timer.start();
    }
  }(std::move(idTables()), std::move(function));
  data_ = std::move(generator);
}

// _____________________________________________________________________________
void Result::validateIdTable(const IdTable& idTable,
                             const std::vector<ColumnIndex>& sortedBy) {
  AD_CONTRACT_CHECK(std::ranges::all_of(sortedBy, [&idTable](size_t numCols) {
    return numCols < idTable.numColumns();
  }));

  [[maybe_unused]] auto compareRowsByJoinColumns =
      [&sortedBy](const auto& row1, const auto& row2) {
        for (size_t col : sortedBy) {
          if (row1[col] != row2[col]) {
            return row1[col] < row2[col];
          }
        }
        return false;
      };
  AD_EXPENSIVE_CHECK(std::ranges::is_sorted(idTable, compareRowsByJoinColumns));
}

// _____________________________________________________________________________
const IdTable& Result::idTable() const {
  AD_CONTRACT_CHECK(isDataEvaluated());
  return std::get<IdTable>(data_);
}

// _____________________________________________________________________________
cppcoro::generator<IdTable>& Result::idTables() const {
  AD_CONTRACT_CHECK(!isDataEvaluated());
  return std::get<cppcoro::generator<IdTable>>(data_);
}

// _____________________________________________________________________________
bool Result::isDataEvaluated() const noexcept {
  return std::holds_alternative<IdTable>(data_);
}

// _____________________________________________________________________________
void Result::cacheDuringConsumption(
    std::function<bool(const std::optional<IdTable>&, const IdTable&)>
        fitInCache,
    std::function<void(Result)> storeInCache) {
  if (isDataEvaluated()) {
    return;
  }

  data_ = ad_utility::wrapGeneratorWithCache(
      std::move(idTables()),
      [fitInCache = std::move(fitInCache)](std::optional<IdTable>& aggregate,
                                           const IdTable& newTable) {
        bool doBothFitInCache = fitInCache(aggregate, newTable);
        if (doBothFitInCache) {
          if (aggregate.has_value()) {
            aggregate.value().insertAtEnd(newTable);
          } else {
            aggregate.emplace(newTable.clone());
          }
        }
        return doBothFitInCache;
      },
      [storeInCache = std::move(storeInCache), sortedBy = sortedBy_,
       localVocab = localVocab_](IdTable idTable) mutable {
        storeInCache(Result{std::move(idTable), std::move(sortedBy),
                            SharedLocalVocabWrapper{std::move(localVocab)}});
      });
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
