// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/Result.h"

#include <absl/cleanup/cleanup.h>

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
auto compareRowsBySortColumns(const std::vector<ColumnIndex>& sortedBy) {
  return [&sortedBy](const auto& row1, const auto& row2) {
    for (ColumnIndex col : sortedBy) {
      if (row1[col] != row2[col]) {
        return row1[col] < row2[col];
      }
    }
    return false;
  };
}

// _____________________________________________________________________________
Result::Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
               SharedLocalVocabWrapper localVocab)
    : data_{std::move(idTable)},
      sortedBy_{std::move(sortedBy)},
      localVocab_{std::move(localVocab.localVocab_)} {
  AD_CONTRACT_CHECK(localVocab_ != nullptr);
  assertSortOrderIsRespected(this->idTable(), sortedBy_);
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
    : data_{GenContainer{
          [](auto idTables, auto sortedBy) -> cppcoro::generator<IdTable> {
            std::optional<IdTable::row_type> previousId = std::nullopt;
            for (IdTable& idTable : idTables) {
              if (!idTable.empty()) {
                if (previousId.has_value()) {
                  AD_EXPENSIVE_CHECK(!compareRowsBySortColumns(sortedBy)(
                      idTable.at(0), previousId.value()));
                }
                previousId = idTable.at(idTable.size() - 1);
              }
              assertSortOrderIsRespected(idTable, sortedBy);
              co_yield std::move(idTable);
            }
          }(std::move(idTables), sortedBy)}},
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
Result::Result(cppcoro::generator<IdTable> idTables,
               std::vector<ColumnIndex> sortedBy, LocalVocabPtr localVocab)
    : Result{std::move(idTables), std::move(sortedBy),
             SharedLocalVocabWrapper{std::move(localVocab)}} {}

// _____________________________________________________________________________
// Apply `LimitOffsetClause` to given `IdTable`.
void resizeIdTable(IdTable& idTable, const LimitOffsetClause& limitOffset) {
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
    std::function<void(std::chrono::microseconds, const IdTable&)>
        limitTimeCallback) {
  // Apply the OFFSET clause. If the offset is `0` or the offset is larger
  // than the size of the `IdTable`, then this has no effect and runtime
  // `O(1)` (see the docs for `std::shift_left`).
  AD_CONTRACT_CHECK(limitTimeCallback);
  if (limitOffset.isUnconstrained()) {
    return;
  }
  if (isFullyMaterialized()) {
    ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
    resizeIdTable(std::get<IdTable>(data_), limitOffset);
    limitTimeCallback(limitTimer.msecs(), idTable());
  } else {
    auto generator = [](cppcoro::generator<IdTable> original,
                        LimitOffsetClause limitOffset,
                        auto limitTimeCallback) -> cppcoro::generator<IdTable> {
      if (limitOffset._limit.value_or(1) == 0) {
        co_return;
      }
      for (IdTable& idTable : original) {
        ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
        size_t originalSize = idTable.numRows();
        resizeIdTable(idTable, limitOffset);
        uint64_t offsetDelta = limitOffset.actualOffset(originalSize);
        limitOffset._offset -= offsetDelta;
        if (limitOffset._limit.has_value()) {
          limitOffset._limit.value() -=
              limitOffset.actualSize(originalSize - offsetDelta);
        }
        limitTimeCallback(limitTimer.value(), idTable);
        if (limitOffset._offset == 0) {
          co_yield idTable;
        }
        if (limitOffset._limit.value_or(1) == 0) {
          break;
        }
      }
    }(std::move(idTables()), limitOffset, std::move(limitTimeCallback));
    data_.emplace<GenContainer>(std::move(generator));
  }
}

// _____________________________________________________________________________
void Result::assertThatLimitWasRespected(const LimitOffsetClause& limitOffset) {
  if (isFullyMaterialized()) {
    uint64_t numRows = idTable().numRows();
    auto limit = limitOffset._limit;
    AD_CONTRACT_CHECK(!limit.has_value() || numRows <= limit.value());
  } else {
    auto generator =
        [](cppcoro::generator<IdTable> original,
           LimitOffsetClause limitOffset) -> cppcoro::generator<IdTable> {
      auto limit = limitOffset._limit;
      uint64_t elementCount = 0;
      for (IdTable& idTable : original) {
        elementCount += idTable.numRows();
        AD_CONTRACT_CHECK(!limit.has_value() || elementCount <= limit.value());
        co_yield idTable;
      }
      AD_CONTRACT_CHECK(!limit.has_value() || elementCount <= limit.value());
    }(std::move(idTables()), limitOffset);
    data_.emplace<GenContainer>(std::move(generator));
  }
}

// _____________________________________________________________________________
void Result::checkDefinedness(const VariableToColumnMap& varColMap) {
  auto performCheck = [](const auto& map, IdTable& idTable) {
    return std::ranges::all_of(map, [&](const auto& varAndCol) {
      const auto& [columnIndex, mightContainUndef] = varAndCol.second;
      if (mightContainUndef == ColumnIndexAndTypeInfo::AlwaysDefined) {
        return std::ranges::all_of(idTable.getColumn(columnIndex), [](Id id) {
          return id.getDatatype() != Datatype::Undefined;
        });
      }
      return true;
    });
  };
  if (isFullyMaterialized()) {
    AD_EXPENSIVE_CHECK(performCheck(varColMap, std::get<IdTable>(data_)));
  } else {
    auto generator = [](cppcoro::generator<IdTable> original,
                        VariableToColumnMap varColMap,
                        auto performCheck) -> cppcoro::generator<IdTable> {
      for (IdTable& idTable : original) {
        // No need to check subsequent idTables assuming the datatypes
        // don't change mid result.
        AD_EXPENSIVE_CHECK(performCheck(varColMap, idTable));
        co_yield idTable;
      }
    }(std::move(idTables()), varColMap, std::move(performCheck));
    data_.emplace<GenContainer>(std::move(generator));
  }
}

// _____________________________________________________________________________
void Result::runOnNewChunkComputed(
    std::function<void(const IdTable&, std::chrono::microseconds)> onNewChunk,
    std::function<void(bool)> onGeneratorFinished) {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  auto generator = [](cppcoro::generator<IdTable> original, auto onNewChunk,
                      auto onGeneratorFinished) -> cppcoro::generator<IdTable> {
    // Call this within destructor to make sure it is also called when an
    // operation stops iterating before reaching the end.
    absl::Cleanup cleanup{
        [&onGeneratorFinished]() { onGeneratorFinished(false); }};
    try {
      ad_utility::timer::Timer timer{ad_utility::timer::Timer::Started};
      for (IdTable& idTable : original) {
        onNewChunk(idTable, timer.value());
        co_yield idTable;
        timer.start();
      }
    } catch (...) {
      std::move(cleanup).Cancel();
      onGeneratorFinished(true);
      throw;
    }
  }(std::move(idTables()), std::move(onNewChunk),
                                                std::move(onGeneratorFinished));
  data_.emplace<GenContainer>(std::move(generator));
}

// _____________________________________________________________________________
void Result::assertSortOrderIsRespected(
    const IdTable& idTable, const std::vector<ColumnIndex>& sortedBy) {
  AD_CONTRACT_CHECK(
      std::ranges::all_of(sortedBy, [&idTable](ColumnIndex colIndex) {
        return colIndex < idTable.numColumns();
      }));

  AD_EXPENSIVE_CHECK(
      std::ranges::is_sorted(idTable, compareRowsBySortColumns(sortedBy)));
}

// _____________________________________________________________________________
const IdTable& Result::idTable() const {
  AD_CONTRACT_CHECK(isFullyMaterialized());
  return std::get<IdTable>(data_);
}

// _____________________________________________________________________________
cppcoro::generator<IdTable>& Result::idTables() const {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  const auto& container = std::get<GenContainer>(data_);
  AD_CONTRACT_CHECK(!container.consumed_->exchange(true));
  return container.generator_;
}

// _____________________________________________________________________________
bool Result::isFullyMaterialized() const noexcept {
  return std::holds_alternative<IdTable>(data_);
}

// _____________________________________________________________________________
void Result::cacheDuringConsumption(
    std::function<bool(const std::optional<IdTable>&, const IdTable&)>
        fitInCache,
    std::function<void(Result)> storeInCache) {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  data_.emplace<GenContainer>(ad_utility::wrapGeneratorWithCache(
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
      }));
}

// _____________________________________________________________________________
void Result::logResultSize() const {
  if (isFullyMaterialized()) {
    LOG(INFO) << "Result has size " << idTable().size() << " x "
              << idTable().numColumns() << std::endl;
  } else {
    LOG(INFO) << "Result has unknown size (not computed yet)" << std::endl;
  }
}
