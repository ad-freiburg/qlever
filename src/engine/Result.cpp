// Copyright 2015 - 2025 The QLever Authors, in particular:
//
// 2015 - 2016 Björn Buchhold, UFR
// 2020 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2025        Brandon Harris, BMW
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// BMW = Bayerische Motoren Werke Aktiengesellschaft (BMW)

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/Result.h"

#include <absl/cleanup/cleanup.h>

#include "backports/shift.h"
#include "util/Exception.h"
#include "util/Generators.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/Timer.h"

// _____________________________________________________________________________
std::string Result::asDebugString() const {
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

namespace {
// _____________________________________________________________________________
// Check if sort order promised by `sortedBy` is kept within `idTable`.
void assertSortOrderIsRespected(const IdTable& idTable,
                                const std::vector<ColumnIndex>& sortedBy) {
  AD_CONTRACT_CHECK(
      ql::ranges::all_of(sortedBy, [&idTable](ColumnIndex colIndex) {
        return colIndex < idTable.numColumns();
      }));

  AD_EXPENSIVE_CHECK(
      ql::ranges::is_sorted(idTable, compareRowsBySortColumns(sortedBy)));
}
}  // namespace

// _____________________________________________________________________________
Result::Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
               SharedLocalVocabWrapper localVocab)
    : data_{IdTableSharedLocalVocabPair{std::move(idTable),
                                        std::move(localVocab.localVocab_)}},
      sortedBy_{std::move(sortedBy)} {
  AD_CONTRACT_CHECK(std::get<IdTableSharedLocalVocabPair>(data_).localVocab_ !=
                    nullptr);
  assertSortOrderIsRespected(this->idTable(), sortedBy_);
}

// _____________________________________________________________________________
Result::Result(IdTablePtr idTablePtr, std::vector<ColumnIndex> sortedBy,
               LocalVocab&& localVocab)
    : data_{IdTableSharedLocalVocabPair{
          std::move(idTablePtr),
          std::make_shared<const LocalVocab>(std::move(localVocab))}},
      sortedBy_{std::move(sortedBy)} {
  const auto& materializedResult = std::get<IdTableSharedLocalVocabPair>(data_);
  AD_CONTRACT_CHECK(std::get<IdTablePtr>(materializedResult.idTableOrPtr_) !=
                    nullptr);
  // Note: This second check can never throw because of how we initialize the
  // pointer above, but it still increases confidence.
  AD_CORRECTNESS_CHECK(materializedResult.localVocab_ != nullptr);
  assertSortOrderIsRespected(this->idTable(), sortedBy_);
}

// _____________________________________________________________________________
Result::Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
               LocalVocab&& localVocab)
    : Result{std::move(idTable), std::move(sortedBy),
             SharedLocalVocabWrapper{std::move(localVocab)}} {}

// _____________________________________________________________________________
Result::Result(IdTableVocabPair pair, std::vector<ColumnIndex> sortedBy)
    : Result{std::move(pair.idTable_), std::move(sortedBy),
             std::move(pair.localVocab_)} {}

// _____________________________________________________________________________
Result::Result(Generator idTables, std::vector<ColumnIndex> sortedBy)
    : Result{LazyResult{std::move(idTables)}, std::move(sortedBy)} {}

// _____________________________________________________________________________
Result::Result(LazyResult idTables, std::vector<ColumnIndex> sortedBy)
    : data_{GenContainer{ad_utility::CachingTransformInputRange(
          std::move(idTables),
          [sortedBy, previousId = std::optional<IdTable::row_type>{}](
              Result::IdTableVocabPair& pair) mutable {
            auto& idTable = pair.idTable_;
            if (!idTable.empty()) {
              if (previousId.has_value()) {
                AD_EXPENSIVE_CHECK(!compareRowsBySortColumns(sortedBy)(
                    idTable.at(0), previousId.value()));
              }
              previousId = idTable.at(idTable.size() - 1);
            }
            assertSortOrderIsRespected(idTable, sortedBy);
            return std::move(pair);
          })}},
      sortedBy_{std::move(sortedBy)} {}

namespace {
// _____________________________________________________________________________
// Apply `LimitOffsetClause` to given `IdTable`.
void resizeIdTable(IdTable& idTable, const LimitOffsetClause& limitOffset) {
  ql::ranges::for_each(
      idTable.getColumns(),
      [offset = limitOffset.actualOffset(idTable.numRows()),
       upperBound =
           limitOffset.upperBound(idTable.numRows())](ql::span<Id> column) {
        ql::shift_left(column.begin(), column.begin() + upperBound, offset);
      });
  // Resize the `IdTable` if necessary.
  size_t targetSize = limitOffset.actualSize(idTable.numRows());
  AD_CORRECTNESS_CHECK(targetSize <= idTable.numRows());
  idTable.resize(targetSize);
  idTable.shrinkToFit();
}

// Apply the `LimitOffsetClause` to the `idTable`, return the result as a deep
// copy of the table.
IdTable makeResizedClone(const IdTable& idTable,
                         const LimitOffsetClause& limitOffset) {
  IdTable result{idTable.getAllocator()};
  result.setNumColumns(idTable.numColumns());
  result.insertAtEnd(idTable, limitOffset.actualOffset(idTable.numRows()),
                     limitOffset.upperBound(idTable.numRows()));
  return result;
}
}  // namespace

// _____________________________________________________________________________
void Result::applyLimitOffset(
    const LimitOffsetClause& limitOffset,
    std::function<void(std::chrono::microseconds, const IdTable&)>
        limitTimeCallback) {
  // Apply the OFFSET clause. If the offset is `0` or the offset is larger
  // than the size of the `IdTable`, then this has no effect and runtime
  // `O(1)` (see the docs for `ql::shift_left`).
  AD_CONTRACT_CHECK(limitTimeCallback);
  if (limitOffset.isUnconstrained()) {
    return;
  }
  if (isFullyMaterialized()) {
    ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
    auto& tableOrPtr =
        std::get<IdTableSharedLocalVocabPair>(data_).idTableOrPtr_;
    std::visit(
        [&limitOffset](auto& arg) {
          if constexpr (ad_utility::isSimilar<decltype(arg), IdTable>) {
            resizeIdTable(arg, limitOffset);
          } else {
            static_assert(ad_utility::isSimilar<decltype(arg), IdTablePtr>);
            arg = std::make_shared<const IdTable>(
                makeResizedClone(*arg, limitOffset));
          }
        },
        tableOrPtr);
    limitTimeCallback(limitTimer.msecs(), idTable());
  } else {
    ad_utility::CachingContinuableTransformInputRange generator{
        idTables(), [limitOffset = limitOffset,
                     limitTimeCallback = std::move(limitTimeCallback)](
                        Result::IdTableVocabPair& pair) mutable {
          if (limitOffset._limit.value_or(1) == 0) {
            return IdTableLoopControl::makeBreak();
          }
          auto& idTable = pair.idTable_;
          ad_utility::timer::Timer limitTimer{
              ad_utility::timer::Timer::Started};
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
            return IdTableLoopControl::yieldValue(std::move(pair));
          } else {
            return IdTableLoopControl::makeContinue();
          }
        }};
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
    ad_utility::CachingTransformInputRange generator{
        idTables(), [limit = limitOffset._limit, elementCount = uint64_t{0}](
                        Result::IdTableVocabPair& pair) mutable {
          elementCount += pair.idTable_.numRows();
          AD_CONTRACT_CHECK(!limit.has_value() ||
                            elementCount <= limit.value());
          return std::move(pair);
        }};
    data_.emplace<GenContainer>(std::move(generator));
  }
}

// _____________________________________________________________________________
void Result::checkDefinedness(const VariableToColumnMap& varColMap) {
  auto performCheck = [](const auto& map, const IdTable& idTable) {
    return ql::ranges::all_of(map, [&](const auto& varAndCol) {
      const auto& [columnIndex, mightContainUndef] = varAndCol.second;
      if (mightContainUndef == ColumnIndexAndTypeInfo::AlwaysDefined) {
        return ql::ranges::all_of(idTable.getColumn(columnIndex), [](Id id) {
          return id.getDatatype() != Datatype::Undefined;
        });
      }
      return true;
    });
  };
  if (isFullyMaterialized()) {
    AD_EXPENSIVE_CHECK(performCheck(varColMap, idTable()));
  } else {
    ad_utility::CachingTransformInputRange generator{
        idTables(),
        [varColMap = varColMap, performCheck = std::move(performCheck)](
            Result::IdTableVocabPair& pair) {
          // The lambda capture is only required when expensive checks are
          // enabled.
          (void)performCheck;
          AD_EXPENSIVE_CHECK(performCheck(varColMap, pair.idTable_));
          return std::move(pair);
        }};
    data_.emplace<GenContainer>(std::move(generator));
  }
}

// _____________________________________________________________________________
void Result::runOnNewChunkComputed(
    std::function<void(const IdTableVocabPair&, std::chrono::microseconds)>
        onNewChunk,
    std::function<void(bool)> onGeneratorFinished) {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  auto inputAsGet = ad_utility::CachingTransformInputRange(
      idTables(), [](auto& input) { return std::move(input); });
  using namespace ad_utility::timer;
  // We need the shared pointer, because we cannot easily have a lambda capture
  // refer to another lambda capture.
  auto sharedFinish = std::make_shared<decltype(onGeneratorFinished)>(
      std::move(onGeneratorFinished));

  // The main lambda that when being called processes the next chunk.
  auto get =
      [inputAsGet = std::move(inputAsGet), sharedFinish,
       cleanup = absl::Cleanup{[&finish = *sharedFinish]() { finish(false); }},
       onNewChunk =
           std::move(onNewChunk)]() mutable -> std::optional<IdTableVocabPair> {
    try {
      Timer timer{Timer::Started};
      auto input = inputAsGet.get();
      if (!input.has_value()) {
        std::move(cleanup).Cancel();
        (*sharedFinish)(false);
        return std::nullopt;
      }
      onNewChunk(input.value(), timer.value());
      return input;
    } catch (...) {
      std::move(cleanup).Cancel();
      (*sharedFinish)(true);
      throw;
    }
  };
  data_.emplace<GenContainer>(
      ad_utility::InputRangeFromGetCallable{std::move(get)});
}

// _____________________________________________________________________________
const IdTable& Result::idTable() const {
  AD_CONTRACT_CHECK(isFullyMaterialized());
  return std::visit(
      [](const auto& arg) -> const IdTable& {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, IdTable>) {
          return arg;
        } else {
          static_assert(std::is_same_v<T, IdTablePtr>);
          return *arg;
        }
      },
      std::get<IdTableSharedLocalVocabPair>(data_).idTableOrPtr_);
}

// _____________________________________________________________________________
Result::LazyResult Result::idTables() const {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  const auto& container = std::get<GenContainer>(data_);
  AD_CONTRACT_CHECK(!container.consumed_->exchange(true));
  return std::move(container.generator_);
}

// _____________________________________________________________________________
bool Result::isFullyMaterialized() const noexcept {
  return std::holds_alternative<IdTableSharedLocalVocabPair>(data_);
}

// _____________________________________________________________________________
void Result::cacheDuringConsumption(
    std::function<bool(const std::optional<IdTableVocabPair>&,
                       const IdTableVocabPair&)>
        fitInCache,
    std::function<void(Result)> storeInCache) {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  data_.emplace<GenContainer>(ad_utility::wrapGeneratorWithCache(
      idTables(),
      [fitInCache = std::move(fitInCache)](
          std::optional<IdTableVocabPair>& aggregate,
          const IdTableVocabPair& newTablePair) {
        bool doBothFitInCache = fitInCache(aggregate, newTablePair);
        if (doBothFitInCache) {
          try {
            if (aggregate.has_value()) {
              auto& value = aggregate.value();
              value.idTable_.insertAtEnd(newTablePair.idTable_);
              value.localVocab_.mergeWith(newTablePair.localVocab_);
            } else {
              aggregate.emplace(newTablePair.idTable_.clone(),
                                newTablePair.localVocab_.clone());
            }
          } catch (const ad_utility::detail::AllocationExceedsLimitException&) {
            // We expect potential memory allocation errors here. In this case
            // we just abort the cached value entirely instead of aborting the
            // query.
            return false;
          }
        }
        return doBothFitInCache;
      },
      [storeInCache = std::move(storeInCache),
       sortedBy = sortedBy_](IdTableVocabPair pair) mutable {
        storeInCache(
            Result{std::move(pair.idTable_), std::move(sortedBy),
                   SharedLocalVocabWrapper{std::move(pair.localVocab_)}});
      }));
}

// _____________________________________________________________________________
void Result::logResultSize() const {
  if (isFullyMaterialized()) {
    AD_LOG_INFO << "Result has size " << idTable().size() << " x "
                << idTable().numColumns() << std::endl;
  } else {
    AD_LOG_INFO << "Result has unknown size (not computed yet)" << std::endl;
  }
}
