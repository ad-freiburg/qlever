// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/Result.h"

#include <absl/cleanup/cleanup.h>

#include "backports/shift.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Generators.h"
#include "util/InputRangeUtils.h"
#include "util/Log.h"
#include "util/Timer.h"

using namespace qlever;

// _____________________________________________________________________________
std::string Result::asDebugString() const {
  std::ostringstream os;
  os << "First (up to) 5 rows of result with size:\n";
  for (size_t i = 0; i < std::min<size_t>(5, idTableView().size()); ++i) {
    for (size_t j = 0; j < idTableView().numColumns(); ++j) {
      os << idTableView()(i, j) << '\t';
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
template <typename IdTableT>
void assertSortOrderIsRespected(const IdTableT& idTable,
                                const std::vector<ColumnIndex>& sortedBy) {
  static_assert(ad_utility::SameAsAny<IdTableT, IdTable, IdTableView<0>>);
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
  AD_CONTRACT_CHECK(
      std::get<IdTableSharedLocalVocabPair>(data_).localVocabPtr() != nullptr);
  assertSortOrderIsRespected(this->idTableView(), sortedBy_);
}

// _____________________________________________________________________________
Result::Result(IdTablePtr idTablePtr, std::vector<ColumnIndex> sortedBy,
               LocalVocab&& localVocab)
    : data_{IdTableSharedLocalVocabPair{
          std::move(idTablePtr),
          std::make_shared<const LocalVocab>(std::move(localVocab))}},
      sortedBy_{std::move(sortedBy)} {
  // The non-null check for `idTablePtr` is performed inside
  // `IdTableSharedLocalVocabPair::makeView`. The check below can never throw
  // because of how we initialize the pointer above, but increases confidence.
  AD_CORRECTNESS_CHECK(
      std::get<IdTableSharedLocalVocabPair>(data_).localVocabPtr() != nullptr);
  assertSortOrderIsRespected(this->idTableView(), sortedBy_);
}

// _____________________________________________________________________________
Result::Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
               LocalVocab&& localVocab)
    : Result{std::move(idTable), std::move(sortedBy),
             SharedLocalVocabWrapper{std::move(localVocab)}} {}

// _____________________________________________________________________________
Result::Result(IdTableView<0> view, std::vector<ColumnIndex> sortedBy,
               LocalVocab&& localVocab)
    : data_{IdTableSharedLocalVocabPair{
          std::move(view),
          std::make_shared<const LocalVocab>(std::move(localVocab))}},
      sortedBy_{std::move(sortedBy)} {
  assertSortOrderIsRespected(idTableView(), sortedBy_);
}

// _____________________________________________________________________________
Result::Result(IdTableVocabPair pair, std::vector<ColumnIndex> sortedBy)
    : Result{std::move(pair.idTable_), std::move(sortedBy),
             std::move(pair.localVocab_)} {}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
Result::Result(Generator idTables, std::vector<ColumnIndex> sortedBy)
    : Result{LazyResult{std::move(idTables)}, std::move(sortedBy)} {}
#endif

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
IdTableView<0> Result::IdTableSharedLocalVocabPair::makeView(
    const std::variant<IdTable, std::shared_ptr<const IdTable>, IdTableView<0>>&
        idTableOrPtr) {
  return std::visit(
      [](const auto& arg) -> IdTableView<0> {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, IdTable>) {
          return arg.template asStaticView<0>();
        } else if constexpr (std::is_same_v<T,
                                            std::shared_ptr<const IdTable>>) {
          AD_CONTRACT_CHECK(arg != nullptr);
          return arg->template asStaticView<0>();
        } else {
          static_assert(std::is_same_v<T, IdTableView<0>>);
          return arg;
        }
      },
      idTableOrPtr);
}

// _____________________________________________________________________________
Result::IdTableSharedLocalVocabPair::IdTableSharedLocalVocabPair(
    IdTable idTable, std::shared_ptr<const LocalVocab> localVocab)
    : idTableOrPtr_{std::move(idTable)},
      localVocab_{std::move(localVocab)},
      view_{makeView(idTableOrPtr_)} {}

// _____________________________________________________________________________
Result::IdTableSharedLocalVocabPair::IdTableSharedLocalVocabPair(
    std::shared_ptr<const IdTable> idTablePtr,
    std::shared_ptr<const LocalVocab> localVocab)
    : idTableOrPtr_{std::move(idTablePtr)},
      localVocab_{std::move(localVocab)},
      view_{makeView(idTableOrPtr_)} {}

// _____________________________________________________________________________
Result::IdTableSharedLocalVocabPair::IdTableSharedLocalVocabPair(
    IdTableView<0> view, std::shared_ptr<const LocalVocab> localVocab)
    : idTableOrPtr_{view},
      localVocab_{std::move(localVocab)},
      view_{std::move(view)} {}

// _____________________________________________________________________________
void Result::IdTableSharedLocalVocabPair::applyLimitOffset(
    const LimitOffsetClause& limitOffset) {
  std::visit(
      [&limitOffset](auto& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, IdTable>) {
          resizeIdTable(arg, limitOffset);
        } else if constexpr (std::is_same_v<T,
                                            std::shared_ptr<const IdTable>>) {
          arg = std::make_shared<const IdTable>(
              makeResizedClone(*arg, limitOffset));
        } else {
          static_assert(std::is_same_v<T, IdTableView<0>>);
          size_t offset = limitOffset.actualOffset(arg.numRows());
          size_t size = limitOffset.actualSize(arg.numRows());
          arg = arg.subView(offset, size);
        }
      },
      idTableOrPtr_);
  // Refresh the view: the underlying data has changed.
  view_ = makeView(idTableOrPtr_);
}

// _____________________________________________________________________________
void Result::applyLimitOffset(
    const LimitOffsetClause& limitOffset,
    std::function<void(std::chrono::microseconds, const IdTableView<0>&)>
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
    std::get<IdTableSharedLocalVocabPair>(data_).applyLimitOffset(limitOffset);
    limitTimeCallback(limitTimer.msecs(), idTableView());
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
          limitTimeCallback(limitTimer.value(),
                            idTable.template asStaticView<0>());
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
    uint64_t numRows = idTableView().numRows();
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
  auto performCheck = [](const auto& map, const auto& idTable) {
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
    AD_EXPENSIVE_CHECK(performCheck(varColMap, idTableView()));
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
    std::function<void(GeneratorState)> onGeneratorFinished) {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  auto inputAsGet = ad_utility::CachingTransformInputRange(
      idTables(), [](auto& input) { return std::move(input); });
  using namespace ad_utility::timer;
  // We need the shared pointer, because we cannot easily have a lambda capture
  // refer to another lambda capture.
  auto sharedFinish = std::make_shared<decltype(onGeneratorFinished)>(
      std::move(onGeneratorFinished));

  // The main lambda that when being called processes the next chunk.
  auto get = [inputAsGet = std::move(inputAsGet), sharedFinish,
              cleanup = absl::Cleanup{[&finish = *sharedFinish]() noexcept {
                ad_utility::ignoreExceptionIfThrows(
                    [&finish]() { finish(GeneratorState::FINISHED); });
              }},
              onNewChunk = std::move(
                  onNewChunk)]() mutable -> std::optional<IdTableVocabPair> {
    try {
      Timer timer{Timer::Started};
      auto input = inputAsGet.get();
      if (!input.has_value()) {
        std::move(cleanup).Cancel();
        (*sharedFinish)(GeneratorState::FINISHED);
        return std::nullopt;
      }
      onNewChunk(input.value(), timer.value());
      return input;
    } catch (const ad_utility::CancellationException&) {
      std::move(cleanup).Cancel();
      (*sharedFinish)(GeneratorState::CANCELLED);
      throw;
    } catch (...) {
      std::move(cleanup).Cancel();
      (*sharedFinish)(GeneratorState::FAILED);
      throw;
    }
  };
  data_.emplace<GenContainer>(
      ad_utility::InputRangeFromGetCallable{std::move(get)});
}

// _____________________________________________________________________________
const IdTableView<0>& Result::idTableView() const {
  AD_CONTRACT_CHECK(isFullyMaterialized());
  return std::get<IdTableSharedLocalVocabPair>(data_).idTableView();
}

// _____________________________________________________________________________
IdTable Result::cloneIdTable() const { return idTableView().clone(); }

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
    AD_LOG_INFO << "Result has size " << idTableView().size() << " x "
                << idTableView().numColumns() << std::endl;
  } else {
    AD_LOG_INFO << "Result has unknown size (not computed yet)" << std::endl;
  }
}
