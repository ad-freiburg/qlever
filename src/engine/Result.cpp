// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/Result.h"

#include <absl/cleanup/cleanup.h>

#include "util/Exception.h"
#include "util/Generators.h"
#include "util/InputRangeUtils.h"
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

struct RunOnNewChunkComputedGenerator
    : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
  std::unique_ptr<ad_utility::InputRangeFromGet<Result::IdTableVocabPair>>
      original_;
  std::function<void(const Result::IdTableVocabPair&,
                     std::chrono::microseconds)>
      onNewChunk_;
  std::function<void(bool)> onGeneratorFinished_;
  ad_utility::timer::Timer timer_;
  bool cleanup_ = true;

  // template<typename ViewT, typename LambdaT>
  // static
  // std::unique_ptr<ad_utility::InputRangeFromGet<Result::IdTableVocabPair>>
  // makeInputRange(ViewT view, LambdaT transform) {
  //     return std::make_unique<ad_utility::CachingTransformInputRange<ViewT,
  //     LambdaT>>(std::move(view), std::move(transform));
  // }

  RunOnNewChunkComputedGenerator(Result::LazyResult&& original, auto onNewChunk,
                                 auto onGeneratorFinished)
      : original_(new ad_utility::CachingTransformInputRange(
            std::move(original),
            [](Result::IdTableVocabPair& pair) { return std::move(pair); })),
        onNewChunk_(std::move(onNewChunk)),
        onGeneratorFinished_(std::move(onGeneratorFinished)),
        timer_(ad_utility::timer::Timer::Stopped) {}

  RunOnNewChunkComputedGenerator(
      std::unique_ptr<ad_utility::InputRangeFromGet<Result::IdTableVocabPair>>
          original,
      auto onNewChunk, auto onGeneratorFinished)
      : original_(std::move(original)),
        onNewChunk_(std::move(onNewChunk)),
        onGeneratorFinished_(std::move(onGeneratorFinished)),
        timer_(ad_utility::timer::Timer::Stopped) {}

  ~RunOnNewChunkComputedGenerator() { finish(); }

  RunOnNewChunkComputedGenerator(const RunOnNewChunkComputedGenerator&) =
      delete;
  RunOnNewChunkComputedGenerator& operator=(
      const RunOnNewChunkComputedGenerator&) = delete;
  RunOnNewChunkComputedGenerator(RunOnNewChunkComputedGenerator&& other)
      : RunOnNewChunkComputedGenerator(std::move(other.original_),
                                       std::move(other.onNewChunk_),
                                       std::move(other.onGeneratorFinished_)) {
    other.cleanup_ = false;
  }
  RunOnNewChunkComputedGenerator& operator=(
      RunOnNewChunkComputedGenerator&& other) {
    if (this != &other) {
      ad_utility::InputRangeFromGet<Result::IdTableVocabPair>::operator=(
          std::move(other));
      this->original_ = std::move(other.original_);
      this->onNewChunk_ = std::move(other.onNewChunk_);
      this->onGeneratorFinished_ = std::move(other.onGeneratorFinished_);
      this->timer_ =
          ad_utility::timer::Timer(ad_utility::timer::Timer::Stopped);
      other.cleanup_ = false;
    }
    return *this;
  }

  std::optional<Result::IdTableVocabPair> get() {
    try {
      timer_.start();
      auto opt_res = original_->get();
      if (!opt_res.has_value()) {
        finish();
        return std::nullopt;
      }
      Result::IdTableVocabPair& pair = opt_res.value();
      onNewChunk_(pair, timer_.value());
      return std::optional{std::move(pair)};
    } catch (...) {
      cleanup_ = false;
      onGeneratorFinished_(true);
      throw;
    }
  }

  void finish() {
    if (cleanup_) {
      onGeneratorFinished_(false);
      cleanup_ = false;
    }
  }
};
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
    : data_{GenContainer{LazyResult{ad_utility::CachingTransformInputRange(
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
          })}}},
      sortedBy_{std::move(sortedBy)} {}

// _____________________________________________________________________________
// Apply `LimitOffsetClause` to given `IdTable`.
void resizeIdTable(IdTable& idTable, const LimitOffsetClause& limitOffset) {
  ql::ranges::for_each(
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
    resizeIdTable(std::get<IdTableSharedLocalVocabPair>(data_).idTable_,
                  limitOffset);
    limitTimeCallback(limitTimer.msecs(), idTable());
  } else {
    ad_utility::CachingContinuableTransformInputRange generator{
        std::move(idTables()),
        [limitOffset = limitOffset,
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
    data_.emplace<GenContainer>(LazyResult(std::move(generator)));
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
        std::move(idTables()),
        [limit = limitOffset._limit,
         elementCount = uint64_t{0}](Result::IdTableVocabPair& pair) mutable {
          elementCount += pair.idTable_.numRows();
          AD_CONTRACT_CHECK(!limit.has_value() ||
                            elementCount <= limit.value());
          return std::move(pair);
        }};
    data_.emplace<GenContainer>(LazyResult(std::move(generator)));
  }
}

// _____________________________________________________________________________
void Result::checkDefinedness(const VariableToColumnMap& varColMap) {
  auto performCheck = [](const auto& map, IdTable& idTable) {
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
    AD_EXPENSIVE_CHECK(performCheck(
        varColMap, std::get<IdTableSharedLocalVocabPair>(data_).idTable_));
  } else {
    ad_utility::CachingTransformInputRange generator{
        std::move(idTables()),
        [varColMap = varColMap, performCheck = std::move(performCheck)](
            Result::IdTableVocabPair& pair) {
          AD_EXPENSIVE_CHECK(performCheck_(varColMap, pair.idTable_));
          return std::move(pair);
        }};
    data_.emplace<GenContainer>(LazyResult(std::move(generator)));
  }
}

// _____________________________________________________________________________
void Result::runOnNewChunkComputed(
    std::function<void(const IdTableVocabPair&, std::chrono::microseconds)>
        onNewChunk,
    std::function<void(bool)> onGeneratorFinished) {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  RunOnNewChunkComputedGenerator generator{std::move(idTables()),
                                           std::move(onNewChunk),
                                           std::move(onGeneratorFinished)};
  data_.emplace<GenContainer>(LazyResult(std::move(generator)));
}

// _____________________________________________________________________________
const IdTable& Result::idTable() const {
  AD_CONTRACT_CHECK(isFullyMaterialized());
  return std::get<IdTableSharedLocalVocabPair>(data_).idTable_;
}

// _____________________________________________________________________________
Result::LazyResult& Result::idTables() const {
  AD_CONTRACT_CHECK(!isFullyMaterialized());
  const auto& container = std::get<GenContainer>(data_);
  AD_CONTRACT_CHECK(!container.consumed_->exchange(true));
  return container.generator_;
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
      std::move(idTables()),
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
    LOG(INFO) << "Result has size " << idTable().size() << " x "
              << idTable().numColumns() << std::endl;
  } else {
    LOG(INFO) << "Result has unknown size (not computed yet)" << std::endl;
  }
}
