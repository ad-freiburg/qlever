// Copyright 2015 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/Result.h"

#include "engine/LocalVocab.h"
#include "util/Exception.h"
#include "util/IteratorWrapper.h"
#include "util/Log.h"
#include "util/Timer.h"

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
ProtoResult::ProtoResult(IdTable idTable, std::vector<ColumnIndex> sortedBy,
                         SharedLocalVocabWrapper localVocab)
    : storage_{StorageType{std::move(idTable), std::move(sortedBy),
                           std::move(localVocab.localVocab_)}} {
  AD_CONTRACT_CHECK(storage_.localVocab_ != nullptr);
  validateIdTable(storage_.idTable(), storage_.sortedBy_);
}

// _____________________________________________________________________________
ProtoResult::ProtoResult(IdTable idTable, std::vector<ColumnIndex> sortedBy,
                         LocalVocab&& localVocab)
    : ProtoResult{std::move(idTable), std::move(sortedBy),
                  SharedLocalVocabWrapper{std::move(localVocab)}} {}

// _____________________________________________________________________________
ProtoResult::ProtoResult(cppcoro::generator<IdTable> idTables,
                         std::vector<ColumnIndex> sortedBy,
                         SharedLocalVocabWrapper localVocab)
    : storage_{
          StorageType{[](auto idTables,
                         auto sortedBy) mutable -> cppcoro::generator<IdTable> {
                        for (IdTable& idTable : idTables) {
                          validateIdTable(idTable, sortedBy);
                          co_yield std::move(idTable);
                        }
                      }(std::move(idTables), sortedBy),
                      std::move(sortedBy), std::move(localVocab.localVocab_)}} {
  AD_CONTRACT_CHECK(storage_.localVocab_ != nullptr);
}

// _____________________________________________________________________________
ProtoResult::ProtoResult(cppcoro::generator<IdTable> idTables,
                         std::vector<ColumnIndex> sortedBy,
                         LocalVocab&& localVocab)
    : ProtoResult{std::move(idTables), std::move(sortedBy),
                  SharedLocalVocabWrapper{std::move(localVocab)}} {}

// _____________________________________________________________________________
void ProtoResult::applyLimitOffset(
    const LimitOffsetClause& limitOffset,
    std::function<void(std::chrono::milliseconds)> limitTimeCallback) {
  // Apply the OFFSET clause. If the offset is `0` or the offset is larger
  // than the size of the `IdTable`, then this has no effect and runtime
  // `O(1)` (see the docs for `std::shift_left`).
  AD_CONTRACT_CHECK(limitTimeCallback);
  if (storage_.isDataEvaluated()) {
    ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
    modifyIdTable(storage_.idTable(), limitOffset);
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
    }(std::move(storage_.idTables()), limitOffset,
        std::move(limitTimeCallback));
    storage_.idTables() = std::move(generator);
  }
}

// _____________________________________________________________________________
void ProtoResult::enforceLimitOffset(const LimitOffsetClause& limitOffset) {
  if (storage_.isDataEvaluated()) {
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
    }(std::move(storage_.idTables()), limitOffset);
    storage_.idTables() = std::move(generator);
  }
}

// _____________________________________________________________
void ProtoResult::checkDefinedness(const VariableToColumnMap& varColMap) {
  auto performCheck =
      [](const auto& map,
         std::optional<DatatypeCountsPerColumn>& datatypeCountsPerColumn,
         IdTable& idTable) {
        const auto& datatypesPerColumn = getOrComputeDatatypeCountsPerColumn(
            datatypeCountsPerColumn, idTable);
        return std::ranges::all_of(map, [&](const auto& varAndCol) {
          const auto& [columnIndex, mightContainUndef] = varAndCol.second;
          bool hasUndefined =
              datatypesPerColumn.at(columnIndex)
                  .at(static_cast<size_t>(Datatype::Undefined)) != 0;
          return mightContainUndef ==
                     ColumnIndexAndTypeInfo::PossiblyUndefined ||
                 !hasUndefined;
        });
      };
  if (isDataEvaluated()) {
    std::optional<DatatypeCountsPerColumn> datatypeCountsPerColumn;
    AD_EXPENSIVE_CHECK(
        performCheck(varColMap, datatypeCountsPerColumn, storage_.idTable()));
  } else {
    auto generator = [](cppcoro::generator<IdTable> original,
                        VariableToColumnMap varColMap,
                        auto performCheck) -> cppcoro::generator<IdTable> {
      std::optional<DatatypeCountsPerColumn> datatypeCountsPerColumn;
      for (auto&& idTable : original) {
        AD_EXPENSIVE_CHECK(
            performCheck(varColMap, datatypeCountsPerColumn, idTable));
        co_yield std::move(idTable);
      }
    }(std::move(storage_.idTables()), varColMap, std::move(performCheck));
    storage_.idTables() = std::move(generator);
  }
}

// _____________________________________________________________________________
auto ProtoResult::getOrComputeDatatypeCountsPerColumn(
    std::optional<DatatypeCountsPerColumn>& datatypeCountsPerColumn,
    IdTable& idTable) -> const DatatypeCountsPerColumn& {
  if (datatypeCountsPerColumn.has_value()) {
    return datatypeCountsPerColumn.value();
  }
  auto& types = datatypeCountsPerColumn.emplace();
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

// _____________________________________________________________________________
void ProtoResult::validateIdTable(const IdTable& idTable,
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
const IdTable& ProtoResult::idTable() const { return storage_.idTable(); }

// _____________________________________________________________________________
bool ProtoResult::isDataEvaluated() const noexcept {
  return storage_.isDataEvaluated();
}
// _____________________________________________________________________________
CacheableResult::CacheableResult(ProtoResult protoResult)
    : storage_{StorageType{
          protoResult.isDataEvaluated()
              ? decltype(StorageType::data_){std::move(
                    protoResult.storage_.idTable())}
              : decltype(StorageType::data_){ad_utility::CacheableGenerator{
                    std::move(protoResult.storage_.idTables())}},
          std::move(protoResult.storage_.sortedBy_),
          std::move(protoResult.storage_.localVocab_),
      }} {}

// _____________________________________________________________________________
ad_utility::MemorySize CacheableResult::getCurrentSize() const {
  auto calculateSize = [](const IdTable& idTable) {
    return ad_utility::MemorySize::bytes(idTable.size() * idTable.numColumns() *
                                         sizeof(Id));
  };
  if (storage_.isDataEvaluated()) {
    return calculateSize(idTable());
  } else {
    ad_utility::MemorySize totalMemory = 0_B;
    storage_.idTables().forEachCachedValue(
        [&totalMemory, &calculateSize](const IdTable& idTable) {
          totalMemory += calculateSize(idTable);
        });
    return totalMemory;
  }
}

// _____________________________________________________________________________
void CacheableResult::setOnSizeChanged(
    std::function<bool(bool)> onSizeChanged) {
  storage_.idTables().setOnSizeChanged(std::move(onSizeChanged));
}

// _____________________________________________________________________________
void CacheableResult::setOnGeneratorFinished(
    std::function<void(bool)> onGeneratorFinished) {
  storage_.idTables().setOnGeneratorFinished(std::move(onGeneratorFinished));
}

// _____________________________________________________________________________
void CacheableResult::setOnNextChunkComputed(
    std::function<void(std::chrono::milliseconds)> onNextChunkComputed) {
  storage_.idTables().setOnNextChunkComputed(std::move(onNextChunkComputed));
}

// _____________________________________________________________________________
ProtoResult CacheableResult::aggregateTable() const {
  size_t totalRows = 0;
  size_t numCols = 0;
  std::optional<IdTable::Allocator> allocator;
  storage_.idTables().forEachCachedValue(
      [&totalRows, &numCols, &allocator](const IdTable& table) {
        totalRows += table.numRows();
        if (numCols == 0) {
          numCols = table.numColumns();
        }
        if (!allocator.has_value()) {
          allocator = table.getAllocator();
        }
      });
  IdTable idTable{
      numCols, std::move(allocator).value_or(makeAllocatorWithLimit<Id>(0_B))};
  idTable.reserve(totalRows);
  storage_.idTables().forEachCachedValue([&idTable](const IdTable& table) {
    idTable.insertAtEnd(table.begin(), table.end());
  });
  return ProtoResult{
      std::move(idTable), storage_.sortedBy_,
      ProtoResult::SharedLocalVocabWrapper{storage_.localVocab_}};
}

// _____________________________________________________________________________
const IdTable& CacheableResult::idTable() const { return storage_.idTable(); }

// _____________________________________________________________________________
bool CacheableResult::isDataEvaluated() const noexcept {
  return storage_.isDataEvaluated();
}

// _____________________________________________________________________________
Result::Result(std::shared_ptr<const IdTable> idTable,
               std::vector<ColumnIndex> sortedBy, LocalVocabPtr localVocab)
    : storage_{StorageType{std::move(idTable), std::move(sortedBy),
                           std::move(localVocab)}} {}

// _____________________________________________________________________________
Result::Result(cppcoro::generator<const IdTable> idTables,
               std::vector<ColumnIndex> sortedBy, LocalVocabPtr localVocab)
    : storage_{StorageType{std::move(idTables), std::move(sortedBy),
                           std::move(localVocab)}} {}

// _____________________________________________________________________________
const IdTable& Result::idTable() const { return *storage_.idTable(); }

// _____________________________________________________________________________
cppcoro::generator<const IdTable>& Result::idTables() const {
  return storage_.idTables();
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
bool Result::isDataEvaluated() const noexcept {
  return storage_.isDataEvaluated();
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
Result Result::createResultWithFullyEvaluatedIdTable(
    std::shared_ptr<const CacheableResult> cacheableResult) {
  AD_CONTRACT_CHECK(cacheableResult->isDataEvaluated());
  auto sortedBy = cacheableResult->storage_.sortedBy_;
  auto localVocab = cacheableResult->storage_.localVocab_;
  const IdTable* tablePointer = &cacheableResult->idTable();
  return Result{
      std::shared_ptr<const IdTable>{std::move(cacheableResult), tablePointer},
      std::move(sortedBy), std::move(localVocab)};
}

// _____________________________________________________________________________
Result Result::createResultWithFallback(
    std::shared_ptr<const CacheableResult> original,
    std::function<ProtoResult()> fallback,
    std::function<void(std::chrono::milliseconds)> onIteration) {
  AD_CONTRACT_CHECK(!original->isDataEvaluated());
  auto generator = [](std::shared_ptr<const CacheableResult> sharedResult,
                      std::function<ProtoResult()> fallback,
                      auto onIteration) -> cppcoro::generator<const IdTable> {
    size_t index = 0;
    try {
      for (auto&& idTable : sharedResult->storage_.idTables()) {
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
    ProtoResult freshResult = fallback();
    // If data is evaluated this means that this process is not deterministic
    // or that there's a wrong callback used here.
    AD_CORRECTNESS_CHECK(!freshResult.isDataEvaluated());
    auto start = std::chrono::steady_clock::now();
    for (auto&& idTable : freshResult.storage_.idTables()) {
      auto stop = std::chrono::steady_clock::now();
      if (onIteration) {
        onIteration(std::chrono::duration_cast<std::chrono::milliseconds>(
            stop - start));
      }
      if (index > 0) {
        index--;
        continue;
      }
      co_yield idTable;
      start = std::chrono::steady_clock::now();
    }
    auto stop = std::chrono::steady_clock::now();
    if (onIteration) {
      onIteration(
          std::chrono::duration_cast<std::chrono::milliseconds>(stop - start));
    }
  };
  return Result{
      generator(original, std::move(fallback), std::move(onIteration)),
      original->storage_.sortedBy_, original->storage_.localVocab_};
}

// _____________________________________________________________________________
Result Result::createResultAsMasterConsumer(
    std::shared_ptr<const CacheableResult> original,
    std::function<void()> onIteration) {
  AD_CONTRACT_CHECK(!original->isDataEvaluated());
  auto generator = [](auto original,
                      auto onIteration) -> cppcoro::generator<const IdTable> {
    using ad_utility::IteratorWrapper;
    auto& generator = original->storage_.idTables();
    for (const IdTable& idTable : IteratorWrapper{generator, true}) {
      if (onIteration) {
        onIteration();
      }
      co_yield idTable;
    }
  };
  auto sortedBy = original->storage_.sortedBy_;
  auto localVocab = original->storage_.localVocab_;
  return Result{generator(std::move(original), std::move(onIteration)),
                std::move(sortedBy), std::move(localVocab)};
}
