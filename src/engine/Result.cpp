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
Result::Result(IdTable idTable, std::vector<ColumnIndex> sortedBy,
               SharedLocalVocabWrapper localVocab)
    : data_{std::move(idTable)},
      sortedBy_{std::move(sortedBy)},
      localVocab_{std::move(localVocab.localVocab_)} {
  AD_CONTRACT_CHECK(localVocab_ != nullptr);
  validateIdTable(std::get<IdTable>(data_), sortedBy_);
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
    : data_{ad_utility::CacheableGenerator{
          [](auto idTables,
             auto sortedBy) mutable -> cppcoro::generator<IdTable> {
            for (IdTable& idTable : idTables) {
              validateIdTable(idTable, sortedBy);
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
Result::Result(cppcoro::generator<const IdTable> idTables,
               std::vector<ColumnIndex> sortedBy, LocalVocabPtr localVocab)
    : data_{std::move(idTables)},
      sortedBy_{std::move(sortedBy)},
      localVocab_{std::move(localVocab)} {}

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
  AD_CONTRACT_CHECK(
      !std::holds_alternative<cppcoro::generator<const IdTable>>(data_));
  using Gen = ad_utility::CacheableGenerator<IdTable>;
  if (std::holds_alternative<IdTable>(data_)) {
    ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
    modifyIdTable(std::get<IdTable>(data_), limitOffset);
    limitTimeCallback(limitTimer.msecs());
  } else if (std::holds_alternative<Gen>(data_)) {
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
    }(std::move(std::get<Gen>(data_)).extractGenerator(), limitOffset,
        std::move(limitTimeCallback));
    data_.emplace<Gen>(std::move(generator));
  } else {
    AD_FAIL();
  }
}

// _____________________________________________________________________________
void Result::enforceLimitOffset(const LimitOffsetClause& limitOffset) {
  AD_CONTRACT_CHECK(
      !std::holds_alternative<cppcoro::generator<const IdTable>>(data_));
  using Gen = ad_utility::CacheableGenerator<IdTable>;
  if (std::holds_alternative<IdTable>(data_)) {
    AD_CONTRACT_CHECK(idTable().numRows() ==
                      limitOffset.actualSize(idTable().numRows()));
  } else if (std::holds_alternative<Gen>(data_)) {
    auto generator =
        [](cppcoro::generator<IdTable> original,
           LimitOffsetClause limitOffset) -> cppcoro::generator<IdTable> {
      size_t elementCount = 0;
      for (auto&& idTable : original) {
        elementCount += idTable.numRows();
        AD_CONTRACT_CHECK(elementCount <= limitOffset.actualSize(elementCount));
        co_yield std::move(idTable);
      }
      AD_CONTRACT_CHECK(elementCount == limitOffset.actualSize(elementCount));
    }(std::move(std::get<Gen>(data_)).extractGenerator(), limitOffset);
    data_.emplace<Gen>(std::move(generator));
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
  auto& idTable = std::get<IdTable>(data_);
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
  return std::get<IdTable>(data_);
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
      data_);
}

// _____________________________________________________________________________
bool Result::isDataEvaluated() const {
  return std::holds_alternative<IdTable>(data_);
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

ad_utility::MemorySize Result::getCurrentSize() const {
  auto calculateSize = [](const IdTable& idTable) {
    return ad_utility::MemorySize::bytes(idTable.size() * idTable.numColumns() *
                                         sizeof(Id));
  };
  if (isDataEvaluated()) {
    return calculateSize(idTable());
  } else {
    using Gen = ad_utility::CacheableGenerator<IdTable>;
    // This should only ever get called on the "wrapped" generator stored in the
    // cache.
    AD_CONTRACT_CHECK(std::holds_alternative<Gen>(data_));
    ad_utility::MemorySize totalMemory = 0_B;
    std::get<Gen>(data_).forEachCachedValue(
        [&totalMemory, &calculateSize](const IdTable& idTable) {
          totalMemory += calculateSize(idTable);
        });
    return totalMemory;
  }
}

// _____________________________________________________________________________
void Result::setOnSizeChanged(std::function<bool(bool)> onSizeChanged) {
  using Gen = ad_utility::CacheableGenerator<IdTable>;
  // This should only ever get called on the "wrapped" generator stored in the
  // cache.
  AD_CONTRACT_CHECK(std::holds_alternative<Gen>(data_));
  std::get<Gen>(data_).setOnSizeChanged(std::move(onSizeChanged));
}

// _____________________________________________________________________________
void Result::setOnGeneratorFinished(
    std::function<void(bool)> onGeneratorFinished) {
  using Gen = ad_utility::CacheableGenerator<IdTable>;
  // This should only ever get called on the "wrapped" generator stored in the
  // cache.
  AD_CONTRACT_CHECK(std::holds_alternative<Gen>(data_));
  std::get<Gen>(data_).setOnGeneratorFinished(std::move(onGeneratorFinished));
}

// _____________________________________________________________________________
void Result::setOnNextChunkComputed(
    std::function<void(std::chrono::milliseconds)> onNextChunkComputed) {
  using Gen = ad_utility::CacheableGenerator<IdTable>;
  // This should only ever get called on the "wrapped" generator stored in the
  // cache.
  AD_CONTRACT_CHECK(std::holds_alternative<Gen>(data_));
  std::get<Gen>(data_).setOnNextChunkComputed(std::move(onNextChunkComputed));
}

Result Result::aggregateTable() const {
  using Gen = ad_utility::CacheableGenerator<IdTable>;
  AD_CONTRACT_CHECK(std::holds_alternative<Gen>(data_));
  size_t totalRows = 0;
  size_t numCols = 0;
  std::optional<IdTable::Allocator> allocator;
  std::get<Gen>(data_).forEachCachedValue(
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
  std::get<Gen>(data_).forEachCachedValue([&idTable](const IdTable& table) {
    idTable.insertAtEnd(table.begin(), table.end());
  });
  return Result{std::move(idTable), sortedBy_,
                SharedLocalVocabWrapper{localVocab_}};
}

// _____________________________________________________________________________
Result Result::createResultWithFallback(
    std::shared_ptr<const Result> original, std::function<Result()> fallback,
    std::function<void(std::chrono::milliseconds)> onIteration) {
  AD_CONTRACT_CHECK(!original->isDataEvaluated());
  auto generator = [](std::shared_ptr<const Result> sharedResult,
                      std::function<Result()> fallback,
                      auto onIteration) -> cppcoro::generator<const IdTable> {
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
    auto start = std::chrono::steady_clock::now();
    for (auto&& idTable : freshResult.idTables()) {
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
      original->sortedBy_, original->localVocab_};
}

// _____________________________________________________________________________
Result Result::createResultAsMasterConsumer(
    std::shared_ptr<const Result> original, std::function<void()> onIteration) {
  using Gen = ad_utility::CacheableGenerator<IdTable>;
  AD_CONTRACT_CHECK(std::holds_alternative<Gen>(original->data_));
  auto generator = [](auto original,
                      auto onIteration) -> cppcoro::generator<const IdTable> {
    using ad_utility::IteratorWrapper;
    auto& generator = std::get<Gen>(original->data_);
    for (const IdTable& idTable : IteratorWrapper{generator, true}) {
      if (onIteration) {
        onIteration();
      }
      co_yield idTable;
    }
  };
  return Result{generator(original, std::move(onIteration)),
                original->sortedBy_, original->localVocab_};
}
