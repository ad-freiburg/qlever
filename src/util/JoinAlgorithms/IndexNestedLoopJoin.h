//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_JOINALGORITHMS_INDEXNESTEDLOOPJOIN_H
#define QLEVER_SRC_UTIL_JOINALGORITHMS_INDEXNESTEDLOOPJOIN_H

#include <vector>

#include "engine/CallFixedSize.h"
#include "engine/JoinHelpers.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "util/CancellationHandle.h"
#include "util/ChunkedForLoop.h"
#include "util/CompilerExtensions.h"
#include "util/Exception.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"

namespace joinAlgorithms::indexNestedLoop {

namespace detail {
// Helper class for `IndexNestedLoopJoin::matchLeft` that simply tracks which
// rows from the left have found a match so far.
struct Filler {
  // Should conceptually be bool, but doesn't allow the compiler to use
  // memset in `matchLeft`.
  std::vector<char, ad_utility::AllocatorWithLimit<char>> matchTracker_;

  explicit Filler(size_t size,
                  const ad_utility::AllocatorWithLimit<char>& allocator)
      : matchTracker_(size, 0, allocator) {}

  AD_ALWAYS_INLINE void track(size_t offset, size_t size, size_t) {
    AD_EXPENSIVE_CHECK(offset + size <= matchTracker_.size());
    ql::ranges::fill(matchTracker_.begin() + offset,
                     matchTracker_.begin() + offset + size, 1);
  }
};

// Helper class for `IndexNestedLoopJoin::matchLeft` tracks matches to be used
// by `OptionalJoin`.
struct Adder {
  std::vector<std::array<size_t, 2>> matchingPairs_;
  // Should conceptually be bool, but doesn't allow the compiler to use
  // memset in `matchLeft`.
  std::vector<char, ad_utility::AllocatorWithLimit<char>> missingIndices_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  size_t numJoinColumns_;
  bool keepJoinColumns_;

  explicit Adder(size_t size,
                 const ad_utility::AllocatorWithLimit<char>& allocator,
                 ad_utility::SharedCancellationHandle cancellationHandle,
                 size_t numJoinColumns, bool keepJoinColumns)
      : missingIndices_(size, true, allocator),
        cancellationHandle_{std::move(cancellationHandle)},
        numJoinColumns_{numJoinColumns},
        keepJoinColumns_{keepJoinColumns} {}

  AD_ALWAYS_INLINE void track(size_t offset, size_t size, size_t rightIndex) {
    for (size_t i = 0; i < size; i++) {
      matchingPairs_.push_back({offset + i, rightIndex});
    }
    ql::ranges::fill(missingIndices_.begin() + offset,
                     missingIndices_.begin() + offset + size, false);
  }

  // Turn collected indices in `matchingPairs_` and write them into an actual
  // result table and clear `matchingPairs_` afterwards.
  void materializeTables(IdTable& result, IdTableView<0> left,
                         IdTableView<0> right) {
    size_t originalSize = result.size();
    result.resize(originalSize + matchingPairs_.size());
    auto numColsInResult = left.numColumns() + right.numColumns() -
                           (1 + (!keepJoinColumns_)) * numJoinColumns_;
    AD_CORRECTNESS_CHECK(result.numColumns() == numColsInResult);
    ColumnIndex resultColIdx = 0;
    auto numColsToDrop =
        static_cast<size_t>(!keepJoinColumns_) * numJoinColumns_;
    for (auto source : ad_utility::OwningView{left.getColumns()} |
                           ql::views::drop(numColsToDrop)) {
      auto target = result.getColumn(resultColIdx);
      size_t offset = originalSize;
      for (const auto& [leftIdx, rightIdx] : matchingPairs_) {
        target[offset] = source[leftIdx];
        ++offset;
      }
      cancellationHandle_->throwIfCancelled();
      ++resultColIdx;
    }
    for (auto source : ad_utility::OwningView{right.getColumns()} |
                           ql::views::drop(numJoinColumns_)) {
      auto target = result.getColumn(resultColIdx);
      size_t offset = originalSize;
      for (const auto& [leftIdx, rightIdx] : matchingPairs_) {
        target[offset] = source[rightIdx];
        ++offset;
      }
      cancellationHandle_->throwIfCancelled();
      ++resultColIdx;
    }
    matchingPairs_.clear();
  }

  // Scan `missingIndices_` for indices that haven't found a match so far and
  // fill them with undef on the right side.
  void materializeMissing(IdTable& result, IdTableView<0> left) {
    size_t counter = std::reduce(missingIndices_.begin(), missingIndices_.end(),
                                 static_cast<size_t>(0));
    size_t originalSize = result.size();
    result.resize(originalSize + counter);
    ColumnIndex resultColIdx = 0;
    auto numColsToDrop =
        static_cast<size_t>(!keepJoinColumns_) * numJoinColumns_;
    for (auto source : ad_utility::OwningView{left.getColumns()} |
                           ql::views::drop(numColsToDrop)) {
      auto target = result.getColumn(resultColIdx);
      size_t targetIndex = originalSize;
      for (size_t i = 0; i < missingIndices_.size(); ++i) {
        if (missingIndices_[i]) {
          target[targetIndex] = source[i];
          ++targetIndex;
        }
      }
      cancellationHandle_->throwIfCancelled();
      ++resultColIdx;
    }
    for (auto col : ad_utility::OwningView{result.getColumns()} |
                        ql::views::drop(resultColIdx)) {
      ad_utility::chunkedFill(
          ql::ranges::subrange{col.begin() + originalSize, col.end()},
          Id::makeUndefined(), qlever::joinHelpers::CHUNK_SIZE,
          [this]() { cancellationHandle_->throwIfCancelled(); });
    }
  }
};

// Range to consume and transform a lazy result and apply the `OptionalJoin`
// algorithm to it. This does not preserve sort order.
template <typename ComputeMatches>
class OptionalJoinRange
    : public ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
  // Kept for correct lifetime.
  std::shared_ptr<const Result> leftResult_;
  std::shared_ptr<const Result> rightResult_;
  const LocalVocab& leftVocab_;
  const IdTable& leftTable_;
  Result::LazyResult rightTables_;
  Adder matchTracker_;
  size_t resultWidth_;
  ad_utility::JoinColumnMapping joinColumnData_;
  ComputeMatches computeMatches_;
  bool lastProcessed_ = false;

 public:
  OptionalJoinRange(std::shared_ptr<const Result> leftResult,
                    std::shared_ptr<const Result> rightResult,
                    const LocalVocab& leftVocab, const IdTable& leftTable,
                    Result::LazyResult rightTables, Adder matchTracker,
                    size_t resultWidth,
                    ad_utility::JoinColumnMapping joinColumnData,
                    ComputeMatches computeMatches)
      : leftResult_{std::move(leftResult)},
        rightResult_{std::move(rightResult)},
        leftVocab_{leftVocab},
        leftTable_{leftTable},
        rightTables_{std::move(rightTables)},
        matchTracker_{std::move(matchTracker)},
        resultWidth_{resultWidth},
        joinColumnData_{std::move(joinColumnData)},
        computeMatches_{std::move(computeMatches)} {}

  std::optional<Result::IdTableVocabPair> get() override {
    if (lastProcessed_) {
      return std::nullopt;
    }
    auto next = rightTables_.get();
    if (next.has_value()) {
      auto& [idTable, localVocab] = next.value();
      computeMatches_(matchTracker_, idTable);
      IdTable resultTable{resultWidth_, leftTable_.getAllocator()};
      matchTracker_.materializeTables(
          resultTable,
          leftTable_.asColumnSubsetView(joinColumnData_.permutationLeft()),
          idTable.asColumnSubsetView(joinColumnData_.permutationRight()));
      resultTable.setColumnSubset(joinColumnData_.permutationResult());
      localVocab.mergeWith(leftVocab_);
      return Result::IdTableVocabPair{std::move(resultTable),
                                      std::move(localVocab)};
    }
    lastProcessed_ = true;
    IdTable resultTable{resultWidth_, leftTable_.getAllocator()};
    matchTracker_.materializeMissing(
        resultTable,
        leftTable_.asColumnSubsetView(joinColumnData_.permutationLeft()));
    if (resultTable.empty()) {
      return std::nullopt;
    }
    resultTable.setColumnSubset(joinColumnData_.permutationResult());
    return Result::IdTableVocabPair{std::move(resultTable), leftVocab_.clone()};
  }
};
}  // namespace detail

// This class implements an index nested loop join using binary search to match
// entries. The benefit of this method over the "regular" join algorithms is
// that it doesn't require the right side to be sorted, potentially allowing you
// to skip an expensive sort operation entirely. The downside is that the left
// side has to be fully materialized. Currently handling undef values is
// unsupported. `matchLeft` can be used with different types to accommodate
// different types of joins.
class IndexNestedLoopJoin {
  std::vector<std::array<ColumnIndex, 2>> joinColumns_;
  std::shared_ptr<const Result> leftResult_;
  std::shared_ptr<const Result> rightResult_;

 public:
  IndexNestedLoopJoin(std::vector<std::array<ColumnIndex, 2>> joinColumns,
                      std::shared_ptr<const Result> leftResult,
                      std::shared_ptr<const Result> rightResult)
      : joinColumns_{std::move(joinColumns)},
        leftResult_{std::move(leftResult)},
        rightResult_{std::move(rightResult)} {
    AD_CONTRACT_CHECK(leftResult_->isFullyMaterialized());
  }

 private:
  // Checks with entries in `rightTable` match entries in `leftTable`, and
  // writes for the matching row indices on the left the value `true` into
  // `matchTracker`.
  template <typename T, int JOIN_COLUMNS>
  static void matchLeft(T& matchTracker, IdTableView<JOIN_COLUMNS> leftTable,
                        IdTableView<JOIN_COLUMNS> rightTable) {
    auto leftColumns = leftTable.getColumns();
    size_t rightIndex = 0;
    for (const auto& rightRow : rightTable) {
      size_t leftOffset = 0;
      size_t leftSize = leftTable.size();
      for (const auto& [rightId, leftCol] :
           ::ranges::zip_view(rightRow, leftColumns)) {
        AD_EXPENSIVE_CHECK(!rightId.isUndefined());
        auto currentStart = leftCol.begin() + leftOffset;
        auto subrange = ql::ranges::equal_range(
            currentStart, currentStart + leftSize, rightId, std::less{});
        leftSize = ql::ranges::size(subrange);
        if (subrange.empty()) {
          break;
        }
        leftOffset = ql::ranges::distance(leftCol.begin(), subrange.begin());
      }
      matchTracker.track(leftOffset, leftSize, rightIndex);
      ++rightIndex;
    }
  }

 public:
  // Main function for MINUS and EXISTS operations.
  std::vector<char, ad_utility::AllocatorWithLimit<char>> computeExistance() {
    detail::Filler matchTracker{
        leftResult_->idTable().size(),
        leftResult_->idTable().getAllocator().as<char>()};
    std::vector<ColumnIndex> leftColumns;
    std::vector<ColumnIndex> rightColumns;
    for (const auto& [leftCol, rightCol] : joinColumns_) {
      leftColumns.push_back(leftCol);
      rightColumns.push_back(rightCol);
    }
    ad_utility::callFixedSizeVi(
        static_cast<int>(joinColumns_.size()),
        [this, &matchTracker, &leftColumns, &rightColumns](auto JOIN_COLUMNS) {
          IdTableView<JOIN_COLUMNS> leftTable =
              leftResult_->idTable()
                  .asColumnSubsetView(leftColumns)
                  .template asStaticView<JOIN_COLUMNS>();
          auto matchHelper = [&matchTracker, &leftTable, &rightColumns,
                              &JOIN_COLUMNS](const IdTable& idTable) {
            matchLeft(matchTracker, leftTable,
                      idTable.asColumnSubsetView(rightColumns)
                          .template asStaticView<JOIN_COLUMNS>());
          };
          if (rightResult_->isFullyMaterialized()) {
            matchHelper(rightResult_->idTable());
          } else {
            for (const auto& [idTable, _] : rightResult_->idTables()) {
              matchHelper(idTable);
            }
          }
        });
    return std::move(matchTracker.matchTracker_);
  }

 public:
  // Main function for OPTIONAL operation.
  Result::LazyResult computeOptionalJoin(
      bool yieldOnce, size_t resultWidth,
      ad_utility::SharedCancellationHandle cancellationHandle,
      size_t numColsRight, bool keepJoinColumns) && {
    detail::Adder matchTracker{leftResult_->idTable().size(),
                               leftResult_->idTable().getAllocator().as<char>(),
                               std::move(cancellationHandle),
                               joinColumns_.size(), keepJoinColumns};
    return ad_utility::callFixedSizeVi(
        static_cast<int>(joinColumns_.size()),
        [this, &matchTracker, yieldOnce, resultWidth, numColsRight,
         keepJoinColumns](auto JOIN_COLUMNS) -> Result::LazyResult {
          const IdTable& leftTable = leftResult_->idTable();
          size_t numColsLeft = leftTable.numColumns();
          ad_utility::JoinColumnMapping joinColumnData{
              joinColumns_, numColsLeft, numColsRight, keepJoinColumns};
          IdTableView<JOIN_COLUMNS> leftTableView =
              leftTable.asColumnSubsetView(joinColumnData.jcsLeft())
                  .template asStaticView<JOIN_COLUMNS>();
          auto matchHelper = [&matchTracker, &leftTableView, &JOIN_COLUMNS,
                              rightColumns = joinColumnData.jcsRight()](
                                 const IdTable& idTable) {
            matchLeft(matchTracker, leftTableView,
                      idTable.asColumnSubsetView(rightColumns)
                          .template asStaticView<JOIN_COLUMNS>());
          };
          IdTable resultTable{resultWidth, leftTable.getAllocator()};
          LocalVocab mergedVocab = leftResult_->getCopyOfLocalVocab();
          if (rightResult_->isFullyMaterialized()) {
            matchHelper(rightResult_->idTable());
            matchTracker.materializeTables(
                resultTable,
                leftTable.asColumnSubsetView(joinColumnData.permutationLeft()),
                rightResult_->idTable().asColumnSubsetView(
                    joinColumnData.permutationRight()));
            matchTracker.materializeMissing(
                resultTable,
                leftTable.asColumnSubsetView(joinColumnData.permutationLeft()));
            mergedVocab.mergeWith(rightResult_->localVocab());
          } else if (yieldOnce) {
            for (const auto& [idTable, localVocab] : rightResult_->idTables()) {
              matchHelper(idTable);
              matchTracker.materializeTables(
                  resultTable,
                  leftTable.asColumnSubsetView(
                      joinColumnData.permutationLeft()),
                  idTable.asColumnSubsetView(
                      joinColumnData.permutationRight()));
              mergedVocab.mergeWith(localVocab);
            }
            matchTracker.materializeMissing(
                resultTable,
                leftTable.asColumnSubsetView(joinColumnData.permutationLeft()));
          } else {
            const LocalVocab& leftVocab = leftResult_->localVocab();
            auto rightTables = rightResult_->idTables();
            auto rightColumns = joinColumnData.jcsRight();
            return Result::LazyResult{detail::OptionalJoinRange{
                std::move(leftResult_), std::move(rightResult_), leftVocab,
                leftTable, std::move(rightTables), std::move(matchTracker),
                resultWidth, std::move(joinColumnData),
                [leftTableView = std::move(leftTableView),
                 rightColumns = std::move(rightColumns), JOIN_COLUMNS](
                    detail::Adder& adder, const IdTable& rightTable) {
                  matchLeft(adder, leftTableView,
                            rightTable.asColumnSubsetView(rightColumns)
                                .template asStaticView<JOIN_COLUMNS>());
                }}};
          }
          resultTable.setColumnSubset(joinColumnData.permutationResult());
          return Result::LazyResult{std::array{Result::IdTableVocabPair{
              std::move(resultTable), std::move(mergedVocab)}}};
        });
  }
};
}  // namespace joinAlgorithms::indexNestedLoop

#endif  // QLEVER_SRC_UTIL_JOINALGORITHMS_INDEXNESTEDLOOPJOIN_H
