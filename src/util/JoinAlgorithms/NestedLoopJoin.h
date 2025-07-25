//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_JOINALGORITHMS_NESTEDLOOPJOIN_H
#define QLEVER_SRC_UTIL_JOINALGORITHMS_NESTEDLOOPJOIN_H

#include <vector>

#include "engine/CallFixedSize.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "util/Exception.h"

class NestedLoopJoin {
  std::vector<std::array<ColumnIndex, 2>> joinColumns_;
  std::shared_ptr<const Result> leftResult_;
  std::shared_ptr<const Result> rightResult_;

  const IdTable& leftTable_;

  std::optional<Result::IdTableVocabPair> currentRightTable_ = std::nullopt;

 public:
  NestedLoopJoin(std::vector<std::array<ColumnIndex, 2>> joinColumns,
                 std::shared_ptr<const Result> leftResult,
                 std::shared_ptr<const Result> rightResult)
      : joinColumns_{std::move(joinColumns)},
        leftResult_{std::move(leftResult)},
        rightResult_{std::move(rightResult)},
        leftTable_{leftResult_->idTable()} {}

 private:
  template <int JOIN_COLUMNS>
  void matchLeft(std::vector<size_t>& matchCounter,
                 IdTableView<JOIN_COLUMNS> leftTable,
                 IdTableView<JOIN_COLUMNS> rightTable) {
    AD_CORRECTNESS_CHECK(matchCounter.size() == leftTable.size());
    for (const auto& rightRow : rightTable) {
      size_t leftOffset = 0;
      size_t leftSize = leftTable.size();
      for (const auto& [rightId, leftCol] :
           ::ranges::zip_view(rightRow, leftTable.getColumns())) {
        AD_CORRECTNESS_CHECK(!rightId.isUndefined());
        ql::ranges::subrange rangeToCheck{
            leftCol.begin() + leftOffset,
            leftCol.begin() + leftOffset + leftSize};
        auto subrange =
            ql::ranges::equal_range(rangeToCheck, rightId, std::less{});
        leftSize = ql::ranges::size(subrange);
        if (subrange.empty()) {
          break;
        }
        leftOffset = ql::ranges::distance(leftCol.begin(), subrange.begin());
      }
      for (size_t i = 0; i < leftSize; ++i) {
        ++matchCounter[leftOffset + i];
      }
    }
  }

 public:
  std::vector<size_t> computeCounter() {
    return ad_utility::callFixedSize(
        static_cast<int>(joinColumns_.size()), [this]<int JOIN_COLUMNS>() {
          std::vector<size_t> matchCounter(leftTable_.size(), 0);
          std::array<ColumnIndex, JOIN_COLUMNS> leftColumns;
          std::array<ColumnIndex, JOIN_COLUMNS> rightColumns;
          size_t idx = 0;
          for (const auto& [leftCol, rightCol] : joinColumns_) {
            leftColumns[idx] = leftCol;
            rightColumns[idx] = rightCol;
            ++idx;
          }
          IdTableView<JOIN_COLUMNS> leftTable =
              leftTable_.asColumnSubsetView(leftColumns)
                  .template asStaticView<JOIN_COLUMNS>();
          auto matchHelper = [this, &matchCounter, &leftTable,
                              &rightColumns](const IdTable& idTable) {
            this->matchLeft(matchCounter, leftTable,
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
          return matchCounter;
        });
  }
};

#endif  // QLEVER_SRC_UTIL_JOINALGORITHMS_NESTEDLOOPJOIN_H
