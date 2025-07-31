//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_JOINALGORITHMS_INDEXNESTEDLOOPJOIN_H
#define QLEVER_SRC_UTIL_JOINALGORITHMS_INDEXNESTEDLOOPJOIN_H

#include <vector>

#include "engine/CallFixedSize.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "util/CompilerExtensions.h"
#include "util/Exception.h"

// This class implements an index nested loop join using binary search to match
// entries. The benefit of this method over the "regular" join algorithms is
// that it doesn't require the right side to be sorted, potentially allowing you
// to skip an expensive sort operation entirely. The downside is that the left
// side has to be fully materialized. Currently handling undef values is
// unsupported.
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
  template <int JOIN_COLUMNS>
  static void matchLeft(std::vector<char>& matchTracker,
                        IdTableView<JOIN_COLUMNS> leftTable,
                        IdTableView<JOIN_COLUMNS> rightTable) {
    AD_CORRECTNESS_CHECK(matchTracker.size() == leftTable.size());
    auto leftColumns = leftTable.getColumns();
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
      ql::ranges::fill(matchTracker.begin() + leftOffset,
                       matchTracker.begin() + leftOffset + leftSize, 1);
    }
  }

 public:
  std::vector<char> computeTracker() {
    // Should conceptually be bool, but doesn't allow the compiler to use
    // memset in `matchLeft`.
    std::vector<char> matchTracker(leftResult_->idTable().size(), 0);
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
    return matchTracker;
  }
};

#endif  // QLEVER_SRC_UTIL_JOINALGORITHMS_INDEXNESTEDLOOPJOIN_H
