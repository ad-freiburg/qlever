//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_JOINALGORITHMS_SPECIALOPTIONALJOINFORBLOCKS_H
#define QLEVER_SRC_UTIL_JOINALGORITHMS_SPECIALOPTIONALJOINFORBLOCKS_H

#include "util/JoinAlgorithms/JoinAlgorithms.h"

namespace ad_utility {

/**
 * @brief Perform a special optional join for input ranges of blocks.
 * This is a simplified implementation that works on blocks of row-like data.
 * Preconditions:
 * - Right input contains no UNDEF values
 * - Left input only contains UNDEF in the last column
 * - Both inputs are sorted lexicographically
 */
template <typename LeftBlocks, typename RightBlocks,
          typename CompatibleRowAction, typename NotFoundAction,
          typename CheckCancellation = Noop>
void specialOptionalJoinForBlocks(LeftBlocks&& leftBlocks,
                                  RightBlocks&& rightBlocks,
                                  CompatibleRowAction& compatibleRowAction,
                                  NotFoundAction& elFromFirstNotFoundAction,
                                  CheckCancellation checkCancellation = {}) {
  auto leftIt = ql::ranges::begin(leftBlocks);
  auto leftEnd = ql::ranges::end(leftBlocks);
  auto rightIt = ql::ranges::begin(rightBlocks);
  auto rightEnd = ql::ranges::end(rightBlocks);

  // Skip empty left blocks.
  while (leftIt != leftEnd && leftIt->empty()) {
    ++leftIt;
  }

  // If left is empty, nothing to do.
  if (leftIt == leftEnd) {
    return;
  }

  // Skip empty right blocks.
  while (rightIt != rightEnd && rightIt->empty()) {
    ++rightIt;
  }

  // If right is empty, all left rows are non-matching.
  if (rightIt == rightEnd) {
    for (auto blockIt = leftIt; blockIt != leftEnd; ++blockIt) {
      const auto& block = *blockIt;
      if (block.empty()) continue;
      compatibleRowAction.setOnlyLeftInputForOptionalJoin(block);
      for (auto it = block.begin(); it != block.end(); ++it) {
        elFromFirstNotFoundAction(it);
      }
    }
    compatibleRowAction.flush();
    return;
  }

  // Helper to get number of columns.
  auto numCols = [](const auto& row) { return row.size(); };

  // Compare all-but-last columns.
  auto compareAllButLast = [&numCols](const auto& a, const auto& b) {
    size_t cols = numCols(a);
    for (size_t i = 0; i < cols - 1; ++i) {
      if (a[i] != b[i]) {
        return a[i] < b[i];
      }
    }
    return false;
  };

  auto compareEqButLast = [&numCols](const auto& a, const auto& b) {
    size_t cols = numCols(a);
    for (size_t i = 0; i < cols - 1; ++i) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  };

  // Process blocks.
  while (leftIt != leftEnd && rightIt != rightEnd) {
    checkCancellation();

    const auto& leftBlock = *leftIt;
    const auto& rightBlock = *rightIt;

    if (leftBlock.empty()) {
      ++leftIt;
      continue;
    }
    if (rightBlock.empty()) {
      ++rightIt;
      continue;
    }

    compatibleRowAction.setInput(leftBlock, rightBlock);

    auto it1 = leftBlock.begin();
    auto end1 = leftBlock.end();
    auto it2 = rightBlock.begin();
    auto end2 = rightBlock.end();

    while (it1 < end1 && it2 < end2) {
      checkCancellation();

      // Skip right rows that are smaller.
      it2 = std::find_if_not(it2, end2, [&](const auto& row) {
        return compareAllButLast(row, *it1);
      });
      if (it2 == end2) {
        break;
      }

      // Skip left rows that are smaller, mark as non-matching.
      auto next1 = std::find_if_not(it1, end1, [&](const auto& row) {
        return compareAllButLast(row, *it2);
      });
      for (auto it = it1; it != next1; ++it) {
        elFromFirstNotFoundAction(it);
      }
      it1 = next1;
      if (it1 == end1) {
        break;
      }

      checkCancellation();

      // Find ranges where all-but-last columns match.
      auto endSame1 = std::find_if_not(it1, end1, [&](const auto& row) {
        return compareEqButLast(row, *it2);
      });
      auto endSame2 = std::find_if_not(it2, end2, [&](const auto& row) {
        return compareEqButLast(*it1, row);
      });

      if (endSame1 == it1) {
        continue;
      }

      // For matching all-but-last columns, join with all matching right rows.
      // The last column is NOT part of the join condition.
      for (auto itL = it1; itL != endSame1; ++itL) {
        for (auto itR = it2; itR != endSame2; ++itR) {
          compatibleRowAction(itL, itR);
        }
      }

      it1 = endSame1;
      it2 = endSame2;
    }

    // Handle remaining left rows.
    for (; it1 != end1; ++it1) {
      elFromFirstNotFoundAction(it1);
    }

    compatibleRowAction.flush();

    // Move to next blocks if current ones are exhausted.
    if (it2 == end2) {
      ++rightIt;
      while (rightIt != rightEnd && rightIt->empty()) {
        ++rightIt;
      }
    }
    if (it1 == end1) {
      ++leftIt;
      while (leftIt != leftEnd && leftIt->empty()) {
        ++leftIt;
      }
    }
  }

  // Process any remaining left blocks.
  while (leftIt != leftEnd) {
    const auto& block = *leftIt;
    if (!block.empty()) {
      compatibleRowAction.setOnlyLeftInputForOptionalJoin(block);
      for (auto it = block.begin(); it != block.end(); ++it) {
        elFromFirstNotFoundAction(it);
      }
      compatibleRowAction.flush();
    }
    ++leftIt;
  }
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_JOINALGORITHMS_SPECIALOPTIONALJOINFORBLOCKS_H
