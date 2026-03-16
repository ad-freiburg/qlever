//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/Engine.h"

#include "engine/CallFixedSize.h"
#include "engine/idTable/IdTableUtils.h"
#include "util/ChunkedForLoop.h"
#include "util/Exception.h"

// The actual implementation of sorting an `IdTable` according to the
// `sortCols`.
void Engine::sort(IdTable& idTable, const std::vector<ColumnIndex>& sortCols) {
  size_t width = idTable.numColumns();

  // Instantiate specialized comparison lambdas for one and two sort columns
  // and use a generic comparison for a higher number of sort columns.
  // TODO<joka921> As soon as we have merged the benchmark, measure whether
  // this is in fact beneficial and whether it should also be applied for a
  // higher number of columns, maybe even using `CALL_FIXED_SIZE` for the
  // number of sort columns.
  // TODO<joka921> Also experiment with sorting algorithms that take the
  // column-based structure of the `IdTable` into account.
  if (sortCols.size() == 1) {
    ad_utility::callFixedSizeVi(width, [&idTable, col = sortCols[0]](auto I) {
      Engine::sort<I>(&idTable, col);
    });
  } else if (sortCols.size() == 2) {
    auto comparison = [c0 = sortCols[0], c1 = sortCols[1]](const auto& row1,
                                                           const auto& row2) {
      if (row1[c0] != row2[c0]) {
        return row1[c0] < row2[c0];
      } else {
        return row1[c1] < row2[c1];
      }
    };
    ad_utility::callFixedSizeVi(idTable.numColumns(),
                                [&idTable, comparison](auto I) {
                                  Engine::sort<I>(&idTable, comparison);
                                });
  } else {
    auto comparison = [&sortCols](const auto& row1, const auto& row2) {
      for (auto& col : sortCols) {
        if (row1[col] != row2[col]) {
          return row1[col] < row2[col];
        }
      }
      return false;
    };
    ad_utility::callFixedSizeVi(idTable.numColumns(),
                                [&idTable, comparison](auto I) {
                                  Engine::sort<I>(&idTable, comparison);
                                });
  }
}

// ___________________________________________________________________________
size_t Engine::countDistinct(IdTableView<0> input,
                             const std::function<void()>& checkCancellation) {
  return idTableUtils::countDistinct(input, checkCancellation);
}

// ___________________________________________________________________________
size_t Engine::countDistinct(const IdTable& input,
                             const std::function<void()>& checkCancellation) {
  return countDistinct(input.asStaticView<0>(), checkCancellation);
}
