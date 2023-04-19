//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/Engine.h"

#include "engine/CallFixedSize.h"

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
    CALL_FIXED_SIZE(width, &Engine::sort, &idTable, sortCols.at(0));
  } else if (sortCols.size() == 2) {
    auto comparison = [c0 = sortCols[0], c1 = sortCols[1]](const auto& row1,
                                                           const auto& row2) {
      if (row1[c0] != row2[c0]) {
        return row1[c0] < row2[c0];
      } else {
        return row1[c1] < row2[c1];
      }
    };
    ad_utility::callFixedSize(idTable.numColumns(),
                              [&idTable, comparison]<int I>() {
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
    ad_utility::callFixedSize(idTable.numColumns(),
                              [&idTable, comparison]<int I>() {
                                Engine::sort<I>(&idTable, comparison);
                              });
  }
}
