// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDTABLEUTILS_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDTABLEUTILS_H

#include <functional>
#include <numeric>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "util/AllocatorWithLimit.h"
#include "util/ChunkedForLoop.h"
#include "util/Exception.h"

namespace idTableUtils {

// Return the number of distinct rows in `input`. All duplicates must be
// adjacent (e.g. the table must be sorted). `checkCancellation()` is invoked
// regularly to support cancellation.
inline size_t countDistinct(IdTableView<0> input,
                            const std::function<void()>& checkCancellation) {
  AD_EXPENSIVE_CHECK(
      ql::ranges::is_sorted(input, ql::ranges::lexicographical_compare),
      "Input to idTableUtils::countDistinct must be sorted");
  if (input.empty()) {
    return 0;
  }
  // Track which adjacent pairs are equal across all columns.
  std::vector<char, ad_utility::AllocatorWithLimit<char>> counter(
      input.numRows() - 1, static_cast<char>(true), input.getAllocator());

  for (const auto& col : input.getColumns()) {
    ad_utility::chunkedForLoop<100'000>(
        0ULL, input.numRows() - 1,
        [&counter, &col](size_t i) {
          counter[i] &= static_cast<char>(col[i] == col[i + 1]);
        },
        [&checkCancellation]() { checkCancellation(); });
  }

  auto numDuplicates = std::accumulate(counter.begin(), counter.end(), 0ULL);
  return input.numRows() - numDuplicates;
}

}  // namespace idTableUtils

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDTABLEUTILS_H
