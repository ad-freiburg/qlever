// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <type_traits>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "util/Log.h"

class Engine {
 public:
  template <size_t WIDTH>
  static void sort(IdTable* tab, const size_t keyColumn) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements ..." << std::endl;
    IdTableStatic<WIDTH> stab = std::move(*tab).toStatic<WIDTH>();
    if constexpr (USE_PARALLEL_SORT) {
      ad_utility::parallel_sort(
          stab.begin(), stab.end(),
          [keyColumn](const auto& a, const auto& b) {
            return a[keyColumn] < b[keyColumn];
          },
          ad_utility::parallel_tag(NUM_SORT_THREADS));
    } else {
      std::sort(stab.begin(), stab.end(),
                [keyColumn](const auto& a, const auto& b) {
                  return a[keyColumn] < b[keyColumn];
                });
    }
    *tab = std::move(stab).toDynamic();
    LOG(TRACE) << "Sort done.\n";
  }
  // The effect of the third template argument is that if C does not have
  // operator() with the specified arguments that returns bool, then this
  // template does not match (so that there is no confusion with the templated
  // function above). This is an instance of SFINAE.
  template <
      int WIDTH, typename C,
      typename = std::enable_if_t<std::is_same_v<
          bool, std::invoke_result_t<C, typename IdTableStatic<WIDTH>::row_type,
                                     typename IdTableStatic<WIDTH>::row_type>>>>
  static void sort(IdTable* tab, C comp) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    IdTableStatic<WIDTH> stab = std::move(*tab).toStatic<WIDTH>();
    if constexpr (USE_PARALLEL_SORT) {
      ad_utility::parallel_sort(stab.begin(), stab.end(), comp,
                                ad_utility::parallel_tag(NUM_SORT_THREADS));
    } else {
      std::sort(stab.begin(), stab.end(), comp);
    }
    *tab = std::move(stab).toDynamic();
    LOG(DEBUG) << "Sort done.\n";
  }

  static void sort(IdTable& idTable, const std::vector<ColumnIndex>& sortCols);

  // Return the number of distinct rows in the `input`. The input must have all
  // duplicates adjacent to each other (e.g. by being sorted), otherwise the
  // behavior is undefined. `checkCancellation()` is invoked regularly and can
  // be used to implement a cancellation mechanism that throws on cancellation.
  static size_t countDistinct(const IdTable& input,
                              const std::function<void()>& checkCancellation);
  static size_t countDistinct(IdTableView<0> input,
                              const std::function<void()>& checkCancellation);
};
