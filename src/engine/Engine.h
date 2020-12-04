// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <algorithm>
#include <iomanip>
#include <type_traits>
#include <vector>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/Log.h"
#include "./IndexSequence.h"
#include "IdTable.h"

class Engine {
 public:
  template <typename Comp, int WIDTH>
  static void filter(const IdTableView<WIDTH>& v, const Comp& comp,
                     IdTableStatic<WIDTH>* result) {
    AD_CHECK(result);
    AD_CHECK(result->size() == 0);
    LOG(DEBUG) << "Filtering " << v.size() << " elements.\n";
    for (const auto& e : v) {
      if (comp(e)) {
        result->push_back(e);
      }
    }
    LOG(DEBUG) << "Filter done, size now: " << result->size() << " elements.\n";
  }

  template <int IN_WIDTH, int FILTER_WIDTH>
  static void filter(const IdTable& dynV, size_t fc1, size_t fc2,
                     const IdTable& dynFilter, IdTable* dynResult) {
    AD_CHECK(dynResult);
    AD_CHECK(dynResult->size() == 0);
    LOG(DEBUG) << "Filtering " << dynV.size()
               << " elements with a filter relation with " << dynFilter.size()
               << "elements\n";

    // Check trivial case.
    if (dynV.size() == 0 || dynFilter.size() == 0) {
      return;
    }

    const IdTableView<IN_WIDTH> v = dynV.asStaticView<IN_WIDTH>();
    const IdTableView<FILTER_WIDTH> filter =
        dynFilter.asStaticView<FILTER_WIDTH>();
    IdTableStatic<IN_WIDTH> result = dynResult->moveToStatic<IN_WIDTH>();

    // Intersect both lists.
    size_t i = 0;
    size_t j = 0;

    while (i < v.size() && j < filter.size()) {
      while (v(i, fc1) < filter(j, 0)) {
        ++i;
        if (i >= v.size()) {
          goto finish;
        }
      }
      while (filter(j, 0) < v(i, fc1)) {
        ++j;
        if (j >= filter.size()) {
          goto finish;
        }
      }
      while (v(i, fc1) == filter(j, 0)) {
        // fc1 match, create cross-product
        // Check fc2
        if (v(i, fc2) == filter(j, 1)) {
          result.push_back(v, i);
          ++i;
          if (i >= v.size()) {
            break;
          }
        } else if (v(i, fc2) < filter(j, 1)) {
          ++i;
          if (i >= v.size()) {
            break;
          }
        } else {
          ++j;
          if (j >= filter.size()) {
            break;
          }
        }
      }
    }
  finish:
    *dynResult = result.moveToDynamic();

    LOG(DEBUG) << "Filter done, size now: " << dynResult->size()
               << " elements.\n";
  }

  template <int WIDTH>
  static void sort(IdTable* tab, const size_t keyColumn) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    IdTableStatic<WIDTH> stab = tab->moveToStatic<WIDTH>();
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
    *tab = stab.moveToDynamic();
    LOG(DEBUG) << "Sort done.\n";
  }

  template <int WIDTH, typename C>
  static void sort(IdTable* tab, C comp) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    IdTableStatic<WIDTH> stab = tab->moveToStatic<WIDTH>();
    if constexpr (USE_PARALLEL_SORT) {
      ad_utility::parallel_sort(stab.begin(), stab.end(), comp,
                                ad_utility::parallel_tag(NUM_SORT_THREADS));
    } else {
      std::sort(stab.begin(), stab.end(), comp);
    }
    *tab = stab.moveToDynamic();
    LOG(DEBUG) << "Sort done.\n";
  }

  /**
   * @brief Removes all duplicates from input with regards to the columns
   *        in keepIndices. The input needs to be sorted on the keep indices,
   *        otherwise the result of this function is undefined.
   **/
  template <int WIDTH>
  static void distinct(const IdTable& dynInput,
                       const std::vector<size_t>& keepIndices,
                       IdTable* dynResult) {
    LOG(DEBUG) << "Distinct on " << dynInput.size() << " elements.\n";
    const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
    IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
    if (input.size() > 0) {
      AD_CHECK_LE(keepIndices.size(), input.cols());
      result = input.clone();

      auto last = std::unique(result.begin(), result.end(),
                              [&keepIndices](const auto& a, const auto& b) {
                                for (size_t i : keepIndices) {
                                  if (a[i] != b[i]) {
                                    return false;
                                  }
                                }
                                return true;
                              });
      result.erase(last, result.end());
      *dynResult = result.moveToDynamic();
      LOG(DEBUG) << "Distinct done.\n";
    }
  }
};
