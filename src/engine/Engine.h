// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <algorithm>
#include <iomanip>
#include <type_traits>
#include <vector>

#include <parallel/algorithm>
#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/Log.h"
#include "./IndexSequence.h"
#include "IdTable.h"

class Engine {
 public:
  template <typename Comp, int WIDTH>
  static void filter(const IdTableStatic<WIDTH>& v, const Comp& comp,
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

    const IdTableStatic<IN_WIDTH> v = dynV.asStaticView<IN_WIDTH>();
    const IdTableStatic<FILTER_WIDTH> filter =
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
      __gnu_parallel::sort(stab.begin(), stab.end(),
                           [keyColumn](const auto& a, const auto& b) {
                             return a[keyColumn] < b[keyColumn];
                           },
                           __gnu_parallel::parallel_tag(NUM_SORT_THREADS));
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
      __gnu_parallel::sort(stab.begin(), stab.end(), comp,
                           __gnu_parallel::parallel_tag(NUM_SORT_THREADS));
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
    const IdTableStatic<WIDTH> input = dynInput.asStaticView<WIDTH>();
    IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
    if (input.size() > 0) {
      AD_CHECK_LE(keepIndices.size(), input.cols());
      result = input;

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

  /**
   * @brief Joins IdTables dynA and dynB on join column jc2, returning
   * the result in dynRes. Creates a cross product for matching rows
   **/
  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void join(const IdTable& dynA, size_t jc1, const IdTable& dynB,
                   size_t jc2, IdTable* dynRes) {
    const IdTableStatic<L_WIDTH> a = dynA.asStaticView<L_WIDTH>();
    const IdTableStatic<R_WIDTH> b = dynB.asStaticView<R_WIDTH>();

    LOG(DEBUG) << "Performing join between two tables.\n";
    LOG(DEBUG) << "A: width = " << a.cols() << ", size = " << a.size() << "\n";
    LOG(DEBUG) << "B: width = " << b.cols() << ", size = " << b.size() << "\n";

    // Check trivial case.
    if (a.size() == 0 || b.size() == 0) {
      return;
    }

    IdTableStatic<OUT_WIDTH> result = dynRes->moveToStatic<OUT_WIDTH>();
    // Cannot just switch l1 and l2 around because the order of
    // items in the result tuples is important.
    if (a.size() / b.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoin(LeftLargerTag{}, a, jc1, b, jc2, &result);
    } else if (b.size() / a.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoin(RightLargerTag{}, a, jc1, b, jc2, &result);
    } else {
      // Intersect both lists.
      size_t i = 0;
      size_t j = 0;
      // while (a(i, jc1) < sent1) {
      while (i < a.size() && j < b.size()) {
        while (a(i, jc1) < b(j, jc2)) {
          ++i;
          if (i >= a.size()) {
            goto finish;
          }
        }

        while (b(j, jc2) < a(i, jc1)) {
          ++j;
          if (j >= b.size()) {
            goto finish;
          }
        }

        while (a(i, jc1) == b(j, jc2)) {
          // In case of match, create cross-product
          // Always fix a and go through b.
          size_t keepJ = j;
          while (a(i, jc1) == b(j, jc2)) {
            result.push_back();
            const size_t backIndex = result.size() - 1;
            for (size_t h = 0; h < a.cols(); h++) {
              result(backIndex, h) = a(i, h);
            }

            // Copy bs columns before the join column
            for (size_t h = 0; h < jc2; h++) {
              result(backIndex, h + a.cols()) = b(j, h);
            }

            // Copy bs columns after the join column
            for (size_t h = jc2 + 1; h < b.cols(); h++) {
              result(backIndex, h + a.cols() - 1) = b(j, h);
            }

            ++j;
            if (j >= b.size()) {
              break;
            }
          }
          ++i;
          if (i >= a.size()) {
            goto finish;
          }
          // If the next i is still the same, reset j.
          if (a(i, jc1) == b(keepJ, jc2)) {
            j = keepJ;
          }
        }
      }
    }
  finish:
    *dynRes = result.moveToDynamic();

    LOG(DEBUG) << "Join done.\n";
    LOG(DEBUG) << "Result: width = " << dynRes->cols()
               << ", size = " << dynRes->size() << "\n";
  }

 private:
  class RightLargerTag {};
  class LeftLargerTag {};

  template <typename TagType, int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void doGallopInnerJoin(const TagType, const IdTableStatic<L_WIDTH>& l1,
                                const size_t jc1,
                                const IdTableStatic<R_WIDTH>& l2,
                                const size_t jc2,
                                IdTableStatic<OUT_WIDTH>* result) {
    // TODO(schnelle) this doesn't actually gallop
    LOG(DEBUG) << "Galloping case.\n";

    size_t i = 0;
    size_t j = 0;
    while (i < l1.size() && j < l2.size()) {
      if constexpr (std::is_same<TagType, RightLargerTag>::value) {
        while (l1(i, jc1) < l2(j, jc2)) {
          ++i;
          if (i >= l1.size()) {
            return;
          }
        }
        if (l2(j, jc2) < l1(i, jc1)) {
          const Id needle = l1(i, jc1);
          j = std::lower_bound(l2.begin() + j, l2.end(), needle,
                               [jc2](const auto& l, const Id& needle) -> bool {
                                 return l[jc2] < needle;
                               }) -
              l2.begin();
          if (j >= l2.size()) {
            return;
          }
        }
      } else {
        if (l2(j, jc2) > l1(i, jc1)) {
          const Id needle = l2(j, jc2);
          i = std::lower_bound(l1.begin() + i, l1.end(), needle,
                               [jc1](const auto& l, const Id needle) -> bool {
                                 return l[jc1] < needle;
                               }) -
              l1.begin();
          if (i >= l1.size()) {
            return;
          }
        }
        while (l1(i, jc1) > l2(j, jc2)) {
          ++j;
          if (j >= l2.size()) {
            return;
          }
        }
      }
      while (l1(i, jc1) == l2(j, jc2)) {
        // In case of match, create cross-product
        // Always fix l1 and go through l2.
        const size_t keepJ = j;
        while (l1(i, jc1) == l2(j, jc2)) {
          size_t rowIndex = result->size();
          result->push_back();
          for (size_t h = 0; h < l1.cols(); h++) {
            (*result)(rowIndex, h) = l1(i, h);
          }
          // Copy l2s columns before the join column
          for (size_t h = 0; h < jc2; h++) {
            (*result)(rowIndex, h + l1.cols()) = l2(j, h);
          }

          // Copy l2s columns after the join column
          for (size_t h = jc2 + 1; h < l2.cols(); h++) {
            (*result)(rowIndex, h + l1.cols() - 1) = l2(j, h);
          }
          ++j;
          if (j >= l2.size()) {
            return;
          }
        }
        ++i;
        if (i >= l1.size()) {
          return;
        }
        // If the next i is still the same, reset j.
        if (l1(i, jc1) == l2(keepJ, jc2)) {
          j = keepJ;
        }
      }
    }
  };
};
