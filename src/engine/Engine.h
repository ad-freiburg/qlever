// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <algorithm>
#include <array>
#include <iomanip>
#include <vector>

#include <parallel/algorithm>
#include "../global/Constants.h"
#include "../global/Id.h"
#include "../global/Pattern.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./IndexSequence.h"
#include "IdTable.h"

using std::array;
using std::vector;

class Engine {
 public:
  template <size_t N, size_t M, size_t LEAVE_OUT_IN_B>
  static inline array<Id, N + M - 1> joinTuple(const array<Id, N>& a,
                                               const array<Id, M>& b) {
    array<Id, N + M - 1> res;
    std::copy(a.begin(), a.end(), res.begin());
    std::copy(b.begin(), b.begin() + LEAVE_OUT_IN_B, res.begin() + N);
    std::copy(b.begin() + LEAVE_OUT_IN_B + 1, b.end(),
              res.begin() + N + LEAVE_OUT_IN_B);
    return res;
  }

  template <typename E, size_t N, size_t... I, size_t M, size_t... J>
  static inline array<E, sizeof...(I) + sizeof...(J)> joinTuples(
      const array<E, N>& a, const array<E, M>& b, IndexSequence<I...>,
      IndexSequence<J...>) {
    return array<E, sizeof...(I) + sizeof...(J)>{{a[I]..., b[J]...}};
  };

  template <typename E, typename A, typename B>
  static inline vector<E> joinTuplesInVec(const A& a, const B& b,
                                          size_t leaveOutInB) {
    vector<E> res;
    res.reserve(a.size() + b.size() - 1);
    res.insert(res.end(), a.begin(), a.end());
    res.insert(res.end(), b.begin(), b.begin() + leaveOutInB);
    res.insert(res.end(), b.begin() + leaveOutInB + 1, b.end());
    return res;
  };

  template <size_t N, size_t M>
  static inline void concatTuple(const array<Id, N>& a, const array<Id, M>& b,
                                 array<Id, N + M>& res) {
    std::copy(a.begin(), a.end(), res.begin());
    std::copy(b.begin(), b.end(), res.begin() + N);
  }

  template <typename A, typename B>
  static void concatTuple(const A& a, const B& b, vector<Id>& res) {
    res.reserve(a.size() + b.size());
    res.insert(res.end(), a.begin(), a.end());
    res.insert(res.end(), b.begin(), b.end());
  };

  template <typename E, size_t N, size_t... I>
  static vector<array<E, sizeof...(I)>> project(const vector<array<E, N>>& tab,
                                                IndexSequence<I...>) {
    vector<array<E, sizeof...(I)>> res;
    res.reserve(tab.size());
    for (const auto& e : tab) {
      res.emplace_back(array<E, sizeof...(I)>{{e[I]...}});
    }
    return res;
  }

  template <typename E>
  static vector<vector<E>> project(const vector<vector<E>>& tab,
                                   const vector<size_t> indexSeq) {
    vector<vector<E>> res;
    res.reserve(tab.size());
    for (const auto& e : tab) {
      vector<E> newEle;
      newEle.reserve(indexSeq.size());
      for (auto i : indexSeq) {
        newEle.push_back(e[i]);
      }
      res.emplace_back(newEle);
    }
    return res;
  }

  template <typename E, size_t N>
  static vector<array<E, N>> filterRelationWithSingleId(
      const vector<array<E, N>>& relation, E entityId, size_t checkColumn);

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
  static void sort(IdTable* tab, size_t keyColumn) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    IdTableStatic<WIDTH> stab = tab->moveToStatic<WIDTH>();
    if constexpr (USE_PARALLEL_SORT) {
      __gnu_parallel::sort(stab.begin(), stab.end(),
                           [&keyColumn](const auto& a, const auto& b) {
                             return a[keyColumn] < b[keyColumn];
                           },
                           __gnu_parallel::parallel_tag(NUM_SORT_THREADS));
    } else {
      std::sort(stab.begin(), stab.end(),
                [&keyColumn](const auto& a, const auto& b) {
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
                       const vector<size_t>& keepIndices, IdTable* dynResult) {
    LOG(DEBUG) << "Distinct on " << dynInput.size() << " elements.\n";
    const IdTableStatic<WIDTH> input = dynInput.asStaticView<WIDTH>();
    IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
    if (input.size() > 0) {
      AD_CHECK_LE(keepIndices.size(), input.cols());
      result = input;

      auto last = std::unique(result.begin(), result.end(),
                              [&keepIndices](const auto& a, const auto& b) {
                                for (auto& i : keepIndices) {
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

  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void join(const IdTable& dynA, size_t jc1, const IdTable& dynB,
                   size_t jc2, IdTable* dynRes) {
    const IdTableStatic<L_WIDTH> a = dynA.asStaticView<L_WIDTH>();
    const IdTableStatic<R_WIDTH> b = dynB.asStaticView<R_WIDTH>();
    IdTableStatic<OUT_WIDTH> result = dynRes->moveToStatic<OUT_WIDTH>();

    LOG(DEBUG) << "Performing join between two tables.\n";
    LOG(DEBUG) << "A: width = " << a.cols() << ", size = " << a.size() << "\n";
    LOG(DEBUG) << "B: width = " << b.cols() << ", size = " << b.size() << "\n";

    // Check trivial case.
    if (a.size() == 0 || b.size() == 0) {
      *dynRes = result.moveToDynamic();
      return;
    }

    // Cannot just switch l1 and l2 around because the order of
    // items in the result tuples is important.
    if (a.size() / b.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoinLeftLarge(a, jc1, b, jc2, &result);
    } else if (b.size() / a.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoinRightLarge(a, jc1, b, jc2, &result);
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
  template <typename E, size_t N, size_t I>
  static vector<array<E, N>> doFilterRelationWithSingleId(
      const vector<array<E, N>>& relation, E entityId) {
    vector<array<E, N>> result;

    // Binary search for start.
    auto itt = std::lower_bound(
        relation.begin(), relation.end(), entityId,
        [](const array<E, N>& a, const Id entityId) { return a[I] < entityId; });

    while (itt != relation.end() && (*itt)[I] == entityId) {
      result.push_back(*itt);
      ++itt;
    }

    return result;
  }

  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void doGallopInnerJoinRightLarge(const IdTableStatic<L_WIDTH>& l1,
                                          const size_t jc1,
                                          const IdTableStatic<R_WIDTH>& l2,
                                          const size_t jc2,
                                          IdTableStatic<OUT_WIDTH>* result) {
    // TODO(schnelle) t rid of goto by using return
    LOG(DEBUG) << "Galloping case.\n";
    size_t i = 0;
    size_t j = 0;
    while (i < l1.size() && j < l2.size()) {
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

  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void doGallopInnerJoinLeftLarge(const IdTableStatic<L_WIDTH>& l1,
                                         const size_t jc1,
                                         const IdTableStatic<R_WIDTH>& l2,
                                         const size_t jc2,
                                         IdTableStatic<OUT_WIDTH>* result) {
    LOG(DEBUG) << "Galloping case.\n";
    size_t i = 0;
    size_t j = 0;
    while (i < l1.size() && j < l2.size()) {
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
