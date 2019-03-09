// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <algorithm>
#include <array>
#include <iomanip>
#include <vector>

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

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    IdTable& l1 = const_cast<IdTable&>(dynV);
    IdTable& l2 = const_cast<IdTable&>(dynFilter);

    Id sent1 = std::numeric_limits<Id>::max();
    Id sent2 = std::numeric_limits<Id>::max() - 1;
    Id sentMatch = std::numeric_limits<Id>::max() - 2;
    auto elem1 = l1[0];
    auto elem2 = l2[0];
    auto match1 = l1[0];
    auto match2 = l2[0];

    l1.emplace_back();
    l1.back()[fc1] = sentMatch;
    l1.back()[fc2] = sentMatch;
    l1.emplace_back();
    l1.back()[fc1] = sent1;

    l2.emplace_back();
    l2.back()[0] = sentMatch;
    l2.back()[1] = sentMatch;
    l2.emplace_back();
    l2.back()[fc2] = sent2;

    const IdTableStatic<IN_WIDTH> v = dynV.asStaticView<IN_WIDTH>();
    const IdTableStatic<FILTER_WIDTH> filter =
        dynFilter.asStaticView<FILTER_WIDTH>();
    IdTableStatic<IN_WIDTH> result = dynResult->moveToStatic<IN_WIDTH>();

    // Intersect both lists.
    size_t i = 0;
    size_t j = 0;

    while (v(i, fc1) < sent1) {
      while (v(i, fc1) < filter(j, 0)) {
        ++i;
      }
      while (filter(j, 0) < v(i, fc1)) {
        ++j;
      }
      while (v(i, fc1) == filter(j, 0)) {
        // fc1 match, create cross-product
        // Check fc2
        if (v(i, fc2) == filter(j, 1)) {
          result.push_back(v, i);
          ++i;
          if (i == v.size()) break;
        } else if (v(i, fc2) < filter(j, 1)) {
          ++i;
          if (i == v.size()) break;
        } else {
          ++j;
          if (j == filter.size()) break;
        }
      }
    }

    // Remove sentinels
    l1.resize(l1.size() - 2);
    l2.resize(l2.size() - 2);
    result.pop_back();
    *dynResult = result.moveToDynamic();

    LOG(DEBUG) << "Filter done, size now: " << dynResult->size()
               << " elements.\n";
  }

  template <int WIDTH>
  static void sort(IdTable* tab, size_t keyColumn) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    IdTableStatic<WIDTH> stab = tab->moveToStatic<WIDTH>();
    std::sort(stab.begin(), stab.end(),
              [&keyColumn](const auto& a, const auto& b) {
                return a[keyColumn] < b[keyColumn];
              });
    *tab = stab.moveToDynamic();
    LOG(DEBUG) << "Sort done.\n";
  }

  template <int WIDTH, typename C>
  static void sort(IdTable* tab, C comp) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    IdTableStatic<WIDTH> stab = tab->moveToStatic<WIDTH>();
    std::sort(stab.begin(), stab.end(), comp);
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
    const IdTableStatic<L_WIDTH> ai = dynA.asStaticView<L_WIDTH>();
    const IdTableStatic<R_WIDTH> bi = dynB.asStaticView<R_WIDTH>();
    IdTableStatic<OUT_WIDTH> result = dynRes->moveToStatic<OUT_WIDTH>();

    LOG(DEBUG) << "Performing join between two tables.\n";
    LOG(DEBUG) << "A: width = " << ai.cols() << ", size = " << ai.size()
               << "\n";
    LOG(DEBUG) << "B: width = " << bi.cols() << ", size = " << bi.size()
               << "\n";

    // Check trivial case.
    if (ai.size() == 0 || bi.size() == 0) {
      *dynRes = result.moveToDynamic();
      return;
    }
    // Check for possible self join (dangerous with sentinels).
    if (ai.cols() == bi.cols() && ai.data() == bi.data()) {
      AD_CHECK_EQ(jc1, jc2);
      doSelfJoin(ai, jc1, &result);
      *dynRes = result.moveToDynamic();
      return;
    }

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    IdTable& l1 = const_cast<IdTable&>(dynA);
    IdTable& l2 = const_cast<IdTable&>(dynB);

    Id sent1 = std::numeric_limits<Id>::max();
    Id sent2 = std::numeric_limits<Id>::max() - 1;
    Id sentMatch = std::numeric_limits<Id>::max() - 2;

    l1.push_back();
    l1.back()[jc1] = sentMatch;
    l1.push_back();
    l1.back()[jc1] = sent1;

    l2.push_back();
    l2.back()[jc2] = sentMatch;
    l2.push_back();
    l2.back()[jc2] = sent2;

    // Update the views
    const IdTableStatic<L_WIDTH> a = dynA.asStaticView<L_WIDTH>();
    const IdTableStatic<R_WIDTH> b = dynB.asStaticView<R_WIDTH>();

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
      while (a(i, jc1) < sent1) {
        while (a(i, jc1) < b(j, jc2)) {
          ++i;
        }
        while (b(j, jc2) < a(i, jc1)) {
          ++j;
        }
        while (a(i, jc1) == b(j, jc2)) {
          // In case of match, create cross-product
          // Always fix a and go through b.
          size_t keepJ = j;
          while (a(i, jc1) == b(j, jc2)) {
            result.push_back();
            size_t backIndex = result.size() - 1;
            size_t aCols = a.cols();
            for (size_t h = 0; h < aCols; h++) {
              result(backIndex, h) = a(i, h);
            }
            for (size_t h = 0; h < b.cols(); h++) {
              if (h < jc2) {
                result(backIndex, h + aCols) = b(j, h);
              } else if (h > jc2) {
                result(backIndex, h + aCols - 1) = b(j, h);
              }
            }
            ++j;
          }
          ++i;
          // If the next i is still the same, reset j.
          if (a(i, jc1) == b(keepJ, jc2)) {
            j = keepJ;
          }
        }
      }
    }
    // Remove sentinels
    l1.resize(l1.size() - 2);
    l2.resize(l2.size() - 2);
    result.pop_back();
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
    auto key = relation[0];
    key[I] = entityId;

    // Binary search for start.
    auto itt = std::lower_bound(
        relation.begin(), relation.end(), key,
        [](const array<E, N>& a, const array<E, N>& b) { return a[I] < b[I]; });

    while (itt != relation.end() && (*itt)[I] == entityId) {
      result.push_back(*itt);
      ++itt;
    }

    return result;
  }

  template <int IN_WIDTH, int OUT_WIDTH>
  static void doSelfJoin(const IdTableStatic<IN_WIDTH>& v, size_t jc,
                         IdTableStatic<OUT_WIDTH>* result) {
    LOG(DEBUG) << "Performing self join on var width table.\n";
    LOG(DEBUG) << "TAB: size = " << v.size() << "\n";

    // Always detect ranges of equal join col values and then
    // build a cross product for each range.
    size_t i = 0;
    while (i < v.size()) {
      const auto& val = v(i, jc);
      size_t from = i++;
      while (v(i, jc) == val) {
        ++i;
      }
      // Range detected, now build cross product
      // v[i][I] is now != val and read to be the next one.
      for (size_t j = from; j < i; ++j) {
        for (size_t k = from; k < i; ++k) {
          size_t rowIndex = result->size();
          result->push_back();
          size_t vCols = v.cols();
          for (size_t h = 0; h < vCols; h++) {
            (*result)(rowIndex, h) = v(j, h);
          }
          for (size_t h = 0; h < vCols; h++) {
            if (h < jc) {
              (*result)(rowIndex, h + vCols) = v(k, h);
            } else if (h < jc) {
              (*result)(rowIndex, h + vCols - 1) = v(k, h);
            }
          }
        }
      }
    }

    LOG(DEBUG) << "Join done.\n";
    LOG(DEBUG) << "Result:  size = " << result->size() << "\n";
  }

  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void doGallopInnerJoinRightLarge(const IdTableStatic<L_WIDTH>& l1,
                                          size_t jc1,
                                          const IdTableStatic<R_WIDTH>& l2,
                                          size_t jc2,
                                          IdTableStatic<OUT_WIDTH>* result) {
    LOG(DEBUG) << "Galloping case.\n";
    size_t i = 0;
    size_t j = 0;
    Id sent1 = std::numeric_limits<Id>::max();
    while (l1(i, jc1) < sent1) {
      while (l1(i, jc1) < l2(j, jc2)) {
        ++i;
      }
      if (l2(j, jc2) < l1(i, jc1)) {
        Id* val = new Id[l2.cols()];
        val[jc2] = l1(i, jc1);
        j = std::lower_bound(l2.begin() + j, l2.end(), val,
                             [jc2](const auto& l, const auto& r) -> bool {
                               return l[jc2] < r[jc2];
                             }) -
            l2.begin();
        delete[] val;
      }
      while (l1(i, jc1) == l2(j, jc2)) {
        // In case of match, create cross-product
        // Always fix l1 and go through l2.
        size_t keepJ = j;
        while (l1(i, jc1) == l2(j, jc2)) {
          size_t rowIndex = result->size();
          result->push_back();
          size_t l1Cols = l1.cols();
          for (size_t h = 0; h < l1Cols; h++) {
            (*result)(rowIndex, h) = l1(i, h);
          }
          for (size_t h = 0; h < l2.cols(); h++) {
            if (h < jc2) {
              (*result)(rowIndex, h + l1Cols) = l2(j, h);
            } else if (h > jc2) {
              (*result)(rowIndex, h + l1Cols - 1) = l2(j, h);
            }
          }
          ++j;
        }
        ++i;
        // If the next i is still the same, reset j.
        if (l1(i, jc1) == l2(keepJ, jc2)) {
          j = keepJ;
        }
      }
    }
  };

  template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
  static void doGallopInnerJoinLeftLarge(const IdTableStatic<L_WIDTH>& l1,
                                         size_t jc1,
                                         const IdTableStatic<R_WIDTH>& l2,
                                         size_t jc2,
                                         IdTableStatic<OUT_WIDTH>* result) {
    LOG(DEBUG) << "Galloping case.\n";
    size_t i = 0;
    size_t j = 0;
    Id sent1 = std::numeric_limits<Id>::max();
    while (l1(i, jc1) < sent1) {
      if (l2(j, jc2) > l1(i, jc1)) {
        Id* val = new Id[l1.cols()];
        val[jc1] = l2(j, jc2);
        i = std::lower_bound(l1.begin() + i, l1.end(), val,
                             [jc1](const auto& l, const auto& r) -> bool {
                               return l[jc1] < r[jc1];
                             }) -
            l1.begin();
        delete[] val;
      }
      while (l1(i, jc1) > l2(j, jc2)) {
        ++j;
      }
      while (l1(i, jc1) == l2(j, jc2)) {
        // In case of match, create cross-product
        // Always fix l1 and go through l2.
        size_t keepJ = j;
        while (l1(i, jc1) == l2(j, jc2)) {
          size_t rowIndex = result->size();
          result->push_back();
          size_t l1Cols = l1.cols();
          for (size_t h = 0; h < l1Cols; h++) {
            (*result)(rowIndex, h) = l1(i, h);
          }
          for (size_t h = 0; h < l2.cols(); h++) {
            if (h < jc2) {
              (*result)(rowIndex, h + l1Cols) = l2(j, h);
            } else if (h > jc2) {
              (*result)(rowIndex, h + l1Cols - 1) = l2(j, h);
            }
          }
          ++j;
        }
        ++i;
        // If the next i is still the same, reset j.
        if (l1(i, jc1) == l2(keepJ, jc2)) {
          j = keepJ;
        }
      }
    }
  };
};
