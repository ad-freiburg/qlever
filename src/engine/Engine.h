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

  template <typename Comp>
  static void filter(const IdTable& v, const Comp& comp, IdTable* result) {
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

  static void filter(const IdTable& v, size_t fc1, size_t fc2,
                     const IdTable& filter, IdTable* result) {
    AD_CHECK(result);
    AD_CHECK(result->size() == 0);
    LOG(DEBUG) << "Filtering " << v.size()
               << " elements with a filter relation with " << filter.size()
               << "elements\n";

    // Check trivial case.
    if (v.size() == 0 || filter.size() == 0) {
      return;
    }

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    IdTable& l1 = const_cast<IdTable&>(v);
    IdTable& l2 = const_cast<IdTable&>(filter);

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

    // Intersect both lists.
    size_t i = 0;
    size_t j = 0;

    while (l1(i, fc1) < sent1) {
      while (l1(i, fc1) < l2(j, 0)) {
        ++i;
      }
      while (l2(j, 0) < l1(i, fc1)) {
        ++j;
      }
      while (l1(i, fc1) == l2(j, 0)) {
        // fc1 match, create cross-product
        // Check fc2
        if (l1(i, fc2) == l2(j, 1)) {
          result->push_back(l1[i]);
          ++i;
          if (i == l1.size()) break;
        } else if (l1(i, fc2) < l2(j, 1)) {
          ++i;
          if (i == l1.size()) break;
        } else {
          ++j;
          if (j == l2.size()) break;
        }
      }
    }

    // Remove sentinels
    l1.resize(l1.size() - 2);
    l2.resize(l2.size() - 2);
    result->pop_back();

    LOG(DEBUG) << "Filter done, size now: " << result->size() << " elements.\n";
  }

  static void sort(IdTable* tab, size_t keyColumn) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    std::sort(tab->begin(), tab->end(),
              [&keyColumn](const auto& a, const auto& b) {
                return a[keyColumn] < b[keyColumn];
              });
    LOG(DEBUG) << "Sort done.\n";
  }

  template <typename C>
  static void sort(IdTable* tab, C comp) {
    LOG(DEBUG) << "Sorting " << tab->size() << " elements.\n";
    std::sort(tab->begin(), tab->end(), comp);
    LOG(DEBUG) << "Sort done.\n";
  }

  static void distinct(const IdTable& v, const vector<size_t>& keepIndices,
                       IdTable* result) {
    LOG(DEBUG) << "Distinct on " << v.size() << " elements.\n";
    if (v.size() > 0) {
      AD_CHECK_LE(keepIndices.size(), v[0].size());
      *result = v;

      auto last = std::unique(result->begin(), result->end(),
                              [&keepIndices](const auto& a, const auto& b) {
                                for (auto& i : keepIndices) {
                                  if (a[i] != b[i]) {
                                    return false;
                                  }
                                }
                                return true;
                              });
      result->erase(last, result->end());

      LOG(DEBUG) << "Distinct done.\n";
    }
  }

  /**
   * @brief Takes a row from each of the input tables and creates a result row
   * @param a A row from table a.
   * @param b A row from table b.
   * @param sizeA The size of a row in table a.
   * @param joinColumnBitmap_a A bitmap in which a bit is 1 if the corresponding
   *                           column is a join column
   * @param joinColumnBitmap_b A bitmap in which a bit is 1 if the corresponding
   *                           column is a join column
   * @param joinColumnAToB Maps join columns in a to their counterparts in b
   * @param res the result row
   */
  static void createOptionalResult(const IdTable& a, size_t aIdx, bool aEmpty,
                                   const IdTable& b, size_t bIdx, bool bEmpty,
                                   int joinColumnBitmap_a,
                                   int joinColumnBitmap_b,
                                   const std::vector<Id>& joinColumnAToB,
                                   IdTable* res) {
    assert(!(aEmpty && bEmpty));
    res->emplace_back();
    size_t rIdx = res->size() - 1;
    if (aEmpty) {
      // Fill the columns of a with ID_NO_VALUE and the rest with b.
      size_t i = 0;
      for (size_t col = 0; col < a.cols(); col++) {
        if ((joinColumnBitmap_a & (1 << col)) == 0) {
          (*res)(rIdx, col) = ID_NO_VALUE;
        } else {
          // if this is one of the join columns use the value in b
          (*res)(rIdx, col) = b(bIdx, joinColumnAToB[col]);
        }
        i++;
      }
      for (size_t col = 0; col < b.cols(); col++) {
        if ((joinColumnBitmap_b & (1 << col)) == 0) {
          // only write the value if it is not one of the join columns in b
          (*res)(rIdx, i) = b(bIdx, col);
          i++;
        }
      }
    } else if (bEmpty) {
      // Fill the columns of b with ID_NO_VALUE and the rest with a
      for (size_t col = 0; col < a.cols(); col++) {
        (*res)(rIdx, col) = a(aIdx, col);
      }
      for (size_t col = a.cols(); col < res->cols(); col++) {
        (*res)(rIdx, col) = ID_NO_VALUE;
      }
    } else {
      // Use the values from both a and b
      unsigned int i = 0;
      for (size_t col = 0; col < a.cols(); col++) {
        (*res)(rIdx, col) = a(aIdx, col);
        i++;
      }
      for (size_t col = 0; col < b.cols(); col++) {
        if ((joinColumnBitmap_b & (1 << col)) == 0) {
          (*res)(rIdx, i) = b(bIdx, col);
          i++;
        }
      }
    }
  }

  /**
   * @brief Joins two result tables on any number of columns, inserting the
   *        special value ID_NO_VALUE for any entries marked as optional.
   * @param a
   * @param b
   * @param aOptional
   * @param bOptional
   * @param joinColumns
   * @param result
   */
  static void optionalJoin(const IdTable& a, const IdTable& b, bool aOptional,
                           bool bOptional,
                           const vector<array<Id, 2>>& joinColumns,
                           IdTable* result) {
    // check for trivial cases
    if ((a.size() == 0 && b.size() == 0) || (a.size() == 0 && !aOptional) ||
        (b.size() == 0 && !bOptional)) {
      return;
    }

    int joinColumnBitmap_a = 0;
    int joinColumnBitmap_b = 0;
    for (const array<Id, 2>& jc : joinColumns) {
      joinColumnBitmap_a |= (1 << jc[0]);
      joinColumnBitmap_b |= (1 << jc[1]);
    }

    // When a is optional this is used to quickly determine
    // in which column of b the value of a joined column can be found.
    std::vector<Id> joinColumnAToB;
    if (aOptional) {
      uint32_t maxJoinColA = 0;
      for (const array<Id, 2>& jc : joinColumns) {
        if (jc[0] > maxJoinColA) {
          maxJoinColA = jc[0];
        }
      }
      joinColumnAToB.resize(maxJoinColA + 1);
      for (const array<Id, 2>& jc : joinColumns) {
        joinColumnAToB[jc[0]] = jc[1];
      }
    }

    // Deal with one of the two tables beeing both empty and optional
    if (a.size() == 0 && aOptional) {
      for (size_t ib = 0; ib < b.size(); ib++) {
        createOptionalResult(a, 0, true, b, ib, false, joinColumnBitmap_a,
                             joinColumnBitmap_b, joinColumnAToB, result);
      }
      return;
    } else if (b.size() == 0 && bOptional) {
      for (size_t ia = 0; ia < a.size(); ia++) {
        createOptionalResult(a, ia, false, b, 0, true, joinColumnBitmap_a,
                             joinColumnBitmap_b, joinColumnAToB, result);
      }
      return;
    }

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    IdTable& l1 = const_cast<IdTable&>(a);
    IdTable& l2 = const_cast<IdTable&>(b);
    Id sentVal = std::numeric_limits<Id>::max() - 1;

    l1.emplace_back();
    l2.emplace_back();
    for (size_t i = 0; i < l1.cols(); i++) {
      l1.back()[i] = sentVal;
    }
    for (size_t i = 0; i < l2.cols(); i++) {
      l2.back()[i] = sentVal;
    }

    bool matched = false;
    size_t ia = 0, ib = 0;
    while (ia < a.size() - 1 && ib < b.size() - 1) {
      // Join columns 0 are the primary sort columns
      while (a(ia, joinColumns[0][0]) < b(ib, joinColumns[0][1])) {
        if (bOptional) {
          createOptionalResult(a, ia, false, b, ib, true, joinColumnBitmap_a,
                               joinColumnBitmap_b, joinColumnAToB, result);
        }
        ia++;
      }
      while (b[ib][joinColumns[0][1]] < a[ia][joinColumns[0][0]]) {
        if (aOptional) {
          createOptionalResult(a, ia, true, b, ib, false, joinColumnBitmap_a,
                               joinColumnBitmap_b, joinColumnAToB, result);
        }
        ib++;
      }

      // check if the rest of the join columns also match
      matched = true;
      for (size_t joinColIndex = 0; joinColIndex < joinColumns.size();
           joinColIndex++) {
        const array<Id, 2>& joinColumn = joinColumns[joinColIndex];
        if (a[ia][joinColumn[0]] < b[ib][joinColumn[1]]) {
          if (bOptional) {
            createOptionalResult(a, ia, false, b, ib, true, joinColumnBitmap_a,
                                 joinColumnBitmap_b, joinColumnAToB, result);
          }
          ia++;
          matched = false;
          break;
        }
        if (b[ib][joinColumn[1]] < a[ia][joinColumn[0]]) {
          if (aOptional) {
            createOptionalResult(a, ia, true, b, ib, false, joinColumnBitmap_a,
                                 joinColumnBitmap_b, joinColumnAToB, result);
          }
          ib++;
          matched = false;
          break;
        }
      }

      // Compute the cross product of the row in a and all matching
      // rows in b.
      while (matched && ia < a.size() && ib < b.size()) {
        // used to reset ib if another cross product needs to be computed
        size_t initIb = ib;

        while (matched) {
          createOptionalResult(a, ia, false, b, ib, false, joinColumnBitmap_a,
                               joinColumnBitmap_b, joinColumnAToB, result);

          ib++;

          // do the rows still match?
          for (const array<Id, 2>& jc : joinColumns) {
            if (ib == b.size() || a[ia][jc[0]] != b[ib][jc[1]]) {
              matched = false;
              break;
            }
          }
        }
        ia++;
        // Check if the next row in a also matches the initial row in b
        matched = true;
        for (const array<Id, 2>& jc : joinColumns) {
          if (ia == a.size() || a[ia][jc[0]] != b[initIb][jc[1]]) {
            matched = false;
            break;
          }
        }
        // If they match reset ib and compute another cross product
        if (matched) {
          ib = initIb;
        }
      }
    }

    // remove the sentinels
    l1.pop_back();
    l2.pop_back();
    if (result->back()[joinColumns[0][0]] == sentVal) {
      result->pop_back();
    }

    // If the table of which we reached the end is optional, add all entries
    // of the other table.
    if (aOptional && ib < b.size()) {
      while (ib < b.size()) {
        createOptionalResult(a, ia, true, b, ib, false, joinColumnBitmap_a,
                             joinColumnBitmap_b, joinColumnAToB, result);

        ++ib;
      }
    }
    if (bOptional && ia < a.size()) {
      while (ia < a.size()) {
        createOptionalResult(a, ia, false, b, ib, true, joinColumnBitmap_a,
                             joinColumnBitmap_b, joinColumnAToB, result);
        ++ia;
      }
    }
  }

  static void join(const IdTable& a, size_t jc1, const IdTable& b, size_t jc2,
                   IdTable* result) {
    LOG(DEBUG) << "Performing join between two tables.\n";
    LOG(DEBUG) << "A: width = " << a.cols() << ", size = " << a.size() << "\n";
    LOG(DEBUG) << "B: width = " << b.cols() << ", size = " << b.size() << "\n";

    // Check trivial case.
    if (a.size() == 0 || b.size() == 0) {
      return;
    }
    // Check for possible self join (dangerous with sentinels).
    if (a.cols() == b.cols() && a.data() == b.data()) {
      AD_CHECK_EQ(jc1, jc2);
      doSelfJoin(a, jc1, result);
      return;
    }

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    IdTable& l1 = const_cast<IdTable&>(a);
    IdTable& l2 = const_cast<IdTable&>(b);

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

    // Cannot just switch l1 and l2 around because the order of
    // items in the result tuples is important.
    if (l1.size() / l2.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoinLeftLarge(l1, jc1, l2, jc2, result);
    } else if (l2.size() / l1.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoinRightLarge(l1, jc1, l2, jc2, result);
    } else {
      // Intersect both lists.
      size_t i = 0;
      size_t j = 0;
      while (l1(i, jc1) < sent1) {
        while (l1(i, jc1) < l2(j, jc2)) {
          ++i;
        }
        while (l2(j, jc2) < l1(i, jc1)) {
          ++j;
        }
        while (l1(i, jc1) == l2(j, jc2)) {
          // In case of match, create cross-product
          // Always fix l1 and go through l2.
          size_t keepJ = j;
          while (l1(i, jc1) == l2(j, jc2)) {
            result->push_back();
            size_t backIndex = result->size() - 1;
            size_t l1Cols = l1.cols();
            for (size_t h = 0; h < l1Cols; h++) {
              (*result)(backIndex, h) = l1(i, h);
            }
            for (size_t h = 0; h < l2.cols(); h++) {
              if (h < jc2) {
                (*result)(backIndex, h + l1Cols) = l2(j, h);
              } else if (h > jc2) {
                (*result)(backIndex, h + l1Cols - 1) = l2(j, h);
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
    }
    // Remove sentinels
    l1.resize(l1.size() - 2);
    l2.resize(l2.size() - 2);
    result->pop_back();

    LOG(DEBUG) << "Join done.\n";
    LOG(DEBUG) << "Result: width = " << (a.cols() + b.cols() - 1)
               << ", size = " << result->size() << "\n";
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

  static void doSelfJoin(const IdTable& v, size_t jc, IdTable* result) {
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
          result->push_back();
          IdTable::Row row = result->back();
          for (size_t h = 0; h < v.cols(); h++) {
            row[h] = v(j, h);
          }
          size_t vCols = v.cols();
          for (size_t h = 0; h < v.cols(); h++) {
            if (h < jc) {
              row[vCols + h] = v(k, h);
            } else if (h < jc) {
              row[vCols + h - 1] = v(k, h);
            }
          }
        }
      }
    }

    LOG(DEBUG) << "Join done.\n";
    LOG(DEBUG) << "Result:  size = " << result->size() << "\n";
  }

  static void doGallopInnerJoinRightLarge(const IdTable& l1, size_t jc1,
                                          const IdTable& l2, size_t jc2,
                                          IdTable* result) {
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
          result->push_back();
          IdTable::Row row = result->back();
          size_t l1Cols = l1.cols();
          for (size_t h = 0; h < l1Cols; h++) {
            row[h] = l1(i, h);
          }
          for (size_t h = 0; h < l2.cols(); h++) {
            if (h < jc2) {
              row[h + l1Cols] = l2(j, h);
            } else if (h > jc2) {
              row[h + l1Cols - 1] = l2(j, h);
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

  static void doGallopInnerJoinLeftLarge(const IdTable& l1, size_t jc1,
                                         const IdTable& l2, size_t jc2,
                                         IdTable* result) {
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
          result->push_back();
          IdTable::Row row = result->back();
          size_t l1Cols = l1.cols();
          for (size_t h = 0; h < l1Cols; h++) {
            row[h] = l1(i, h);
          }
          for (size_t h = 0; h < l2.cols(); h++) {
            if (h < jc2) {
              row[h + l1Cols] = l2(j, h);
            } else if (h > jc2) {
              row[h + l1Cols - 1] = l2(j, h);
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
<<<<<<< HEAD

  template <typename E, size_t N, size_t I>
  static void doSelfJoin(const vector<array<E, N>>& v,
                         vector<array<E, N + N - 1>>* result) {
    LOG(DEBUG) << "Performing self join on fixed width tables.\n";
    LOG(DEBUG) << "TAB: width = " << N << ", size = " << v.size() << "\n";

    // Always detect ranges of equal join col values and then
    // build a cross product for each range.
    size_t i = 0;
    while (i < v.size()) {
      const auto& val = v[i][I];
      size_t from = i++;
      while (v[i][I] == val) {
        ++i;
      }
      // Range detected, now build cross product
      // v[i][I] is now != val and read to be the next one.
      for (size_t j = from; j < i; ++j) {
        for (size_t k = from; k < i; ++k) {
          result->emplace_back(joinTuples(v[j], v[k], GenSeq<N>(),
                                          GenSeqLo<N, (I < N ? I : N - 1)>()));
        }
      }
    }

    LOG(DEBUG) << "Join done.\n";
    LOG(DEBUG) << "Result: width = " << (N + N - 1)
               << ", size = " << result->size() << "\n";
  }
=======
>>>>>>> Replaced the old ResultTable data structures with IdTable
};
