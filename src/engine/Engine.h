// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>
#include <algorithm>

#include "../util/Log.h"
#include "./IndexSequence.h"
#include "../global/Id.h"
#include "../util/Exception.h"
#include "../global/Constants.h"

using std::vector;
using std::array;

class Engine {
public:

  template<size_t N, size_t M, size_t LEAVE_OUT_IN_B>
  static inline array<Id, N + M - 1> joinTuple(
      const array<Id, N>& a,
      const array<Id, M>& b) {
    array<Id, N + M - 1> res;
    std::copy(a.begin(), a.end(), res.begin());
    std::copy(b.begin(), b.begin() + LEAVE_OUT_IN_B, res.begin() + N);
    std::copy(b.begin() + LEAVE_OUT_IN_B + 1, b.end(), res.begin() + N
                                                       + LEAVE_OUT_IN_B);
    return res;
  }

  template<typename E, size_t N, size_t... I, size_t M, size_t... J>
  static inline array<E, sizeof...(I) + sizeof...(J)> joinTuples(
      const array<E, N>& a, const array<E, M>& b,
      IndexSequence<I...>, IndexSequence<J...>) {
    return array<E, sizeof...(I) + sizeof...(J)> {{a[I]..., b[J]...}};
  };

  template<typename E, typename A, typename B>
  static inline vector<E> joinTuplesInVec(
      const A& a, const B& b, size_t leaveOutInB) {
    vector<E> res;
    res.reserve(a.size() + b.size() - 1);
    res.insert(res.end(), a.begin(), a.end());
    res.insert(res.end(), b.begin(), b.begin() + leaveOutInB);
    res.insert(res.end(), b.begin() + leaveOutInB + 1, b.end());
    return res;
  };

  template<size_t N, size_t M>
  static inline void concatTuple(
      const array<Id, N>& a,
      const array<Id, M>& b,
      array<Id, N + M>& res) {
    std::copy(a.begin(), a.end(), res.begin());
    std::copy(b.begin(), b.end(), res.begin() + N);
  }

  template<typename A, typename B>
  static void concatTuple(const A& a, const B& b, vector<Id>& res) {
    res.reserve(a.size() + b.size());
    res.insert(res.end(), a.begin(), a.end());
    res.insert(res.end(), b.begin(), b.end());
  };


  template<typename E, size_t N, size_t... I>
  static vector<array<E, sizeof...(I)>> project(
      const vector<array<E, N>>& tab, IndexSequence<I...>) {
    vector<array<E, sizeof...(I)>> res;
    res.reserve(tab.size());
    for (const auto& e: tab) {
      res.emplace_back(array<E, sizeof...(I)> {{e[I]...}});
    }
    return res;
  }

  template<typename E>
  static vector<vector<E>> project(
      const vector<vector<E>>& tab, const vector<size_t> indexSeq) {
    vector<vector<E>> res;
    res.reserve(tab.size());
    for (const auto& e: tab) {
      vector<E> newEle;
      newEle.reserve(indexSeq.size());
      for (auto i: indexSeq) {
        newEle.push_back(e[i]);
      }
      res.emplace_back(newEle);
    }
    return res;
  }

  template<typename E, size_t N>
  static vector<array<E, N>> filterRelationWithSingleId(
      const vector<array<E, N>>& relation,
      E entityId, size_t checkColumn);

  template<typename E, size_t N, size_t M>
  static void join(
      const vector<array<E, N>>& a,
      size_t joinColumn1,
      const vector<array<E, M>>& b,
      size_t joinColumn2,
      vector<array<E, (N + M - 1)>>* result);

  template<typename E, typename A, typename B>
  static void join(const A& a, size_t jc1, const B& b, size_t jc2,
                   vector<vector<E>>* result);

  template<typename E, typename A>
  static void selfJoin(const A& a, size_t jc,
                       vector<vector<E>>* result);

  template<typename E, size_t N, typename Comp>
  static void filter(
      const vector<array<E, N>>& v,
      const Comp& comp,
      vector<array<E, N>>* result) {
    AD_CHECK(result);
    AD_CHECK(result->size() == 0);
    LOG(DEBUG) << "Filtering " << v.size() << " elements.\n";
    for (const auto& e: v) {
      if (comp(e)) {
        result->push_back(e);
      }
    }
    LOG(DEBUG) << "Filter done, size now: " << result->size()
               << " elements.\n";
  }

  template<typename E, typename Comp>
  static void filter(
      const vector<vector<E>>& v,
      const Comp& comp,
      vector<vector<E>>* result) {
    AD_CHECK(result);
    AD_CHECK(result->size() == 0);
    LOG(DEBUG) << "Filtering " << v.size() << " elements.\n";
    for (const auto& e: v) {
      if (comp(e)) {
        result->push_back(e);
      }
    }
    LOG(DEBUG) << "Filter done, size now: " << result->size()
               << " elements.\n";
  }

  template<typename T>
  static void filter(
      const vector<T>& v,
      size_t fc1,
      size_t fc2,
      const vector<array<Id, 2>>& filter,
      vector<T>* result) {
    AD_CHECK(result);
    AD_CHECK(result->size() == 0);
    LOG(DEBUG) << "Filtering " << v.size() <<
               " elements with a filter relation with " << filter.size() <<
               "elements\n";

    // Check trivial case.
    if (v.size() == 0 || filter.size() == 0) { return; }

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    vector<T>& l1 = const_cast<vector<T>&>(v);
    vector<array<Id, 2>>& l2 = const_cast<vector<array<Id, 2>>&>(filter);

    Id sent1 = std::numeric_limits<Id>::max();
    Id sent2 = std::numeric_limits<Id>::max() - 1;
    Id sentMatch = std::numeric_limits<Id>::max() - 2;
    auto elem1 = l1[0];
    auto elem2 = l2[0];
    auto match1 = l1[0];
    auto match2 = l2[0];

    match1[fc1] = sentMatch;
    match1[fc2] = sentMatch;
    match2[0] = sentMatch;
    match2[1] = sentMatch;
    elem1[fc1] = sent1;
    elem2[0] = sent2;
    l1.push_back(match1);
    l2.push_back(match2);
    l1.push_back(elem1);
    l2.push_back(elem2);
    // Intersect both lists.
    size_t i = 0;
    size_t j = 0;

    while (l1[i][fc1] < sent1) {
      while (l1[i][fc1] < l2[j][0]) { ++i; }
      while (l2[j][0] < l1[i][fc1]) { ++j; }
      while (l1[i][fc1] == l2[j][0]) {
        // fc1 match, create cross-product
        // Check fc2
        if (l1[i][fc2] == l2[j][1]) {
          result->push_back(l1[i]);
          ++i;
          if (i == l1.size()) break;
        } else if (l1[i][fc2] < l2[j][1]) {
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

    LOG(DEBUG) << "Filter done, size now: " << result->size()
               << " elements.\n";
  }

  template<typename E, size_t N>
  static void sort(vector<array<E, N>>& tab, size_t keyColumn) {
    LOG(DEBUG) << "Sorting " << tab.size() << " elements.\n";
    std::sort(tab.begin(), tab.end(),
              [&keyColumn](const array<E, N>& a, const array<E, N>& b) {
                return a[keyColumn] < b[keyColumn];
              });
    LOG(DEBUG) << "Sort done.\n";
  }

  template<typename E>
  static void sort(vector<vector<E>>& tab, size_t keyColumn) {
    LOG(DEBUG) << "Sorting " << tab.size() << " elements.\n";
    std::sort(tab.begin(), tab.end(),
              [&keyColumn](const vector<E>& a, const vector<E>& b) {
                return a[keyColumn] < b[keyColumn];
              });
    LOG(DEBUG) << "Sort done.\n";
  }

  template<typename E, size_t N, typename C>
  static void sort(vector<array<E, N>>& tab, C comp) {
    LOG(DEBUG) << "Sorting " << tab.size() << " elements.\n";
    std::sort(tab.begin(), tab.end(), comp);
    LOG(DEBUG) << "Sort done.\n";
  }

  template<typename E, typename C>
  static void sort(vector<vector<E>>& tab, C comp) {
    LOG(DEBUG) << "Sorting " << tab.size() << " elements.\n";
    std::sort(tab.begin(), tab.end(), comp);
    LOG(DEBUG) << "Sort done.\n";
  }

  template<typename E, size_t N>
  static void distinct(const vector<array<E, N>>& v,
                       const vector<size_t>& keepIndices,
                       vector<array<E, N>>* result) {
    LOG(DEBUG) << "Distinct on " << v.size() << " elements.\n";
    AD_CHECK_LE(keepIndices.size(), N);
    *result = v;
    switch (keepIndices.size()) {
      case 1: {
        size_t keyCol = keepIndices[0];
        auto last = std::unique(result->begin(), result->end(),
                                [&keyCol](const array<E, N>& a,
                                          const array<E, N>& b) {
                                  return a[keyCol] == b[keyCol];
                                });
        result->erase(last, result->end());
        break;
      }
      case 2: {
        size_t keyCol = keepIndices[0];
        size_t keyCol2 = keepIndices[1];
        auto last = std::unique(result->begin(), result->end(),
                                [&keyCol, &keyCol2](const array<E, N>& a,
                                                    const array<E, N>& b) {
                                  return a[keyCol] == b[keyCol]
                                         && a[keyCol2] == b[keyCol2];
                                });
        result->erase(last, result->end());
        break;
      }
      case 3: {
        size_t keyCol = keepIndices[0];
        size_t keyCol2 = keepIndices[1];
        size_t keyCol3 = keepIndices[2];
        auto last = std::unique(result->begin(), result->end(),
                                [&keyCol, &keyCol2, &keyCol3](
                                    const array<E, N>& a,
                                    const array<E, N>& b) {
                                  return a[keyCol] == b[keyCol]
                                         && a[keyCol2] == b[keyCol2]
                                         && a[keyCol3] == b[keyCol3];
                                });
        result->erase(last, result->end());
        break;
      }
      case 4: {
        size_t keyCol = keepIndices[0];
        size_t keyCol2 = keepIndices[1];
        size_t keyCol3 = keepIndices[2];
        size_t keyCol4 = keepIndices[3];
        auto last = std::unique(result->begin(), result->end(),
                                [&keyCol, &keyCol2, &keyCol3, &keyCol4](
                                    const array<E, N>& a,
                                    const array<E, N>& b) {
                                  return a[keyCol] == b[keyCol]
                                         && a[keyCol2] == b[keyCol2]
                                         && a[keyCol3] == b[keyCol3]
                                         && a[keyCol4] == b[keyCol4];
                                });
        result->erase(last, result->end());
        break;
      }
      case 5: {
        size_t keyCol = keepIndices[0];
        size_t keyCol2 = keepIndices[1];
        size_t keyCol3 = keepIndices[2];
        size_t keyCol4 = keepIndices[3];
        size_t keyCol5 = keepIndices[4];
        auto last = std::unique(result->begin(), result->end(),
                                [&keyCol, &keyCol2, &keyCol3,
                                    &keyCol4, &keyCol5](
                                    const array<E, N>& a,
                                    const array<E, N>& b) {
                                  return a[keyCol] == b[keyCol]
                                         && a[keyCol2] == b[keyCol2]
                                         && a[keyCol3] == b[keyCol3]
                                         && a[keyCol4] == b[keyCol4]
                                         && a[keyCol5] == b[keyCol5];

                                });
        result->erase(last, result->end());
        break;
      }
    }
    LOG(DEBUG) << "Distinct done.\n";
  }

  template<typename E>
  static void distinct(const vector<vector<E>>& v,
                       const vector<size_t>& keepIndices,
                       vector<vector<E>>* result) {
    LOG(DEBUG) << "Distinct on " << v.size() << " elements.\n";
    if (v.size() > 0) {
      AD_CHECK_LE(keepIndices.size(), v[0].size());
      *result = v;

      auto last = std::unique(result->begin(), result->end(),
                              [&keepIndices](const vector<E>& a,
                                             const vector<E>& b) {
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

  template<typename A, typename B, typename E, int K>
  static void optionalJoin(const A& a, const B& b,
                           bool aOptional, bool bOptional,
                           const vector<array<size_t, 2>> &jcls,
                           vector<array<E, K>> *result) {
    // check for trivial case
    if (a.size() == 0 || b.size() == 0) {
      return;
    }

    int jcls_a = 0;
    int jcls_b = 0;
    for (array<size_t, 2> jc : jcls) {
      jcls_a |= (1 << jc[0]);
      jcls_b |= (1 << jc[1]);
    }
    // avoid compiler warnings
    (void) bOptional;
    (void) aOptional;

    // TODO improve this using sentinels etc.
    size_t ia = 0, ib = 0;
    while (ia < a.size() && ib < b.size()) {
      for (const array<size_t, 2> &jc : jcls) {
        if (a[ia][jc[0]] < b[ia][jc[1]]) {
          if (bOptional) {
            array<E, K> res;
            unsigned int i = 0;
            for (size_t col = 0; col < a[ia].size(); col++) {
              res[i] = a[ia][col];
              i++;
            }
            for (size_t col = 0; col < b[ib].size(); col++) {
              if ((jcls_b & (1 << col)) == 0) {
                res[i] = ID_NO_VALUE;
                i++;
              }
            }
            result->push_back(res);
          }
          ia++;
          continue;
        } else if (b[ia][jc[0]] < a[ia][jc[1]]) {
          if (aOptional) {
            array<E, K> res;
            unsigned int i = 0;
            for (size_t col = 0; col < a[ia].size(); col++) {
              if ((jcls_a & (1 << col)) == 0) {
                res[i] = ID_NO_VALUE;
              } else {
                res[i] = a[ia][col];
              }
              i++;
            }
            for (size_t col = 0; col < b[ib].size(); col++) {
              if ((jcls_b & (1 << col)) == 0) {
                res[i] = b[ib][col];
                i++;
              }
            }
            result->push_back(res);
          }
          ib++;
          continue;
        }
      }
      array<E, K> res;
      unsigned int i = 0;
      // TODO can we use compile time constant column widths
      // (as template parameters) to allow for loop unrolling by the compiler?
      for (size_t col = 0; col < a[ia].size(); col++) {
        res[i] = a[ia][col];
        i++;
      }
      for (size_t col = 0; col < b[ib].size(); col++) {
        if ((jcls_b & (1 << col)) == 0) {
          res[i] = a[ib][col];
          i++;
        }
      }
      result->push_back(res);
    }
  }


private:

  template<typename E, size_t N, size_t I>
  static vector<array<E, N>> doFilterRelationWithSingleId(
      const vector<array<E, N>>& relation,
      E entityId) {
    vector<array<E, N>> result;
    auto key = relation[0];
    key[I] = entityId;

    // Binary search for start.
    auto itt = std::lower_bound(relation.begin(), relation.end(), key,
                                [](const array<E, N>& a,
                                   const array<E, N>& b) {
                                  return a[I] < b[I];
                                });

    while (itt != relation.end() && (*itt)[I] == entityId) {
      result.push_back(*itt);
      ++itt;
    }

    return result;
  }

  template<typename E, size_t N, size_t I, size_t M, size_t J>
  static void doJoin(
      const vector<array<E, N>>& a, const vector<array<E, M>>& b,
      vector<array<E, (N + M - 1)>>* result) {

    LOG(DEBUG) << "Performing join between two fixed width tables.\n";
    LOG(DEBUG) << "A: witdth = " << N << ", size = " << a.size() << "\n";
    LOG(DEBUG) << "B: witdth = " << M << ", size = " << b.size() << "\n";


    // Typedefs can hopefully prevent insanity. Read as:
    // "Tuple 1", "Tuple 2", "Tuple for Result", etc.
    typedef array<E, N> T1;
    typedef array<E, M> T2;
    typedef vector<T1> V1;
    typedef vector<T2> V2;


    // Check trivial case.
    if (a.size() == 0 || b.size() == 0) { return; }
    // Check for possible self join (dangerous with sentinels).
    if (N == M) {
      if (reinterpret_cast<uintptr_t>(&a) ==
          reinterpret_cast<uintptr_t>(&b)) {
        AD_CHECK_EQ(I, J);
        doSelfJoin<E, N, I>(a, reinterpret_cast<vector<
            array<E, (N + N - 1)>>*>(result));
        return;
      }
    }

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    V1& l1 = const_cast<V1&>(a);
    V2& l2 = const_cast<V2&>(b);

    E sent1 = std::numeric_limits<E>::max();
    E sent2 = std::numeric_limits<E>::max() - 1;
    E sentMatch = std::numeric_limits<E>::max() - 2;
    auto elem1 = l1[0];
    auto elem2 = l2[0];
    auto match1 = l1[0];
    auto match2 = l2[0];

    match1[I] = sentMatch;
    match2[J] = sentMatch;
    elem1[I] = sent1;
    elem2[J] = sent2;
    l1.push_back(match1);
    l2.push_back(match2);
    l1.push_back(elem1);
    l2.push_back(elem2);

    // Cannot just switch l1 and l2 around because the order of
    // items in the result tuples is important.
    if (l1.size() / l2.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoinLeftLarge<E, N, I, M, J>(l1, l2, result);
    } else if (l2.size() / l1.size() > GALLOP_THRESHOLD) {
      doGallopInnerJoinRightLarge<E, N, I, M, J>(l1, l2, result);
    } else {
      // Intersect both lists.
      size_t i = 0;
      size_t j = 0;
      while (l1[i][I] < sent1) {
        while (l1[i][I] < l2[j][J]) { ++i; }
        while (l2[j][J] < l1[i][I]) { ++j; }
        while (l1[i][I] == l2[j][J]) {
          // In case of match, create cross-product
          // Always fix l1 and go through l2.
          size_t keepJ = j;
          while (l1[i][I] == l2[j][J]) {
            result->emplace_back(
                joinTuples(l1[i], l2[j],
                           GenSeq<N>(), GenSeqLo<M, (J < M ? J : M - 1)>()));
            ++j;
          }
          ++i;
          // If the next i is still the same, reset j.
          if (l1[i][I] == l2[keepJ][J]) {
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
    LOG(DEBUG) << "Result: width = " << (N + M - 1) << ", size = " <<
               result->size() << "\n";
  }

  template<typename E, size_t N, size_t I, size_t M, size_t J>
  static void doGallopInnerJoinRightLarge(
      const vector<array<E, N>>& l1, const vector<array<E, M>>& l2,
      vector<array<E, (N + M - 1)>>* result) {
    LOG(DEBUG) << "Galloping case.\n";
    size_t i = 0;
    size_t j = 0;
    E sent1 = std::numeric_limits<E>::max();
    while (l1[i][I] < sent1) {
      while (l1[i][I] < l2[j][J]) { ++i; }
      if (l2[j][J] < l1[i][I]) {
        array<E, M> val;
        val[J] = l1[i][I];
        j = std::lower_bound(l2.begin() + j, l2.end(), val,
                         [](const array<E, M>& l,
                            const array<E, M>& r) -> bool {
                           return l[J] < r[J];
                         }) - l2.begin();
      }
      while (l1[i][I] == l2[j][J]) {
        // In case of match, create cross-product
        // Always fix l1 and go through l2.
        size_t keepJ = j;
        while (l1[i][I] == l2[j][J]) {
          result->emplace_back(
              joinTuples(l1[i], l2[j],
                         GenSeq<N>(), GenSeqLo<M, (J < M ? J : M - 1)>()));
          ++j;
        }
        ++i;
        // If the next i is still the same, reset j.
        if (l1[i][I] == l2[keepJ][J]) {
          j = keepJ;
        }
      }
    }
  };

  template<typename E, size_t N, size_t I, size_t M, size_t J>
  static void doGallopInnerJoinLeftLarge(
      const vector<array<E, N>>& l1, const vector<array<E, M>>& l2,
      vector<array<E, (N + M - 1)>>* result) {
    LOG(DEBUG) << "Galloping case.\n";
    size_t i = 0;
    size_t j = 0;
    E sent1 = std::numeric_limits<E>::max();
    while (l1[i][I] < sent1) {
      if (l2[j][J] > l1[i][I]) {
        array<E, N> val;
        val[I] = l2[j][J];
        i = std::lower_bound(l1.begin() + i, l1.end(), val,
                             [](const array<E, N>& l,
                                const array<E, N>& r) -> bool {
                               return l[I] < r[I];
                             }) - l1.begin();
      }
      while (l1[i][I] > l2[j][J]) { ++j; }
      while (l1[i][I] == l2[j][J]) {
        // In case of match, create cross-product
        // Always fix l1 and go through l2.
        size_t keepJ = j;
        while (l1[i][I] == l2[j][J]) {
          result->emplace_back(
              joinTuples(l1[i], l2[j],
                         GenSeq<N>(), GenSeqLo<M, (J < M ? J : M - 1)>()));
          ++j;
        }
        ++i;
        // If the next i is still the same, reset j.
        if (l1[i][I] == l2[keepJ][J]) {
          j = keepJ;
        }
      }
    }
  };

  template<typename E, size_t N, size_t I>
  static void doSelfJoin(const vector<array<E, N>>& v,
                         vector<array<E, N + N - 1>>* result) {

    LOG(DEBUG) << "Performing self join on fixed width tables.\n";
    LOG(DEBUG) << "TAB: witdth = " << N << ", size = " << v.size() << "\n";

    // Always detect ranges of equal join col values and then
    // build a cross product for each range.
    size_t i = 0;
    while (i < v.size()) {
      const auto& val = v[i][I];
      size_t from = i++;
      while (v[i][I] == val) { ++i; }
      // Range detected, now build cross product
      // v[i][I] is now != val and read to be the next one.
      for (size_t j = from; j < i; ++j) {
        for (size_t k = from; k < i; ++k) {
          result->emplace_back(joinTuples(v[j], v[k],
                                          GenSeq<N>(),
                                          GenSeqLo<N, (I < N ? I : N -
                                                                   1)>()));
        }
      }
    }


    LOG(DEBUG) << "Join done.\n";
    LOG(DEBUG) << "Result: width = " << (N + N - 1) << ", size = " <<
               result->size() << "\n";
  }
};

template<typename E, typename A, typename B>
void Engine::join(const A& a, size_t jc1, const B& b, size_t jc2,
                  vector<vector<E>>* result) {

  LOG(DEBUG) << "Performing join that leads to var size rows.\n";
  LOG(DEBUG) << "A: size = " << a.size() << "\n";
  LOG(DEBUG) << "B: size = " << b.size() << "\n";

  // Check trivial case.
  if (a.size() == 0 || b.size() == 0) { return; }
  // Check for possible self join (dangerous with sentinels).
  if (reinterpret_cast<uintptr_t>(&a) == reinterpret_cast<uintptr_t>(&b)) {
    AD_CHECK_EQ(jc1, jc2)
    selfJoin(a, jc1, result);
    return;
  }

  // Cast away constness so we can add sentinels that will be removed
  // in the end and create and add those sentinels.
  A& l1 = const_cast<A&>(a);
  B& l2 = const_cast<B&>(b);
  E sent1 = std::numeric_limits<E>::max();
  E sent2 = std::numeric_limits<E>::max() - 1;
  E sentMatch = std::numeric_limits<E>::max() - 2;
  auto elem1 = l1[0];
  auto elem2 = l2[0];
  auto match1 = l1[0];
  auto match2 = l2[0];

  match1[jc1] = sentMatch;
  match2[jc2] = sentMatch;
  elem1[jc1] = sent1;
  elem2[jc2] = sent2;
  l1.push_back(match1);
  l2.push_back(match2);
  l1.push_back(elem1);
  l2.push_back(elem2);

  // Intersect both lists.
  // TODO: Improve the start by a binary search in the bigger list.
  // This could set the index in the smaller list to >= 0.
  size_t i = 0;
  size_t j = 0;

  while (l1[i][jc1] < sent1) {
    while (l2[j][jc2] < l1[i][jc1]) { ++j; }
    while (l1[i][jc1] < l2[j][jc2]) { ++i; }
    while (l1[i][jc1] == l2[j][jc2]) {
      // In case of match, create cross-product
      // Always fix l1 and go through l2.
      size_t keepJ = j;
      while (l1[i][jc1] == l2[j][jc2]) {
        result->emplace_back(joinTuplesInVec<Id>(l1[i], l2[j], jc2));
        ++j;
      }
      ++i;
      // If the next i is still the same, reset j.
      if (l1[i][jc1] == l2[keepJ][jc2]) {
        j = keepJ;
      }
    }
  }

  // Remove sentinels
  l1.resize(l1.size() - 2);
  l2.resize(l2.size() - 2);
  result->resize(result->size() - 1);

  LOG(DEBUG) << "Join done.\n";
  LOG(DEBUG) << "Result: size = " << result->size() << "\n";
}

template<typename E, typename A>
void Engine::selfJoin(const A& v, size_t jc, vector<vector<E>>* result) {
  LOG(DEBUG) << "Performing self join on var width table.\n";
  LOG(DEBUG) << "TAB: size = " << v.size() << "\n";

  // Always detect ranges of equal join col values and then
  // build a cross product for each range.
  size_t i = 0;
  while (i < v.size()) {
    const auto& val = v[i][jc];
    size_t from = i++;
    while (v[i][jc] == val) { ++i; }
    // Range detected, now build cross product
    // v[i][I] is now != val and read to be the next one.
    for (size_t j = from; j < i; ++j) {
      for (size_t k = from; k < i; ++k) {
        result->emplace_back(joinTuplesInVec<E>(v[j], v[k], jc));
      }
    }
  }

  LOG(DEBUG) << "Join done.\n";
  LOG(DEBUG) << "Result:  size = " << result->size() << "\n";
}
