// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "IndexSequence.h"
#include "Id.h"

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
    std::copy(b.begin() + LEAVE_OUT_IN_B + 1, b.end(), res.begin() + N);
    return res;
  }

  template<typename E, size_t N, size_t... I, size_t M, size_t... J>
  static inline array<E, sizeof...(I) + sizeof...(J)> joinTuples(
      const array <E, N>& a, const array <E, M>& b,
      IndexSequence<I...>, IndexSequence<J...>) {
    return array < E, sizeof...(I) + sizeof...(J) > {{a[I]..., b[J]...}};
  };

  template<typename E, size_t N, size_t... I>
  static vector<array<E, sizeof...(I)>> project(
      const vector<array<E, N>>& tab, IndexSequence<I...>) {
    vector<array<E, sizeof...(I)>> res;
    res.reserve(tab.size());
    for (const auto& e: tab) {
      res.emplace_back(array < E, sizeof...(I) > {{e[I]...}});
    }
    return res;
  }

  template<typename E>
  static vector<vector<E>> project(
      const vector <vector<E>>& tab, const vector <size_t> indexSeq) {
    vector <vector<E>> res;
    res.reserve(tab.size());
    for (const auto& e: tab) {
      vector <E> newEle;
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
  static vector<array<E, (N + M - 1)>> join(
      const vector<array<E, N>>& a,
      size_t joinColumn1,
      const vector<array<E, M>>& b,
      size_t joinColumn2);

  template<typename E, size_t N>
  static void sort(vector<array<E, N>>& tab, size_t keyColumn) {
    std::sort(tab.begin(), tab.end(),
        [&keyColumn](const array <E, N>& a, const array <E, N>& b) {
          return a[keyColumn] < b[keyColumn];
        });
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
        [](const array <E, N>& a, const array <E, N>& b) {return a[I] < b[I];});

    while (itt != relation.end() && (*itt)[I] == entityId) {
      result.push_back(*itt);
      ++itt;
    }

    return result;
  }

  template<typename E, size_t N, size_t I, size_t M, size_t J>
  static vector<array<E, (N + M - 1)>> doJoin(
      const vector<array<E, N>>& a, const vector<array<E, M>>& b) {

    // Typedefs can hopefully prevent insanity. Read as:
    // "Tuple 1", "Tuple 2", "Tuple for Result", etc.
    typedef array <E, N> T1;
    typedef array <E, M> T2;
    typedef array <E, (N + M - 1)> TR;
    typedef vector <T1> V1;
    typedef vector <T2> V2;
    typedef vector <TR> VR;

    VR res;

    // Check trivial case.
    if (a.size() == 0 || b.size() == 0) {return res;}

    // Cast away constness so we can add sentinels that will be removed
    // in the end and create and add those sentinels.
    V1& l1 = const_cast<V1&>(a);
    V2& l2 = const_cast<V2&>(b);
    E sent1 = std::numeric_limits<E>::max();
    E sent2 = sent1 - 1;
    auto elem1 = l1[0];
    auto elem2 = l2[0];
    elem1[I] = sent1;
    elem2[J] = sent2;
    l1.push_back(elem1);
    l2.push_back(elem2);

    // Intersect both lists.
    // TODO: Improve the start by a binary search in the bigger list.
    // This could set the index in the smaller list to >= 0.
    size_t i = 0;
    size_t j = 0;

    while (l1[i][I] < sent1) {
      // In case of match, create cross-product
      if (l1[i][I] == l2[j][J]) {
        // Fix l2, go through l1
        size_t keepI = i;
        while (l1[i][I] == l2[j][J]) {
          res.emplace_back(joinTuples(
              l1[i], l2[j], GenSeq<N>(), GenSeqLo<M, (J < M ? J : M - 1)>()));
          ++i;
        }
        size_t nextI = i;
        i = keepI;
        // Fix l1, go through l2
        ++j; // The first one has already been done.
        while (l1[i][I] == l2[j][J]) {
          res.emplace_back(joinTuples(
              l1[i], l2[j], GenSeq<N>(), GenSeqLo<M, (J < M ? J : M - 1)>()));
          ++j;
        }
        i = nextI;
      }
      while (l1[i][I] < l2[j][J])
        ++i;
      while (l2[j][J] < l1[i][I] && l2[j][J] < sent2)
        ++j;
    }

    // Remove sentinels
    l1.resize(l1.size() - 1);
    l2.resize(l2.size() - 1);

    return res;
  }
};