// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "../engine/IndexSequence.h"
#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "./Vocabulary.h"

using std::array;
using std::vector;

using ad_utility::HashSet;

class FTSAlgorithms {
 public:
  typedef vector<array<Id, 1>> WidthOneList;
  typedef vector<array<Id, 2>> WidthTwoList;
  typedef vector<array<Id, 3>> WidthThreeList;
  typedef vector<array<Id, 4>> WidthFourList;
  typedef vector<array<Id, 5>> WidthFiveList;
  typedef vector<vector<Id>> VarWidthList;

  static void filterByRange(const IdRange& idRange,
                            const vector<TextRecordIndex>& blockCids,
                            const vector<WordIndex>& blockWids,
                            const vector<Score>& blockScores,
                            vector<TextRecordIndex>& resultCids,
                            vector<Score>& resultScores);

  static void intersect(const vector<TextRecordIndex>& matchingContexts,
                        const vector<TextRecordIndex>& eBlockCids,
                        const vector<Id>& eBlockWids,
                        const vector<Score>& eBlockScores,
                        vector<TextRecordIndex>& resultCids,
                        vector<Id>& resultEids, vector<Score>& resultScores);

  static void intersectTwoPostingLists(const vector<TextRecordIndex>& cids1,
                                       const vector<Score>& scores1,
                                       const vector<TextRecordIndex>& cids2,
                                       const vector<Score>& scores2,
                                       vector<TextRecordIndex>& resultCids,
                                       vector<Score>& resultScores);

  static void getTopKByScores(const vector<Id>& cids,
                              const vector<Score>& scores, size_t k,
                              WidthOneList* result);

  static void aggScoresAndTakeTopKContexts(const vector<TextRecordIndex>& cids,
                                           const vector<Id>& eids,
                                           const vector<Score>& scores,
                                           size_t k, IdTable* dynResult);

  template <int WIDTH>
  static void multVarsAggScoresAndTakeTopKContexts(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, size_t nofVars, size_t kLimit,
      IdTable* dynResult);

  // Special case with only top-1 context(s).
  template <int WIDTH>
  static void multVarsAggScoresAndTakeTopContext(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, size_t nofVars, IdTable* dynResult);

  template <typename Row>
  static void aggScoresAndTakeTopKContexts(vector<Row>& nonAggRes, size_t k,
                                           vector<Row>& res);

  // Special case where k == 1.
  template <int WIDTH>
  static void aggScoresAndTakeTopContext(const vector<TextRecordIndex>& cids,
                                         const vector<Id>& eids,
                                         const vector<Score>& scores,
                                         IdTable* dynResult);

  // K-way intersect whereas there may be word ids / entity ids
  // parallel to the last list in the vectors.
  // That list (param: eids) can be given or null.
  // If it is null, resEids is left untouched, otherwise resEids
  // will contain word/entity for the matching contexts.
  static void intersectKWay(const vector<vector<TextRecordIndex>>& cidVecs,
                            const vector<vector<Score>>& scoreVecs,
                            vector<Id>* lastListEids,
                            vector<TextRecordIndex>& resCids,
                            vector<Id>& resEids, vector<Score>& resScores);

  // Constructs the cross-product between entity postings of this
  // context and matching subtree result tuples.
  template <size_t I>
  static inline void appendCrossProduct(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, size_t from, size_t toExclusive,
      const ad_utility::HashMap<Id, vector<array<Id, I>>>& subRes,
      vector<array<Id, 3 + I>>& res) {
    LOG(TRACE) << "Append cross-product called for a context with "
               << toExclusive - from << " postings.\n";
    vector<array<Id, I>> contextSubRes;
    for (size_t i = from; i < toExclusive; ++i) {
      auto it = subRes.find(eids[i]);
      if (it != subRes.end()) {
        for (auto& tup : it->second) {
          contextSubRes.push_back(tup);
        }
      }
    }
    for (size_t i = from; i < toExclusive; ++i) {
      for (auto& tup : contextSubRes) {
        res.emplace_back(concatTupleOld(eids[i], Id::make(scores[i]),
                                        Id::make(cids[i]), tup, GenSeq<I>()));
      }
    }
  }

  template <size_t... I>
  static inline array<Id, 3 + sizeof...(I)> concatTuple(
      const array<Id, 2>& l, Id cid, const array<Id, sizeof...(I)>& r,
      IndexSequence<I...>) {
    return array<Id, 3 + sizeof...(I)>{{l[0], l[1], cid, r[I]...}};
  }

  template <size_t... I>
  static inline array<Id, 3 + sizeof...(I)> concatTupleOld(
      Id eid, Id score, Id cid, const array<Id, sizeof...(I)>& r,
      IndexSequence<I...>) {
    return array<Id, 3 + sizeof...(I)>{{eid, score, cid, r[I]...}};
  }

  template <typename InTup, size_t I>
  static inline void fillTuple(Id cid, Id score, const InTup& in,
                               array<Id, I>& out) {
    out[0] = cid;
    out[1] = score;
    size_t n = 2;
    for (auto e : in) {
      out[n++] = e;
    }
  }

  template <typename InTup>
  static inline void fillTuple(Id cid, Id score, const InTup& in,
                               vector<Id>& out) {
    out.reserve(in.size() + 2);
    out.push_back(cid);
    out.push_back(score);
    out.insert(std::end(out), std::begin(in), std::end(in));
  }

  template <size_t I>
  static inline void fillThreeTuple(Id cid, Id score, Id eid,
                                    array<Id, I>& out) {
    out[0] = cid;
    out[1] = score;
    out[2] = eid;
  }

  static inline void fillThreeTuple(Id cid, Id score, Id eid, vector<Id>& out) {
    out.resize(3);
    out[0] = cid;
    out[1] = score;
    out[2] = eid;
  }

  template <typename Iter1, typename Iter2, size_t I>
  static inline void fillTuple(Id cid, Id score, Iter1 keyBegin, Iter1 keyEnd,
                               Iter2 fBegin, Iter2 fEnd, array<Id, I>& out) {
    out[0] = cid;
    out[1] = score;
    size_t n = 2;
    while (keyBegin != keyEnd) {
      out[n++] = *keyBegin;
      ++keyBegin;
    }
    while (fBegin != fEnd) {
      out[n++] = *fBegin;
      ++fBegin;
    }
    assert(n == I);
  }

  template <typename Iter, size_t I>
  static inline void fillTuple(Id cid, Id score, Iter keyBegin, Iter keyEnd,
                               Id eid, array<Id, I>& out) {
    out[0] = cid;
    out[1] = score;
    size_t n = 2;
    while (keyBegin != keyEnd) {
      out[n++] = *keyBegin;
      ++keyBegin;
    }
    out[n++] = eid;
    assert(n == I);
  }

  template <typename Iter>
  static inline void fillTuple(Id cid, Id score, Iter keyBegin, Iter keyEnd,
                               Id eid, vector<Id>& out) {
    out.push_back(cid);
    out.push_back(score);
    while (keyBegin != keyEnd) {
      out.push_back(*keyBegin);
      ++keyBegin;
    }
    out.push_back(eid);
  }

  template <typename Iter1, typename Iter2>
  static inline void fillTuple(Id cid, Id score, Iter1 keyBegin, Iter1 keyEnd,
                               Iter2 fBegin, Iter2 fEnd, vector<Id>& out) {
    out.push_back(cid);
    out.push_back(score);
    out.insert(out.end(), keyBegin, keyEnd);
    out.insert(out.end(), fBegin, fEnd);
  }

  static void appendCrossProduct(const vector<TextRecordIndex>& cids,
                                 const vector<Id>& eids,
                                 const vector<Score>& scores, size_t from,
                                 size_t toExclusive,
                                 const ad_utility::HashSet<Id>& subRes1,
                                 const ad_utility::HashSet<Id>& subRes2,
                                 vector<array<Id, 5>>& res);

  static void appendCrossProduct(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, size_t from, size_t toExclusive,
      const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>&,
      vector<vector<Id>>& res);

  template <int WIDTH>
  static void oneVarFilterAggScoresAndTakeTopKContexts(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
      size_t k, IdTable* dynResult);

  static void oneVarFilterAggScoresAndTakeTopKContexts(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, const HashSet<Id>& fSet, size_t k,
      IdTable* dynResult);

  template <int WIDTH>
  static void multVarsFilterAggScoresAndTakeTopKContexts(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
      size_t nofVars, size_t kLimit, IdTable* dynResult);

  template <int WIDTH>
  static void multVarsFilterAggScoresAndTakeTopKContexts(
      const vector<TextRecordIndex>& cids, const vector<Id>& eids,
      const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
      size_t kLimit, IdTable* dynResult);
};
