// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include <array>
#include <unordered_map>

#include "../global/Id.h"
#include "./Vocabulary.h"
#include "../engine/IndexSequence.h"

using std::vector;
using std::array;
using std::unordered_map;


class FTSAlgorithms {
public:
  typedef vector<array<Id, 1>> WidthOneList;
  typedef vector<array<Id, 2>> WidthTwoList;
  typedef vector<array<Id, 3>> WidthThreeList;

  static void filterByRange(const IdRange& idRange, const vector<Id>& blockCids,
                            const vector<Id>& blockWids,
                            const vector<Score>& blockScores,
                            vector<Id>& resultCids,
                            vector<Score>& resultScores);

  static void intersect(const vector<Id>& matchingContexts,
                        const vector<Score>& matchingContextScores,
                        const vector<Id>& eBlockCids,
                        const vector<Id>& eBlockWids,
                        const vector<Score>& eBlockScores,
                        vector<Id>& resultCids,
                        vector<Id>& resultEids, vector<Score>& resultScores);

  static void intersectTwoPostingLists(const vector<Id>& cids1,
                                       const vector<Score>& scores1,
                                       const vector<Id>& cids2,
                                       const vector<Score>& scores2,
                                       vector<Id>& resultCids,
                                       vector<Score>& resultScores);

  static void getTopKByScores(const vector<Id>& cids,
                              const vector<Score>& scores,
                              size_t k, WidthOneList *result);

  static void aggScoresAndTakeTopKContexts(const vector<Id>& cids,
                                           const vector<Id>& eids,
                                           const vector<Score>& scores,
                                           size_t k,
                                           WidthThreeList *result);

  // Special case where k == 1.
  static void aggScoresAndTakeTopContext(const vector<Id>& cids,
                                         const vector<Id>& eids,
                                         const vector<Score>& scores,
                                         WidthThreeList *result);

  // K-way intersect whereas there may be word ids / entity ids
  // parallel to the last list in the vectors.
  // That list (param: eids) can be given or null.
  // If it is null, resEids is left untouched, otherwise resEids
  // will contain word/entity for the matching contexts.
  static void intersectKWay(const vector<vector<Id>>& cidVecs,
                            const vector<vector<Score>>& scoreVecs,
                            vector<Id> *eids,
                            vector<Id>& resCids,
                            vector<Id>& resEids,
                            vector<Score>& resScores);

  // Constructs the cross-product between entity postings of this
  // context and matching subtree result tuples.
  template<size_t I>
  static inline void appendCrossProduct(
      const vector<array<Id, 3>>& contextEPostings,
      const unordered_map<Id, vector<array<Id, I>>>& subRes,
      vector<array<Id, 3 + I>>& res) {
      LOG(TRACE) << "Append cross-product called for a context with " <<
                 contextEPostings.size() << " postings.\n";
      for (size_t i = 0; i < contextEPostings.size(); ++i) {
          for (size_t j = 0; j < contextEPostings.size(); ++j) {
              auto it = subRes.find(contextEPostings[j][0]);
              if (it != subRes.end()) {
                  const auto& tuples = it->second;
                  for (auto& tup : tuples) {
                      res.emplace_back(concatTuple(contextEPostings[i], tup, GenSeq<I>()));
                  }
              }
          }
      }
  }

  template<size_t... I>
  static inline array<Id, 3 + sizeof...(I)> concatTuple(
      const array<Id, 3>& l, const array<Id, sizeof...(I)>& r,
      IndexSequence<I...>) {
    return array<Id, 3 + sizeof...(I)>{{l[0], l[1], l[2], r[I]...}};
  }
};

