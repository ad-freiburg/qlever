// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include <array>

#include "../global/Id.h"
#include "./Vocabulary.h"

using std::vector;
using std::array;

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
                 const vector<Id>& eBlockCids, const vector<Id>& eBlockWids,
                 const vector<Score>& eBlockScores, vector<Id>& resultCids,
                 vector<Id>& resultEids, vector<Score>& resultScores);

  static void intersectTwoPostingLists(const vector<Id>& cids1,
                                const vector<Score>& scores1,
                                const vector<Id>& cids2,
                                const vector<Score>& scores2,
                                vector<Id>& resultCids,
                                vector<Score>& resultScores);

  static void getTopKByScores(const vector<Id>& cids, const vector<Score>& scores,
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

};

