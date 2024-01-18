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
#include "./Index.h"
#include "./Vocabulary.h"

using std::array;
using std::vector;

using ad_utility::HashSet;

class FTSAlgorithms {
  using WordEntityPostings = Index::WordEntityPostings;

 public:
  // Filters all wep entries out where the wid does not lay inside the
  // idRange.
  static WordEntityPostings filterByRangeWep(
      const IdRange<WordVocabIndex>& idRange,
      const WordEntityPostings& wepPreFilter);

  // Filters all IdTable entries out where the WordIndex does not lay inside the
  // idRange.
  static IdTable filterByRange(const IdRange<WordVocabIndex>& idRange,
                               const IdTable& idPreFilter);

  // Intersects matchingContextsWep and eBlockWep on the cids_ attribute. If
  // there are multiple matches for the same cid then we calculate every
  // possible combination of eids and wids.
  //
  // Example:
  // matchingContextsWep.cids_        : 1 4 5 5 7
  // matchingContextsWep.wids_.at(0)  : 3 4 3 4 3
  // --------------------------------------------
  // eBlockWep.cids_                  : 4 5 5 8
  // eBlockWep.eids_                  : 2 1 2 1
  // ============================================
  // resultWep.cids_                  : 4 5 5 5 5
  // resultWep.eids_                  : 2 1 2 2 1
  // resultWep.wids_.at(0)            : 4 3 4 3 4
  static WordEntityPostings crossIntersect(
      const WordEntityPostings& matchingContextsWep,
      const WordEntityPostings& eBlockWep);

  // Intersects a list of weps on the cids_ attribute.
  // If there are multiple matches for the same cid the we calculate every
  // possible combination of eids and wids.
  //
  // Example:
  // wepVecs[0].cids_ :    0  1  2 10
  // wepVecs[0].scores:    1  1  1  1
  // wepVecs[0].wids_[0]:  3  2  5  3
  // --------------------------------------------
  // wepVecs[1].cids_ :    0  0  0 10
  // wepVecs[1].scores:    1  1  1  1
  // wepVecs[1].wids_[0]:  8  7  6  9
  // --------------------------------------------
  // wepVecs[2].cids_ :    0  6  8 10
  // wepVecs[2].scores:    1  1  1  3
  // wepVecs[2].wids_[0]: 23 22 25 23
  // --------------------------------------------
  // wepVecs[3].cids_ :    0  0  3  4 10 10 11
  // wepVecs[3].scores:    1  4  1  4  1  4  1
  // wepVecs[3].wids_[0]: 33 29 45 76 42 31 30
  // --------------------------------------------
  // eids:                 1  4  1  4  1  2  3
  // ============================================
  // wepResult.eids_:      1  4  1  4  1  4  1  2
  // wepResult.cids_:      0  0  0  0  0  0 10 10
  // wepResult.scores:     4  7  4  7  4  7  6  9
  // wepResult.wids_[0]:   3  3  3  3  3  3  3  3
  // wepResult.wids_[1]:   8  8  7  7  6  6  9  9
  // wepResult.wids_[2]:  23 23 23 23 23 23 23 23
  // wepResult.wids_[3]:  33 29 33 29 33 29 42 31

  static WordEntityPostings crossIntersectKWay(
      const vector<WordEntityPostings>& wepVecs, vector<Id>* lastListEids);

  // Writes the wep entries to an IdTable but at most k contexts per entity. The
  // rest gets discarded. Note that the contexts with the highest score get
  // selected.
  template <int WIDTH>
  static void aggScoresAndTakeTopKContexts(const WordEntityPostings& wep,
                                           size_t k, IdTable* dynResult);

  // Special case where k == 1.
  template <int WIDTH>
  static void aggScoresAndTakeTopContext(const WordEntityPostings& wep,
                                         IdTable* dynResult);

  // Writes the wep entries to an IdTable but at most k contexts per entity. The
  // rest gets discarded. Note that the contexts with the highest score get
  // selected. Also calculates the right combinations of the multiple entities.
  template <int WIDTH>
  static void multVarsAggScoresAndTakeTopKContexts(
      const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
      IdTable* dynResult);

  // Special case where k == 1.
  template <int WIDTH>
  static void multVarsAggScoresAndTakeTopContext(const WordEntityPostings& wep,
                                                 size_t nofVars,
                                                 IdTable* dynResult);

  // Writes the wep entries to an IdTable but at most k contexts per entity. The
  // rest gets discarded. Note that the contexts with the highest score get
  // selected. Also filters out entries whose entity is not in the HashMap.
  template <int WIDTH>
  static void oneVarFilterAggScoresAndTakeTopKContexts(
      const WordEntityPostings& wep,
      const ad_utility::HashMap<Id, IdTable>& fMap, size_t k,
      IdTable* dynResult);

  // Same but for a HashSet instead of a HashMap
  template <int WIDTH>
  static void oneVarFilterAggScoresAndTakeTopKContexts(
      const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
      IdTable* dynResult);

  // Writes the wep entries to an IdTable but at most k contexts per entity. The
  // rest gets discarded. Note that the contexts with the highest score get
  // selected. Also calculates the right combinations of the multiple entities
  // and filters out entries whose entity is not in the HashMap.
  template <int WIDTH>
  static void multVarsFilterAggScoresAndTakeTopKContexts(
      const WordEntityPostings& wep,
      const ad_utility::HashMap<Id, IdTable>& fMap, size_t nofVars,
      size_t kLimit, IdTable* dynResult);

  // Same but for a HashSet instead of a HashMap
  template <int WIDTH>
  static void multVarsFilterAggScoresAndTakeTopKContexts(
      const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
      size_t kLimit, IdTable* dynResult);
};
