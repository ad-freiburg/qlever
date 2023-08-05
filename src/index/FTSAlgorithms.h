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
 public:

  static Index::WordEntityPostings filterByRange(
      const IdRange<WordVocabIndex>& idRange,
      const Index::WordEntityPostings& wepPreFilter);

  static Index::WordEntityPostings crossIntersect(
      const Index::WordEntityPostings& matchingContextsWep,
      const Index::WordEntityPostings& eBlockWep);

  template <int WIDTH>
  static void aggScoresAndTakeTopKContexts(const Index::WordEntityPostings& wep,
                                           size_t k, IdTable* dynResult);

  template <int WIDTH>
  static void multVarsAggScoresAndTakeTopKContexts(
      const Index::WordEntityPostings& wep, size_t nofVars, size_t kLimit,
      IdTable* dynResult);

  // Special case with only top-1 context(s).
  template <int WIDTH>
  static void multVarsAggScoresAndTakeTopContext(
      const Index::WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);

  // Special case where k == 1.
  template <int WIDTH>
  static void aggScoresAndTakeTopContext(const Index::WordEntityPostings& wep,
                                         IdTable* dynResult);

  // K-way intersect whereas there may be word ids / entity ids
  // parallel to the last list in the vectors.
  // That list (param: eids) can be given or null.
  // If it is null, resEids is left untouched, otherwise resEids
  // will contain word/entity for the matching contexts.
  static Index::WordEntityPostings crossIntersectKWay(
      const vector<Index::WordEntityPostings>& wepVecs,
      vector<Id>* lastListEids);

  template <int WIDTH>
  static void oneVarFilterAggScoresAndTakeTopKContexts(
      const Index::WordEntityPostings& wep,
      const ad_utility::HashMap<Id, IdTable>& fMap, size_t k,
      IdTable* dynResult);

  template <int WIDTH>
  static void oneVarFilterAggScoresAndTakeTopKContexts(
      const Index::WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
      IdTable* dynResult);

  template <int WIDTH>
  static void multVarsFilterAggScoresAndTakeTopKContexts(
      const Index::WordEntityPostings& wep,
      const ad_utility::HashMap<Id, IdTable>& fMap, size_t nofVars,
      size_t kLimit, IdTable* dynResult);

  template <int WIDTH>
  static void multVarsFilterAggScoresAndTakeTopKContexts(
      const Index::WordEntityPostings& wep, const HashSet<Id>& fSet,
      size_t nofVars, size_t kLimit, IdTable* dynResult);
};
