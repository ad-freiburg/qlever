// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./FTSAlgorithms.h"

#include <cmath>
#include <map>
#include <set>
#include <utility>

#include "../util/HashMap.h"
#include "../util/HashSet.h"

using std::pair;

// _____________________________________________________________________________
void FTSAlgorithms::filterByRange(const IdRange& idRange,
                                  const vector<TextRecordIndex>& blockCids,
                                  const vector<WordIndex>& blockWids,
                                  const vector<Score>& blockScores,
                                  vector<TextRecordIndex>& resultCids,
                                  vector<Score>& resultScores) {
  AD_CHECK(blockCids.size() == blockWids.size());
  AD_CHECK(blockCids.size() == blockScores.size());
  LOG(DEBUG) << "Filtering " << blockCids.size()
             << " elements by ID range...\n";

  resultCids.resize(blockCids.size() + 2);
  resultScores.resize(blockCids.size() + 2);
  size_t nofResultElements = 0;

  for (size_t i = 0; i < blockWids.size(); ++i) {
    // TODO<joka921> proper Ids for the text stuff.
    if (blockWids[i] >= idRange._first && blockWids[i] <= idRange._last) {
      resultCids[nofResultElements] = blockCids[i];
      resultScores[nofResultElements++] = blockScores[i];
    }
  }

  resultCids.resize(nofResultElements);
  resultScores.resize(nofResultElements);

  AD_CHECK(resultCids.size() == resultScores.size());
  LOG(DEBUG) << "Filtering by ID range done. Result has " << resultCids.size()
             << " elements.\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::intersect(const vector<TextRecordIndex>& matchingContexts,
                              const vector<TextRecordIndex>& eBlockCids,
                              const vector<Id>& eBlockWids,
                              const vector<Score>& eBlockScores,
                              vector<TextRecordIndex>& resultCids,
                              vector<Id>& resultEids,
                              vector<Score>& resultScores) {
  LOG(DEBUG) << "Intersection to filter the entity postings from a block "
             << "so that only matching ones remain\n";
  LOG(DEBUG) << "matchingContexts size: " << matchingContexts.size() << '\n';
  LOG(DEBUG) << "eBlockCids size: " << eBlockCids.size() << '\n';
  // Handle trivial empty case
  if (matchingContexts.empty() || eBlockCids.empty()) {
    return;
  }
  resultCids.reserve(eBlockCids.size());
  resultCids.clear();
  resultEids.reserve(eBlockCids.size());
  resultEids.clear();
  resultScores.reserve(eBlockCids.size());
  resultScores.clear();

  size_t i = 0;
  size_t j = 0;

  while (i < matchingContexts.size() && j < eBlockCids.size()) {
    while (matchingContexts[i] < eBlockCids[j]) {
      ++i;
      if (i >= matchingContexts.size()) {
        return;
      }
    }
    while (eBlockCids[j] < matchingContexts[i]) {
      ++j;
      if (j >= eBlockCids.size()) {
        return;
      }
    }
    while (matchingContexts[i] == eBlockCids[j]) {
      // Make sure we get all matching elements from the entity list (l2)
      // that match the current context.
      // If there are multiple elements for that context in l1,
      // we can safely skip them unless we want to incorporate the scores
      // later on.
      resultCids.push_back(eBlockCids[j]);
      resultEids.push_back(eBlockWids[j]);
      resultScores.push_back(eBlockScores[j]);
      j++;
      if (j >= eBlockCids.size()) {
        break;
      }
    }
    ++i;
  }
}

// _____________________________________________________________________________
void FTSAlgorithms::intersectTwoPostingLists(
    const vector<TextRecordIndex>& cids1, const vector<Score>& scores1,
    const vector<TextRecordIndex>& cids2, const vector<Score>& scores2,
    vector<TextRecordIndex>& resultCids, vector<Score>& resultScores) {
  LOG(DEBUG) << "Intersection of words lists of sizes " << cids1.size()
             << " and " << cids2.size() << '\n';
  // Handle trivial empty case
  if (cids1.empty() || cids2.empty()) {
    return;
  }
  // TODO(schnelle): Need clear because a test reuses the result
  // vectors. we should probably just specify that it appends
  resultCids.reserve(cids1.size());
  resultCids.clear();
  resultScores.reserve(cids1.size());
  resultScores.clear();

  size_t i = 0;
  size_t j = 0;

  while (i < cids1.size() && j < cids2.size()) {
    while (cids1[i] < cids2[j]) {
      ++i;
      if (i >= cids1.size()) {
        return;
      }
    }
    while (cids2[j] < cids1[i]) {
      ++j;
      if (j >= cids2.size()) {
        return;
      }
    }
    while (cids1[i] == cids2[j]) {
      resultCids.push_back(cids2[j]);
      resultScores.push_back(scores1[i] + scores2[j]);
      ++i;
      ++j;
      if (i >= cids1.size() || j >= cids2.size()) {
        break;
      }
    }
  }
}

// _____________________________________________________________________________
void FTSAlgorithms::intersectKWay(
    const vector<vector<TextRecordIndex>>& cidVecs,
    const vector<vector<Score>>& scoreVecs, vector<Id>* lastListEids,
    vector<TextRecordIndex>& resCids, vector<Id>& resEids,
    vector<Score>& resScores) {
  size_t k = cidVecs.size();
  {
    if (cidVecs[k - 1].size() == 0) {
      LOG(DEBUG) << "Empty list involved, no intersect necessary.\n";
      return;
    }
    LOG(DEBUG) << "K-way intersection of " << k << " lists of sizes: ";
    for (const auto& l : cidVecs) {
      LOG(DEBUG) << l.size() << ' ';
    }
    LOG(DEBUG) << '\n';
  }

  const bool entityMode = lastListEids != nullptr;

  size_t minSize = std::numeric_limits<size_t>::max();
  if (entityMode) {
    minSize = lastListEids->size();
  } else {
    for (size_t i = 0; i < cidVecs.size(); ++i) {
      if (cidVecs[i].size() == 0) {
        return;
      }
      if (cidVecs[i].size() < minSize) {
        minSize = cidVecs[i].size();
      }
    }
  }

  resCids.reserve(minSize + 2);
  resCids.resize(minSize);
  resScores.reserve(minSize + 2);
  resScores.resize(minSize);
  if (lastListEids) {
    resEids.reserve(minSize + 2);
    resEids.resize(minSize);
  }

  // For intersection, we don't need a PQ.
  // The algorithm:
  // Remember the current context and the length of the streak
  // (i.e. in how many lists was that context found).
  // If the streak reaches k, write the context to the result.
  // Until then, go through lists in a round-robin way and advance until
  // a) the context is found or
  // b) a higher context is found without a match before (reset cur and streak).
  // Stop as soon as one list cannot advance.

  // I think no PQ is needed, because unlike for merge, elements that
  // do not occur in all lists, don't have to be visited in the right order.

  vector<size_t> nextIndices;
  nextIndices.resize(cidVecs.size(), 0);
  TextRecordIndex currentContext = cidVecs[k - 1][0];
  size_t currentList = k - 1;  // Has the fewest different contexts. Start here.
  size_t streak = 0;
  size_t n = 0;
  while (true) {  // break when one list cannot advance
    size_t thisListSize = cidVecs[currentList].size();
    if (nextIndices[currentList] == thisListSize) {
      break;
    }
    while (cidVecs[currentList][nextIndices[currentList]] < currentContext) {
      nextIndices[currentList] += 1;
      if (nextIndices[currentList] == thisListSize) {
        break;
      }
    }
    if (nextIndices[currentList] == thisListSize) {
      break;
    }
    TextRecordIndex atId = cidVecs[currentList][nextIndices[currentList]];
    if (atId == currentContext) {
      if (++streak == k) {
        Score s = 0;
        for (size_t i = 0; i < k - 1; ++i) {
          s += scoreVecs[i][(i == currentList ? nextIndices[i]
                                              : nextIndices[i] - 1)];
        }
        if (entityMode) {
          // If entities are involved, there may be multiple postings
          // for one context. Handle all matching the current context.
          size_t matchInEL = (k - 1 == currentList ? nextIndices[k - 1]
                                                   : nextIndices[k - 1] - 1);
          while (matchInEL < cidVecs[k - 1].size() &&
                 cidVecs[k - 1][matchInEL] == currentContext) {
            resCids[n] = currentContext;
            resEids[n] = (*lastListEids)[matchInEL];
            resScores[n++] = s + scoreVecs[k - 1][matchInEL];
            ++matchInEL;
          }
          nextIndices[k - 1] = matchInEL;
        } else {
          resCids[n] = currentContext;
          resScores[n++] =
              s +
              scoreVecs[k - 1][(k - 1 == currentList ? nextIndices[k - 1]
                                                     : nextIndices[k - 1] - 1)];
        }
        // Optimization: The last list will feature the fewest different
        // contexts. After a match, always advance in that list
        currentList = k - 1;
        continue;
      }
    } else {
      streak = 1;
      currentContext = atId;
    }
    nextIndices[currentList++] += 1;
    if (currentList == k) {
      currentList = 0;
    }  // wrap around
  }

  resCids.resize(n);
  resScores.resize(n);
  if (entityMode) {
    resEids.resize(n);
  }
  LOG(DEBUG) << "Intersection done. Size: " << resCids.size() << "\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::getTopKByScores(const vector<Id>& cids,
                                    const vector<Score>& scores, size_t k,
                                    WidthOneList* result) {
  AD_CHECK_EQ(cids.size(), scores.size());
  k = std::min(k, cids.size());
  LOG(DEBUG) << "Call getTopKByScores (partial sort of " << cids.size()
             << " contexts by score)...\n";
  vector<size_t> indices;
  indices.resize(scores.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = i;
  }
  LOG(DEBUG) << "Doing the partial sort...\n";
  std::partial_sort(
      indices.begin(), indices.begin() + k, indices.end(),
      [&scores](size_t a, size_t b) { return scores[a] > scores[b]; });
  LOG(DEBUG) << "Packing the final WidthOneList of cIds...\n";
  result->reserve(k + 2);
  result->resize(k);
  for (size_t i = 0; i < k; ++i) {
    (*result)[i] = {{cids[indices[i]]}};
  }
  LOG(DEBUG) << "Done with getTopKByScores.\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::aggScoresAndTakeTopKContexts(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t k, IdTable* dynResult) {
  AD_CHECK_EQ(cids.size(), eids.size());
  AD_CHECK_EQ(cids.size(), scores.size());
  LOG(DEBUG) << "Going from an entity, context and score list of size: "
             << cids.size() << " elements to a table with distinct entities "
             << "and at most " << k << " contexts per entity.\n";

  // The default case where k == 1 can use a map for a O(n) solution
  if (k == 1) {
    aggScoresAndTakeTopContext<3>(cids, eids, scores, dynResult);
    return;
  }

  // Use a set (ordered) and keep it at size k for the context scores
  // This achieves O(n log k)
  LOG(DEBUG) << "Heap-using case with " << k << " contexts per entity...\n";

  using ScoreToContext = std::set<pair<Score, TextRecordIndex>>;
  using ScoreAndStC = pair<Score, ScoreToContext>;
  using AggMap = ad_utility::HashMap<Id, ScoreAndStC>;
  AggMap map;
  for (size_t i = 0; i < eids.size(); ++i) {
    if (!map.contains(eids[i])) {
      ScoreToContext inner;
      inner.insert(std::make_pair(scores[i], cids[i]));
      map[eids[i]] = std::make_pair(1, inner);
    } else {
      auto& val = map[eids[i]];
      // val.first += scores[i];
      ++val.first;
      ScoreToContext& stc = val.second;
      if (stc.size() < k || stc.begin()->first < scores[i]) {
        if (stc.size() == k) {
          stc.erase(*stc.begin());
        }
        stc.insert(std::make_pair(scores[i], cids[i]));
      };
    }
  }
  IdTableStatic<3> result = dynResult->moveToStatic<3>();
  result.reserve(map.size() * k + 2);
  for (auto it = map.begin(); it != map.end(); ++it) {
    Id eid = it->first;
    Id entityScore = Id::make(it->second.first);
    ScoreToContext& stc = it->second.second;
    for (auto itt = stc.rbegin(); itt != stc.rend(); ++itt) {
      result.push_back({Id::make(itt->second), entityScore, eid});
    }
  }
  *dynResult = result.moveToDynamic();

  // The result is NOT sorted due to the usage of maps.
  // Resorting the result is a separate operation now.
  // Benefit 1) it's not always necessary to sort.
  // Benefit 2) The result size can be MUCH smaller than n.
  LOG(DEBUG) << "Done. There are " << dynResult->size()
             << " entity-score-context tuples now.\n";
}

// _____________________________________________________________________________
template <typename Row>
void FTSAlgorithms::aggScoresAndTakeTopKContexts(vector<Row>& nonAggRes,
                                                 size_t k, vector<Row>& res) {
  AD_CHECK(res.size() == 0);
  LOG(DEBUG) << "Aggregating scores from a list of size " << nonAggRes.size()
             << " while keeping the top " << k << " contexts each.\n";

  if (nonAggRes.size() == 0) return;

  size_t width = nonAggRes[0].size();
  std::sort(nonAggRes.begin(), nonAggRes.end(),
            [&width](const Row& l, const Row& r) {
              if (l[0] == r[0]) {
                for (size_t i = 3; i < width; ++i) {
                  if (l[i] == r[i]) continue;
                  return l[i] < r[i];
                }
                return l[1] < r[1];
              }
              return l[0] < r[0];
            });

  res.push_back(nonAggRes[0]);
  size_t contextsInResult = 1;
  for (size_t i = 1; i < nonAggRes.size(); ++i) {
    bool same = false;
    if (nonAggRes[i][0] == res.back()[0]) {
      same = true;
      for (size_t j = 3; j < width; ++j) {
        if (nonAggRes[i][j] != res.back()[j]) {
          same = false;
          break;
        }
      }
    }
    if (same) {
      ++contextsInResult;
      if (contextsInResult <= k) {
        res.push_back(nonAggRes[i]);
      }
    } else {
      // Other

      // update scores on last
      for (size_t j = res.size() - std::min(contextsInResult, k);
           j < res.size(); ++j) {
        assert(j < i);
        assert(j < res.size());
        res[j][1] = Id::make(contextsInResult);
      }

      // start with current
      res.push_back(nonAggRes[i]);
      contextsInResult = 1;
    }
  }

  LOG(DEBUG) << "Done. There are " << res.size()
             << " entity-score-context tuples now.\n";
}

template void FTSAlgorithms::aggScoresAndTakeTopKContexts(
    vector<array<Id, 4>>& nonAggRes, size_t k, vector<array<Id, 4>>& res);

template void FTSAlgorithms::aggScoresAndTakeTopKContexts(
    vector<array<Id, 5>>& nonAggRes, size_t k, vector<array<Id, 5>>& res);

template void FTSAlgorithms::aggScoresAndTakeTopKContexts(
    vector<vector<Id>>& nonAggRes, size_t k, vector<vector<Id>>& res);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::aggScoresAndTakeTopContext(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult) {
  LOG(DEBUG) << "Special case with 1 contexts per entity...\n";
  typedef ad_utility::HashMap<Id, pair<Score, pair<TextRecordIndex, Score>>>
      AggMap;
  AggMap map;
  for (size_t i = 0; i < eids.size(); ++i) {
    if (!map.contains(eids[i])) {
      map[eids[i]] = std::make_pair(1, std::make_pair(cids[i], scores[i]));
      // map[eids[i]] = std::make_pair(scores[i],
      // std::make_pair(cids[i], scores[i]));
    } else {
      auto& val = map[eids[i]];
      // val.first += scores[i];
      ++val.first;
      if (val.second.second < scores[i]) {
        val.second = std::make_pair(cids[i], scores[i]);
      };
    }
  }
  IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
  result.reserve(map.size() + 2);
  result.resize(map.size());
  size_t n = 0;
  for (auto it = map.begin(); it != map.end(); ++it) {
    result(n, 0) = Id::make(it->second.second.first);
    result(n, 1) = Id::make(it->second.first);
    result(n, 2) = it->first;
    n++;
  }
  AD_CHECK_EQ(n, result.size());
  *dynResult = result.moveToDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size()
             << " context-score-entity tuples now.\n";
}

template void FTSAlgorithms::aggScoresAndTakeTopContext<0>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<1>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<2>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<3>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<4>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<5>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<6>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<7>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<8>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<9>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<10>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult) {
  if (cids.size() == 0) {
    return;
  }
  if (kLimit == 1) {
    multVarsAggScoresAndTakeTopContext<WIDTH>(cids, eids, scores, nofVars,
                                              dynResult);
  } else {
    // Go over contexts.
    // For each context build a cross product of width 2.
    // Store them in a map, use a pair of id's as key and
    // an appropriate hash function.
    // Use a set (ordered) and keep it at size k for the context scores
    // This achieves O(n log k)
    LOG(DEBUG) << "Heap-using case with " << kLimit
               << " contexts per entity...\n";
    using ScoreToContext = std::set<pair<Score, TextRecordIndex>>;
    using ScoreAndStC = pair<Score, ScoreToContext>;
    using AggMap = ad_utility::HashMap<vector<Id>, ScoreAndStC>;
    AggMap map;
    vector<Id> entitiesInContext;
    TextRecordIndex currentCid = cids[0];
    Score cscore = scores[0];

    for (size_t i = 0; i < cids.size(); ++i) {
      if (cids[i] == currentCid) {
        entitiesInContext.push_back(eids[i]);
        // cscore += scores[i];
      } else {
        // Calculate a cross product and add/update the map
        size_t nofPossibilities =
            static_cast<size_t>(pow(entitiesInContext.size(), nofVars));
        for (size_t j = 0; j < nofPossibilities; ++j) {
          vector<Id> key;
          key.reserve(nofVars);
          size_t n = j;
          for (size_t k = 0; k < nofVars; ++k) {
            key.push_back(entitiesInContext[n % entitiesInContext.size()]);
            n /= entitiesInContext.size();
          }
          if (map.count(key) == 0) {
            ScoreToContext inner;
            inner.insert(std::make_pair(cscore, currentCid));
            map[key] = std::make_pair(1, inner);
          } else {
            auto& val = map[key];
            // val.first += scores[i];
            ++val.first;
            ScoreToContext& stc = val.second;
            if (stc.size() < kLimit || stc.begin()->first < cscore) {
              if (stc.size() == kLimit) {
                stc.erase(*stc.begin());
              }
              stc.insert(std::make_pair(cscore, currentCid));
            };
          }
        }
        entitiesInContext.clear();
        currentCid = cids[i];
        cscore = scores[i];
        entitiesInContext.push_back(eids[i]);
      }
    }
    // Deal with the last context
    // Calculate a cross product and add/update the map
    size_t nofPossibilities =
        static_cast<size_t>(pow(entitiesInContext.size(), nofVars));
    for (size_t j = 0; j < nofPossibilities; ++j) {
      vector<Id> key;
      key.reserve(nofVars);
      size_t n = j;
      for (size_t k = 0; k < nofVars; ++k) {
        key.push_back(entitiesInContext[n % entitiesInContext.size()]);
        n /= entitiesInContext.size();
      }
      if (map.count(key) == 0) {
        ScoreToContext inner;
        inner.insert(std::make_pair(cscore, currentCid));
        map[key] = std::make_pair(1, inner);
      } else {
        auto& val = map[key];
        // val.first += scores[i];
        ++val.first;
        ScoreToContext& stc = val.second;
        if (stc.size() < kLimit || stc.begin()->first < cscore) {
          if (stc.size() == kLimit) {
            stc.erase(*stc.begin());
          }
          stc.insert(std::make_pair(cscore, currentCid));
        };
      }
    }
    // Iterate over the map and populate the result.
    IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
    for (auto it = map.begin(); it != map.end(); ++it) {
      ScoreToContext& stc = it->second.second;
      for (auto itt = stc.rbegin(); itt != stc.rend(); ++itt) {
        size_t n = result.size();
        result.push_back();
        result(n, 0) = Id::make(itt->second);
        result(n, 1) = Id::make(it->second.first);
        for (size_t k = 0; k < nofVars; ++k) {
          result(n, k + 2) = it->first[k];  // eid
        }
      }
    }
    *dynResult = result.moveToDynamic();
    LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
  }
}

template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<0>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<1>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<2>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<3>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<4>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<5>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<6>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<7>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<8>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<9>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<10>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, size_t kLimit,
    IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsAggScoresAndTakeTopContext(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult) {
  LOG(DEBUG) << "Special case with 1 contexts per entity...\n";
  // Go over contexts.
  // For each context build a cross product of width 2.
  // Store them in a map, use a pair of id's as key and
  // an appropriate hash function.
  using AggMap = ad_utility::HashMap<std::vector<Id>,
                                     pair<Score, pair<TextRecordIndex, Score>>>;
  // Note: vector{k} initializes with a single value k, as opposed to
  // vector<Id>(k), which initializes a vector with k default constructed
  // arguments.
  // TODO<joka921> proper Id types
  vector<Id> emptyKey{std::numeric_limits<Id>::max()};
  AggMap map;
  vector<Id> entitiesInContext;
  TextRecordIndex currentCid = cids[0];
  Score cscore = scores[0];

  for (size_t i = 0; i < cids.size(); ++i) {
    if (cids[i] == currentCid) {
      entitiesInContext.push_back(eids[i]);
      // cscore = std::max(cscore, scores[i]);
    } else {
      // Calculate a cross product and add/update the map
      size_t nofPossibilities =
          static_cast<size_t>(pow(entitiesInContext.size(), nofVars));
      for (size_t j = 0; j < nofPossibilities; ++j) {
        vector<Id> key;
        key.reserve(nofVars);
        size_t n = j;
        for (size_t k = 0; k < nofVars; ++k) {
          key.push_back(entitiesInContext[n % entitiesInContext.size()]);
          n /= entitiesInContext.size();
        }
        if (map.count(key) == 0) {
          map[key] = std::make_pair(1, std::make_pair(currentCid, cscore));
        } else {
          auto& val = map[key];
          // val.first += scores[i];
          ++val.first;
          if (val.second.second < cscore) {
            val.second = std::make_pair(currentCid, cscore);
          };
        }
      }
      entitiesInContext.clear();
      currentCid = cids[i];
      cscore = scores[i];
      entitiesInContext.push_back(eids[i]);
    }
  }
  // Deal with the last context
  size_t nofPossibilities =
      static_cast<size_t>(pow(entitiesInContext.size(), nofVars));
  for (size_t j = 0; j < nofPossibilities; ++j) {
    vector<Id> key;
    key.reserve(nofVars);
    size_t n = j;
    for (size_t k = 0; k < nofVars; ++k) {
      key.push_back(entitiesInContext[n % entitiesInContext.size()]);
      n /= entitiesInContext.size();
    }
    if (map.count(key) == 0) {
      map[key] = std::make_pair(1, std::make_pair(currentCid, cscore));
    } else {
      auto& val = map[key];
      // val.first += scores[i];
      ++val.first;
      if (val.second.second < cscore) {
        val.second = std::make_pair(currentCid, cscore);
      };
    }
  }
  IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
  result.reserve(map.size() + 2);
  result.resize(map.size());
  size_t n = 0;

  // Iterate over the map and populate the result.
  for (auto it = map.begin(); it != map.end(); ++it) {
    result(n, 0) = Id::make(it->second.second.first);
    result(n, 1) = Id::make(it->second.first);
    for (size_t k = 0; k < nofVars; ++k) {
      result(n, k + 2) = it->first[k];
    }
    n++;
  }
  AD_CHECK_EQ(n, result.size());
  *dynResult = result.moveToDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
}

template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<0>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<1>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<2>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<3>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<4>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<5>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<6>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<7>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<8>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<9>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<10>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t nofVars, IdTable* dynResult);

// _____________________________________________________________________________
void FTSAlgorithms::appendCrossProduct(const vector<TextRecordIndex>& cids,
                                       const vector<Id>& eids,
                                       const vector<Score>& scores, size_t from,
                                       size_t toExclusive,
                                       const ad_utility::HashSet<Id>& subRes1,
                                       const ad_utility::HashSet<Id>& subRes2,
                                       vector<array<Id, 5>>& res) {
  LOG(TRACE) << "Append cross-product called for a context with "
             << toExclusive - from << " postings.\n";
  vector<Id> contextSubRes1;
  vector<Id> contextSubRes2;
  ad_utility::HashSet<Id> done;
  for (size_t i = from; i < toExclusive; ++i) {
    if (done.count(eids[i])) {
      continue;
    }
    done.insert(eids[i]);
    if (subRes1.count(eids[i]) > 0) {
      contextSubRes1.push_back(eids[i]);
    }
    if (subRes2.count(eids[i]) > 0) {
      contextSubRes2.push_back(eids[i]);
    }
  }
  for (size_t i = from; i < toExclusive; ++i) {
    for (size_t j = 0; j < contextSubRes1.size(); ++j) {
      for (size_t k = 0; k < contextSubRes2.size(); ++k) {
        res.emplace_back(
            array<Id, 5>{{eids[i], Id::make(scores[i]), Id::make(cids[i]),
                          contextSubRes1[j], contextSubRes2[k]}});
      }
    }
  }
}

// _____________________________________________________________________________
void FTSAlgorithms::appendCrossProduct(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, size_t from, size_t toExclusive,
    const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResMaps,
    vector<vector<Id>>& res) {
  vector<vector<vector<Id>>> subResMatches;
  subResMatches.resize(subResMaps.size());
  ad_utility::HashSet<Id> distinctEids;
  for (size_t i = from; i < toExclusive; ++i) {
    if (distinctEids.count(eids[i])) {
      continue;
    }
    distinctEids.insert(eids[i]);
    for (size_t j = 0; j < subResMaps.size(); ++j) {
      if (subResMaps[j].count(eids[i]) > 0) {
        for (const vector<Id>& row : subResMaps[j].find(eids[i])->second) {
          subResMatches[j].push_back(row);
        }
      }
    }
  }
  for (size_t i = from; i < toExclusive; ++i) {
    // In order to create the cross product between subsets,
    // we compute the number of result rows and use
    // modulo operations to index the correct sources.

    // Example: cross product between sets of sizes a x b x c
    // Then the n'th row is composed of:
    // n % a               from a,
    // (n / a) % b         from b,
    // ((n / a) / b) % c   from c.
    size_t nofResultRows = 1;
    for (size_t j = 0; j < subResMatches.size(); ++j) {
      nofResultRows *= subResMatches[j].size();
    }

    for (size_t n = 0; n < nofResultRows; ++n) {
      vector<Id> resRow = {eids[i], Id::make(scores[i]), Id::make(cids[i])};
      for (size_t j = 0; j < subResMatches.size(); ++j) {
        size_t index = n;
        for (size_t k = 0; k < j; ++k) {
          index /= subResMatches[k].size();
        }
        index %= subResMatches[j].size();
        vector<Id>& append = subResMatches[j][index];
        resRow.insert(resRow.end(), append.begin(), append.end());
      }
      res.push_back(resRow);
    }
  }
}

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult) {
  AD_CHECK_EQ(cids.size(), eids.size());
  AD_CHECK_EQ(cids.size(), scores.size());
  LOG(DEBUG) << "Going from an entity, context and score list of size: "
             << cids.size()
             << " elements to a table with filtered distinct entities "
             << "and at most " << k << " contexts per entity.\n";
  if (cids.size() == 0 || fMap.size() == 0) {
    return;
  }
  // TODO: add code to speed up for k==1

  // Use a set (ordered) and keep it at size k for the context scores
  // This achieves O(n log k)
  LOG(DEBUG) << "Heap-using case with " << k << " contexts per entity...\n";

  using ScoreToContext = std::set<pair<Score, TextRecordIndex>>;
  using ScoreAndStC = pair<Score, ScoreToContext>;
  using AggMap = ad_utility::HashMap<Id, ScoreAndStC>;
  AggMap map;
  for (size_t i = 0; i < eids.size(); ++i) {
    if (fMap.count(eids[i]) > 0) {
      if (map.count(eids[i]) == 0) {
        ScoreToContext inner;
        inner.insert(std::make_pair(scores[i], cids[i]));
        map[eids[i]] = std::make_pair(1, inner);
      } else {
        auto& val = map[eids[i]];
        // val.first += scores[i];
        ++val.first;
        ScoreToContext& stc = val.second;
        if (stc.size() < k || stc.begin()->first < scores[i]) {
          if (stc.size() == k) {
            stc.erase(*stc.begin());
          }
          stc.insert(std::make_pair(scores[i], cids[i]));
        };
      }
    }
  }
  IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
  result.reserve(map.size() * k + 2);
  for (auto it = map.begin(); it != map.end(); ++it) {
    Id eid = it->first;
    Id score = Id::make(it->second.first);
    ScoreToContext& stc = it->second.second;
    for (auto itt = stc.rbegin(); itt != stc.rend(); ++itt) {
      for (auto fRow : fMap.find(eid)->second) {
        size_t n = result.size();
        result.push_back();
        result(n, 0) = Id::make(itt->second);  // cid
        result(n, 1) = score;
        for (size_t i = 0; i < fRow.size(); i++) {
          result(n, 2 + i) = fRow[i];
        }
      }
    }
  }
  *dynResult = result.moveToDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
};

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<0>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<1>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<2>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<3>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<4>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<5>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<6>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<7>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<8>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<9>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<10>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

// _____________________________________________________________________________
void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult) {
  AD_CHECK_EQ(cids.size(), eids.size());
  AD_CHECK_EQ(cids.size(), scores.size());
  LOG(DEBUG) << "Going from an entity, context and score list of size: "
             << cids.size()
             << " elements to a table with filtered distinct entities "
             << "and at most " << k << " contexts per entity.\n";

  if (cids.size() == 0 || fSet.size() == 0) {
    return;
  }

  // TODO: add code to speed up for k==1

  // Use a set (ordered) and keep it at size k for the context scores
  // This achieves O(n log k)
  LOG(DEBUG) << "Heap-using case with " << k << " contexts per entity...\n";

  using ScoreToContext = std::set<pair<Score, TextRecordIndex>>;
  using ScoreAndStC = pair<Score, ScoreToContext>;
  using AggMap = ad_utility::HashMap<Id, ScoreAndStC>;
  AggMap map;
  for (size_t i = 0; i < eids.size(); ++i) {
    if (fSet.count(eids[i]) > 0) {
      if (map.count(eids[i]) == 0) {
        ScoreToContext inner;
        inner.insert(std::make_pair(scores[i], cids[i]));
        map[eids[i]] = std::make_pair(1, inner);
      } else {
        auto& val = map[eids[i]];
        // val.first += scores[i];
        ++val.first;
        ScoreToContext& stc = val.second;
        if (stc.size() < k || stc.begin()->first < scores[i]) {
          if (stc.size() == k) {
            stc.erase(*stc.begin());
          }
          stc.insert(std::make_pair(scores[i], cids[i]));
        };
      }
    }
  }
  IdTableStatic<3> result = dynResult->moveToStatic<3>();
  result.reserve(map.size() * k + 2);
  for (auto it = map.begin(); it != map.end(); ++it) {
    Id eid = it->first;
    Id score = Id::make(it->second.first);
    ScoreToContext& stc = it->second.second;
    for (auto itt = stc.rbegin(); itt != stc.rend(); ++itt) {
      result.push_back({Id::make(itt->second), score, eid});
    }
  }
  *dynResult = result.moveToDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
};

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult) {
  if (cids.size() == 0 || fMap.size() == 0) {
    return;
  }
  // Go over contexts.
  // For each context build a cross product with one part being from the filter.
  // Store them in a map, use a pair of id's as key and
  // an appropriate hash function.
  // Use a set (ordered) and keep it at size kLimit for the context scores.
  LOG(DEBUG) << "Heap-using case with " << kLimit
             << " contexts per entity...\n";
  using ScoreToContext = std::set<pair<Score, TextRecordIndex>>;
  using ScoreAndStC = pair<Score, ScoreToContext>;
  using AggMap = ad_utility::HashMap<vector<Id>, ScoreAndStC>;
  AggMap map;
  vector<Id> entitiesInContext;
  vector<Id> filteredEntitiesInContext;
  TextRecordIndex currentCid = cids[0];
  Score cscore = scores[0];

  for (size_t i = 0; i < cids.size(); ++i) {
    if (cids[i] == currentCid) {
      if (fMap.count(eids[i]) > 0) {
        filteredEntitiesInContext.push_back(eids[i]);
      }
      entitiesInContext.push_back(eids[i]);
      // cscore += scores[i];
    } else {
      if (filteredEntitiesInContext.size() > 0) {
        // Calculate a cross product and add/update the map
        size_t nofPossibilities =
            filteredEntitiesInContext.size() *
            static_cast<size_t>(pow(entitiesInContext.size(), nofVars - 1));
        for (size_t j = 0; j < nofPossibilities; ++j) {
          vector<Id> key;
          key.reserve(nofVars);
          size_t n = j;
          key.push_back(
              filteredEntitiesInContext[n % filteredEntitiesInContext.size()]);
          n /= filteredEntitiesInContext.size();
          for (size_t k = 1; k < nofVars; ++k) {
            key.push_back(entitiesInContext[n % entitiesInContext.size()]);
            n /= entitiesInContext.size();
          }
          if (map.count(key) == 0) {
            ScoreToContext inner;
            inner.insert(std::make_pair(cscore, currentCid));
            map[key] = std::make_pair(1, inner);
          } else {
            auto& val = map[key];
            // val.first += scores[i];
            ++val.first;
            ScoreToContext& stc = val.second;
            if (stc.size() < kLimit || stc.begin()->first < cscore) {
              if (stc.size() == kLimit) {
                stc.erase(*stc.begin());
              }
              stc.insert(std::make_pair(cscore, currentCid));
            };
          }
        }
      }
      entitiesInContext.clear();
      filteredEntitiesInContext.clear();
      currentCid = cids[i];
      cscore = scores[i];
      entitiesInContext.push_back(eids[i]);
      if (fMap.count(eids[i]) > 0) {
        filteredEntitiesInContext.push_back(eids[i]);
      }
    }
  }
  // Deal with the last context
  if (filteredEntitiesInContext.size() > 0) {
    // Calculate a cross product and add/update the map
    size_t nofPossibilities =
        filteredEntitiesInContext.size() *
        static_cast<size_t>(pow(entitiesInContext.size(), nofVars - 1));
    for (size_t j = 0; j < nofPossibilities; ++j) {
      vector<Id> key;
      key.reserve(nofVars);
      size_t n = j;
      key.push_back(
          filteredEntitiesInContext[n % filteredEntitiesInContext.size()]);
      n /= filteredEntitiesInContext.size();
      for (size_t k = 1; k < nofVars; ++k) {
        key.push_back(entitiesInContext[n % entitiesInContext.size()]);
        n /= entitiesInContext.size();
      }
      if (map.count(key) == 0) {
        ScoreToContext inner;
        inner.insert(std::make_pair(cscore, currentCid));
        map[key] = std::make_pair(1, inner);
      } else {
        auto& val = map[key];
        // val.first += scores[i];
        ++val.first;
        ScoreToContext& stc = val.second;
        if (stc.size() < kLimit || stc.begin()->first < cscore) {
          if (stc.size() == kLimit) {
            stc.erase(*stc.begin());
          }
          stc.insert(std::make_pair(cscore, currentCid));
        };
      }
    }
  }

  // Iterate over the map and populate the result.
  IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
  for (auto it = map.begin(); it != map.end(); ++it) {
    ScoreToContext& stc = it->second.second;
    Id rscore = Id::make(it->second.first);
    for (auto itt = stc.rbegin(); itt != stc.rend(); ++itt) {
      const vector<Id>& keyEids = it->first;
      const IdTable& filterRows = fMap.find(keyEids[0])->second;
      for (auto fRow : filterRows) {
        size_t n = result.size();
        result.push_back();
        result(n, 0) = Id::make(itt->second);  // cid
        result(n, 1) = rscore;
        size_t off = 2;
        for (size_t i = 1; i < keyEids.size(); i++) {
          result(n, off) = keyEids[i];
          off++;
        }
        for (size_t i = 0; i < fRow.size(); i++) {
          result(n, off) = fRow[i];
          off++;
        }
      }
    }
  }
  *dynResult = result.moveToDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
}

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<0>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<1>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<2>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<3>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<4>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<5>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<6>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<7>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<8>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<9>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<10>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult) {
  if (cids.size() == 0 || fSet.size() == 0) {
    return;
  }
  // Go over contexts.
  // For each context build a cross product with one part being from the filter.
  // Store them in a map, use a pair of id's as key and
  // an appropriate hash function.
  // Use a set (ordered) and keep it at size kLimit for the context scores.
  LOG(DEBUG) << "Heap-using case with " << kLimit
             << " contexts per entity...\n";
  using ScoreToContext = std::set<pair<Score, TextRecordIndex>>;
  using ScoreAndStC = pair<Score, ScoreToContext>;
  using AggMap = ad_utility::HashMap<vector<Id>, ScoreAndStC>;
  AggMap map;
  vector<Id> entitiesInContext;
  vector<Id> filteredEntitiesInContext;
  TextRecordIndex currentCid = cids[0];
  Score cscore = scores[0];

  for (size_t i = 0; i < cids.size(); ++i) {
    if (cids[i] == currentCid) {
      if (fSet.count(eids[i]) > 0) {
        filteredEntitiesInContext.push_back(eids[i]);
      }
      entitiesInContext.push_back(eids[i]);
      // cscore += scores[i];
    } else {
      if (filteredEntitiesInContext.size() > 0) {
        // Calculate a cross product and add/update the map
        size_t nofPossibilities =
            filteredEntitiesInContext.size() *
            static_cast<size_t>(pow(entitiesInContext.size(), nofVars - 1));
        for (size_t j = 0; j < nofPossibilities; ++j) {
          vector<Id> key;
          key.reserve(nofVars);
          size_t n = j;
          key.push_back(
              filteredEntitiesInContext[n % filteredEntitiesInContext.size()]);
          n /= filteredEntitiesInContext.size();
          for (size_t k = 1; k < nofVars; ++k) {
            key.push_back(entitiesInContext[n % entitiesInContext.size()]);
            n /= entitiesInContext.size();
          }
          if (!map.contains(key)) {
            ScoreToContext inner;
            inner.insert(std::make_pair(cscore, currentCid));
            map[key] = std::make_pair(1, inner);
          } else {
            auto& val = map[key];
            // val.first += scores[i];
            ++val.first;
            ScoreToContext& stc = val.second;
            if (stc.size() < kLimit || stc.begin()->first < cscore) {
              if (stc.size() == kLimit) {
                stc.erase(*stc.begin());
              }
              stc.insert(std::make_pair(cscore, currentCid));
            };
          }
        }
      }
      entitiesInContext.clear();
      filteredEntitiesInContext.clear();
      currentCid = cids[i];
      cscore = scores[i];
      entitiesInContext.push_back(eids[i]);
      if (fSet.count(eids[i]) > 0) {
        filteredEntitiesInContext.push_back(eids[i]);
      }
    }
  }
  // Deal with the last context
  if (filteredEntitiesInContext.size() > 0) {
    // Calculate a cross product and add/update the map
    size_t nofPossibilities =
        filteredEntitiesInContext.size() *
        static_cast<size_t>(pow(entitiesInContext.size(), nofVars - 1));
    for (size_t j = 0; j < nofPossibilities; ++j) {
      vector<Id> key;
      key.reserve(nofVars);
      size_t n = j;
      key.push_back(
          filteredEntitiesInContext[n % filteredEntitiesInContext.size()]);
      n /= filteredEntitiesInContext.size();
      for (size_t k = 1; k < nofVars; ++k) {
        key.push_back(entitiesInContext[n % entitiesInContext.size()]);
        n /= entitiesInContext.size();
      }
      if (map.count(key) == 0) {
        ScoreToContext inner;
        inner.insert(std::make_pair(cscore, currentCid));
        map[key] = std::make_pair(1, inner);
      } else {
        auto& val = map[key];
        // val.first += scores[i];
        ++val.first;
        ScoreToContext& stc = val.second;
        if (stc.size() < kLimit || stc.begin()->first < cscore) {
          if (stc.size() == kLimit) {
            stc.erase(*stc.begin());
          }
          stc.insert(std::make_pair(cscore, currentCid));
        };
      }
    }
  }

  // Iterate over the map and populate the result.
  IdTableStatic<WIDTH> result = dynResult->moveToStatic<WIDTH>();
  for (auto it = map.begin(); it != map.end(); ++it) {
    ScoreToContext& stc = it->second.second;
    Id rscore = Id::make(it->second.first);
    for (auto itt = stc.rbegin(); itt != stc.rend(); ++itt) {
      const vector<Id>& keyEids = it->first;
      size_t n = result.size();
      result.push_back();
      result(n, 0) = Id::make(itt->second);  // cid
      result(n, 1) = rscore;
      size_t off = 2;
      for (size_t i = 1; i < keyEids.size(); i++) {
        result(n, 2 + i) = keyEids[i];
        off++;
      }
      result(n, off) = keyEids[0];
    }
  }
  *dynResult = result.moveToDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
}

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<0>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<1>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<2>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<3>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<4>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<5>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<6>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<7>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<8>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<9>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<10>(
    const vector<TextRecordIndex>& cids, const vector<Id>& eids,
    const vector<Score>& scores, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
