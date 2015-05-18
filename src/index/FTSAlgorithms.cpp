// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <utility>
#include <unordered_map>
#include "./FTSAlgorithms.h"

using std::pair;
using std::unordered_map;

// _____________________________________________________________________________
void FTSAlgorithms::filterByRange(const IdRange& idRange,
                                  const vector<Id>& blockCids,
                                  const vector<Id>& blockWids,
                                  const vector<Score>& blockScores,
                                  vector<Id>& resultCids,
                                  vector<Score>& resultScores) {
  AD_CHECK(blockCids.size() == blockWids.size());
  AD_CHECK(blockCids.size() == blockScores.size());
  LOG(DEBUG) << "Filtering " << blockCids.size() <<
             " elements by ID range...\n";

  resultCids.resize(blockCids.size() + 2);
  resultScores.resize(blockCids.size() + 2);
  size_t nofResultElements = 0;

  for (size_t i = 0; i < blockWids.size(); ++i) {
    if (blockWids[i] >= idRange._first && blockWids[i] <= idRange._last) {
      resultCids[nofResultElements] = blockCids[i];
      resultScores[nofResultElements++] = blockScores[i];
    }
  }

  resultCids.resize(nofResultElements);
  resultScores.resize(nofResultElements);

  AD_CHECK(resultCids.size() == resultScores.size());
  LOG(DEBUG) << "Filtering by ID range done. Result has " <<
             resultCids.size() << " elements.\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::getTopKByScores(const vector<Id>& cids,
                                    const vector<Score>& scores,
                                    size_t k,
                                    WidthOneList *result) {
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
  std::partial_sort(indices.begin(), indices.begin() + k, indices.end(),
                    [&scores](size_t a, size_t b) {
                      return scores[a] > scores[b];
                    });
  LOG(DEBUG) << "Packing the final WidthOneList of cIds...\n";
  result->reserve(k + 2);
  result->resize(k);
  for (size_t i = 0; i < k; ++i) {
    (*result)[i] = {{cids[indices[i]]}};
  }
  LOG(DEBUG) << "Done with getTopKByScores.\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::aggScoresAndTakeTopKContexts(const vector<Id>& cids,
                                                 const vector<Id>& eids,
                                                 const vector<Score>& scores,
                                                 size_t k,
                                                 WidthThreeList *result) {
  AD_CHECK_EQ(cids.size(), eids.size());
  AD_CHECK_EQ(cids.size(), scores.size());
  LOG(DEBUG) << "Going from an entity, context and score list of size: "
             << cids.size() << " elements to a table with distinct entities "
             << "and at most " << k << " contexts per entity.\n";

  // The default case where k == 1 can use a map for a O(n) solution
  if (k == 1) {
    aggScoresAndTakeTopContext(cids, eids, scores, result);
    return;
  }

  // Otherwise use a max heap of size k for the context scores  to achieve
  // O(k log n)
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           "> 1 contexts per entity not yet implemented.")


  // The result is NOT sorted due to the usage of maps.
  // Resorting the result is a separate operation now.
  // Benefit 1) it's not always necessary to sort.
  // Benefit 2) The result size can be MUCH smaller than n.
  LOG(DEBUG) << "Done. There are " << result->size() <<
             " entity-score-context tuples now.\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::aggScoresAndTakeTopContext(const vector<Id>& cids,
                                               const vector<Id>& eids,
                                               const vector<Score>& scores,
                                               WidthThreeList *result) {
  LOG(DEBUG) << "Special case with 1 contexts per entity...\n";
  typedef unordered_map<Id, pair<Score, pair<Id, Score>>> AggMap;
  AggMap map;
  for (size_t i = 0; i < eids.size(); ++i) {
    if (map.count(eids[i]) == 0) {
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
  result->reserve(map.size() + 2);
  result->resize(map.size());
  size_t n = 0;
  for (auto it = map.begin(); it != map.end(); ++it) {
    (*result)[n++] =
        array<Id, 3>{{
                         it->first,  // entity
                         static_cast<Id>(it->second.first),  // entity score
                         it->second.second.first  // top context Id
                     }};
  }
  AD_CHECK_EQ(n, result->size());
  LOG(DEBUG) << "Done. There are " << result->size() <<
             " entity-score-context tuples now.\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::intersect(const vector<Id>& matchingContexts,
                              const vector<Score>& matchingContextScores,
                              const vector<Id>& eBlockCids,
                              const vector<Id>& eBlockWids,
                              const vector<Score>& eBlockScores,
                              vector<Id>& resultCids,
                              vector<Id>& resultEids,
                              vector<Score>& resultScores) {
  LOG(DEBUG) << "Intersection to filter the entity postings from a block "
             << "so that only matching ones remain\n";
  LOG(DEBUG) << "matchingContexts size: " << matchingContexts.size() << '\n';
  LOG(DEBUG) << "eBlockCids size: " << eBlockCids.size() << '\n';
  resultCids.reserve(eBlockCids.size() + 2);
  resultEids.reserve(eBlockCids.size() + 2);
  resultScores.reserve(eBlockCids.size() + 2);
  resultCids.resize(eBlockCids.size());
  resultEids.resize(eBlockCids.size());
  resultScores.resize(eBlockCids.size());
  // Cast away constness so we can add sentinels that will be removed
  // in the end and create and add those sentinels.
  // Note: this is only efficient if capacity + 2 >= size for the input
  // context lists. For now, we assume that all lists are read from disk
  // where they had more than enough size allocated.
  auto& l1 = const_cast<vector<Id>&>(matchingContexts);
  auto& l2 = const_cast<vector<Id>&>(eBlockCids);
  // The two below are needed for the final sentinel match
  auto& wids = const_cast<vector<Id>&>(eBlockWids);
  auto& scores = const_cast<vector<Score>&>(eBlockScores);

  Id sent1 = std::numeric_limits<Id>::max();
  Id sent2 = std::numeric_limits<Id>::max() - 1;
  Id sentMatch = std::numeric_limits<Id>::max() - 2;

  l1.push_back(sentMatch);
  l2.push_back(sentMatch);
  l1.push_back(sent1);
  l2.push_back(sent2);
  wids.push_back(0);
  scores.push_back(0);

  size_t i = 0;
  size_t j = 0;
  size_t n = 0;

  while (l1[i] < sent1) {
    while (l1[i] < l2[j]) { ++i; }
    while (l2[j] < l1[i]) { ++j; }
    while (l1[i] == l2[j]) {
      // Make sure we get all matching elements from the entity list (l2)
      // that match the current context.
      // If there are multiple elements for that context in l1,
      // we can safely skip them unless we want to incorporate the scores
      // later on.
      resultCids[n] = l2[j];
      resultEids[n] = wids[j];
      resultScores[n++] = scores[j++];
    }
    ++i;
  }

  // Remove sentinels
  l1.resize(l1.size() - 2);
  l2.resize(l2.size() - 2);
  wids.resize(wids.size() - 1);
  scores.resize(scores.size() - 1);
  resultCids.resize(n - 1);
  resultEids.resize(n - 1);
  resultScores.resize(n - 1);
  LOG(DEBUG) << "Intersection done. Size: " << resultCids.size() << "\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::intersectTwoPostingLists(const vector<Id>& cids1,
                                             const vector<Score>& scores1,
                                             const vector<Id>& cids2,
                                             const vector<Score>& scores2,
                                             vector<Id>& resultCids,
                                             vector<Score>& resultScores) {
  LOG(DEBUG) << "Intersection of words lists of sizes " << cids1.size() <<
             " and " << cids2.size() << '\n';
  resultCids.reserve(cids1.size() + 2);
  resultScores.reserve(cids1.size() + 2);
  resultCids.resize(cids1.size());
  resultScores.resize(cids1.size());
  // Cast away constness so we can add sentinels that will be removed
  // in the end and create and add those sentinels.
  // Note: this is only efficient if capacity + 2 >= size for the input
  // context lists. For now, we assume that all lists are read from disk
  // where they had more than enough size allocated.
  auto& l1 = const_cast<vector<Id>&>(cids1);
  auto& l2 = const_cast<vector<Id>&>(cids2);
  // The two below are needed for the final sentinel match
  auto& s1 = const_cast<vector<Score>&>(scores1);
  auto& s2 = const_cast<vector<Score>&>(scores2);

  Id sent1 = std::numeric_limits<Id>::max();
  Id sent2 = std::numeric_limits<Id>::max() - 1;
  Id sentMatch = std::numeric_limits<Id>::max() - 2;

  l1.push_back(sentMatch);
  l2.push_back(sentMatch);
  l1.push_back(sent1);
  l2.push_back(sent2);
  s1.push_back(0);
  s2.push_back(0);

  size_t i = 0;
  size_t j = 0;
  size_t n = 0;

  while (l1[i] < sent1) {
    while (l1[i] < l2[j]) { ++i; }
    while (l2[j] < l1[i]) { ++j; }
    while (l1[i] == l2[j]) {
      resultCids[n] = l2[j];
      resultScores[n++] = s1[i] + s2[j];
      ++i;
      ++j;
    }
  }

  // Remove sentinels
  l1.resize(l1.size() - 2);
  l2.resize(l2.size() - 2);
  s1.resize(s1.size() - 1);
  s2.resize(s2.size() - 1);
  resultCids.resize(n - 1);
  resultScores.resize(n - 1);

  LOG(DEBUG) << "Intersection done. Size: " << resultCids.size() << "\n";
}

// _____________________________________________________________________________
void FTSAlgorithms::intersectKWay(const vector<vector<Id>>& cidVecs,
                                  const vector<vector<Score>>& scoreVecs,
                                  vector<Id> *lastListEids,
                                  vector<Id>& resCids,
                                  vector<Id>& resEids,
                                  vector<Score>& resScores) {
  size_t k = cidVecs.size();
  LOG(DEBUG) << "K-way intersection of " << k << " lists of sizes:\n";

  const bool entityMode = lastListEids != nullptr;

  size_t minSize = std::numeric_limits<size_t>::max();
  if (entityMode) {
    minSize = lastListEids->size();
  } else {
    for (size_t i = 0; i < cidVecs.size(); ++i) {
      if (cidVecs[i].size() == 0) { return; }
      if (cidVecs[i].size() < minSize) { minSize = cidVecs[i].size(); }
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
  Id currentContext = cidVecs[k - 1][0];
  size_t currentList = k - 1; // Has the fewest different contexts. Start here.
  size_t streak = 0;
  size_t n = 0;
  while (true) {  // break when one list cannot advance
    size_t thisListSize = cidVecs[currentList].size();
    while (cidVecs[currentList][nextIndices[currentList]] < currentContext) {
      nextIndices[currentList] += 1;
      if (nextIndices[currentList] == thisListSize) { break; }
    }
    if (nextIndices[currentList] == thisListSize) { break; }
    Id atId = cidVecs[currentList][nextIndices[currentList]];
    if (atId == currentContext) {
      if (++streak == k) {
        Score s = 0;
        for (size_t i = 0; i < k - 1; ++i) {
          s += scoreVecs[i][(i == currentList ?
                             nextIndices[i] : nextIndices[i] - 1)];
        }
        if (entityMode) {
          // If entities are involved, there may be multiple postings
          // for one context. Handle all matching the current context.
          while (nextIndices[k - 1] - 1 < cidVecs[k - 1].size()
                 && cidVecs[k - 1][nextIndices[k - 1] - 1] == currentContext) {
            resCids[n] = currentContext;
            resEids[n] = (*lastListEids)[nextIndices[k - 1] - 1];
            resScores[n++] = s + scoreVecs[k - 1][nextIndices[k - 1] - 1];
            nextIndices[k - 1] += 1;
          }
        } else {
          resCids[n] = currentContext;
          resScores[n++] = s + scoreVecs[k - 1][(k - 1 == currentList ?
                                                 nextIndices[k - 1] :
                                                 nextIndices[k - 1] - 1)];
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
    if (currentList == k) { currentList = 0; }  // wrap around
  }


  resCids.resize(n);
  resScores.resize(n);
  if (entityMode) { resEids.resize(n); }
  LOG(DEBUG) << "Intersection done. Size: " << resCids.size() << "\n";
}