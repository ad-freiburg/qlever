// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./FTSAlgorithms.h"

#include <cmath>
#include <map>
#include <set>
#include <utility>

#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/SuppressWarnings.h"

using std::pair;

// _____________________________________________________________________________
Index::WordEntityPostings FTSAlgorithms::filterByRangeWep(
    const IdRange<WordVocabIndex>& idRange,
    const WordEntityPostings& wepPreFilter) {
  AD_CONTRACT_CHECK(wepPreFilter.wids_.size() == 1);
  AD_CONTRACT_CHECK(wepPreFilter.cids_.size() ==
                    wepPreFilter.wids_.at(0).size());
  AD_CONTRACT_CHECK(wepPreFilter.cids_.size() == wepPreFilter.scores_.size());
  LOG(DEBUG) << "Filtering " << wepPreFilter.cids_.size()
             << " elements by ID range...\n";

  WordEntityPostings wepResult;
  const vector<WordIndex>& wordIdsInput = wepPreFilter.wids_.at(0);
  vector<WordIndex>& wordIdsResult = wepResult.wids_.at(0);

  wepResult.cids_.resize(wepPreFilter.cids_.size() + 2);
  wepResult.scores_.resize(wepPreFilter.cids_.size() + 2);
  wordIdsResult.resize(wepPreFilter.cids_.size() + 2);

  size_t nofResultElements = 0;

  for (size_t i = 0; i < wordIdsInput.size(); ++i) {
    // TODO<joka921> proper Ids for the text stuff.
    // The mapping from words that appear in text records to `WordIndex`es is
    // stored in a `Vocabulary` that stores `VocabIndex`es, so we have to
    // convert between those two types.
    // TODO<joka921> Can we make the returned `IndexType` a template parameter
    // of the vocabulary, s.t. we have a vocabulary that stores `WordIndex`es
    // directly?
    if (wordIdsInput[i] >= idRange.first().get() &&
        wordIdsInput[i] <= idRange.last().get()) {
      wepResult.cids_[nofResultElements] = wepPreFilter.cids_[i];
      wepResult.scores_[nofResultElements] = wepPreFilter.scores_[i];
      wordIdsResult[nofResultElements++] = wordIdsInput[i];
    }
  }

  wepResult.cids_.resize(nofResultElements);
  wepResult.scores_.resize(nofResultElements);
  wordIdsResult.resize(nofResultElements);

  AD_CONTRACT_CHECK(wepResult.cids_.size() == wepResult.scores_.size());
  AD_CONTRACT_CHECK(wepResult.cids_.size() == wordIdsResult.size());
  LOG(DEBUG) << "Filtering by ID range done. Result has "
             << wepResult.cids_.size() << " elements.\n";
  return wepResult;
}

// _____________________________________________________________________________
IdTable FTSAlgorithms::filterByRange(const IdRange<WordVocabIndex>& idRange,
                                     const IdTable& idTablePreFilter) {
  AD_CONTRACT_CHECK(idTablePreFilter.numColumns() == 2);
  LOG(DEBUG) << "Filtering " << idTablePreFilter.getColumn(0).size()
             << " elements by ID range...\n";

  IdTable idTableResult{idTablePreFilter.getAllocator()};
  idTableResult.setNumColumns(2);
  idTableResult.resize(idTablePreFilter.getColumn(0).size());

  decltype(auto) resultCidColumn = idTableResult.getColumn(0);
  decltype(auto) resultWidColumn = idTableResult.getColumn(1);
  size_t nofResultElements = 0;
  decltype(auto) preFilterCidColumn = idTablePreFilter.getColumn(0);
  decltype(auto) preFilterWidColumn = idTablePreFilter.getColumn(1);
  // TODO<C++23> Use views::zip.
  for (size_t i = 0; i < preFilterWidColumn.size(); ++i) {
    // TODO<joka921> proper Ids for the text stuff.
    // The mapping from words that appear in text records to `WordIndex`es is
    // stored in a `Vocabulary` that stores `VocabIndex`es, so we have to
    // convert between those two types.
    // TODO<joka921> Can we make the returned `IndexType` a template parameter
    // of the vocabulary, s.t. we have a vocabulary that stores `WordIndex`es
    // directly?
    if (preFilterWidColumn[i].getWordVocabIndex() >= idRange.first() &&
        preFilterWidColumn[i].getWordVocabIndex() <= idRange.last()) {
      resultCidColumn[nofResultElements] = preFilterCidColumn[i];
      resultWidColumn[nofResultElements] = preFilterWidColumn[i];
      nofResultElements++;
    }
  }

  idTableResult.resize(nofResultElements);

  LOG(DEBUG) << "Filtering by ID range done. Result has "
             << idTableResult.numRows() << " elements.\n";
  return idTableResult;
}

// _____________________________________________________________________________
Index::WordEntityPostings FTSAlgorithms::crossIntersect(
    const WordEntityPostings& matchingContextsWep,
    const WordEntityPostings& eBlockWep) {
  AD_CONTRACT_CHECK(matchingContextsWep.wids_.size() == 1);
  LOG(DEBUG)
      << "Intersection to filter the word-entity postings from a block so that "
      << "only entries remain where the context matches. If there are multiple "
      << "entries with the same cid, then the crossproduct of them remains.\n";
  LOG(DEBUG) << "matchingContextsWep.cids_ size: "
             << matchingContextsWep.cids_.size() << '\n';
  LOG(DEBUG) << "eBlockWep.cids_ size: " << eBlockWep.cids_.size() << '\n';
  WordEntityPostings resultWep;
  // Handle trivial empty case
  if (matchingContextsWep.cids_.empty() || eBlockWep.cids_.empty()) {
    return resultWep;
  }
  resultWep.wids_.at(0).reserve(eBlockWep.cids_.size());
  resultWep.wids_.at(0).clear();
  resultWep.cids_.reserve(eBlockWep.cids_.size());
  resultWep.cids_.clear();
  resultWep.eids_.reserve(eBlockWep.cids_.size());
  resultWep.eids_.clear();
  resultWep.scores_.reserve(eBlockWep.cids_.size());
  resultWep.scores_.clear();

  size_t i = 0;
  size_t j = 0;

  while (i < matchingContextsWep.cids_.size() && j < eBlockWep.cids_.size()) {
    while (matchingContextsWep.cids_[i] < eBlockWep.cids_[j]) {
      ++i;
      if (i >= matchingContextsWep.cids_.size()) {
        return resultWep;
      }
    }
    while (eBlockWep.cids_[j] < matchingContextsWep.cids_[i]) {
      ++j;
      if (j >= eBlockWep.cids_.size()) {
        return resultWep;
      }
    }
    while (matchingContextsWep.cids_[i] == eBlockWep.cids_[j]) {
      size_t k = 0;
      while (matchingContextsWep.cids_[i + k] == matchingContextsWep.cids_[i]) {
        // Make sure that we get every combination of eids and wid where their
        // cids match.
        resultWep.wids_.at(0).push_back(matchingContextsWep.wids_.at(0)[i + k]);
        resultWep.cids_.push_back(eBlockWep.cids_[j]);
        resultWep.eids_.push_back(eBlockWep.eids_[j]);
        resultWep.scores_.push_back(eBlockWep.scores_[j]);
        k++;
        if (i + k >= matchingContextsWep.cids_.size()) {
          break;
        }
      }
      j++;
      if (j >= eBlockWep.cids_.size()) {
        break;
      }
    }
    ++i;
  }
  return resultWep;
}

// _____________________________________________________________________________
Index::WordEntityPostings FTSAlgorithms::crossIntersectKWay(
    const vector<WordEntityPostings>& wepVecs, vector<Id>* lastListEids) {
  size_t k = wepVecs.size();
  AD_CONTRACT_CHECK(k >= 1);
  WordEntityPostings resultWep;
  resultWep.wids_.resize(k);
  {
    if (wepVecs[k - 1].cids_.empty()) {
      LOG(DEBUG) << "Empty list involved, no intersect necessary.\n";
      return resultWep;
    }
    LOG(DEBUG) << "K-way intersection of " << k << " lists of sizes: ";
    for (const auto& l : wepVecs) {
      LOG(DEBUG) << l.cids_.size() << ' ';
    }
    LOG(DEBUG) << '\n';
  }

  const bool entityMode = lastListEids != nullptr;

  size_t minSize = std::numeric_limits<size_t>::max();
  if (entityMode) {
    minSize = lastListEids->size();
  } else {
    for (size_t i = 0; i < k; ++i) {
      if (wepVecs[i].cids_.empty()) {
        return resultWep;
      }
      if (wepVecs[i].cids_.size() < minSize) {
        minSize = wepVecs[i].cids_.size();
      }
    }
  }
  AD_CORRECTNESS_CHECK(minSize != std::numeric_limits<size_t>::max());

  resultWep.cids_.reserve(minSize);
  resultWep.scores_.reserve(minSize);
  if (lastListEids) {
    resultWep.eids_.reserve(minSize);
  }
  for (size_t j = 0; j < k; j++) {
    AD_CONTRACT_CHECK(wepVecs[j].wids_.size() == 1);
    resultWep.wids_[j].reserve(minSize);
  }

  // For intersection, we don't need a PQ.
  // The algorithm:
  // Remember the current context and the length of the streak
  // (i.e. in how many weps was that context found).
  // If the streak reaches k, write the context to the result.
  // If there are multiple entries for this context in one or more weps
  // calculate every possible combination and write them all on the result.
  // Until then, go through weps in a round-robin way and advance until
  // a) the context is found or
  // b) a higher context is found without a match before (reset cur and streak).
  // Stop as soon as one list cannot advance.

  // I think no PQ is needed, because unlike for merge, elements that
  // do not occur in all weps, don't have to be visited in the right order.

  vector<size_t> nextIndices;
  nextIndices.resize(wepVecs.size(), 0);
  TextRecordIndex currentContext = wepVecs[k - 1].cids_[0];
  size_t currentList = k - 1;  // Has the fewest different contexts. Start here.
  size_t streak = 0;
  size_t n = 0;
  while (true) {  // Break when one list cannot advance
    size_t thisListSize = wepVecs[currentList].cids_.size();
    if (nextIndices[currentList] == thisListSize) {
      break;
    }
    while (wepVecs[currentList].cids_[nextIndices[currentList]] <
           currentContext) {
      nextIndices[currentList] += 1;
      if (nextIndices[currentList] == thisListSize) {
        break;
      }
    }
    if (nextIndices[currentList] == thisListSize) {
      break;
    }
    TextRecordIndex atId = wepVecs[currentList].cids_[nextIndices[currentList]];
    if (atId == currentContext) {
      if (++streak == k) {  // If there is a match on the cids of all weps...
        Score s = 0;
        vector<size_t> currentIndices;
        currentIndices.resize(k, 0);
        for (size_t i = 0; i < k; ++i) {
          size_t index =
              (i == currentList ? nextIndices[i] : nextIndices[i] - 1);
          currentIndices[i] = index;
          if (i != k - 1) {
            s += wepVecs[i].scores_[index];
          }
        }
        int kIndex = k - 1;
        vector<size_t> offsets;
        offsets.resize(k, 0);
        while (kIndex >= 0) {  // Go over every combination of Indices where
                               // cids_[i] equals currentContext.
          if (currentIndices[kIndex] + offsets[kIndex] >=
                  wepVecs[kIndex].cids_.size() ||
              wepVecs[kIndex].cids_[currentIndices[kIndex] + offsets[kIndex]] !=
                  currentContext) {
            offsets[kIndex] = 0;
            kIndex--;
            if (kIndex >= 0) {
              offsets[kIndex]++;
            }
          } else {
            // Here takes the actual writing place.
            kIndex = k - 1;
            resultWep.cids_.push_back(currentContext);
            if (entityMode) {
              resultWep.eids_.push_back(
                  (*lastListEids)[currentIndices[k - 1] + offsets[k - 1]]);
            }
            resultWep.scores_.push_back(
                s +
                wepVecs[k - 1].scores_[currentIndices[k - 1] + offsets[k - 1]]);
            for (int c = k - 1; c >= 0; c--) {
              resultWep.wids_[c].push_back(
                  wepVecs[c].wids_.at(0)[currentIndices[c] + offsets[c]]);
            }
            n++;
            offsets[kIndex]++;
          }
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
    nextIndices[currentList] += 1;
    currentList++;
    if (currentList == k) {
      currentList = 0;
    }  // wrap around
  }

  resultWep.cids_.resize(n);
  resultWep.scores_.resize(n);
  if (entityMode) {
    resultWep.eids_.resize(n);
  }
  for (size_t j = 0; j < k; j++) {
    resultWep.wids_[j].resize(n);
  }
  LOG(DEBUG) << "Intersection done. Size: " << resultWep.cids_.size() << "\n";

  return resultWep;
}

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::aggScoresAndTakeTopKContexts(const WordEntityPostings& wep,
                                                 size_t k, IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);
  AD_CONTRACT_CHECK(wep.cids_.size() == wep.eids_.size());
  AD_CONTRACT_CHECK(wep.cids_.size() == wep.scores_.size());
  LOG(DEBUG)
      << "Going from a WordEntityPostings-Element consisting of an entity,"
      << " context, word and score list of size: " << wep.cids_.size()
      << " elements to a table with distinct entities "
      << "and at most " << k << " contexts per entity.\n";

  size_t numTerms = dynResult->numColumns() - 3;

  // The default case where k == 1 can use a map for a O(n) solution
  if (k == 1) {
    aggScoresAndTakeTopContext<WIDTH>(wep, dynResult);
    return;
  }

  AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
  for (size_t i = 0; i < numTerms; i++) {
    AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
  }

  // Use a set (ordered) and keep it at size k for the context scores
  // This achieves O(n log k)
  LOG(DEBUG) << "Heap-using case with " << k << " contexts per entity...\n";

  using AggMapContextAndEntityToWord =
      ad_utility::HashMap<std::pair<Id, TextRecordIndex>,
                          vector<vector<WordIndex>>>;
  using ScoreAndContextSet = std::set<std::pair<Score, TextRecordIndex>>;
  using AggMapEntityToSaCS =
      ad_utility::HashMap<Id, std::pair<Score, ScoreAndContextSet>>;
  AggMapContextAndEntityToWord mapEaCtW;
  AggMapEntityToSaCS mapEtSaCS;
  vector<WordIndex> wids;
  wids.resize(numTerms);
  for (size_t i = 0; i < wep.eids_.size(); ++i) {
    for (size_t l = 0; l < numTerms; l++) {
      wids[l] = wep.wids_[l].empty() ? 0 : wep.wids_[l][i];
    }
    mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].emplace_back(wids);
    if (!mapEtSaCS.contains(wep.eids_[i])) {
      mapEtSaCS[wep.eids_[i]].second.emplace(wep.scores_[i], wep.cids_[i]);
      mapEtSaCS[wep.eids_[i]].first = 1;
    } else {
      if (mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].size() >= 2) {
        continue;
      }
      mapEtSaCS[wep.eids_[i]].first++;
      ScoreAndContextSet& sacs = mapEtSaCS[wep.eids_[i]].second;
      // Make sure that sacs only contains the top k contexts for wep.eids_[i]
      if (sacs.size() < k || sacs.begin()->first < wep.scores_[i]) {
        if (sacs.size() == k) {
          sacs.erase(*sacs.begin());
        }
        sacs.emplace(wep.scores_[i], wep.cids_[i]);
      }
    }
  }

  // Write remaining wep entries into an IdTable
  IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
  result.reserve(mapEtSaCS.size() + 2);
  for (auto it = mapEtSaCS.begin(); it != mapEtSaCS.end(); ++it) {
    std::vector<ValueId> row;
    row.resize(result.numColumns());
    const Id eid = it->first;
    const Id entityScore = Id::makeFromInt(it->second.first);
    const ScoreAndContextSet& sacs = it->second.second;
    row[1] = entityScore;
    row[2] = eid;
    for (auto itt = sacs.rbegin(); itt != sacs.rend(); ++itt) {
      const TextRecordIndex cid = itt->second;
      row[0] = Id::makeFromTextRecordIndex(cid);
      const vector<vector<WordIndex>> widsVec =
          mapEaCtW[std::make_pair(eid, cid)];
      for (size_t j = 0; j < widsVec.size(); j++) {
        for (size_t l = 0; l < numTerms; l++) {
          row[3 + l] =
              Id::makeFromWordVocabIndex(WordVocabIndex::make(widsVec[j][l]));
        }
        result.push_back(row);
      }
    }
  }
  *dynResult = std::move(result).toDynamic();

  // The result is NOT sorted due to the usage of maps.
  // Resorting the result is a separate operation now.
  // Benefit 1) it's not always necessary to sort.
  // Benefit 2) The result size can be MUCH smaller than n.
  LOG(DEBUG) << "Done. There are " << dynResult->size()
             << " entity-word-score-context tuples now.\n";
}

template void FTSAlgorithms::aggScoresAndTakeTopKContexts<0>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<1>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<2>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<3>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<4>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<5>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<6>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<7>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<8>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<9>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopKContexts<10>(
    const WordEntityPostings& wep, size_t k, IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::aggScoresAndTakeTopContext(const WordEntityPostings& wep,
                                               IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);
  LOG(DEBUG) << "Special case with 1 contexts per entity...\n";
  using AggMapContextAndEntityToWord =
      ad_utility::HashMap<std::pair<Id, TextRecordIndex>,
                          vector<vector<WordIndex>>>;
  using ScoreAndContext = std::pair<Score, TextRecordIndex>;
  using AggMapEntityToSaC =
      ad_utility::HashMap<Id, std::pair<Score, ScoreAndContext>>;
  AggMapContextAndEntityToWord mapEaCtW;
  AggMapEntityToSaC mapEtSaC;

  size_t numTerms = dynResult->numColumns() - 3;

  AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
  for (size_t i = 0; i < numTerms; i++) {
    AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
  }

  vector<WordIndex> wids;
  wids.resize(numTerms);
  for (size_t i = 0; i < wep.eids_.size(); ++i) {
    for (size_t l = 0; l < numTerms; l++) {
      wids[l] = wep.wids_[l].empty() ? 0 : wep.wids_[l][i];
    }
    mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].emplace_back(wids);
    if (!mapEtSaC.contains(wep.eids_[i])) {
      mapEtSaC[wep.eids_[i]].second =
          std::make_pair(wep.scores_[i], wep.cids_[i]);
      mapEtSaC[wep.eids_[i]].first = 1;
    } else {
      if (mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].size() >= 2) {
        continue;
      }
      mapEtSaC[wep.eids_[i]].first++;
      ScoreAndContext& sac = mapEtSaC[wep.eids_[i]].second;
      // Make sure that sac only contains the top context for wep.eids_[i]
      if (sac.first < wep.scores_[i]) {
        sac = std::make_pair(wep.scores_[i], wep.cids_[i]);
      };
    }
  }
  // Write remaining wep entries into an IdTable
  IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
  result.reserve(mapEtSaC.size() + 2);
  for (auto it = mapEtSaC.begin(); it != mapEtSaC.end(); ++it) {
    std::vector<ValueId> row;
    row.resize(result.numColumns());
    const ScoreAndContext& sac = it->second.second;
    const Id eid = it->first;
    const Id entityScore = Id::makeFromInt(it->second.first);
    row[0] = Id::makeFromTextRecordIndex(sac.second);
    row[1] = entityScore;
    row[2] = eid;
    const vector<vector<WordIndex>> widsVec =
        mapEaCtW[std::make_pair(eid, sac.second)];
    for (size_t j = 0; j < widsVec.size(); j++) {
      for (size_t l = 0; l < numTerms; l++) {
        row[3 + l] =
            Id::makeFromWordVocabIndex(WordVocabIndex::make(widsVec[j][l]));
      }
      result.push_back(row);
    }
  }
  *dynResult = std::move(result).toDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size()
             << " context-score-entity tuples now.\n";
}

template void FTSAlgorithms::aggScoresAndTakeTopContext<0>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<1>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<2>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<3>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<4>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<5>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<6>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<7>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<8>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<9>(
    const WordEntityPostings& wep, IdTable* dynResult);
template void FTSAlgorithms::aggScoresAndTakeTopContext<10>(
    const WordEntityPostings& wep, IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);

  size_t numTerms = dynResult->numColumns() - 2 - nofVars;

  if (wep.cids_.empty()) {
    return;
  }
  if (kLimit == 1) {
    multVarsAggScoresAndTakeTopContext<WIDTH>(wep, nofVars, dynResult);
  } else {
    // Go over contexts.
    // For each context build a cross product of width 2.
    // Store them in a map, use a pair of id's as key and
    // an appropriate hash function.
    // Use a set (ordered) and keep it at size k for the context scores
    // This achieves O(n log k)
    LOG(DEBUG) << "Heap-using case with " << kLimit
               << " contexts per entity...\n";

    AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
    for (size_t i = 0; i < numTerms; i++) {
      AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
    }

    using AggMapContextAndEntityToWord =
        ad_utility::HashMap<std::pair<vector<Id>, TextRecordIndex>,
                            std::set<vector<WordIndex>>>;
    using ScoreAndContextSet = std::set<std::pair<Score, TextRecordIndex>>;
    using AggMapEntityToSaCS =
        ad_utility::HashMap<vector<Id>, std::pair<Score, ScoreAndContextSet>>;
    AggMapContextAndEntityToWord mapEaCtW;
    AggMapEntityToSaCS mapEtSaCS;
    vector<Id> entitiesInContext;
    TextRecordIndex currentCid = wep.cids_[0];
    Score cscore = wep.scores_[0];
    ad_utility::HashMap<Id, std::set<vector<WordIndex>>> currentCombinedWids;
    vector<WordIndex> wids;
    wids.resize(numTerms);
    for (size_t l = 0; l < numTerms; l++) {
      AD_CONTRACT_CHECK(wep.wids_[l].size() == wep.cids_.size());
      wids[l] = wep.wids_[l][0];
    }
    currentCombinedWids[wep.eids_[0]].insert(wids);

    for (size_t i = 0; i < wep.cids_.size(); ++i) {
      if (wep.cids_[i] == currentCid) {
        for (size_t l = 0; l < numTerms; l++) {
          wids[l] = wep.wids_[l][i];
        }
        if (entitiesInContext.empty() ||
            entitiesInContext.back() != wep.eids_[i]) {
          entitiesInContext.push_back(wep.eids_[i]);
        }
        currentCombinedWids[wep.eids_[i]].insert(wids);
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
          for (size_t k = 0; k < nofVars; k++) {
            for (auto ccw = currentCombinedWids[key[k]].begin();
                 ccw != currentCombinedWids[key[k]].end(); ++ccw) {
              mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
            }
          }
          if (!mapEtSaCS.contains(key)) {
            mapEtSaCS[key].second.emplace(cscore, currentCid);
            mapEtSaCS[key].first = 1;
          } else {
            mapEtSaCS[key].first++;
            ScoreAndContextSet& sacs = mapEtSaCS[key].second;
            // Make sure that sacs only contains the top k contexts for
            // wep.eids_[i]
            if (sacs.size() < kLimit || sacs.begin()->first < cscore) {
              if (sacs.size() == kLimit) {
                sacs.erase(*sacs.begin());
              }
              sacs.emplace(cscore, currentCid);
            }
          }
        }
        entitiesInContext.clear();
        currentCid = wep.cids_[i];
        cscore = wep.scores_[i];
        currentCombinedWids.clear();
        for (size_t l = 0; l < numTerms; l++) {
          wids[l] = wep.wids_[l][i];
        }
        currentCombinedWids[wep.eids_[i]].insert(wids);
        entitiesInContext.push_back(wep.eids_[i]);
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
      for (size_t k = 0; k < nofVars; k++) {
        for (auto ccw = currentCombinedWids[key[k]].begin();
             ccw != currentCombinedWids[key[k]].end(); ++ccw) {
          mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
        }
      }
      if (!mapEtSaCS.contains(key)) {
        mapEtSaCS[key].second.emplace(cscore, currentCid);
        mapEtSaCS[key].first = 1;
      } else {
        mapEtSaCS[key].first++;
        ScoreAndContextSet& sacs = mapEtSaCS[key].second;
        // Make sure that sacs only contains the top k contexts for wep.eids_[i]
        if (sacs.size() < kLimit || sacs.begin()->first < cscore) {
          if (sacs.size() == kLimit) {
            sacs.erase(*sacs.begin());
          }
          sacs.emplace(cscore, currentCid);
        }
      }
    }
    // Write remaining wep entries into an IdTable
    IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
    result.reserve(mapEtSaCS.size() + 2);
    for (auto it = mapEtSaCS.begin(); it != mapEtSaCS.end(); ++it) {
      std::vector<ValueId> row;
      row.resize(result.numColumns());
      const Id entityScore = Id::makeFromInt(it->second.first);
      const ScoreAndContextSet& sacs = it->second.second;
      row[1] = entityScore;
      for (size_t k = 0; k < nofVars; ++k) {
        row[k + 2] = it->first[k];  // eid
      }
      for (auto itt = sacs.rbegin(); itt != sacs.rend(); ++itt) {
        const TextRecordIndex cid = itt->second;
        row[0] = Id::makeFromTextRecordIndex(cid);
        for (auto wids = mapEaCtW[std::make_pair(it->first, cid)].begin();
             wids != mapEaCtW[std::make_pair(it->first, cid)].end(); wids++) {
          for (size_t l = 0; l < numTerms; l++) {
            row[2 + nofVars + l] =
                Id::makeFromWordVocabIndex(WordVocabIndex::make(wids->at(l)));
          }
          result.push_back(row);
        }
      }
    }
    *dynResult = std::move(result).toDynamic();
    LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
  }
}

template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<0>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<1>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<2>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<3>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<4>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<5>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<6>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<7>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<8>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<9>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopKContexts<10>(
    const WordEntityPostings& wep, size_t nofVars, size_t kLimit,
    IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsAggScoresAndTakeTopContext(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);
  LOG(DEBUG) << "Special case with 1 contexts per entity...\n";

  size_t numTerms = dynResult->numColumns() - 2 - nofVars;

  AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
  for (size_t i = 0; i < numTerms; i++) {
    AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
  }

  // Go over contexts.
  // For each context build a cross product of width 2.
  // Store them in a map, use a pair of id's as key and
  // an appropriate hash function.
  using AggMapContextAndEntityToWord =
      ad_utility::HashMap<std::pair<vector<Id>, TextRecordIndex>,
                          std::set<vector<WordIndex>>>;
  using ScoreAndContext = std::pair<Score, TextRecordIndex>;
  using AggMapEntityToSaC =
      ad_utility::HashMap<vector<Id>, std::pair<Score, ScoreAndContext>>;
  AggMapContextAndEntityToWord mapEaCtW;
  AggMapEntityToSaC mapEtSaC;
  // Note: vector{k} initializes with a single value k, as opposed to
  // vector<Id>(k), which initializes a vector with k default constructed
  // arguments.
  // TODO<joka921> proper Id types
  vector<Id> entitiesInContext;
  TextRecordIndex currentCid = wep.cids_[0];
  Score cscore = wep.scores_[0];
  ad_utility::HashMap<Id, std::set<vector<WordIndex>>> currentCombinedWids;
  vector<WordIndex> wids;
  wids.resize(numTerms);
  for (size_t l = 0; l < numTerms; l++) {
    AD_CONTRACT_CHECK(wep.wids_[l].size() == wep.cids_.size());
    wids[l] = wep.wids_[l][0];
  }
  currentCombinedWids[wep.eids_[0]].insert(wids);

  for (size_t i = 0; i < wep.cids_.size(); ++i) {
    if (wep.cids_[i] == currentCid) {
      for (size_t l = 0; l < numTerms; l++) {
        wids[l] = wep.wids_[l][i];
      }
      if (entitiesInContext.empty() ||
          entitiesInContext.back() != wep.eids_[i]) {
        entitiesInContext.push_back(wep.eids_[i]);
      }
      currentCombinedWids[wep.eids_[i]].insert(wids);
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
        for (size_t k = 0; k < nofVars; k++) {
          for (auto ccw = currentCombinedWids[key[k]].begin();
               ccw != currentCombinedWids[key[k]].end(); ++ccw) {
            mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
          }
        }
        if (!mapEtSaC.contains(key)) {
          mapEtSaC[key].second = std::make_pair(cscore, currentCid);
          mapEtSaC[key].first = 1;
        } else {
          mapEtSaC[key].first++;
          ScoreAndContext& sac = mapEtSaC[key].second;
          // Make sure that sac only contains the top context for
          // wep.eids_[i]
          if (sac.first < cscore) {
            sac = std::make_pair(cscore, currentCid);
          }
        }
      }
      entitiesInContext.clear();
      currentCid = wep.cids_[i];
      cscore = wep.scores_[i];
      currentCombinedWids.clear();
      for (size_t l = 0; l < numTerms; l++) {
        wids[l] = wep.wids_[l][i];
      }
      currentCombinedWids[wep.eids_[i]].insert(wids);
      entitiesInContext.push_back(wep.eids_[i]);
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
    for (size_t k = 0; k < nofVars; k++) {
      for (auto ccw = currentCombinedWids[key[k]].begin();
           ccw != currentCombinedWids[key[k]].end(); ++ccw) {
        mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
      }
    }
    if (!mapEtSaC.contains(key)) {
      mapEtSaC[key].second = std::make_pair(cscore, currentCid);
      mapEtSaC[key].first = 1;
    } else {
      mapEtSaC[key].first++;
      ScoreAndContext& sac = mapEtSaC[key].second;
      // Make sure that sac only contains the top context for wep.eids_[i]
      if (sac.first < cscore) {
        sac = std::make_pair(cscore, currentCid);
      }
    }
  }
  // Write remaining wep entries into an IdTable
  IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
  result.reserve(mapEtSaC.size() + 2);
  for (auto it = mapEtSaC.begin(); it != mapEtSaC.end(); ++it) {
    std::vector<ValueId> row;
    row.resize(result.numColumns());
    const ScoreAndContext& sac = it->second.second;
    const Id entityScore = Id::makeFromInt(it->second.first);
    row[0] = Id::makeFromTextRecordIndex(sac.second);
    row[1] = entityScore;
    for (size_t k = 0; k < nofVars; ++k) {
      row[k + 2] = it->first[k];  // eid
    }
    for (auto wids = mapEaCtW[std::make_pair(it->first, sac.second)].begin();
         wids != mapEaCtW[std::make_pair(it->first, sac.second)].end();
         wids++) {
      for (size_t l = 0; l < numTerms; l++) {
        row[2 + nofVars + l] =
            Id::makeFromWordVocabIndex(WordVocabIndex::make(wids->at(l)));
      }
      result.push_back(row);
    }
  }
  *dynResult = std::move(result).toDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
}

template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<0>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<1>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<2>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<3>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<4>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<5>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<6>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<7>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<8>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<9>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);
template void FTSAlgorithms::multVarsAggScoresAndTakeTopContext<10>(
    const WordEntityPostings& wep, size_t nofVars, IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);
  AD_CONTRACT_CHECK(wep.cids_.size() == wep.eids_.size());
  AD_CONTRACT_CHECK(wep.cids_.size() == wep.scores_.size());
  LOG(DEBUG)
      << "Going from a WordEntityPostings-Element consisting of an entity,"
      << " context, word and score list of size: " << wep.cids_.size()
      << " elements to a table with filtered distinct entities "
      << "and at most " << k << " contexts per entity.\n";
  if (wep.cids_.empty() || fMap.empty()) {
    return;
  }

  size_t numTerms =
      dynResult->numColumns() - 2 - fMap.begin()->second.numColumns();

  // TODO: add code to speed up for k==1

  AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
  for (size_t i = 0; i < numTerms; i++) {
    AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
  }

  // Use a set (ordered) and keep it at size k for the context scores
  // This achieves O(n log k)
  LOG(DEBUG) << "Heap-using case with " << k << " contexts per entity...\n";

  using AggMapContextAndEntityToWord =
      ad_utility::HashMap<std::pair<Id, TextRecordIndex>,
                          vector<vector<WordIndex>>>;
  using ScoreAndContextSet = std::set<std::pair<Score, TextRecordIndex>>;
  using AggMapEntityToSaCS =
      ad_utility::HashMap<Id, std::pair<Score, ScoreAndContextSet>>;
  AggMapContextAndEntityToWord mapEaCtW;
  AggMapEntityToSaCS mapEtSaCS;
  vector<WordIndex> wids;
  wids.resize(numTerms);
  for (size_t i = 0; i < wep.eids_.size(); ++i) {
    for (size_t l = 0; l < numTerms; l++) {
      wids[l] = wep.wids_[l].empty() ? 0 : wep.wids_[l][i];
    }
    if (fMap.contains(wep.eids_[i])) {
      mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].emplace_back(wids);
      if (!mapEtSaCS.contains(wep.eids_[i])) {
        mapEtSaCS[wep.eids_[i]].second.emplace(wep.scores_[i], wep.cids_[i]);
        mapEtSaCS[wep.eids_[i]].first = 1;
      } else {
        if (mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].size() >= 2) {
          continue;
        }
        mapEtSaCS[wep.eids_[i]].first++;
        ScoreAndContextSet& sacs = mapEtSaCS[wep.eids_[i]].second;
        // Make sure that sacs only contains the top k contexts for wep.eids_[i]
        if (sacs.size() < k || sacs.begin()->first < wep.scores_[i]) {
          if (sacs.size() == k) {
            sacs.erase(*sacs.begin());
          }
          sacs.emplace(wep.scores_[i], wep.cids_[i]);
        }
      }
    }
  }
  // Write remaining wep entries into an IdTable
  IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
  result.reserve(mapEtSaCS.size() + 2);
  for (auto it = mapEtSaCS.begin(); it != mapEtSaCS.end(); ++it) {
    std::vector<ValueId> row;
    row.resize(result.numColumns());
    const Id eid = it->first;
    const Id entityScore = Id::makeFromInt(it->second.first);
    const ScoreAndContextSet& sacs = it->second.second;
    row[1] = entityScore;
    for (auto itt = sacs.rbegin(); itt != sacs.rend(); ++itt) {
      const TextRecordIndex cid = itt->second;
      row[0] = Id::makeFromTextRecordIndex(cid);
      for (auto fRow : fMap.find(eid)->second) {
        size_t i;
        for (i = 0; i < fRow.numColumns(); i++) {
          row[2 + i] = fRow[i];
        }
        const vector<vector<WordIndex>> widsVec =
            mapEaCtW[std::make_pair(eid, cid)];
        for (size_t j = 0; j < widsVec.size(); j++) {
          for (size_t l = 0; l < numTerms; l++) {
            row[2 + i + l] =
                Id::makeFromWordVocabIndex(WordVocabIndex::make(widsVec[j][l]));
          }
          result.push_back(row);
        }
      }
    }
  }
  *dynResult = std::move(result).toDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
};

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<0>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<1>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<2>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<3>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<4>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<5>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<6>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<7>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<8>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<9>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<10>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t k, IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);
  AD_CONTRACT_CHECK(wep.cids_.size() == wep.eids_.size());
  AD_CONTRACT_CHECK(wep.cids_.size() == wep.scores_.size());
  LOG(DEBUG) << "Going from an entity, context and score list of size: "
             << wep.cids_.size()
             << " elements to a table with filtered distinct entities "
             << "and at most " << k << " contexts per entity.\n";

  if (wep.cids_.empty() || fSet.empty()) {
    return;
  }

  size_t numTerms = dynResult->numColumns() - 3;

  // TODO: add code to speed up for k==1

  AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
  for (size_t i = 0; i < numTerms; i++) {
    AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
  }

  // Use a set (ordered) and keep it at size k for the context scores
  // This achieves O(n log k)
  LOG(DEBUG) << "Heap-using case with " << k << " contexts per entity...\n";

  using AggMapContextAndEntityToWord =
      ad_utility::HashMap<std::pair<Id, TextRecordIndex>,
                          vector<vector<WordIndex>>>;
  using ScoreAndContextSet = std::set<std::pair<Score, TextRecordIndex>>;
  using AggMapEntityToSaCS =
      ad_utility::HashMap<Id, std::pair<Score, ScoreAndContextSet>>;
  AggMapContextAndEntityToWord mapEaCtW;
  AggMapEntityToSaCS mapEtSaCS;
  vector<WordIndex> wids;
  wids.resize(numTerms);
  for (size_t i = 0; i < wep.eids_.size(); ++i) {
    for (size_t l = 0; l < numTerms; l++) {
      wids[l] = wep.wids_[l].empty() ? 0 : wep.wids_[l][i];
    }
    if (fSet.contains(wep.eids_[i])) {
      mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].emplace_back(wids);
      if (!mapEtSaCS.contains(wep.eids_[i])) {
        mapEtSaCS[wep.eids_[i]].second.emplace(wep.scores_[i], wep.cids_[i]);
        mapEtSaCS[wep.eids_[i]].first = 1;
      } else {
        if (mapEaCtW[std::make_pair(wep.eids_[i], wep.cids_[i])].size() >= 2) {
          continue;
        }
        mapEtSaCS[wep.eids_[i]].first++;
        ScoreAndContextSet& sacs = mapEtSaCS[wep.eids_[i]].second;
        // Make sure that sacs only contains the top k contexts for wep.eids_[i]
        if (sacs.size() < k || sacs.begin()->first < wep.scores_[i]) {
          if (sacs.size() == k) {
            sacs.erase(*sacs.begin());
          }
          sacs.emplace(wep.scores_[i], wep.cids_[i]);
        }
      }
    }
  }
  // Write remaining wep entries into an IdTable
  IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
  result.reserve(mapEtSaCS.size() + 2);
  for (auto it = mapEtSaCS.begin(); it != mapEtSaCS.end(); ++it) {
    std::vector<ValueId> row;
    row.resize(result.numColumns());
    const Id eid = it->first;
    const Id entityScore = Id::makeFromInt(it->second.first);
    const ScoreAndContextSet& sacs = it->second.second;
    row[1] = entityScore;
    row[2] = eid;
    for (auto itt = sacs.rbegin(); itt != sacs.rend(); ++itt) {
      row[0] = Id::makeFromTextRecordIndex(itt->second);
      const vector<vector<WordIndex>> widsVec =
          mapEaCtW[std::make_pair(eid, itt->second)];
      for (size_t j = 0; j < widsVec.size(); j++) {
        for (size_t l = 0; l < numTerms; l++) {
          row[3 + l] =
              Id::makeFromWordVocabIndex(WordVocabIndex::make(widsVec[j][l]));
        }
        result.push_back(row);
      }
    }
  }
  *dynResult = std::move(result).toDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
};

template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<0>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<1>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<2>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<3>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<4>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<5>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<6>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<7>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<8>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<9>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);
template void FTSAlgorithms::oneVarFilterAggScoresAndTakeTopKContexts<10>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t k,
    IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);
  if (wep.cids_.empty() || fMap.empty()) {
    return;
  }

  size_t numTerms =
      dynResult->numColumns() - 1 - nofVars - fMap.begin()->second.numColumns();

  AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
  for (size_t i = 0; i < numTerms; i++) {
    AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
  }

  // Go over contexts.
  // For each context build a cross product with one part being from the filter.
  // Store them in a map, use a pair of id's as key and
  // an appropriate hash function.
  // Use a set (ordered) and keep it at size kLimit for the context scores.
  LOG(DEBUG) << "Heap-using case with " << kLimit
             << " contexts per entity...\n";
  using AggMapContextAndEntityToWord =
      ad_utility::HashMap<std::pair<vector<Id>, TextRecordIndex>,
                          std::set<vector<WordIndex>>>;
  using ScoreAndContextSet = std::set<std::pair<Score, TextRecordIndex>>;
  using AggMapEntityToSaCS =
      ad_utility::HashMap<vector<Id>, std::pair<Score, ScoreAndContextSet>>;
  AggMapContextAndEntityToWord mapEaCtW;
  AggMapEntityToSaCS mapEtSaCS;
  vector<Id> entitiesInContext;
  vector<Id> filteredEntitiesInContext;
  TextRecordIndex currentCid = wep.cids_[0];
  Score cscore = wep.scores_[0];
  ad_utility::HashMap<Id, std::set<vector<WordIndex>>> currentCombinedWids;
  vector<WordIndex> wids;
  wids.resize(numTerms);
  for (size_t l = 0; l < numTerms; l++) {
    AD_CONTRACT_CHECK(wep.wids_[l].size() == wep.cids_.size());
    wids[l] = wep.wids_[l][0];
  }
  currentCombinedWids[wep.eids_[0]].insert(wids);

  for (size_t i = 0; i < wep.cids_.size(); ++i) {
    if (wep.cids_[i] == currentCid) {
      for (size_t l = 0; l < numTerms; l++) {
        wids[l] = wep.wids_[l][i];
      }
      if (entitiesInContext.empty() ||
          entitiesInContext.back() != wep.eids_[i]) {
        if (fMap.contains(wep.eids_[i])) {
          filteredEntitiesInContext.push_back(wep.eids_[i]);
        }
        entitiesInContext.push_back(wep.eids_[i]);
      }
      currentCombinedWids[wep.eids_[i]].insert(wids);
      // cscore += scores[i];
    } else {
      if (!filteredEntitiesInContext.empty()) {
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
          for (size_t k = 0; k < nofVars; k++) {
            for (auto ccw = currentCombinedWids[key[k]].begin();
                 ccw != currentCombinedWids[key[k]].end(); ++ccw) {
              mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
            }
          }
          if (!mapEtSaCS.contains(key)) {
            mapEtSaCS[key].second.emplace(cscore, currentCid);
            mapEtSaCS[key].first = 1;
          } else {
            mapEtSaCS[key].first++;
            ScoreAndContextSet& sacs = mapEtSaCS[key].second;
            // Make sure that sacs only contains the top k contexts for
            // wep.eids_[i]
            if (sacs.size() < kLimit || sacs.begin()->first < cscore) {
              if (sacs.size() == kLimit) {
                sacs.erase(*sacs.begin());
              }
              sacs.emplace(cscore, currentCid);
            }
          }
        }
      }
      entitiesInContext.clear();
      filteredEntitiesInContext.clear();
      currentCid = wep.cids_[i];
      cscore = wep.scores_[i];
      currentCombinedWids.clear();
      for (size_t l = 0; l < numTerms; l++) {
        wids[l] = wep.wids_[l][i];
      }
      currentCombinedWids[wep.eids_[i]].insert(wids);
      entitiesInContext.push_back(wep.eids_[i]);
      if (fMap.contains(wep.eids_[i])) {
        filteredEntitiesInContext.push_back(wep.eids_[i]);
      }
    }
  }
  // Deal with the last context
  if (!filteredEntitiesInContext.empty()) {
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
      for (size_t k = 0; k < nofVars; k++) {
        for (auto ccw = currentCombinedWids[key[k]].begin();
             ccw != currentCombinedWids[key[k]].end(); ++ccw) {
          mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
        }
      }
      if (!mapEtSaCS.contains(key)) {
        mapEtSaCS[key].second.emplace(cscore, currentCid);
        mapEtSaCS[key].first = 1;
      } else {
        mapEtSaCS[key].first++;
        ScoreAndContextSet& sacs = mapEtSaCS[key].second;
        // Make sure that sacs only contains the top k contexts for wep.eids_[i]
        if (sacs.size() < kLimit || sacs.begin()->first < cscore) {
          if (sacs.size() == kLimit) {
            sacs.erase(*sacs.begin());
          }
          sacs.emplace(cscore, currentCid);
        }
      }
    }
  }

  // Write remaining wep entries into an IdTable
  IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
  result.reserve(mapEtSaCS.size() + 2);
  for (auto it = mapEtSaCS.begin(); it != mapEtSaCS.end(); ++it) {
    std::vector<ValueId> row;
    row.resize(result.numColumns());
    const Id entityScore = Id::makeFromInt(it->second.first);
    const vector<Id>& keyEids = it->first;
    const IdTable& filterRows = fMap.find(keyEids[0])->second;
    const ScoreAndContextSet& sacs = it->second.second;

    for (auto itt = sacs.rbegin(); itt != sacs.rend(); ++itt) {
      for (auto fRow : filterRows) {
        const TextRecordIndex cid = itt->second;
        size_t off = 2;
        row[0] = Id::makeFromTextRecordIndex(cid);
        row[1] = entityScore;
        for (size_t i = 1; i < keyEids.size(); i++) {
          row[off] = keyEids[i];
          off++;
        }
        for (size_t i = 0; i < fRow.numColumns(); i++) {
          row[off] = fRow[i];
          off++;
        }
        for (auto wids = mapEaCtW[std::make_pair(it->first, cid)].begin();
             wids != mapEaCtW[std::make_pair(it->first, cid)].end(); wids++) {
          for (size_t l = 0; l < numTerms; l++) {
            row[off + l] =
                Id::makeFromWordVocabIndex(WordVocabIndex::make(wids->at(l)));
          }
          result.push_back(row);
        }
      }
    }
  }
  *dynResult = std::move(result).toDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
}

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<0>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<1>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<2>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<3>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<4>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<5>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<6>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<7>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<8>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<9>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<10>(
    const WordEntityPostings& wep, const ad_utility::HashMap<Id, IdTable>& fMap,
    size_t nofVars, size_t kLimit, IdTable* dynResult);

// _____________________________________________________________________________
template <int WIDTH>
void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult) {
  AD_CONTRACT_CHECK(wep.wids_.size() >= 1);
  if (wep.cids_.empty() || fSet.empty()) {
    return;
  }

  size_t numTerms = dynResult->numColumns() - 2 - nofVars;

  AD_CONTRACT_CHECK(wep.wids_.size() >= numTerms);
  for (size_t i = 0; i < numTerms; i++) {
    AD_CONTRACT_CHECK(wep.wids_[i].size() == wep.cids_.size());
  }

  // Go over contexts.
  // For each context build a cross product with one part being from the filter.
  // Store them in a map, use a pair of id's as key and
  // an appropriate hash function.
  // Use a set (ordered) and keep it at size kLimit for the context scores.
  LOG(DEBUG) << "Heap-using case with " << kLimit
             << " contexts per entity...\n";
  using AggMapContextAndEntityToWord =
      ad_utility::HashMap<std::pair<vector<Id>, TextRecordIndex>,
                          std::set<vector<WordIndex>>>;
  using ScoreAndContextSet = std::set<std::pair<Score, TextRecordIndex>>;
  using AggMapEntityToSaCS =
      ad_utility::HashMap<vector<Id>, std::pair<Score, ScoreAndContextSet>>;
  AggMapContextAndEntityToWord mapEaCtW;
  AggMapEntityToSaCS mapEtSaCS;
  vector<Id> entitiesInContext;
  vector<Id> filteredEntitiesInContext;
  TextRecordIndex currentCid = wep.cids_[0];
  Score cscore = wep.scores_[0];
  ad_utility::HashMap<Id, std::set<vector<WordIndex>>> currentCombinedWids;
  vector<WordIndex> wids;
  wids.resize(numTerms);
  for (size_t l = 0; l < numTerms; l++) {
    AD_CONTRACT_CHECK(wep.wids_[l].size() == wep.cids_.size());
    wids[l] = wep.wids_[l][0];
  }
  currentCombinedWids[wep.eids_[0]].insert(wids);

  for (size_t i = 0; i < wep.cids_.size(); ++i) {
    if (wep.cids_[i] == currentCid) {
      for (size_t l = 0; l < numTerms; l++) {
        wids[l] = wep.wids_[l][i];
      }
      if (entitiesInContext.empty() ||
          entitiesInContext.back() != wep.eids_[i]) {
        if (fSet.contains(wep.eids_[i])) {
          filteredEntitiesInContext.push_back(wep.eids_[i]);
        }
        entitiesInContext.push_back(wep.eids_[i]);
      }
      currentCombinedWids[wep.eids_[i]].insert(wids);
      // cscore += scores[i];
    } else {
      if (!filteredEntitiesInContext.empty()) {
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
          for (size_t k = 0; k < nofVars; k++) {
            for (auto ccw = currentCombinedWids[key[k]].begin();
                 ccw != currentCombinedWids[key[k]].end(); ++ccw) {
              mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
            }
          }
          if (!mapEtSaCS.contains(key)) {
            mapEtSaCS[key].second.emplace(cscore, currentCid);
            mapEtSaCS[key].first = 1;
          } else {
            mapEtSaCS[key].first++;
            ScoreAndContextSet& sacs = mapEtSaCS[key].second;
            // Make sure that sacs only contains the top k contexts for
            // wep.eids_[i]
            if (sacs.size() < kLimit || sacs.begin()->first < cscore) {
              if (sacs.size() == kLimit) {
                sacs.erase(*sacs.begin());
              }
              sacs.emplace(cscore, currentCid);
            }
          }
        }
      }
      entitiesInContext.clear();
      filteredEntitiesInContext.clear();
      currentCid = wep.cids_[i];
      cscore = wep.scores_[i];
      currentCombinedWids.clear();
      for (size_t l = 0; l < numTerms; l++) {
        wids[l] = wep.wids_[l][i];
      }
      currentCombinedWids[wep.eids_[i]].insert(wids);
      entitiesInContext.push_back(wep.eids_[i]);
      if (fSet.contains(wep.eids_[i])) {
        filteredEntitiesInContext.push_back(wep.eids_[i]);
      }
    }
  }
  // Deal with the last context
  if (!filteredEntitiesInContext.empty()) {
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
      for (size_t k = 0; k < nofVars; k++) {
        for (auto ccw = currentCombinedWids[key[k]].begin();
             ccw != currentCombinedWids[key[k]].end(); ++ccw) {
          mapEaCtW[std::make_pair(key, currentCid)].insert(*ccw);
        }
      }
      if (!mapEtSaCS.contains(key)) {
        mapEtSaCS[key].second.emplace(cscore, currentCid);
        mapEtSaCS[key].first = 1;
      } else {
        mapEtSaCS[key].first++;
        ScoreAndContextSet& sacs = mapEtSaCS[key].second;
        // Make sure that sacs only contains the top k contexts for wep.eids_[i]
        if (sacs.size() < kLimit || sacs.begin()->first < cscore) {
          if (sacs.size() == kLimit) {
            sacs.erase(*sacs.begin());
          }
          sacs.emplace(cscore, currentCid);
        }
      }
    }
  }

  // Write remaining wep entries into an IdTable
  IdTableStatic<WIDTH> result = std::move(*dynResult).toStatic<WIDTH>();
  result.reserve(mapEtSaCS.size() + 2);
  for (auto it = mapEtSaCS.begin(); it != mapEtSaCS.end(); ++it) {
    std::vector<ValueId> row;
    row.resize(result.numColumns());
    const ScoreAndContextSet& sacs = it->second.second;
    row[1] = Id::makeFromInt(it->second.first);
    const vector<Id>& keyEids = it->first;
    size_t off = 2;
    for (size_t i = 1; i < keyEids.size(); i++) {
      row[off] = keyEids[i];
      off++;
    }
    row[off] = keyEids[0];
    off++;
    for (auto itt = sacs.rbegin(); itt != sacs.rend(); ++itt) {
      const TextRecordIndex cid = itt->second;
      row[0] = Id::makeFromTextRecordIndex(cid);
      for (auto wids = mapEaCtW[std::make_pair(it->first, cid)].begin();
           wids != mapEaCtW[std::make_pair(it->first, cid)].end(); wids++) {
        for (size_t l = 0; l < numTerms; l++) {
          row[off + l] =
              Id::makeFromWordVocabIndex(WordVocabIndex::make(wids->at(l)));
        }
        result.push_back(row);
      }
    }
  }
  *dynResult = std::move(result).toDynamic();
  LOG(DEBUG) << "Done. There are " << dynResult->size() << " tuples now.\n";
}

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<0>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<1>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<2>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<3>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<4>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);

template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<5>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<6>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<7>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<8>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<9>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
template void FTSAlgorithms::multVarsFilterAggScoresAndTakeTopKContexts<10>(
    const WordEntityPostings& wep, const HashSet<Id>& fSet, size_t nofVars,
    size_t kLimit, IdTable* dynResult);
