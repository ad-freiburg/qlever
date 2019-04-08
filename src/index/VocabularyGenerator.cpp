// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "./VocabularyGenerator.h"
#include <fstream>
#include <iostream>
#include <queue>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <parallel/algorithm>
#include "../util/Conversions.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./ConstantsIndexCreation.h"
#include "./Vocabulary.h"

// helper struct used in the priority queue for merging.
// represents tokens/words in a certain partial vocabulary
struct QueueWord {
  QueueWord() = default;
  QueueWord(const string& v, size_t file, Id word)
      : _value(v), _partialFileId(file), _partialWordId(word) {}
  string _value;          // the word
  size_t _partialFileId;  // from which partial vocabulary did this word come
  Id _partialWordId;      // which partial id did the word have in this partial
                          // vocabulary
};

// we sort alphabetically by the token
class QueueCompare {
 public:
  QueueCompare(StringSortComparator comp) : _comp(comp) {}
  bool operator()(const QueueWord& p1, const QueueWord& p2) {
    // if p1 is smaller (alphabetically)
    // _comp will return false if called like this
    // and the priority queue will thus emit p1 first
    return _comp(p2._value, p1._value);
  }

 private:
  StringSortComparator _comp;
};

// ___________________________________________________________________
size_t mergeVocabulary(const std::string& basename, size_t numFiles,
                       Id* langPredLowerBound, Id* langPredUpperBound,
                       StringSortComparator comp) {
  std::vector<std::ifstream> infiles;

  // we will store pairs of <partialId, globalId>
  std::vector<IdPairMMapVec> idVecs;
  std::ofstream outfile(basename + ".vocabulary");
  AD_CHECK(outfile.is_open());
  std::ofstream outfileExternal(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
  AD_CHECK(outfileExternal.is_open());
  std::vector<bool> endOfFile(numFiles, false);

  // Priority queue for the k-way merge
  std::priority_queue<QueueWord, std::vector<QueueWord>, QueueCompare> queue(
      comp);

  // open and prepare all infiles and mmap output vectors
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME +
                         std::to_string(i));
    idVecs.emplace_back(0, basename + PARTIAL_MMAP_IDS + std::to_string(i));
    AD_CHECK(infiles.back().is_open());

    // read the first entry of the vocabulary and add it to the queue
    endOfFile[i] = true;

    uint32_t len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      Id id;
      infiles[i].read((char*)&id, sizeof(id));
      queue.push(QueueWord(word, i, id));
      endOfFile[i] = false;
    }
  }

  // keep track of the last seen word to correctly handle duplicates
  std::string lastWritten = "";
  // the number of words we have written. This also is the global Id of the next
  // word we see, unless it is is equal to the previous word
  size_t totalWritten = 0;
  bool firstLangPredSeen = false;
  *langPredLowerBound = 0;
  *langPredUpperBound = 0;

  // start k-way merge
  while (!queue.empty()) {
    auto top = queue.top();
    queue.pop();

    // avoid duplicates
    if (top._value != lastWritten) {
      lastWritten = top._value;

      // write the new word to the vocabulary
      if (top._value < string({EXTERNALIZED_LITERALS_PREFIX})) {
        outfile << top._value << std::endl;
      } else {
        // we have to strip the externalization character again
        outfileExternal << top._value.substr(1) << std::endl;
      }

      // write id to corresponding vec
      idVecs[top._partialFileId].push_back(
          std::make_pair(top._partialWordId, totalWritten));

      if (top._value.size() > 0 && top._value[0] == '@') {
        if (!firstLangPredSeen) {
          // inclusive
          *langPredLowerBound = totalWritten;
          firstLangPredSeen = true;
        }
        // exclusive
        *langPredUpperBound = totalWritten + 1;
      }
      totalWritten++;
    } else {
      // this is a duplicate which already occured in another partial vocabulary
      // in the last step.
      // we already have increased total written, so for the duplicate
      // we have to subtract one again
      size_t minusOne = totalWritten - 1;
      idVecs[top._partialFileId].push_back(
          std::make_pair(top._partialWordId, minusOne));
    }

    // add next word from the same infile to the priority queue
    if (endOfFile[top._partialFileId]) {
      continue;
    }  // file is exhausted, nothing to add

    size_t i = top._partialFileId;
    endOfFile[i] = true;
    uint32_t len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      Id id;
      infiles[i].read((char*)&id, sizeof(id));
      queue.push(QueueWord(word, i, id));
      endOfFile[i] = false;
    }
  }
  return totalWritten;
}

// ______________________________________________________________________________________________
void writePartialIdMapToBinaryFileForMerging(
    std::shared_ptr<const ad_utility::HashMap<string, Id>> map,
    const string& fileName, StringSortComparator comp, bool doParallelSort) {
  LOG(INFO) << "Creating partial vocabulary from set ...\n";
  std::vector<std::pair<string, Id>> els;
  els.reserve(map->size());
  els.insert(begin(els), begin(*map), end(*map));
  LOG(INFO) << "... sorting ...\n";

  auto pred = [comp](const auto& p1, const auto& p2) {
    return comp(p1.first, p2.first);
  };
  if (USE_PARALLEL_SORT && doParallelSort) {
    __gnu_parallel::sort(begin(els), end(els), pred,
                         __gnu_parallel::parallel_tag(NUM_SORT_THREADS));
  } else {
    std::sort(begin(els), end(els), pred);
  }
  LOG(INFO) << "Done creating vocabulary.\n";

  LOG(INFO) << "Writing vocabulary to binary file " << fileName << "\n";
  std::ofstream out(fileName.c_str(),
                    std::ios_base::out | std::ios_base::binary);
  AD_CHECK(out.is_open());
  for (size_t i = 0; i < els.size(); ++i) {
    // 32 bits should be enough for len of string
    std::string_view word = els[i].first;
    uint32_t len = word.size();
    out.write((char*)&len, sizeof(len));
    out.write(word.data(), len);
    Id id = els[i].second;
    out.write((char*)&id, sizeof(id));
  }
  out.close();
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename) {
  ad_utility::HashMap<Id, Id> res;
  IdPairMMapVecView vec(mmapFilename);
  for (const auto [partialId, globalId] : vec) {
    res[partialId] = globalId;
  }
  return res;
}
