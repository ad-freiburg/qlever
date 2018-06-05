// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "./VocabularyGenerator.h"
#include <unordered_set>
#include <vector>
#include <queue>
#include <string>
#include <utility>
#include <fstream>
#include <iostream>

#include "./ConstantsIndexCreation.h"
#include "../util/Log.h"
#include "../util/Exception.h"

class PairCompare {
 public:
  bool operator()(const std::pair<std::string, size_t>& p1, const std::pair<std::string, size_t>& p2) {
    return p1.first > p2.first;}
};
// ___________________________________________________________________
void mergeVocabulary(const std::string& basename, size_t numFiles) {
  const size_t bufferSize = 5; //see comment below for buffers
  
  std::vector<std::fstream> infiles;
  // currently only one word is buffered, no matter how big the bufferSize is
  // TODO: buffer, or throw the buffer out
  std::vector<std::vector<std::string>> mergeBuf(numFiles);
  std::ofstream outfile(basename + ".vocabulary");
  AD_CHECK(outfile.is_open());
  std::ofstream outfileExternal(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
  AD_CHECK(outfileExternal.is_open());
  std::vector<bool> endOfFile(numFiles, false);
  std::vector<std::streampos> posNextWord;
  std::vector<std::vector<std::string>::iterator> iterators;
  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME + std::to_string(i), std::ios_base::in | std::ios_base::out);
    AD_CHECK(infiles.back().is_open());
    endOfFile[i] = true;
    mergeBuf[i].reserve(bufferSize);

    unsigned int len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      mergeBuf[i].emplace_back();
      mergeBuf[i].back().resize(len);
      infiles[i].read(&(mergeBuf[i].back()[0]), len);
      endOfFile[i] = false;
    
    }
  }


  using pair_T = std::pair<std::string, size_t>;
  std::priority_queue<pair_T, std::vector<pair_T>, PairCompare> queue;
  for (size_t i = 0; i < numFiles; i++) {
    if (!mergeBuf[i].empty()) {
      // the empty is another sanity check in case one of the partial
      // vocabularies is empty (this should probably not happen and will be
      // checked during next refactoring. But the performance overhead in this
      // case is small
      queue.push(std::make_pair(mergeBuf[i].front(), i));
    }
    iterators.push_back(mergeBuf[i].begin());
  }

  std::string lastWritten = "";
  size_t totalWritten = 0;

  while (!queue.empty()) {
    auto top = queue.top();
    queue.pop();


    // avoid duplicates
    if (top.first != lastWritten) {
      lastWritten = top.first;
      if (top.first < string({EXTERNALIZED_LITERALS_PREFIX})) {
        outfile << top.first << std::endl;
      } else {
        outfileExternal << top.first << std::endl;
      }
      infiles[top.second].write((char*)&totalWritten, sizeof(totalWritten));
      totalWritten++;
    } else {
      // always write Index (also in case of duplicates)
      // we already have increased total written, so for the duplicate
      // we have to subtract one again
      auto minusOne = totalWritten - 1;
      infiles[top.second].write((char*)&minusOne, sizeof(totalWritten));
    }

    // refill with top element from current vector
    iterators[top.second]++;
    if (iterators[top.second] == mergeBuf[top.second].end()) {
      // if this condition is false, we have no more words for file i, just
      // skip
      if (endOfFile[top.second]) { continue;}

      // refill vector from file
      mergeBuf[top.second].clear();
      auto i = top.second;
      std::string word;
      endOfFile[top.second] = true;
      uint32_t len;
      if (infiles[i].read((char*)&len, sizeof(len))) {
	mergeBuf[i].emplace_back();
	mergeBuf[i].back().resize(len);
	infiles[i].read(&(mergeBuf[i].back()[0]), len);
	endOfFile[i] = false;
      }
      iterators[i] = mergeBuf[i].begin();
      if (mergeBuf[i].begin() != mergeBuf[i].end()) {
	queue.push(std::make_pair(mergeBuf[i].front(), i));
      }
    } else {
      queue.push(std::make_pair(*(iterators[top.second]), top.second));
    }
  }
}

// ____________________________________________________________________________________________
google::sparse_hash_map<string, Id> vocabMapFromPartialIndexedFile(const string& partialFile) {
  std::ifstream file(partialFile, std::ios_base::binary);
  AD_CHECK(file.is_open());
  google::sparse_hash_map<string, Id> vocabMap;
  uint32_t len;
  while (file.read((char*)&len, sizeof(len))) {
    std::string word;
    word.resize(len);
    file.read(&(word[0]), len);
    size_t idx;
    file.read((char*)&idx, sizeof(idx));
    vocabMap[word] = idx;
  }
  return vocabMap;
}

