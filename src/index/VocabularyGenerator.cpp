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

#include "../util/Exception.h"
#include "../util/Log.h"
#include "./ConstantsIndexCreation.h"

class PairCompare {
 public:
  bool operator()(const std::pair<std::string, size_t>& p1,
                  const std::pair<std::string, size_t>& p2) {
    return p1.first > p2.first;
  }
};

// ___________________________________________________________________
size_t mergeVocabulary(const std::string& basename, size_t numFiles) {
  std::vector<std::fstream> infiles;
  std::ofstream outfile(basename + ".vocabulary");
  AD_CHECK(outfile.is_open());
  std::ofstream outfileExternal(basename + EXTERNAL_LITS_TEXT_FILE_NAME);
  AD_CHECK(outfileExternal.is_open());
  std::vector<bool> endOfFile(numFiles, false);

  using pair_T = std::pair<string, size_t>;
  std::priority_queue<pair_T, std::vector<pair_T>, PairCompare> queue;

  for (size_t i = 0; i < numFiles; i++) {
    infiles.emplace_back(basename + PARTIAL_VOCAB_FILE_NAME + std::to_string(i),
                         std::ios_base::in | std::ios_base::out);
    AD_CHECK(infiles.back().is_open());
    endOfFile[i] = true;

    unsigned int len;
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      queue.push(std::make_pair(word, i));
      endOfFile[i] = false;
    }
  }

  std::string lastWritten = "";
  size_t totalWritten = 0;

  // start k-way merge
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

      // according to the standard, flush() or seek() must be called before
      // switching from read to write. And this is indeed necessary for gcc to
      // avoid nasty bugs.
      // We seek to the current position to avoid them
      infiles[top.second].seekp(infiles[top.second].tellp());
      // write id to partial vocabulary
      infiles[top.second].write((char*)&totalWritten, sizeof(totalWritten));
      totalWritten++;
    } else {
      // this is a duplicate which already occured in another partial vocabulary
      // in the last step
      // we already have increased total written, so for the duplicate
      // we have to subtract one again
      size_t minusOne = totalWritten - 1;
      // seek to current position when switching from read to write
      infiles[top.second].seekp(infiles[top.second].tellg());
      infiles[top.second].write((char*)&minusOne, sizeof(minusOne));
    }

    // add next word from the same infile to the priority queue
    if (endOfFile[top.second]) {
      continue;
    }  // file is exhausted, nothing to add

    size_t i = top.second;
    endOfFile[top.second] = true;
    uint32_t len;
    // seek to current position whe switching from write to read
    infiles[top.second].seekg(infiles[top.second].tellp());
    if (infiles[i].read((char*)&len, sizeof(len))) {
      std::string word(len, '\0');
      infiles[i].read(&(word[0]), len);
      endOfFile[i] = false;
      queue.push(std::make_pair(word, i));
    }
  }
  return totalWritten;
}

// ____________________________________________________________________________________________
google::sparse_hash_map<string, Id> vocabMapFromPartialIndexedFile(
    const string& partialFile) {
  std::ifstream file(partialFile, std::ios_base::binary);
  AD_CHECK(file.is_open());
  google::sparse_hash_map<string, Id> vocabMap;
  uint32_t len;
  while (file.read((char*)&len, sizeof(len))) {
    std::string word(len, '\0');
    file.read(&(word[0]), len);
    size_t idx;
    file.read((char*)&idx, sizeof(idx));
    vocabMap[word] = idx;
  }
  return vocabMap;
}
