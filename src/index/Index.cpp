// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <algorithm>
#include <unordered_set>
#include <stxxl/algorithm>
#include "../parser/TsvParser.h"
#include "./Index.h"


// _____________________________________________________________________________
Index::Index() {

}

// _____________________________________________________________________________
void Index::createFromTsvFile(const string& tsvFile, const string& onDiskBase) {
  _onDiskBase = onDiskBase;
  string indexFilename = _onDiskBase + ".index";
  std::fstream out(indexFilename.c_str(), std::ios_base::out);
  passTsvFileForVocabulary(tsvFile);
  _vocab.writeToFile(onDiskBase + ".vocabulary");
  ExtVec v;
  passTsvFileIntoIdVector(tsvFile, v);
  LOG(INFO) << "Sorting for PSO permutation..." << std::endl;
  stxxl::sort(begin(v), end(v), SortByPSO(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;
  createPermutation(v, _psoMeta, out);
  LOG(INFO) << "Sorting for POS permutation..." << std::endl;;
  stxxl::sort(begin(v), end(v), SortByPOS(), STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;;
  createPermutation(v, _posMeta, out);
  writeMetadata(out);
}

// _____________________________________________________________________________
void Index::passTsvFileForVocabulary(const string& tsvFile) {
  LOG(INFO) << "Making pass over TsvFile " << tsvFile << " for vocabulary.\n";
  array<string, 3> spo;
  TsvParser p(tsvFile);
  std::unordered_set<string> items;
  size_t i = 0;
  while (p.getLine(spo)) {
    items.insert(spo[0]);
    items.insert(spo[1]);
    items.insert(spo[2]);
    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  LOG(INFO) << "Pass done.\n";
  _vocab.createFromSet(items);
}

// _____________________________________________________________________________
void Index::passTsvFileIntoIdVector(const string& tsvFile, ExtVec& data)  {
  LOG(INFO) << "Making pass over TsvFile " << tsvFile
        << " and creating stxxl vector.\n";
  array<string, 3> spo;
  TsvParser p(tsvFile);
  std::unordered_map<string, Id> vocabMap = _vocab.asMap();
  size_t i = 0;
  while (p.getLine(spo)) {
    data.push_back(
        array<Id, 3>{{
            vocabMap.find(spo[0])->second,
            vocabMap.find(spo[1])->second,
            vocabMap.find(spo[2])->second
        }}
    );
    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }
  }
  LOG(INFO) << "Pass done.\n";
}

void Index::createPermutation(Index::ExtVec const& vec,
    IndexMetaData& metaData, std::fstream& out) {
  if (vec.size() == 0) {
    LOG(WARN) << "Attempt to write an empty index!" << std::endl;
    return;
  }

  LOG(INFO) << "Creating an on-disk index permutation." << std::endl;
  // Iterate over the vector and identify relation boundaries
  size_t from = 0;
  size_t i = 0;
  Id currentRel = vec[0][0];
  off_t lastOffset = 0;
  for (; i < vec.size(); ++i) {
    if (vec[i][0] != currentRel) {
      metaData.add(writeRel(out, lastOffset, vec, from, i));
      lastOffset = metaData.getSizeOfData();
    }
  }
  if (i < vec.size()) {
    metaData.add(writeRel(out, lastOffset, vec, from, i));
  }
  out.close();
  LOG(INFO) << "Done creatingindex permutation." << std::endl;
}

// _____________________________________________________________________________
RelationMetaData Index::writeRel(std::fstream& out, off_t currentOffset,
    const ExtVec& data, size_t fromIndex, size_t toIndexInclusive) {
  return RelationMetaData();
}

// _____________________________________________________________________________
void Index::writeMetadata(std::fstream& out) {

}


