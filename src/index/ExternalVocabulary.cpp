// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./ExternalVocabulary.h"

#include <fstream>

#include "../util/Log.h"

// _____________________________________________________________________________
string ExternalVocabulary::operator[](Id id) const {
  off_t ft[2];
  off_t& from = ft[0];
  off_t& to = ft[1];
  off_t at = _startOfOffsets + id * sizeof(off_t);
  at += _file.read(ft, sizeof(ft), at);
  assert(to > from);
  size_t nofBytes = static_cast<size_t>(to - from);
  string word(nofBytes, '\0');
  // TODO in C++17 we'll get non-const pointer std::string::data() use it then
  _file.read(&word.front(), nofBytes, from);
  return word;
}

// _____________________________________________________________________________
Id ExternalVocabulary::binarySearchInVocab(const string& word) const {
  Id lower = 0;
  Id upper = _size;
  while (lower < upper) {
    Id i = (lower + upper) / 2;
    string w = (*this)[i];
    if (w < word) {
      lower = i + 1;
    } else if (w > word) {
      upper = i - 1;
    } else if (w == word) {
      return i;
    }
  }
  return lower;
}

// _____________________________________________________________________________
void ExternalVocabulary::buildFromVector(const vector<string>& v,
                                         const string& fileName) {
  _file.open(fileName.c_str(), "w");
  vector<off_t> offsets;
  off_t currentOffset = 0;
  _size = v.size();
  for (size_t i = 0; i < v.size(); ++i) {
    offsets.push_back(currentOffset);
    currentOffset += _file.write(v[i].data(), v[i].size());
  }
  _startOfOffsets = currentOffset;
  offsets.push_back(_startOfOffsets);
  _file.write(offsets.data(), offsets.size() * sizeof(off_t));
  _file.close();
  initFromFile(fileName);
}

// _____________________________________________________________________________
void ExternalVocabulary::buildFromTextFile(const string& textFileName,
                                         const string& outFileName) {
  _file.open(outFileName.c_str(), "w");
  std::ifstream infile(textFileName);
  AD_CHECK(infile.is_open());
  vector<off_t> offsets;
  off_t currentOffset = 0;
  _size = 0;
  std::string word;
  while (std::getline(infile, word)) {
    offsets.push_back(currentOffset);
    currentOffset += _file.write(word.data(), word.size());
    _size++;
  }
  _startOfOffsets = currentOffset;
  offsets.push_back(_startOfOffsets);
  _file.write(offsets.data(), offsets.size() * sizeof(off_t));
  _file.close();
  initFromFile(outFileName);
}

// _____________________________________________________________________________
void ExternalVocabulary::initFromFile(const string& file) {
  _file.open(file.c_str(), "r");
  if (_file.empty()) {
    _size = 0;
  } else {
    off_t posLastOfft = _file.getLastOffset(&_startOfOffsets);
    _size = (posLastOfft - _startOfOffsets) / sizeof(off_t);
  }
  LOG(INFO) << "Initialized external vocabulary. It contains " << _size
            << " elements." << std::endl;
}
