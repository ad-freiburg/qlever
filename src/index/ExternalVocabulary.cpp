// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include "./ExternalVocabulary.h"

#include <fstream>

#include "../parser/RdfEscaping.h"
#include "../parser/Tokenizer.h"
#include "../util/BufferedVector.h"
#include "../util/Log.h"

// _____________________________________________________________________________
template <class Comp>
string ExternalVocabulary<Comp>::operator[](Id id) const {
  off_t ft[2];
  off_t& from = ft[0];
  off_t& to = ft[1];
  off_t at = _startOfOffsets + id * sizeof(off_t);
  at += _file.read(ft, sizeof(ft), at);
  assert(to > from);
  size_t nofBytes = static_cast<size_t>(to - from);
  string word(nofBytes, '\0');
  _file.read(word.data(), nofBytes, from);
  return word;
}

// _____________________________________________________________________________
template <class Comp>
Id ExternalVocabulary<Comp>::binarySearchInVocab(const string& word) const {
  Id lower = 0;
  Id upper = _size;
  while (lower < upper) {
    Id i = (lower + upper) / 2;
    string w = (*this)[i];
    int cmp = _caseComparator.compare(w, word, Comp::Level::TOTAL);
    if (cmp < 0) {
      lower = i + 1;
    } else if (cmp > 0) {
      upper = i - 1;
    } else if (cmp == 0) {
      return i;
    }
  }
  return lower;
}

// _____________________________________________________________________________
template <class Comp>
void ExternalVocabulary<Comp>::buildFromVector(const vector<string>& v,
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
template <class Comp>
void ExternalVocabulary<Comp>::buildFromTextFile(const string& textFileName,
                                                 const string& outFileName) {
  _file.open(outFileName.c_str(), "w");
  std::ifstream infile(textFileName);
  AD_CHECK(infile.is_open());
  ad_utility::BufferedVector<off_t> offsets(
      1'000'000'000, textFileName + ".offset.tmp.buffer");
  off_t currentOffset = 0;
  _size = 0;
  std::string word;
  while (std::getline(infile, word)) {
    // The temporary file for the to-be-externalized vocabulary strings is
    // line-based, just like the normal vocabulary file. Therefore, \n and \ are
    // escaped there. When we read from this file, we have to unescape these.
    word = RdfEscaping::unescapeNewlinesAndBackslashes(word);
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
template <class Comp>
void ExternalVocabulary<Comp>::initFromFile(const string& file) {
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

template class ExternalVocabulary<TripleComponentComparator>;
template class ExternalVocabulary<SimpleStringComparator>;
