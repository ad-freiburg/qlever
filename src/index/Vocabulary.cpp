// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <iostream>
#include <fstream>

#include "../util/File.h"
#include "./Vocabulary.h"
#include "../util/HashSet.h"
#include "../util/HashMap.h"

using std::string;

// _____________________________________________________________________________
Vocabulary::Vocabulary() {
}

// _____________________________________________________________________________
Vocabulary::~Vocabulary() {
}


// _____________________________________________________________________________
void Vocabulary::readFromFile(const string& fileName,
                              const string& extLitsFileName) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << "\n";
  _words.clear();
  std::fstream in(fileName.c_str(), std::ios_base::in);
  string line;
  while (std::getline(in, line)) {
    _words.push_back(line);
  }
  in.close();
  LOG(INFO) << "Done reading vocabulary from file.\n";
  if (extLitsFileName.size() > 0) {
    LOG(INFO) << "Registering external vocabulary for literals.\n";
    _externalLiterals.initFromFile(extLitsFileName);
    LOG(INFO) << "Done registering external vocabulary for literals.\n";
  }
}

// _____________________________________________________________________________
void Vocabulary::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << "\n";
  std::ofstream out(fileName.c_str(), std::ios_base::out);
  for (size_t i = 0; i + 1 < _words.size(); ++i) {
    out << _words[i] << '\n';
  }
  if (_words.size() > 0) {
    out << _words[_words.size() - 1];
  }
  out.close();
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________________
void Vocabulary::createFromSet(const ad_utility::HashSet<string>& set) {
  LOG(INFO) << "Creating vocabulary from set ...\n";
  _words.clear();
  _words.reserve(set.size());
  _words.insert(begin(_words), begin(set), end(set));
  LOG(INFO) << "... sorting ...\n";
  std::sort(begin(_words), end(_words));
  LOG(INFO) << "Done creating vocabulary.\n";
}

// _____________________________________________________________________________
ad_utility::HashMap<string, Id> Vocabulary::asMap() {
  ad_utility::HashMap<string, Id> map;
  for (size_t i = 0; i < _words.size(); ++i) {
    map[_words[i]] = i;
  }
  return map;
}

// _____________________________________________________________________________
void Vocabulary::externalizeLiterals(const string& fileName) {
  auto ext = std::lower_bound(_words.begin(), _words.end(),
                              string({EXTERNALIZED_LITERALS_PREFIX}));
  size_t nofInternal = ext - _words.begin();
  vector<string> extVocab;
  extVocab.insert(extVocab.end(), ext, _words.end());
  _words.resize(nofInternal);
  _externalLiterals.buildFromVector(extVocab, fileName);
}
