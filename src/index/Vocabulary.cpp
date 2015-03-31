// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <iostream>
#include <fstream>

#include "../util/File.h"
#include "./Vocabulary.h"

using std::string;

// _____________________________________________________________________________
Vocabulary::Vocabulary() {
}
// _____________________________________________________________________________
Vocabulary::~Vocabulary() {
}


// _____________________________________________________________________________
void Vocabulary::readFromFile(const string& fileName) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << "\n";
  _words.clear();
  std::fstream in(fileName.c_str(), std::ios_base::in);
  string line;
  while (std::getline(in, line)) {
    _words.push_back(line);
  }
  in.close();
  LOG(INFO) << "Done reading vocabulary from file.\n";
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
void Vocabulary::createFromSet(const std::unordered_set<string>& set) {
  LOG(INFO) << "Creating vocabulary from set ...\n";
  _words.clear();
  _words.reserve(set.size());
  _words.insert(begin(_words), begin(set), end(set));
  LOG(INFO) << "... sorting ...\n";
  std::sort(begin(_words), end(_words));
  LOG(INFO) << "Done creating vocabulary.\n";
}

// _____________________________________________________________________________
std::unordered_map<string, Id> Vocabulary::asMap() {
  std::unordered_map<string, Id> map;
  map.reserve(_words.size());
  for (size_t i = 0; i < _words.size(); ++i) {
    map[_words[i]] = i;
  }
  return map;
}
