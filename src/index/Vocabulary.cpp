// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <iostream>
#include <string>
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
  _words.clear();
  std::fstream in(fileName.c_str(), std::ios_base::in);
  string line;
  while (std::getline(in, line)) {
    _words.push_back(line);
  }
  in.close();
}

// _____________________________________________________________________________
void Vocabulary::writeToFile(const string& fileName) const {
  std::fstream out(fileName.c_str(), std::ios_base::out);
  for (size_t i = 0; i + 1 < _words.size(); ++i) {
    out << _words[i] << '\n';
  }
  if (_words.size() > 0) {
    out << _words[_words.size() - 1];
  }
  out.close();
}

