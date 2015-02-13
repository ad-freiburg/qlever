// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <iostream>
#include <string>

#include "../util/File.h"
#include "./Vocabulary.h"

using std::string;

namespace ad_semsearch {
// _____________________________________________________________________________
Vocabulary::Vocabulary() {
}
// _____________________________________________________________________________
Vocabulary::~Vocabulary() {
}
// _____________________________________________________________________________
void Vocabulary::readFromFile(const string& fileName) {
  _words.clear();
  ad_utility::File file(fileName.c_str(), "r");
  char buf[BUFFER_SIZE_WORD];
  file.readIntoVector(&_words, buf, BUFFER_SIZE_WORD);
}
// _____________________________________________________________________________
string Vocabulary::asString(void) const {
  std::ostringstream os;
  os << "Vocabulary with size: " << size();
  if (size() == 1) {
    os << "; Word: " << _words[0];
  }
  if (size() > 1) {
    os << "; Words: " << _words[0];
    if (size() > 2) {
      os << ", ...";
    }
    os << ", " << _words[size() - 1];
  }
  return os.str();
}
}
