// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "index/vocabulary/VocabularyInMemory.h"

using std::string;

// _____________________________________________________________________________
void VocabularyInMemory::open(const string& fileName) {
  AD_LOG_INFO << "Reading vocabulary from file " << fileName << " ..."
              << std::endl;
  _words.clear();
  ad_utility::serialization::FileReadSerializer file(fileName);
  file >> _words;
  AD_LOG_INFO << "Done, number of words: " << size() << std::endl;
}

// _____________________________________________________________________________
void VocabularyInMemory::writeToFile(const string& fileName) const {
  AD_LOG_INFO << "Writing vocabulary to file " << fileName << " ..."
              << std::endl;
  ad_utility::serialization::FileWriteSerializer file{fileName};
  file << _words;
  AD_LOG_INFO << "Done, number of words: " << _words.size() << std::endl;
}
