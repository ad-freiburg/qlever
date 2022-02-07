// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "./SimpleVocabulary.h"

#include <fstream>
#include <iostream>

using std::string;

// _____________________________________________________________________________
void SimpleVocabulary::readFromFile(const string& fileName) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << " ..."
            << std::endl;
  _words.clear();
  ad_utility::serialization::FileReadSerializer file(fileName);
  file >> _words;
  LOG(INFO) << "Done, number of words: " << _words.size() << std::endl;
}

// _____________________________________________________________________________
void SimpleVocabulary::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer file{fileName};
  file << _words;
  LOG(INFO) << "Done writing vocabulary to file.\n";
}
