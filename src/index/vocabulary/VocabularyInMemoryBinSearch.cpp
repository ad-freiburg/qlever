// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "./VocabularyInMemoryBinSearch.h"

using std::string;

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::open(const string& fileName) {
  _words.clear();
  {
    ad_utility::serialization::FileReadSerializer file(fileName);
    file >> _words;
  }
  {
    ad_utility::serialization::FileReadSerializer idFile(fileName + ".ids");
    idFile >> offsets_;
  }
}

// _____________________________________________________________________________
void VocabularyInMemoryBinSearch::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << " ..." << std::endl;
  ad_utility::serialization::FileWriteSerializer file{fileName};
  file << _words;
  LOG(INFO) << "Done, number of words: " << _words.size() << std::endl;
}
