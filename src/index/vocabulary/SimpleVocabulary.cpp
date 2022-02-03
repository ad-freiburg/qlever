// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include <fstream>
#include <iostream>

#include "./SimpleVocabulary.h"

using std::string;

// _____________________________________________________________________________
template <typename C>
void SimpleVocabulary<C>::readFromFile(const string& fileName
                                    ) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << " ..."
            << std::endl;
  _words.clear();
  ad_utility::serialization::FileReadSerializer file(fileName);
  file >> _words;
  LOG(INFO) << "Done, number of words: " << _words.size() << std::endl;
}

// _____________________________________________________________________________
template <typename C>
void SimpleVocabulary<C>::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer file{fileName};
  file << _words;
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________________
template <typename C>
template <typename StringType, typename Comparator>
SimpleVocabulary<C>::SearchResult SimpleVocabulary<C>::lower_bound(
    const StringType& word, Comparator comparator) const {
  SearchResult result;
  // TODO<joka921> do we still need the lower_bound in the _words?
  result._id = _words.lower_bound(word, comparator);
  if (result._id < _words.size()) {
    resultInternal._word = _words[result._id];
  }
  return result;
}

// _____________________________________________________________________________
template <typename C>
template <typename StringType, typename Comparator>
SimpleVocabulary<C>::SearchResult SimpleVocabulary<S, C>::upper_bound(
    const StringType& word, Comparator comparator) const {
  SearchResult result;
  // TODO<joka921> do we still need the lower_bound in the _words?
  result._id = _words.upper_bound(word, comparator);
  if (result._id < _words.size()) {
    resultInternal._word = _words[result._id];
  }
  return result;
}
template SimpleVocabulary<char>;
template SimpleVocabulary<CompressedChar>;
