// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <string>
#include <vector>
#include "../global/Id.h"
#include "../util/File.h"

using std::string;
using std::vector;

//! On-disk vocabulary. Very small (O(1)) memory consumption.
//! Layout: <term1><term2>..<termn><offsets><startOfOffsets>
//! <offsets> are n off_t where the i's off_t specifies the position
//! of term i in the file.
//! To obtain item i, read two offsets from
//! startofOffsets + i * sizeof(off_t) and then read the strings in between.
//! To obtain an ID for a term, do a binary search where each random access
//! uses the steps described above.
class ExternalVocabulary {
 public:
  void buildFromVector(const vector<string>& v, const string& fileName);

  void initFromFile(const string& file);

  //! Get the word with the given id
  //! (as non-reference, returning a cost ref is not possible, because the
  //! string does not necessarily already exist in memory - unlike for an
  //! internal vocabulary)
  string operator[](Id id) const;

  //! Get the number of words in the vocabulary.
  size_t size() const { return _size; }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, Id* id) const {
    *id = binarySearchInVocab(word);
    return *id < _size && (*this)[*id] == word;
  }

 private:
  mutable ad_utility::File _file;
  off_t _startOfOffsets;
  size_t _size;

  Id binarySearchInVocab(const string& word) const;
};
