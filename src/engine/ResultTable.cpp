// Copyright 2015 -2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/ResultTable.h"

#include <cassert>

#include "global/Id.h"
#include "global/ValueId.h"

// _____________________________________________________________________________
Id LocalVocab::getIdAndAddIfNotContained(const std::string& word) {
  if (constructionHasFinished_) {
    throw std::runtime_error(
        "Invalid use of `LocalVocab`: You must not call "
        "`getIdAndAddIfNotContained` after `endConstructionPhase` has been "
        "called");
  }
  // The following code avoids computing the hash for `word` twice in case we
  // see it for the first time (note that hashing a string is not cheap). The
  // return value of the `insert` operation is a pair, where `result.first` is
  // an iterator to the (already existing or newly inserted) key-value pair, and
  // `result.second` is a `bool`, which is `true` if and only if the value was
  // newly inserted.
  auto result = wordsToIdsMap_.insert(
      {word,
       Id::makeFromLocalVocabIndex(LocalVocabIndex::make(words_.size()))});
  if (result.second) {
    words_.push_back(word);
  }
  return result.first->second;
}

// _____________________________________________________________________________
void LocalVocab::startConstructionPhase() {
  if (!words_.empty()) {
    throw std::runtime_error(
        "Invalid use of `LocalVocab`: `startConstructionPhase` must currently "
        "only be called when the vocabulary is still empty");
  }
}

// _____________________________________________________________________________
void LocalVocab::endConstructionPhase() {
  wordsToIdsMap_.clear();
  constructionHasFinished_ = true;
}

// _____________________________________________________________________________
ResultTable::ResultTable(ad_utility::AllocatorWithLimit<Id> allocator)
    : _sortedBy(),
      _idTable(std::move(allocator)),
      _resultTypes(),
      // TODO: Why initialize with a pointer to an empty local vocabulary
      // instead of with nullptr?
      _localVocab(std::make_shared<LocalVocab>()) {}

// _____________________________________________________________________________
void ResultTable::clear() {
  _localVocab = nullptr;
  _idTable.clear();
}

// _____________________________________________________________________________
ResultTable::~ResultTable() { clear(); }

// _____________________________________________________________________________
string ResultTable::asDebugString() const {
  std::ostringstream os;
  os << "First (up to) 5 rows of result with size:\n";
  for (size_t i = 0; i < std::min<size_t>(5, _idTable.size()); ++i) {
    for (size_t j = 0; j < _idTable.cols(); ++j) {
      os << _idTable(i, j) << '\t';
    }
    os << '\n';
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t ResultTable::size() const { return _idTable.size(); }
