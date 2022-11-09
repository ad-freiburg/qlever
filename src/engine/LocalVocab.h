// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include "absl/container/node_hash_map.h"

// A class for maintaing a local vocabulary with contiguous (local) IDs. This is
// meant for words that are not part of the normal vocabulary (constructed from
// the input data at indexing time).
//
// TODO: This is a first version of this class with basic functionality. Note
// that the local vocabulary used to be a simple `std::vector<std::string>`
// defined inside of the `ResultTable` class. You gotta start somewhere.
class LocalVocab {
 public:
  // Create a new, empty local vocabulary.
  LocalVocab() = default;

  // Prevent accidental copying of a local vocabulary (it can be quite large),
  // but moving it is OK.
  //
  // TODO: does the default move do the "right" thing, that is, move the hash
  // map instead of copying it?
  LocalVocab(const LocalVocab&) = delete;
  LocalVocab(LocalVocab&&) = default;

  // Get ID of a word in the local vocabulary. If the word was already
  // contained, return the already existing ID. If the word was not yet
  // contained, add it, and return the new ID.
  Id getIdAndAddIfNotContained(const std::string& word);

  // The number of words in the vocabulary.
  size_t size() const { return idsToWordsMap_.size(); }

  // Return true if and only if the local vocabulary is empty.
  bool empty() const { return idsToWordsMap_.empty(); }

  // Return a const reference to the word.
  const std::string& getWord(LocalVocabIndex localVocabIndex) const;

  // Merge two local vocabularies if at least one of them is empty. If both are
  // non-empty, throws an exception.
  //
  // TODO: Eventually, we want to have one local vocab for the whole query to
  // which each operation writes (one after the other). Then we don't need a
  // merge function anymore.
  static std::shared_ptr<LocalVocab> mergeLocalVocabsIfOneIsEmpty(
      std::shared_ptr<LocalVocab> localVocab1,
      std::shared_ptr<LocalVocab> localVocab2);

 private:
  // A map of the words in the local vocabulary to their local IDs. This is a
  // node hash map because we need the addresses of the words (which are of type
  // `std::string`) to remain stable over their lifetime in the hash map because
  // we refer to them in `wordsToIdsMap_` below.
  absl::node_hash_map<std::string, Id> wordsToIdsMap_;

  // A map of the local IDs to the words. Since the IDs are contiguous, we can
  // use a `std::vector`. We store pointers to the actual words in
  // `wordsToIdsMap_` to avoid storing every word twice. This saves space, but
  // costs us an indirection when looking up a word by its ID.
  std::vector<const std::string*> idsToWordsMap_;

  // The next free local ID (will be incremented by one each time we add a new
  // word).
  Id nextFreeId_ = Id::makeFromLocalVocabIndex(LocalVocabIndex::make(0));
};
