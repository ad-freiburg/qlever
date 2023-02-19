// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/node_hash_map.h"
#include "global/Id.h"

// A class for maintaing a local vocabulary with contiguous (local) IDs. This is
// meant for words that are not part of the normal vocabulary (constructed from
// the input data at indexing time).
//
// TODO: This is a first version of this class with basic functionality. Note
// that the local vocabulary used to be a simple `std::vector<std::string>`
// defined inside of the `ResultTable` class. You gotta start somewhere.
class LocalVocab {
 private:
  // A map of the words in the local vocabulary to their local IDs. This is a
  // node hash map because we need the addresses of the words (which are of type
  // `std::string`) to remain stable over their lifetime in the hash map because
  // we refer to them in `wordsToIdsMap_` below.
  absl::node_hash_map<std::string, LocalVocabIndex> wordsToIndexesMap_;

  // A map of the local IDs to the words. Since the IDs are contiguous, we can
  // use a `std::vector`. We store pointers to the actual words in
  // `wordsToIdsMap_` to avoid storing every word twice. This saves space, but
  // costs us an indirection when looking up a word by its ID.
  std::vector<const std::string*> indexesToWordsMap_;

  // The next free local ID (will be incremented by one each time we add a new
  // word).
  LocalVocabIndex nextFreeIndex_ = LocalVocabIndex::make(0);

  // Indicator that the local vocabulary is read only. This will be set once the
  // local vocabulary is shared between more than one result; see the respective
  // methods in `ResultTable`.
  bool readOnly_ = false;

 public:
  // Create a new, empty local vocabulary.
  LocalVocab() = default;

  // Prevent accidental copying of a local vocabulary.
  LocalVocab(const LocalVocab&) = delete;
  LocalVocab& operator=(const LocalVocab&) = delete;

  // Make a deep copy explicitly.
  LocalVocab clone() const;

  // Moving a local vocabulary is not problematic (though the typical use case
  // in our code is to copy shared pointers to local vocabularies).
  LocalVocab(LocalVocab&&) = default;
  LocalVocab& operator=(LocalVocab&&) = default;

  // Make the local vocabulary read only.
  void makeReadOnly() { readOnly_ = true; }

  // Get the index of a word in the local vocabulary. If the word was already
  // contained, return the already existing index. If the word was not yet
  // contained, add it, and return the new index.
  LocalVocabIndex getIndexAndAddIfNotContained(const std::string& word);
  LocalVocabIndex getIndexAndAddIfNotContained(std::string&& word);

  // The number of words in the vocabulary.
  size_t size() const { return indexesToWordsMap_.size(); }

  // Return true if and only if the local vocabulary is empty.
  bool empty() const { return indexesToWordsMap_.empty(); }

  // Return a const reference to the word.
  const std::string& getWord(LocalVocabIndex localVocabIndex) const;

 private:
  // Common implementation for the two variants of
  // `getIndexAndAddIfNotContainedImpl` above.
  template <typename WordT>
  LocalVocabIndex getIndexAndAddIfNotContainedImpl(WordT&& word);
};
