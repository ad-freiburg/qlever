// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <cstdlib>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "absl/container/node_hash_set.h"
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
  // we hand out pointers to them.
  using Set = absl::node_hash_set<StringAligned16>;
  std::shared_ptr<Set> primaryWordSet_ = std::make_shared<Set>();

  // Local vocabularies from child operations that were merged into this
  // vocabulary s.t. the pointers are kept alive. They have to be `const`
  // because they are possibly shared concurrently (for example via the cache).
  std::vector<std::shared_ptr<const Set>> otherWordSets_;

  auto& primaryWordSet() { return *primaryWordSet_; }
  const auto& primaryWordSet() const { return *primaryWordSet_; }

 public:
  // Create a new, empty local vocabulary.
  LocalVocab() = default;

  // Prevent accidental copying of a local vocabulary.
  LocalVocab(const LocalVocab&) = delete;
  LocalVocab& operator=(const LocalVocab&) = delete;

  // Make a logical copy. The clone will have an empty primary set so it can
  // safely be modified. The contents are copied as shared pointers to const, so
  // the function runs in linear time in the number of word sets.
  LocalVocab clone() const;

  // Moving a local vocabulary is not problematic (though the typical use case
  // in our code is to copy shared pointers to local vocabularies).
  LocalVocab(LocalVocab&&) = default;
  LocalVocab& operator=(LocalVocab&&) = default;

  // Get the index of a word in the local vocabulary. If the word was already
  // contained, return the already existing index. If the word was not yet
  // contained, add it, and return the new index.
  LocalVocabIndex getIndexAndAddIfNotContained(const std::string& word);
  LocalVocabIndex getIndexAndAddIfNotContained(std::string&& word);

  // Get the index of a word in the local vocabulary, or std::nullopt if it is
  // not contained. This is useful for testing.
  std::optional<LocalVocabIndex> getIndexOrNullopt(
      const std::string& word) const;

  // The number of words in the vocabulary.
  // Note: This is not constant time, but linear in the number of word sets.
  size_t size() const {
    auto result = primaryWordSet().size();
    for (const auto& previous : otherWordSets_) {
      result += previous->size();
    }
    return result;
  }

  // Return true if and only if the local vocabulary is empty.
  bool empty() const { return size() == 0; }

  // Return a const reference to the word.
  const std::string& getWord(LocalVocabIndex localVocabIndex) const;

  // Create a local vocab that contains and keeps alive all the words from each
  // of the `vocabs`. The primary word set of the newly created vocab is empty.
  static LocalVocab merge(std::span<const LocalVocab*> vocabs);

  // Return all the words from all the word sets as a vector.
  std::vector<std::string> getAllWordsForTesting() const;

 private:
  // Common implementation for the two variants of
  // `getIndexAndAddIfNotContainedImpl` above.
  template <typename WordT>
  LocalVocabIndex getIndexAndAddIfNotContainedImpl(WordT&& word);
};
