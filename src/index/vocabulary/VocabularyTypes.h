//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <optional>

// A word and its index in the vocabulary from which it was obtained. Also
// contains a special state `end()` which can be queried by the `isEnd()`
// function. This can be used to represent words that are larger than the
// largest word in the vocabulary, similar to a typical `end()` iterator.
class WordAndIndex {
 private:
  std::optional<std::pair<std::string, uint64_t>> wordAndIndex_;

 public:
  // Query for the special `end` semantics.
  bool isEnd() const { return !wordAndIndex_.has_value(); }

  // Return the word. Throws if `isEnd() == true`.
  const std::string& word() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().first;
  }

  // Return the index. Throws if `isEnd() == true`.
  uint64_t index() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().second;
  }

  // Return the index. If `isEnd() == true`, return the `defaultValue`.
  uint64_t indexOr(uint64_t defaultValue) const {
    return isEnd() ? defaultValue : index();
  }

  // The next valid indices before `index()`. If `nullopt` either no
  // such index exists (because `index()` is already the first valid index),
  // or it simply wasn't set. This member is currently used to communicate
  // between the `VocabularyInMemoryBinSearch` and the
  // `InternalExternalVocabulary`.
  std::optional<uint64_t> previousIndex_ = std::nullopt;

  // The default constructor creates a `WordAndIndex` with `isEnd() == true`.
  WordAndIndex() = default;

  // Explicit factory function for the end state.
  static WordAndIndex end() { return {}; }

  // Constructors for the ordinary non-end case.
  WordAndIndex(std::string word, uint64_t index)
      : wordAndIndex_{std::in_place, std::move(word), index} {}
  WordAndIndex(std::string_view word, uint64_t index)
      : wordAndIndex_{std::in_place, std::string{word}, index} {}

  // bool operator==(const WordAndIndex& result) const = default;

  // This operator provides human-readable output for a `WordAndIndex`,
  // useful for testing.
  friend std::ostream& operator<<(std::ostream& os, const WordAndIndex& wi) {
    os << "WordAndIndex :";
    if (wi.isEnd()) {
      os << "[end]";
    } else {
      os << '"' << wi.word() << '"';
      os << wi.index();
    }
    return os;
  }
};
