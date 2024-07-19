//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <optional>

/// A word and its index in the vocabulary from which it was obtained. A word
/// that is larger than all words in the vocabulary is represented by
/// `{std::nullopt, largestIndexInVocabulary + 1}`
class WordAndIndex {
 private:
  std::optional<std::pair<std::string, uint64_t>> wordAndIndex_;

 public:
  bool isEnd() const { return !wordAndIndex_.has_value(); }
  const std::string& word() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().first;
  }
  uint64_t index() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().second;
  }

  uint64_t indexOr(uint64_t defaultValue) const {
    return isEnd() ? defaultValue : index();
  }
  // The next valid indices before `_index`. If `nullopt` either no
  // such index exists (because `_index` is already the first valid index),
  // or it simply wasn't set. This member is currently used to communicate
  // between the `VocabularyInMemoryBinSearch` and the
  // `InternalExternalVocabulary`.
  std::optional<uint64_t> previousIndex_;

  // The default is the `end`status
  // TODO<joka921> check if this works in all cases.
  WordAndIndex() = default;

  static WordAndIndex end() { return {}; }

  // Constructors that are needed to construct a word and index from one of
  // `std::string`, `std::string_view`, `std::optional<std::string>`,
  // `std::optional<std::string_view>` and the index.
  WordAndIndex(std::string word, uint64_t index)
      : wordAndIndex_{std::in_place, std::move(word), index} {}
  WordAndIndex(std::string_view word, uint64_t index)
      : wordAndIndex_{std::in_place, std::string{word}, index} {}

  bool operator==(const WordAndIndex& result) const = default;

  // Comparison is only done by the `_index`, since the order of the `word_`
  // depends on the order in the vocabulary, which is unknown to the
  // `WordAndIndex`
  // TODO<joka921> Why do we need this?
  auto operator<=>(const WordAndIndex& rhs) const {
    return wordAndIndex_ <=> rhs.wordAndIndex_;
  }

  // TODO<joka921> Reinstate once everything else works.
  /*
  // This operator provides human-readable output for a `WordAndIndex`,
  // useful for testing.
  friend std::ostream& operator<<(std::ostream& os, const WordAndIndex& wi) {
    os << "WordAndIndex : \"";
    os << (wi.word_.value_or("nullopt")) << '"';
    os << wi.index_ << ", ";
    return os;
  }
   */
};
