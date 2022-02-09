//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VOCABULARYTYPES_H
#define QLEVER_VOCABULARYTYPES_H

/// A word and its index in the vocabulary from which it was obtained. A word
/// that is larger than all words in the vocabulary is represented by
/// `{std::nullopt, largestIndexInVocabulary + 1}`
struct WordAndIndex {
  std::optional<std::string> _word;
  uint64_t _index;

  WordAndIndex() = default;
  // Constructors that are needed to construct a word and index from one of
  // `std::string`, `std::string_view`, `std::optional<std::string>`,
  // `std::optional<std::string_view>` and the index.
  WordAndIndex(std::optional<std::string> word, uint64_t index)
      : _word{std::move(word)}, _index{index} {}
  WordAndIndex(const std::string& word, uint64_t index)
      : _word{word}, _index{index} {}
  WordAndIndex(std::optional<std::string_view> word, uint64_t index)
      : _index{index} {
    if (word.has_value()) {
      _word.emplace(word.value());
    }
  }
  WordAndIndex(std::nullopt_t, uint64_t index) : _index{index} {}

  bool operator==(const WordAndIndex& result) const = default;
  [[nodiscard]] bool has_value() const { return _word.has_value(); }

  // Comparison is only done by the `_index`, since the order of the `_word`
  // depends on the order in the vocabulary, which is unknown to the
  // `WordAndIndex`
  auto operator<=>(const WordAndIndex& rhs) const {
    return _index <=> rhs._index;
  }

  // This operator provides human-readable output for a `WordAndIndex`,
  // useful for testing.
  friend std::ostream& operator<<(std::ostream& os, const WordAndIndex& wi) {
    os << "WordAndIndex : ";
    os << (wi._word.value_or("nullopt"));
    os << wi._index << ", ";
    return os;
  }
};

#endif  // QLEVER_VOCABULARYTYPES_H
