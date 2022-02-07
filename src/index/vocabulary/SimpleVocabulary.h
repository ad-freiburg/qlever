// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <string>
#include <string_view>

#include "../../global/Pattern.h"
#include "../CompressedString.h"
#include "../StringSortComparator.h"

//! A vocabulary. Wraps a `CompactVectorOfStrings<char>`
//! and provides additional methods for reading and writing to/from file,
//! and retrieval via binary search.
class SimpleVocabulary {
 public:
  using CharType = char;
  using StringView = std::basic_string_view<CharType>;
  using String = std::basic_string<CharType>;
  using Words = CompactVectorOfStrings<CharType>;

  /// The result of `lower_bound` and `upper_bound`. Return the index of the
  /// result as well as the word that this index points to. The `_word` is
  /// `std::nullopt` iff `_id == size()`, which means that the searched word is
  /// larger than the largest word in the vocabulary.
  struct SearchResult {
    uint64_t _id;
    std::optional<StringView> _word;
    auto operator<=>(const SearchResult& res) const { return _id <=> res._id; }
    bool operator==(const SearchResult&) const = default;
  };

 private:
  // The actual storage
  Words _words;

 public:
  /// Construct an empty vocabulary
  SimpleVocabulary() = default;

  /// Construct the vocabulary from `Words`
  explicit SimpleVocabulary(Words words) : _words{std::move(words)} {}

  // Vocabularies are movable
  SimpleVocabulary& operator=(SimpleVocabulary&&) noexcept = default;
  SimpleVocabulary(SimpleVocabulary&&) noexcept = default;

  /// Read the vocabulary from a file. The file must have been created by a call
  /// to `writeToFile` or using a `WordWriter`.
  void readFromFile(const string& fileName);

  /// Write the vocabulary to a file.
  void writeToFile(const string& fileName) const;

  /// Return the total number of words
  [[nodiscard]] size_t size() const { return _words.size(); }

  /// Return the `i-th` word. The behavior is undefined if `i >= size()`
  auto operator[](uint64_t i) const { return _words[i]; }

  /// Return a `SearchResult` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  SearchResult lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    SearchResult result;
    result._id =
        std::lower_bound(_words.begin(), _words.end(), word, comparator) -
        _words.begin();
    if (result._id < _words.size()) {
      result._word = _words[result._id];
    }
    return result;
  }

  /// Return a `SearchResult` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  SearchResult upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    SearchResult result;
    result._id =
        std::upper_bound(_words.begin(), _words.end(), word, comparator) -
        _words.begin();
    if (result._id < _words.size()) {
      result._word = _words[result._id];
    }
    return result;
  }

 public:
  /// A helper type which can be used to directly write a vocabulary to disk
  /// word-by-word, without having to materialize it in RAM first. See the
  /// documentation of `CompactVectorOfStrings` for details.
  using WordWriter = typename Words::Writer;

  /// Create an iterable generator, that yields the `SimpleVocabulary` from the
  /// file, without materializing the whole vocabulary in RAM. See the
  /// documentaion of `CompactVectorOfStrings` for details.
  static auto makeWordDiskIterator(const string& filename) {
    return Words::diskIterator(filename);
  }
};