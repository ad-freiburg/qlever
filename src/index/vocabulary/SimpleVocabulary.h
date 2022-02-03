// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
//

#pragma once

#include <string>
#include <string_view>

#include "../../global/Pattern.h"
#include "../StringSortComparator.h"
#include "../CompressedString.h"



//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
template <class CharTypeT>
class SimpleVocabulary {

 public:

  SimpleVocabulary() = default;
  SimpleVocabulary& operator=(SimpleVocabulary&&) noexcept = default;
  SimpleVocabulary(SimpleVocabulary&&) noexcept = default;

  using CharType = CharTypeT;
  using StringView = std::basic_string_view<CharType>;
  using String = std::basic_string<CharType>;

  // The result of methods like `lower_bound`, `upper_bound`, `prefix_range`.
  // `_word` is nullopt if the Id does is out of range (e.g. "not found" in
  // connection with `lower_bound`).
  struct SearchResult {
    uint64_t _id;
    std::optional<StringView> _word;
    auto operator<=>(const SearchResult& res) const { return _id <=> res._id; }
  };

  virtual ~SimpleVocabulary() = default;

  //! clear all the contents
  // TODO<joka921> is this needed.
  void clear() {
    _words.clear();
  }

  //! Read the vocabulary from file.
  void readFromFile(const string& fileName);

  //! Write the vocabulary to a file.
  void writeToFile(const string& fileName) const;
  //! Get the word with the given id or an empty optional if the
  //! word is not in the vocabulary.
  const std::optional<StringView> operator[](Id id) const;

  [[nodiscard]] size_t size() const {
    return _words.size();
  }

  template <typename InternalStringType, typename Comparator >
  // Wraps std::lower_bound and returns an index instead of an iterator
  SearchResult lower_bound(const InternalStringType& word, Comparator comparator) const;

  template <typename InternalStringType, typename Comparator >
  SearchResult upper_bound(const InternalStringType& word, Comparator comparator) const;

 private:
  // vector<StringType> _words;
  CompactVectorOfStrings<char>
      _words;

 public:
  using WordWriter = decltype(_words)::Writer;
  static auto makeWordDiskIterator(const string& filename) {
    return decltype(_words)::diskIterator(filename);
  }
};

using InternalCharVocabulary = SimpleVocabulary<char>;
using InternalCompressedCharVocabulary = SimpleVocabulary<CompressedChar>;