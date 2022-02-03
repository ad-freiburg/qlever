// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
//

#pragma once

#include <algorithm>
#include <cassert>
#include <fstream>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../global/Pattern.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "./StringSortComparator.h"


//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
template <class CharTypeT, class ComparatorType>
class SimpleVocabulary {

 public:



  using SortLevel = typename ComparatorType::Level;
  SimpleVocabulary(){} = default;
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
    bool operator<=>(const SearchResult& res) const { return _id <=> res._id; }
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
  const std::optional<String_view> operator[](Id id) const;

  [[nodiscard]] size_t size() const {
    return _words.size();
  }

  void setLocale(const std::string& language, const std::string& country,
                 bool ignorePunctuation);

  // _____________________________________________________________________
  const ComparatorType& getCaseComparator() const { return _caseComparator; }

  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return _caseComparator.getLocaleManager();
  }

  // Wraps std::lower_bound and returns an index instead of an iterator
  SearchResult lower_bound(StringView word,
                           SortLevel level = SortLevel::QUARTERNARY) const;

  template <typename InternalStringType, typename Comparator >
  // Wraps std::lower_bound and returns an index instead of an iterator
  SearchResult lower_bound(const InternalStringType& word, Comparator comparator) const;

  // _______________________________________________________________
  SearchResult upper_bound(const string& word, SortLevel level) const;
  SearchResult upper_bound(const InternalStringType& word, Comparator comparator) const;

  auto getComparatorForSortLevel(const SortLevel level) const {
    return [this, level](auto&& a, auto&& b) {
      return this->_caseComparator(a, b,
                                   level);
    };
  }

 private:
  // vector<StringType> _words;
  CompactVectorOfStrings<char>
      _words;
  ComparatorType _caseComparator;

 public:
  using WordWriter = decltype(_words)::Writer;
  static auto makeWordDiskIterator(const string& filename) {
    return decltype(_words)::diskIterator(filename);
  }
};