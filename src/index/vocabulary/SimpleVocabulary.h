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
#include "./CompressedString.h"
#include "./StringSortComparator.h"
#include "ExternalVocabulary.h"

using std::string;
using std::vector;

struct IdRange {
  IdRange() : _first(), _last() {}

  IdRange(Id first, Id last) : _first(first), _last(last) {}

  Id _first;
  Id _last;
};

//! Stream operator for convenience.
inline std::ostream& operator<<(std::ostream& stream, const IdRange& idRange) {
  return stream << '[' << idRange._first << ", " << idRange._last << ']';
}


//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
template <class ComparatorType>
class SimpleVocabulary {

  // The result of methods like `lower_bound`, `upper_bound`, `prefix_range`.
  // `_word` is nullopt if the Id does is out of range (e.g. "not found" in
  // connection with `lower_bound`).
  struct SearchResult {
    uint64_t _id;
    std::optional<std::string> _word;
    bool operator<=>(const SearchResult& res) const { return _id <=> res._id; }
  };

 public:
  using SortLevel = typename ComparatorType::Level;
  SimpleVocabulary(){} = default;
  SimpleVocabulary& operator=(SimpleVocabulary&&) noexcept = default;
  SimpleVocabulary(SimpleVocabulary&&) noexcept = default;

  virtual ~SimpleVocabulary() = default;

  //! clear all the contents, but not the settings for prefixes etc
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
  //! Only enabled when uncompressed which also means no externalization
  const std::optional<std::string_view> operator[](Id id) const;

  [[nodiscard]] size_t size() const {
    return _words.size();
  }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, Id* id) const;

  template <typename U = StringType, typename = enable_if_compressed<U>>
  void printRangesForDatatypes();

  // only used during Index building, not needed for compressed vocabulary
  void createFromSet(const ad_utility::HashSet<std::string>& set);

  void setLocale(const std::string& language, const std::string& country,
                 bool ignorePunctuation);

  // _____________________________________________________________________
  const ComparatorType& getCaseComparator() const { return _caseComparator; }

  /// returns the range of IDs where strings of the vocabulary start with the
  /// prefix according to the collation level the first Id is included in the
  /// range, the last one not. Currently only supports the Primary collation
  /// level, due to limitations in the StringSortComparators
  [[nodiscard]] std::pair<SearchResult, SearchResult> prefix_range(
      const string& prefix) const;

  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return _caseComparator.getLocaleManager();
  }

  template <typename InternalStringType>
  // Wraps std::lower_bound and returns an index instead of an iterator
  SearchResult lower_bound(const InternalStringType& word,
                           SortLevel level = SortLevel::QUARTERNARY) const;

  // _______________________________________________________________
  SearchResult upper_bound(const string& word, SortLevel level) const;

 private:
  auto getComparatorForSortLevel(const SortLevel level) const {
    auto expandIfCompressed = [&](const auto& input) {
      if constexpr (requires() {
                      { input.front() }
                      ->ad_utility::SimilarTo<CompressedChar>;
                    }) {
        return this->expandPrefix(input);
      } else {
        return input;
      }
    };
    return [this, level, expandIfCompressed](auto&& a, auto&& b) {
      return this->_caseComparator(expandIfCompressed(a), expandIfCompressed(b),
                                   level);
    };
  }

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