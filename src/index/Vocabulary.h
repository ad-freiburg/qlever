// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <algorithm>
#include <cassert>
#include <functional>
#include <google/sparse_hash_map>
#include <optional>
#include <string>
#include <vector>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
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

class PrefixComparator {
 public:
  explicit PrefixComparator(size_t prefixLength)
      : _prefixLength(prefixLength) {}

  bool operator()(const string& lhs, const string& rhs) const {
    return (lhs.size() > _prefixLength ? lhs.substr(0, _prefixLength) : lhs) <
           (rhs.size() > _prefixLength ? rhs.substr(0, _prefixLength) : rhs);
  }

 private:
  const size_t _prefixLength;
};

//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
class Vocabulary {
 public:
  Vocabulary();

  virtual ~Vocabulary();

  //! Read the vocabulary from file.
  void readFromFile(const string& fileName, const string& extLitsFileName = "");

  //! Write the vocabulary to a file.
  void writeToFile(const string& fileName) const;
  //! Write to binary file to prepare the merging. Format:
  // 4 Bytes strlen, then character bytes, then 8 bytes zeros for global id
  void writeToBinaryFileForMerging(const string& fileName) const;

  //! Append a word to the vocabulary. Wraps the std::vector method.
  void push_back(const string& word) { _words.push_back(word); }

  //! Get the word with the given id or an empty optional if the
  //! word is not in the vocabulary.
  //! Note: So as to keep things as references this does *NOT* check the
  //! externalized part of the vocabulary
  const std::optional<std::reference_wrapper<const string>> operator[](
      Id id) const {
    if (id < _words.size()) {
      return _words[static_cast<size_t>(id)];
    } else {
      return std::nullopt;
    }
  }

  const std::optional<string> idToOptionalString(Id id) const {
    std::optional<string> res = (*this)[id];
    if (!res && id != ID_NO_VALUE) {
      id -= _words.size();
      AD_CHECK(id < _externalLiterals.size());
      res = _externalLiterals[id];
    }
    return res;
  }

  //! Get the number of words in the vocabulary.
  size_t size() const { return _words.size(); }

  //! Get the number of words plus the number of externalized words
  size_t totalVocabularySize() const {
    return _words.size() + _externalLiterals.size();
  }

  //! Reserve space for the given number of words.
  void reserve(unsigned int n) { _words.reserve(n); }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, Id* id) const {
    if (word[0] != '\"' || !shouldBeExternalized(word)) {
      *id = lower_bound(word);
      return *id < _words.size() && _words[*id] == word;
    }
    bool success = _externalLiterals.getId(word, id);
    *id += _words.size();
    return success;
  }

  Id getValueIdForLT(const string& indexWord) const {
    Id lb = lower_bound(indexWord);
    return lb;
  }

  Id getValueIdForLE(const string& indexWord) const {
    Id lb = lower_bound(indexWord);
    if (_words[lb] != indexWord && lb > 0) {
      // If indexWord is not in the vocab, it may be that
      // we ended up one too high. We don't want this to match in LE.
      // The one before is actually lower than index word but that's fine
      // b/c of the LE comparison.
      --lb;
    }
    return lb;
  }

  Id getValueIdForGT(const string& indexWord) const {
    Id lb = lower_bound(indexWord);
    if (_words[lb] != indexWord && lb > 0) {
      // If indexWord is not in the vocab, lb points to the next value.
      // But if this happened, we know that there is nothing in between and it's
      // fine to use one lower
      --lb;
    }
    return lb;
  }

  Id getValueIdForGE(const string& indexWord) const {
    Id lb = lower_bound(indexWord);
    return lb;
  }

  //! Get an Id range that matches a prefix.
  //! Return value signals if something was found at all.
  bool getIdRangeForFullTextPrefix(const string& word, IdRange* range) const {
    AD_CHECK_EQ(word[word.size() - 1], PREFIX_CHAR);
    range->_first = lower_bound(word.substr(0, word.size() - 1));
    range->_last = upper_bound(word.substr(0, word.size() - 1), range->_first,
                               PrefixComparator(word.size() - 1)) -
                   1;
    bool success = range->_first < _words.size() &&
                   ad_utility::startsWith(_words[range->_first],
                                          word.substr(0, word.size() - 1)) &&
                   range->_last < _words.size() &&
                   ad_utility::startsWith(_words[range->_last],
                                          word.substr(0, word.size() - 1)) &&
                   range->_first <= range->_last;
    if (success) {
      AD_CHECK_LT(range->_first, _words.size());
      AD_CHECK_LT(range->_last, _words.size());
    }
    return success;
  }

  void createFromSet(const ad_utility::HashSet<string>& set);

  google::sparse_hash_map<string, Id> asMap();

  void externalizeLiterals(const string& fileName);
  void externalizeLiteralsFromTextFile(const string& textFileName,
                                       const string& outFileName) {
    _externalLiterals.buildFromTextFile(textFileName, outFileName);
  }

  static bool shouldBeExternalized(const string& word);
  static string getLanguage(const string& literal);

 private:
  // Wraps std::lower_bound and returns an index instead of an iterator
  Id lower_bound(const string& word) const {
    return static_cast<Id>(
        std::lower_bound(_words.begin(), _words.end(), word) - _words.begin());
  }

  Id upper_bound(const string& word) const {
    return static_cast<Id>(
        std::upper_bound(_words.begin(), _words.end(), word) - _words.begin());
  }

  // Wraps std::lower_bound and returns an index instead of an iterator
  Id lower_bound(const string& word, size_t first) const {
    return static_cast<Id>(
        std::lower_bound(_words.begin() + first, _words.end(), word) -
        _words.begin());
  }

  // Wraps std::upper_bound and returns an index instead of an iterator
  // Only compares words that have at most word.size() or to prefixes of
  // that length otherwise.
  Id upper_bound(const string& word, size_t first,
                 PrefixComparator comp) const {
    AD_CHECK_LE(first, _words.size());
    vector<string>::const_iterator it =
        std::upper_bound(_words.begin() + first, _words.end(), word, comp);
    Id retVal =
        (it == _words.end()) ? size() : static_cast<Id>(it - _words.begin());
    AD_CHECK_LE(retVal, size());
    return retVal;
  }

  vector<string> _words;
  ExternalVocabulary _externalLiterals;
};
