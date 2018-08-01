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
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>
#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "./CompressedString.h"
#include "ExternalVocabulary.h"

using std::string;
using std::vector;
using json = nlohmann::json;

template <class StringType>
struct AccessReturnTypeGetter {};

template <>
struct AccessReturnTypeGetter<string> {
  using type = const string&;
};
template <>
struct AccessReturnTypeGetter<CompressedString> {
  using type = string;
};

template <class StringType>
using AccessReturnType_t = typename AccessReturnTypeGetter<StringType>::type;

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

// simple class for members of a prefix compression codebook
struct Prefix {
  Prefix() = default;
  Prefix(char prefix, const string& fulltext)
      : _prefix(prefix), _fulltext(fulltext) {}

  char _prefix;
  string _fulltext;
};

// forward declaration for PrefixComparator
template <class StringType>
class Vocabulary;

template <class S>
class PrefixComparator {
 public:
  explicit PrefixComparator(size_t prefixLength, const Vocabulary<S>* vocab)
      : _prefixLength(prefixLength), _vocab(vocab) {}

  bool operator()(const string& lhs, const string& rhs) const;
  bool operator()(const CompressedString& lhs, const string& rhs) const;
  bool operator()(const string& lhs, const CompressedString& rhs) const;

 private:
  const size_t _prefixLength;
  const Vocabulary<S>* _vocab;
};

//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
//! Template parameters that are supported are:
//! std::string -> no compression is applied
//! CompressedString -> prefix compression is applied
template <class StringType>
class Vocabulary {
 public:
  // TODO<joka921, niklas> It would be cleaner to put the enable_if into the
  // class declaration but this would need a lot of code restructuring.
  // Can I leave it like this, the compiler errors are as decent as they become
  // with templates
  template <
      typename = std::enable_if_t<std::is_same_v<StringType, string> ||
                                  std::is_same_v<StringType, CompressedString>>>
  Vocabulary(){};

  // variable for dispatching
  static constexpr bool _isCompressed =
      std::is_same_v<StringType, CompressedString>;

  virtual ~Vocabulary() = default;

  //! clear all the contents, but not the settings for prefixes etc
  void clear() {
    _words.clear();
    _externalLiterals.clear();
  }
  //! Read the vocabulary from file.
  void readFromFile(const string& fileName, const string& extLitsFileName = "");

  //! Write the vocabulary to a file.
  // We don't need to write compressed vocabularies with the current index
  // building procedure
  template <typename = std::enable_if_t<!_isCompressed>>
  void writeToFile(const string& fileName) const;

  //! Write to binary file to prepare the merging. Format:
  // 4 Bytes strlen, then character bytes, then 8 bytes zeros for global id
  template <typename = std::enable_if_t<!_isCompressed>>
  void writeToBinaryFileForMerging(const string& fileName) const;

  //! Append a word to the vocabulary. Wraps the std::vector method.
  void push_back(const string& word) {
    if constexpr (_isCompressed) {
      _words.push_back(compressPrefix(word));
    } else {
      _words.push_back(word);
    }
  }

  //! Get the word with the given id or an empty optional if the
  //! word is not in the vocabulary.
  //! Only enabled when uncompressed which also means no externalization
  template <typename = std::enable_if_t<!_isCompressed>>
  const std::optional<std::reference_wrapper<const string>> operator[](
      Id id) const {
    if (id < _words.size()) {
      return _words[static_cast<size_t>(id)];
    } else {
      return std::nullopt;
    }
  }

  //! Get the word with the given id or an empty optional if the
  //! word is not in the vocabulary. Returns an lvalue because compressed or
  //! externalized words don't allow references
  template <typename = std::enable_if_t<_isCompressed>>
  const std::optional<string> idToOptionalString(Id id) const {
    if (id < _words.size()) {
      // internal, prefixCompressed word
      return expandPrefix(_words[static_cast<size_t>(id)]);
    } else if (id == ID_NO_VALUE) {
      return std::nullopt;
    } else {
      // this word must be externalized
      id -= _words.size();
      AD_CHECK(id < _externalLiterals.size());
      return _externalLiterals[id];
    }
  }

  //! Get the word with the given id.
  //! lvalue for compressedString and const& for string-based vocabulary
  AccessReturnType_t<StringType> at(Id id) const {
    if constexpr (_isCompressed) {
      return expandPrefix(_words[static_cast<size_t>(id)]);
    } else {
      return _words[static_cast<size_t>(id)];
    }
  }

  // AccessReturnType_t<StringType> at(Id id) const { return operator[](id); }

  //! Get the number of words in the vocabulary.
  size_t size() const { return _words.size(); }

  //! Reserve space for the given number of words.
  void reserve(unsigned int n) { _words.reserve(n); }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, Id* id) const {
    if (!shouldBeExternalized(word)) {
      *id = lower_bound(word);
      return *id < _words.size() && at(*id) == word;
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
    if (at(lb) != indexWord && lb > 0) {
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
    if (at(lb) != indexWord && lb > 0) {
      // If indexWord is not in the vocab, lb points to the next value.
      // But if this happened, we know that there is nothing in between and
      // it's
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
    range->_last =
        upper_bound(word.substr(0, word.size() - 1), range->_first,
                    PrefixComparator<StringType>(word.size() - 1, this)) -
        1;
    bool success = range->_first < _words.size() &&
                   ad_utility::startsWith(at(range->_first),
                                          word.substr(0, word.size() - 1)) &&
                   range->_last < _words.size() &&
                   ad_utility::startsWith(at(range->_last),
                                          word.substr(0, word.size() - 1)) &&
                   range->_first <= range->_last;
    if (success) {
      AD_CHECK_LT(range->_first, _words.size());
      AD_CHECK_LT(range->_last, _words.size());
    }
    return success;
  }

  // only used during Index building, not needed for compressed vocabulary
  template <typename = std::enable_if_t<!_isCompressed>>
  void createFromSet(const ad_utility::HashSet<StringType>& set);

  template <typename = std::enable_if_t<!_isCompressed>>
  google::sparse_hash_map<string, Id> asMap();

  static bool isLiteral(const string& word);

  template <bool isEntity = false>
  bool shouldBeExternalized(const string& word) const;

  bool shouldEntityBeExternalized(const string& word) const;

  // only still needed for text vocabulary
  template <typename = std::enable_if_t<!_isCompressed>>
  void externalizeLiterals(const string& fileName);

  void externalizeLiteralsFromTextFile(const string& textFileName,
                                       const string& outFileName) {
    _externalLiterals.buildFromTextFile(textFileName, outFileName);
  }

  const ExternalVocabulary& getExternalVocab() const {
    return _externalLiterals;
  }

  static string getLanguage(const string& literal);

  // _____________________________________________________
  template <typename = std::enable_if_t<_isCompressed>>
  string expandPrefix(const CompressedString& word) const;

  // _____________________________________________
  template <typename = std::enable_if_t<_isCompressed>>
  CompressedString compressPrefix(const string& word) const;

  // ____________________________________________
  // TODO<joka921> actually we don't want to read the complete file here
  // Move to Index class, where a settings file can be read.
  // TODO throw out, actually we want to automatically create our prefixes
  /*
  void initializeNewPrefixes(const string& filename);
  */

  // TODO< this overload is needed for creating the prefix heuristic
  template <class StringRange, typename = std::enable_if_t<_isCompressed>>
  void initializeNewPrefixes(const StringRange& prefixes);

  // set the list of prefixes for words which will become part of the
  // externalized vocabulary. Good for entity names that normally don't appear
  // in queries or results but take a lot of space (e.g. Wikidata statements)
  template <class StringRange>
  void initializeExternalizePrefixes(const StringRange& prefixes);

  // ______________________________________________________
  // set the prefixes used for compression
  template <typename = std::enable_if_t<_isCompressed>>
  void initializeRestartPrefixes(const json& j);

  // needed by function prefixCompressFile
  template <typename = std::enable_if_t<_isCompressed>>
  json getJsonForPrefixes() const;

  // Compress the file at path infile, write to file at outfile using the
  // specified prefixes.
  // Arguments:
  //   infile - path to original vocabulary, one word per line
  //   outfile- output path. Will be overwritten by also one word per line
  //            in the same order as the infile
  //   prefixes - a list of prefixes which we will compress
  //
  //   Returns: A json array with information about the prefixes,
  //            j[2]="ablab" means, that the prefix "ablab" was encoded by the
  //            byte \x02
  template <typename = std::enable_if_t<_isCompressed>>
  static json prefixCompressFile(const string& infile, const string& outfile,
                                 const vector<string>& prefixes);

 private:
  // Wraps std::lower_bound and returns an index instead of an iterator
  Id lower_bound(const string& word) const {
    if constexpr (_isCompressed) {
      auto pred = [this](const CompressedString& a, const string& b) {
        return this->expandPrefix(a) < b;
      };
      return static_cast<Id>(
          std::lower_bound(_words.begin(), _words.end(), word, pred) -
          _words.begin());
    } else {
      return static_cast<Id>(
          std::lower_bound(_words.begin(), _words.end(), word) -
          _words.begin());
    }
  }

  // _______________________________________________________________
  Id upper_bound(const string& word) const {
    if constexpr (_isCompressed) {
      auto pred = [this](const string& a, const CompressedString& b) {
        return a < this->expandPrefix(b);
      };
      return static_cast<Id>(
          std::upper_bound(_words.begin(), _words.end(), word, pred) -
          _words.begin());
    } else {
      return static_cast<Id>(
          std::upper_bound(_words.begin(), _words.end(), word) -
          _words.begin());
    }
  }

  // Wraps std::lower_bound and returns an index instead of an iterator
  Id lower_bound(const string& word, size_t first) const {
    if constexpr (_isCompressed) {
      auto pred = [this](const CompressedString& a, const string& b) {
        return this->expandPrefix(a) < b;
      };
      return static_cast<Id>(
          std::lower_bound(_words.begin() + first, _words.end(), word, pred) -
          _words.begin());
    } else {
      return static_cast<Id>(
          std::lower_bound(_words.begin() + first, _words.end(), word) -
          _words.begin());
    }
  }

  // Wraps std::upper_bound and returns an index instead of an iterator
  // Only compares words that have at most word.size() or to prefixes of
  //
  // that length otherwise.
  Id upper_bound(const string& word, size_t first,
                 PrefixComparator<StringType> comp) const {
    AD_CHECK_LE(first, _words.size());
    typename vector<StringType>::const_iterator it =
        std::upper_bound(_words.begin() + first, _words.end(), word, comp);
    Id retVal =
        (it == _words.end()) ? size() : static_cast<Id>(it - _words.begin());
    AD_CHECK_LE(retVal, size());
    return retVal;
  }

  // TODO<joka921> these following two members are only used with the
  // compressed vocabulary. They don't use much space if empty, but still it
  // would be cleaner to throw them out when using the uncompressed version
  //
  // list of all prefixes and their codewords, sorted descending by the length
  // of the prefixes. Used for lookup when encoding strings
  std::vector<Prefix> _prefixVec{};

  // maps (numeric) keys to the prefix they encode.
  // currently only 128 prefixes are supported.
  array<std::string, NUM_COMPRESSION_PREFIXES> _prefixMap{""};

  // If a word starts with one of those prefixes it will be externalized
  vector<std::string> _externalizedPrefixes;

  vector<StringType> _words;
  ExternalVocabulary _externalLiterals;
};

#include "./VocabularyImpl.h"
