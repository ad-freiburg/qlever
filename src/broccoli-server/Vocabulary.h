// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef SERVER_VOCABULARY_H_
#define SERVER_VOCABULARY_H_

#include <assert.h>
#include <string>
#include <vector>
#include <algorithm>

#include "./Globals.h"
#include "../util/Exception.h"
#include "../util/StringUtils.h"
#include "./Conversions.h"
#include "../util/Log.h"
#include "./Identifiers.h"

using std::string;
using std::vector;

namespace ad_semsearch {
struct IdRange {
  IdRange() :
      _first(), _last() {
  }

  IdRange(Id first, Id last) :
      _first(first), _last(last) {
  }

  Id _first;
  Id _last;
};

//! Stream operator for convenience.
inline std::ostream& operator<<(std::ostream& stream, const IdRange& idRange) {
  return stream << '[' << idRange._first << ", " << idRange._last << ']';
}

class PrefixComparator {
  public:
  explicit PrefixComparator(size_t prefixLength) :
      _prefixLength(prefixLength) {
  }

  bool operator()(const string& lhs, const string& rhs) const {
    return (lhs.size() > _prefixLength ? lhs.substr(0, _prefixLength) : lhs)
        < (rhs.size() > _prefixLength ? rhs.substr(0, _prefixLength) : rhs);
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
  void readFromFile(const string& fileName);

  //! Get a string version of the vocabulary that can be understood by humans.
  string asString(void) const;

  //! Append a word to the vocabulary. Wraps the std::vector method.
  void push_back(const string& word) {
    _words.push_back(word);
  }

  //! Get the word with the given id (as const, not as lvalue).
  const string& operator[](Id id) const {
    return _words[static_cast<size_t>(getPureValue(id))];
  }

  //! Get the number of words in the vocabulary.
  size_t size() const {
    return _words.size();
  }

  //! Reserve space for the given number of words.
  void reserve(unsigned int n) {
    _words.reserve(n);
  }

  //! Get an Id from the vocabulary for some ontology word.
  bool getIdForOntologyWord(const string& word, Id* id) const {
    Id index = lower_bound(word);
    *id = FIRST_ENTITY_ID + index;
    return index < _words.size() && _words[index] == word;
  }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getIdForFullTextWord(const string& word, Id* id) const {
    *id = FIRST_WORD_ID + lower_bound(word);
    return *id < _words.size() && _words[getPureValue(*id)] == word;
  }

  //! Get an Id range that matches a prefix.
  //! Should only be used with full-text vocabularies / words.
  //! Return value signals if something was found at all.
  bool getIdRangeForFullTextPrefix(const string& word, IdRange* range) const {
    if (word.size() > MIN_WORD_PREFIX_SIZE) {
      AD_CHECK_EQ(word[word.size() - 1], PREFIX_CHAR);
      range->_first = getFirstId(IdType::WORD_ID)
          + lower_bound(word.substr(0, word.size() - 1));
      range->_last = getFirstId(IdType::WORD_ID)
          + upper_bound(word.substr(0, word.size() - 1), range->_first,
              PrefixComparator(word.size() - 1)) - 1;
      bool success = range->_first < _words.size()
          && ad_utility::startsWith(_words[getPureValue(range->_first)],
              word.substr(0, word.size() - 1)) && range->_last < _words.size()
          && ad_utility::startsWith(_words[getPureValue(range->_last)],
              word.substr(0, word.size() - 1))
          && range->_first <= range->_last;
      if (success) {
        AD_CHECK_LT(range->_first, _words.size());
        AD_CHECK_LT(range->_last, _words.size());
      }
      return success;
    }
    return false;
  }

  //! Get an Id range that matches a prefix.
  //! Should only be used with full-text vocabularies / words.
  //! Return value signals if something was found at all.
  //! This method does not check of the min prefix size and should
  //! only be used when accessing the vocabulary
  //! for the ontology pseudo prefixes.
  bool getIdRangeForPrefixNoPrefixSizeCheck(const string& word,
      IdRange* range) const {
    AD_CHECK_EQ(word[word.size() - 1], PREFIX_CHAR);
    range->_first = getFirstId(IdType::WORD_ID)
        + lower_bound(word.substr(0, word.size() - 1));
    range->_last = getFirstId(IdType::WORD_ID)
        + upper_bound(word.substr(0, word.size() - 1), range->_first,
            PrefixComparator(word.size() - 1)) - 1;
    bool success = range->_first < _words.size()
        && ad_utility::startsWith(_words[getPureValue(range->_first)],
            word.substr(0, word.size() - 1)) && range->_last < _words.size()
        && ad_utility::startsWith(_words[getPureValue(range->_last)],
            word.substr(0, word.size() - 1)) && range->_first <= range->_last;
    if (success) {
      AD_CHECK_LT(range->_first, _words.size());
      AD_CHECK_LT(range->_last, _words.size());
    }
    return success;
  }

  //! Get an Id range that matches a prefix.
  //! Return value signals if something was found at all.
  bool getIdRangeForOntologyPrefix(const string& prefix,
      IdRange* range) const {
    AD_CHECK_EQ(prefix[prefix.size() - 1], PREFIX_CHAR);
    range->_first = FIRST_ENTITY_ID
        + lower_bound(prefix.substr(0, prefix.size() - 1));
    range->_last = FIRST_ENTITY_ID
        + upper_bound(prefix.substr(0, prefix.size() - 1),
            getPureValue(range->_first), PrefixComparator(prefix.size() - 1))
        - 1;
    bool success = ad_utility::startsWith(_words[getPureValue(range->_first)],
        prefix.substr(0, prefix.size() - 1));
    if (success) {
      AD_CHECK_LT(getPureValue(range->_first), _words.size());
      AD_CHECK_LT(getPureValue(range->_last), _words.size());
    }
    return success;
  }

  //! Returns false if the range is empty.
  bool getIdRangeBetweenTwoValuesInclusive(const string& lower,
      const string& upper, IdRange* idRange) const {
    string first = convertOntologyValueToIndexWord(lower);
    string last = convertOntologyValueToIndexWord(upper);
    Id lb = lower_bound(first);
    if (lb >= _words.size()) {
      return false;
    }
    idRange->_first = FIRST_ENTITY_ID + lb;
    idRange->_last = FIRST_ENTITY_ID
        + std::min(lower_bound(last), Id(_words.size() - 1));

    if (_words[getPureValue(idRange->_last)] > last) {
      idRange->_last--;
    }
    return idRange->_first < idRange->_last
        || _words[getPureValue(idRange->_first)] == first
        || _words[getPureValue(idRange->_first)] == last;
  }

  private:

  // Wraps std::lower_bound and returns an index instead of an iterator
  Id lower_bound(const string& word) const {
    return std::lower_bound(_words.begin(), _words.end(), word)
        - _words.begin();
  }

  // Wraps std::lower_bound and returns an index instead of an iterator
  Id lower_bound(const string& word, size_t first) const {
    return std::lower_bound(_words.begin() + first, _words.end(), word)
        - _words.begin();
  }

  // Wraps std::upper_bound and returns an index instead of an iterator
  // Only compares words that have at most word.size() or to prefixes of
  // that length otherwise.
  Id upper_bound(const string& word, size_t first,
      PrefixComparator comp) const {
    AD_CHECK_LE(first, _words.size());
    vector<string>::const_iterator it = std::upper_bound(
        _words.begin() + first, _words.end(), word, comp);
    Id retVal = (it == _words.end()) ? size() : it - _words.begin();
    AD_CHECK_LE(retVal, size());
    return retVal;
  }

  vector<string> _words;
};
}

#endif  // SERVER_VOCABULARY_H_
