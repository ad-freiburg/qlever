// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
//

#pragma once

#include <algorithm>
#include <cassert>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/locale.hpp>

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

/**
 * \brief Comparator for std::strings that optionally supports
 * case-insensitivity.
 *
 * If the constructor is called with ignoreCase=false it is an ordinary string
 * compare using std::less<std::string>.
 * If ignoreCase=True the operator behaves as follows:
 *
 * - the inputs can either be Literals or non-literals like IRIs
 *   In case the type is different, return the standard ordering to keep
 *   literals and IRIs disjoint in the order
 *
 * - split both literals "vaLue"@lang into its vaLue and possibly empty
 *   langtag. For IRIs, the value is the complete string and the langtag is
 * empty
 * - compare the strings according to the lowercase version of their value
 * - in case the lowercase versions are equal, return the order of the language
 * tags
 * - If the strings are still the same, return the order of the original inner
 * values.
 *
 * This gives us a strict ordering on strings.
 */
class StringSortComparator {
 public:
  using Facet = decltype(std::use_facet<boost::locale::collator<char>>(std::locale()));
  using Level = boost::locale::collator_base::level_type;
  StringSortComparator(const StringSortComparator&) = default;
  // Convert an rdf-literal "value"@lang (langtag is optional)
  // to the first possible literal with the same case-insensitive value
  // ("VALUE") in this case. This is done by conversion to uppercase
  // (uppercase comes before lowercase in ASCII/Utf) and removing
  // possible language tags.

  StringSortComparator() = default;
  StringSortComparator(const std::locale& loc) : _locale(loc) {}
  StringSortComparator& operator=(const StringSortComparator& other) {
    _locale = other._locale;
    return *this;
  }

  // A rdf literal or iri split into its components
  struct SplitVal {
    SplitVal() = default;
    SplitVal(bool lit, std::string trans, std::string_view l)
            : isLiteral(lit), transformedVal(std::move(trans)), langtag(l) {}
    bool isLiteral;  // was the value an rdf-literal
    std::string transformedVal;  // the original inner value, transformed by a locale()
    std::string langtag;  // the language tag, possibly empty
  };



  /*
   * @brief The actual comparison operator.
   */
  bool operator()(std::string_view a, std::string_view b, const boost::locale::collator_base::level_type level = boost::locale::collator_base::identical) const {
    return compareViews(a, b, level) < 0;
  }

  bool operator()(std::string_view a , const SplitVal& spB, const boost::locale::collator_base::level_type level) const {
    auto spA = extractAndTransformComparable(a, level);
    return compareCStyle(spA, spB);
  }


  bool equals(std::string_view a, std::string_view b, const boost::locale::collator_base::level_type level = boost::locale::collator_base::identical) const {
    return compareViews(a, b, level) == 0;
  }

  int compareViews(std::string_view a, std::string_view b, const boost::locale::collator_base::level_type level = boost::locale::collator_base::identical) const {
    // the facet and level argument are actually ignored TODO: fix this.
    auto splitA = extractComparable<SplitValNonOwning>(a, getFacet(), level);
    auto splitB = extractComparable<SplitValNonOwning>(b, getFacet(), level);
    if (splitA.isLiteral != splitB.isLiteral) {
      // only one is a literal, compare by the first character to separate
      // datatypes.
      return a < b;
    }
    return compareByLocale(splitA, splitB, level);
  }


  struct SplitValNonOwning {
    SplitValNonOwning() = default;
    SplitValNonOwning(bool lit, std::string_view trans, std::string_view l)
            : isLiteral(lit), transformedVal(std::move(trans)), langtag(l) {}
    bool isLiteral;  // was the value an rdf-literal
    std::string_view transformedVal;  // the original inner value, transformed by a locale()
    std::string_view langtag;  // the language tag, possibly empty
  };

  /// @brief split a literal or iri into its components and convert the inner value
  ///           accor
  SplitVal extractAndTransformComparable(std::string_view a, const Level level) const {
    return extractComparable<SplitVal>(a, getFacet(), level);
  }

  SplitValNonOwning extractNonTransformingComparable(std::string_view a, const Level level) const {
    return extractComparable<SplitValNonOwning>(a, getFacet(), level);
  }

  template<class SplitValType>
  static bool compareCStyle(const SplitValType& a, const SplitValType& b) {
    return compare(a, b, [](const std::string& a, const std::string& b){ return std::strcmp(a.c_str(), b.c_str());}) < 0;
  }

  template<class SplitValType>
  int compareByLocale(const SplitValType& a, const SplitValType& b, const Level level = Level::identical) const {
    const auto c = [this, level](const auto& a, const auto& b) {return getFacet().compare(level, a.data(), a.data() + a.size(), b.data(), b.data() + b.size());};
    return compare(a, b, c);
  }


  /// @brief the inner comparison logic
  /// Comp must return 0 on equality of inner strings, <0 if a < b and >0 if  a > b
  template<class SplitValType, class Comp>
  static int compare(const SplitValType& a, const SplitValType& b, Comp comp) {
    int res = comp(a.transformedVal, b.transformedVal);
    if (res == 0) {
      return a.langtag.compare(b.langtag);
    }
    return res < 0;
  }
private:
  std::locale _locale;
  Facet& getFacet() const {return std::use_facet<boost::locale::collator<char>>(_locale);}

  template<class SplitValType>
  static SplitValType extractComparable(std::string_view a, Facet facet, const Level level) {
    std::string_view res = a;
    bool isLiteral = false;
    std::string_view langtag;
    if (ad_utility::startsWith(res, "\"")) {
      // In the case of prefix filters we might also have
      // Literals that do not have the closing quotation mark
      isLiteral = true;
      res.remove_prefix(1);
      auto endPos = ad_utility::findLiteralEnd(res, "\"");
      if (endPos != string::npos) {
        // this should also be fine if there is no langtag (endPos == size()
        // according to cppreference.com
        langtag = res.substr(endPos + 1);
        res.remove_suffix(res.size() - endPos);
      } else {
        langtag = "";
      }
    }
    if constexpr (std::is_same_v<SplitValType, SplitVal>) {
      return {isLiteral, facet.transform(level, res.data(), res.data() + res.size()), langtag};
    } else if constexpr (std::is_same_v<SplitValType, SplitValNonOwning>) {
      return {isLiteral, res, langtag};
    } else {
      SplitValType().ThisShouldNotCompile();
    }
  }
};

//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
//! Template parameters that are supported are:
//! std::string -> no compression is applied
//! CompressedString -> prefix compression is applied
template <class StringType>
class Vocabulary {
  template <typename T, typename R = void>
  using enable_if_compressed =
      std::enable_if_t<std::is_same_v<T, CompressedString>>;

  template <typename T, typename R = void>
  using enable_if_uncompressed =
      std::enable_if_t<!std::is_same_v<T, CompressedString>>;

 public:
  using SortLevel = StringSortComparator::Level;
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
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
  void writeToFile(const string& fileName) const;

  //! Write to binary file to prepare the merging. Format:
  // 4 Bytes strlen, then character bytes, then 8 bytes zeros for global id
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
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
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
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
  template <typename U = StringType, typename = enable_if_compressed<U>>
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
      // works for the case insensitive version because
      // of the strict ordering.
      return *id < _words.size() && at(*id) == word;
    }
    bool success = _externalLiterals.getId(word, id);
    *id += _words.size();
    return success;
  }

  Id getValueIdForLT(const string& indexWord, const SortLevel level) const {
    Id lb = lower_bound(indexWord, level);
    return lb;
  }
  Id getValueIdForGE(const string& indexWord, const SortLevel level) const {
    return getValueIdForLT(indexWord, level);
  }

  Id getValueIdForLE(const string& indexWord, const SortLevel level) const {
    Id lb = upper_bound(indexWord, level);
    if (lb > 0) {
      // We actually retrieved the first word that is bigger than our entry.
      // TODO<joka921>: What to do, if the 0th entry is already too big?
      --lb;
    }
    return lb;
  }

  Id getValueIdForGT(const string& indexWord, const SortLevel level) const {
    return getValueIdForLE(indexWord, level);
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
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
  void createFromSet(const ad_utility::HashSet<StringType>& set);

  template <typename U = StringType, typename = enable_if_uncompressed<U>>
  ad_utility::HashMap<string, Id> asMap();

  static bool isLiteral(const string& word);
  static bool isExternalizedLiteral(const string& word);

  bool shouldBeExternalized(const string& word) const;

  bool shouldEntityBeExternalized(const string& word) const;

  bool shouldLiteralBeExternalized(const string& word) const;

  // only still needed for text vocabulary
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
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
  //
  template <typename U = StringType, typename = enable_if_compressed<U>>
  string expandPrefix(const CompressedString& word) const;

  // _____________________________________________
  template <typename U = StringType, typename = enable_if_compressed<U>>
  CompressedString compressPrefix(const string& word) const;

  // initialize compression with a list of prefixes
  // The prefixes do not have to be in any specific order
  //
  // StringRange prefixes can be of any type where
  // for (const string& el : prefixes {}
  // works
  template <typename StringRange, typename U = StringType,
            typename = enable_if_compressed<U>>
  void initializePrefixes(const StringRange& prefixes);

  // set the list of prefixes for words which will become part of the
  // externalized vocabulary. Good for entity names that normally don't appear
  // in queries or results but take a lot of space (e.g. Wikidata statements)
  //
  // StringRange prefixes can be of any type where
  // for (const string& el : prefixes {}
  // works
  template <class StringRange>
  void initializeExternalizePrefixes(const StringRange& prefixes);

  // set the list of languages (in "en" language code format) that should be
  // kept internalized. By default this is just English
  //
  // StringRange prefixes can be of any type where
  // for (const string& el : prefixes {}
  // works
  template <class StringRange>
  void initializeInternalizedLangs(const StringRange& prefixes);

  // Compress the file at path infile, write to file at outfile using the
  // specified prefixes.
  // Arguments:
  //   infile - path to original vocabulary, one word per line
  //   outfile- output path. Will be overwritten by also one word per line
  //            in the same order as the infile
  //   prefixes - a list of prefixes which we will compress
  template <typename U = StringType, typename = enable_if_compressed<U>>
  static void prefixCompressFile(const string& infile, const string& outfile,
                                 const vector<string>& prefixes);

  void setLocale(const std::string& localeName) {
    boost::locale::generator gen;
    _locale = std::locale(gen(localeName));
    _caseComparator = StringSortComparator(_locale);
  }

  std::locale getLocale() const {
    return _locale;
  }

  // ___________________________________________________________________
  bool isCaseInsensitiveOrdering() const {
    return _locale != std::locale();
  }

  // _____________________________________________________________________
  const StringSortComparator& getCaseComparator() const {
    return _caseComparator;
  }

  std::pair<Id, Id> prefix_range(const string& prefix, const StringSortComparator::Level level) const {
    if (prefix.empty()) {
      return {0, _words.size()};
    }
    Id lb = lower_bound(prefix, level);
    auto transformed = _caseComparator.extractAndTransformComparable(prefix, level);
    unsigned char last = transformed.transformedVal.back();
    if (last < 255) {
      transformed.transformedVal.back() += 1;
    } else {
      transformed.transformedVal.push_back('\0');
    }

    auto pred = getLowerBoundLambda<StringSortComparator::SplitVal>(level);
    auto ub =  static_cast<Id>(
            std::lower_bound(_words.begin(), _words.end(), transformed, pred) -
            _words.begin());

    return {lb, ub};
  }


  std::pair<Id, Id> equal_range(const string& word, const StringSortComparator::Level level = StringSortComparator::Level::identical) {
    const auto& [beg, end] = std::equal_range(_words.begin(), _words.end(), word, getLowerBoundLambda(level));
    return {beg - _words.begin(), end - _words.begin()};
  }

 private:

  template<class R = std::string>
  auto getLowerBoundLambda(const StringSortComparator::Level level) const {
    if constexpr (_isCompressed) {
      return [this, level](const CompressedString& a, const R& b) {
        return this->_caseComparator(this->expandPrefix(a), b, level);
      };
    } else {
      return [this, level](const string& a, const R& b) {
        return this->_caseComparator(a, b, level);
      };
    }
  }

  auto getUpperBoundLambda(const StringSortComparator::Level level) const {
    if constexpr (_isCompressed) {
      return [this, level](const std::string &a, const CompressedString &b) {
        return this->_caseComparator(a, this->expandPrefix(b), level);
      };
    } else {
      return getLowerBoundLambda();
    }
  }
  // Wraps std::lower_bound and returns an index instead of an iterator
  Id lower_bound(const string& word, const SortLevel level = SortLevel::identical) const {
    return static_cast<Id>(
            std::lower_bound(_words.begin(), _words.end(), word, getLowerBoundLambda(level)) -
            _words.begin());
  }

  // _______________________________________________________________
  Id upper_bound(const string& word, const SortLevel level) const {
    return static_cast<Id>(
            std::upper_bound(_words.begin(), _words.end(), word, getUpperBoundLambda(level)) -
            _words.begin());
  }

  // Wraps std::upper_bound and returns an index instead of an iterator
  // Only compares words that have at most word.size() or to prefixes of
  //
  // that length otherwise.
  Id upper_bound(const string& word, size_t first,
                 PrefixComparator<StringType> comp) const {
    AD_CHECK_LE(first, _words.size());
    // the prefix comparator handles the case-insensitive compare if activated
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

  // If a word uses one of these language tags it will be internalized,
  // defaults to English
  vector<std::string> _internalizedLangs{"en"};

  vector<StringType> _words;
  ExternalVocabulary _externalLiterals;
  StringSortComparator _caseComparator;
  std::locale _locale;  // default constructed as c locale
};

#include "./VocabularyImpl.h"
