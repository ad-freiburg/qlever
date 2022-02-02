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

template <class StringType>
struct AccessReturnTypeGetter {};

template <>
struct AccessReturnTypeGetter<string> {
  using type = std::string_view;
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
  Prefix(char prefix, string fulltext)
      : _prefix(prefix), _fulltext(std::move(fulltext)) {}

  char _prefix;
  string _fulltext;
};

//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
//! Template parameters that are supported are:
//! std::string -> no compression is applied
//! CompressedString -> prefix compression is applied
template <class StringType, class ComparatorType>
class Vocabulary {
  // The different type of data that is stored in the vocabulary
  enum class Datatypes { Literal, Iri, Float, Date };

  // The result of methods like `lower_bound`, `upper_bound`, `prefix_range`. `_word` is nullopt if the Id does is out of range (e.g. "not found" in connection with `lower_bound`).
  struct SearchResult {
    ad_utility::CompleteId _id;
    std::optional<std::string> _word;

    // TODO<joka921> Add operator <=> to NamedTypes
    bool operator<(const SearchResult& res) const {
      return _id < res._id;
    }
    bool operator>(const SearchResult& res) const {
      return _id > res._id;
    }
  };

  template <typename T, typename R = void>
  using enable_if_compressed =
      std::enable_if_t<std::is_same_v<T, CompressedString>>;

  template <typename T, typename R = void>
  using enable_if_uncompressed =
      std::enable_if_t<!std::is_same_v<T, CompressedString>>;

  using IdManager = ad_utility::DefaultIdManager;

 public:
  using SortLevel = typename ComparatorType::Level;
  template <
      typename = std::enable_if_t<std::is_same_v<StringType, string> ||
                                  std::is_same_v<StringType, CompressedString>>>

  Vocabulary(){};
  Vocabulary& operator=(Vocabulary&&) noexcept = default;
  Vocabulary(Vocabulary&&) noexcept = default;

  // variable for dispatching
  static constexpr bool _isCompressed =
      std::is_same_v<StringType, CompressedString>;

  // do we store "chars" or "compressedChars"
  using CharType = std::conditional_t<_isCompressed, CompressedChar, char>;

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

  //! Get the word with the given id or an empty optional if the
  //! word is not in the vocabulary.
  //! Only enabled when uncompressed which also means no externalization
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
  const std::optional<std::string_view> operator[](Id id) const;

  //! Get the word with the given id or an empty optional if the
  //! word is not in the vocabulary. Returns an lvalue because compressed or
  //! externalized words don't allow references
  template <typename U = StringType, typename = enable_if_compressed<U>>
  [[nodiscard]] const std::optional<string> idToOptionalString(Id id) const;

  //! Get the word with the given id.
  //! lvalue for compressedString and const& for string-based vocabulary
  AccessReturnType_t<StringType> getInternalWordFromId(ad_utility::InternalId id) const;

  // AccessReturnType_t<StringType> at(Id id) const { return operator[](id); }

  //! Get the number of words in the vocabulary.
  [[nodiscard]] size_t internalSize() const { return _words.size().get(); }

  template <typename U = StringType, typename = enable_if_uncompressed<U>>
  [[nodiscard]] size_t size() const { return internalSize(); }


  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, Id* id) const;

  Id getValueIdForLT(const string& indexWord, const SortLevel level) const {
    auto [lb, word] = lower_bound(indexWord, level);
    return lb.get();
  }
  Id getValueIdForGE(const string& indexWord, const SortLevel level) const {
    return getValueIdForLT(indexWord, level);
  }

  Id getValueIdForLE(const string& indexWord, const SortLevel level) const {
    auto [lbCompleteId, word] = upper_bound(indexWord, level);
    auto lb = lbCompleteId.get();

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
  //! CAVEAT! TODO<discovered by joka921>: This is only used for the text index,
  //! and uses a range, where the last index is still within the range which is
  //! against C++ conventions!
  // consider using the prefixRange function.
  bool getIdRangeForFullTextPrefix(const string& word, IdRange* range) const;

  ad_utility::HashMap<Datatypes, std::pair<Id, Id>> getRangesForDatatypes()
      const;

  template <typename U = StringType, typename = enable_if_compressed<U>>
  void printRangesForDatatypes();

  // only used during Index building, not needed for compressed vocabulary
  void createFromSet(const ad_utility::HashSet<std::string>& set);

  static bool isLiteral(const string& word);
  static bool isExternalizedLiteral(const string& word);

  [[nodiscard]] bool shouldBeExternalized(const string& word) const;

  [[nodiscard]] bool shouldEntityBeExternalized(const string& word) const;

  [[nodiscard]] bool shouldLiteralBeExternalized(const string& word) const;

  void externalizeLiteralsFromTextFile(const string& textFileName,
                                       const string& outFileName) {
    _externalLiterals.buildFromTextFile(textFileName, outFileName);
  }

  const ExternalVocabulary<ComparatorType>& getExternalVocab() const {
    return _externalLiterals;
  }

  static string getLanguage(const string& literal);

  // _____________________________________________________
  //
  template <typename U = StringType, typename = enable_if_compressed<U>>
  [[nodiscard]] string expandPrefix(std::string_view word) const;

  template <typename U = StringType, typename = enable_if_compressed<U>>
  [[nodiscard]] string expandPrefix(std::span<const CompressedChar> input) const {
    return expandPrefix(std::string_view{reinterpret_cast<const char*>(input.data()), input.size()});
  }

  template <typename U = StringType, typename = enable_if_compressed<U>>
  [[nodiscard]] string expandPrefix(std::vector<CompressedChar> input) const {
    return expandPrefix(std::string_view{reinterpret_cast<const char*>(input.data()), input.size()});
  }

  // _____________________________________________
  template <typename U = StringType, typename = enable_if_compressed<U>>
  [[nodiscard]] CompressedString compressPrefix(const string& word) const;

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
  static void prefixCompressFile(auto wordIterator,
                                 const vector<string>& prefixes,
                                 const auto& compressedWordAction) {
    Vocabulary v;
    v.initializePrefixes(prefixes);
    for (const auto& word : wordIterator) {
      compressedWordAction(v.compressPrefix(word));
    }
  }

  void setLocale(const std::string& language, const std::string& country,
                 bool ignorePunctuation);

  // _____________________________________________________________________
  const ComparatorType& getCaseComparator() const { return _caseComparator; }

  /// returns the range of IDs where strings of the vocabulary start with the
  /// prefix according to the collation level the first Id is included in the
  /// range, the last one not. Currently only supports the Primary collation
  /// level, due to limitations in the StringSortComparators
  [[nodiscard]] std::pair<SearchResult, SearchResult> prefix_range(const string& prefix) const;

  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return _caseComparator.getLocaleManager();
  }

  template<typename InternalStringType>
  // Wraps std::lower_bound and returns an index instead of an iterator
  SearchResult lower_bound(const InternalStringType& word,
                 SortLevel level = SortLevel::QUARTERNARY) const;

  // _______________________________________________________________
  SearchResult upper_bound(const string& word, SortLevel level) const;

 private:

  auto getComparatorForSortLevel(const SortLevel level) const {
    auto expandIfCompressed = [&](const auto& input) {
      if constexpr( requires() {{input.front()} -> ad_utility::SimilarTo<CompressedChar>;}) {
        return this->expandPrefix(input);
      } else {
        return input;
      }
    };
      return [this, level, expandIfCompressed](auto&& a, auto&& b) {
        return this->_caseComparator(expandIfCompressed(a), expandIfCompressed(b), level); };
  }

  /*
  template <class R = std::string>
  auto getLowerBoundLambda(const SortLevel level) const {
    if constexpr (_isCompressed) {
      return [this, level](std::string_view a, const R& b) {
        return this->_caseComparator(this->expandPrefix(a), b, level);
      };
    } else {
      return [this, level](std::string_view a, const auto& b) {
        return this->_caseComparator(a, b, level);
      };
    }
  }

  auto getUpperBoundLambda(const SortLevel level) const {
    if constexpr (_isCompressed) {
      return [this, level](const std::string& a, std::string_view b) {
        return this->_caseComparator(a, this->expandPrefix(b), level);
      };
    } else {
      return getLowerBoundLambda(level);
    }
  }
   */


  // TODO<joka921> these following two members are only used with the
  // compressed vocabulary. They don't use much space if empty, but still it
  // would be cleaner to throw them out when using the uncompressed version
  //
  // list of all prefixes and their codewords, sorted descending by the length
  // of the prefixes. Used for lookup when encoding strings
  std::vector<Prefix> _prefixVec{};

  // maps (numeric) keys to the prefix they encode.
  // currently only 128 prefixes are supported.
  std::array<std::string, NUM_COMPRESSION_PREFIXES> _prefixMap{""};

  // If a word starts with one of those prefixes it will be externalized
  vector<std::string> _externalizedPrefixes;

  // If a word uses one of these language tags it will be internalized,
  // defaults to English
  vector<std::string> _internalizedLangs{"en"};

  // vector<StringType> _words;
  CompactVectorOfStrings<CharType, ad_utility::InternalId, ad_utility::InternalSignedId> _words;
  using UncompressedVector = CompactVectorOfStrings<char>;
  ExternalVocabulary<ComparatorType> _externalLiterals;
  ComparatorType _caseComparator;

 public:
  using WordWriter = decltype(_words)::Writer;
  using UncompressedWordWriter = UncompressedVector ::Writer;
  static auto makeWordDiskIterator(const string& filename) {
    return decltype(_words)::diskIterator(filename);
  }

  static auto makeUncompressedWordDiskIterator(const string& filename) {
    return UncompressedVector::diskIterator(filename);
  }
};

using RdfsVocabulary = Vocabulary<CompressedString, TripleComponentComparator>;
using TextVocabulary = Vocabulary<std::string, SimpleStringComparator>;