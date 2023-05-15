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
#include "./vocabulary/CompressedVocabulary.h"
#include "./vocabulary/PrefixCompressor.h"
#include "./vocabulary/UnicodeVocabulary.h"
#include "./vocabulary/VocabularyInMemory.h"
#include "VocabularyOnDisk.h"

using std::string;
using std::vector;

template <class StringType>
using AccessReturnType_t =
    std::conditional_t<std::is_same_v<StringType, CompressedString>,
                       std::string, std::string_view>;

template <typename IndexT = WordVocabIndex>
struct IdRange {
  IdRange() : _first(), _last() {}

  IdRange(IndexT first, IndexT last) : _first(first), _last(last) {}

  IndexT _first;
  IndexT _last;
};

//! Stream operator for convenience.
template <typename IndexType>
inline std::ostream& operator<<(std::ostream& stream,
                                const IdRange<IndexType>& idRange) {
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

//! A vocabulary. Wraps a vector of strings
//! and provides additional methods for retrieval.
//! Template parameters that are supported are:
//! std::string -> no compression is applied
//! CompressedString -> prefix compression is applied
template <typename StringType, typename ComparatorType, typename IndexT>
class Vocabulary {
  // The different type of data that is stored in the vocabulary
  enum class Datatypes { Literal, Iri, Float, Date };

  template <typename T, typename R = void>
  using enable_if_compressed =
      std::enable_if_t<std::is_same_v<T, CompressedString>>;

  template <typename T, typename R = void>
  using enable_if_uncompressed =
      std::enable_if_t<!std::is_same_v<T, CompressedString>>;

 public:
  using SortLevel = typename ComparatorType::Level;
  using IndexType = IndexT;

  template <
      typename = std::enable_if_t<std::is_same_v<StringType, string> ||
                                  std::is_same_v<StringType, CompressedString>>>
  Vocabulary() {}
  Vocabulary& operator=(Vocabulary&&) noexcept = default;
  Vocabulary(Vocabulary&&) noexcept = default;

  // variable for dispatching
  static constexpr bool _isCompressed =
      std::is_same_v<StringType, CompressedString>;

  virtual ~Vocabulary() = default;

  //! clear all the contents, but not the settings for prefixes etc
  void clear() {
    _internalVocabulary.close();
    _externalVocabulary.close();
  }
  //! Read the vocabulary from file.
  void readFromFile(const string& fileName, const string& extLitsFileName = "");

  //! Write the vocabulary to a file.
  // We don't need to write compressed vocabularies with the current index
  // building procedure
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
  void writeToFile(const string& fileName) const;

  //! Get the word with the given idx or an empty optional if the
  //! word is not in the vocabulary.
  //! Only enabled when uncompressed which also means no externalization
  template <typename U = StringType, typename = enable_if_uncompressed<U>>
  std::optional<std::string_view> operator[](IndexType idx) const;

  //! Get the word with the given idx or an empty optional if the
  //! word is not in the vocabulary. Returns an lvalue because compressed or
  //! externalized words don't allow references
  template <typename U = StringType>
  [[nodiscard]] std::optional<string> indexToOptionalString(
      IndexType idx) const;

  //! Get the word with the given idx.
  //! lvalue for compressedString and const& for string-based vocabulary
  AccessReturnType_t<StringType> at(IndexType idx) const;

  // AccessReturnType_t<StringType> at(IndexType idx) const { return
  // operator[](id); }

  //! Get the number of words in the vocabulary.
  [[nodiscard]] size_t size() const { return _internalVocabulary.size(); }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, IndexType* idx) const;

  //! Get an Id range that matches a prefix.
  //! Return value signals if something was found at all.
  //! CAVEAT! TODO<discovered by joka921>: This is only used for the text index,
  //! and uses a range, where the last index is still within the range which is
  //! against C++ conventions!
  // consider using the prefixRange function.
  bool getIdRangeForFullTextPrefix(const string& word,
                                   IdRange<IndexType>* range) const;

  ad_utility::HashMap<Datatypes, std::pair<IndexType, IndexType>>
  getRangesForDatatypes() const;

  template <typename U = StringType, typename = enable_if_compressed<U>>
  void printRangesForDatatypes();

  // only used during Index building, not needed for compressed vocabulary
  void createFromSet(const ad_utility::HashSet<std::string>& set);

  static bool isLiteral(const string& word);

  bool shouldBeExternalized(const string& word) const;

  bool shouldEntityBeExternalized(const string& word) const;

  bool shouldLiteralBeExternalized(const string& word) const;

  // only still needed for text vocabulary
  void externalizeLiteralsFromTextFile(const string& textFileName,
                                       const string& outFileName) {
    _externalVocabulary.getUnderlyingVocabulary().buildFromTextFile(
        textFileName, outFileName);
  }

  static string getLanguage(const string& literal);

  // initialize compression with a list of prefixes
  // The prefixes do not have to be in any specific order
  //
  // StringRange prefixes can be of any type where
  // for (const string& el : prefixes {}
  // works
  template <typename StringRange, typename U = StringType,
            typename = enable_if_compressed<U>>
  void buildCodebookForPrefixCompression(const StringRange& prefixes);

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

  void setLocale(const std::string& language, const std::string& country,
                 bool ignorePunctuation);

  // _____________________________________________________________________
  const ComparatorType& getCaseComparator() const {
    return _internalVocabulary.getComparator();
  }

  /// returns the range of IDs where strings of the vocabulary start with the
  /// prefix according to the collation level the first Id is included in the
  /// range, the last one not. Currently only supports the Primary collation
  /// level, due to limitations in the StringSortComparators
  std::pair<IndexType, IndexType> prefix_range(const string& prefix) const;

  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return getCaseComparator().getLocaleManager();
  }

  // Wraps std::lower_bound and returns an index instead of an iterator
  IndexType lower_bound(const string& word,
                        const SortLevel level = SortLevel::QUARTERNARY) const;

  // _______________________________________________________________
  IndexType upper_bound(const string& word, const SortLevel level) const;

 private:
  // If a word starts with one of those prefixes it will be externalized
  vector<std::string> _externalizedPrefixes;

  // If a word uses one of these language tags it will be internalized,
  // defaults to English
  vector<std::string> _internalizedLangs{"en"};

  using PrefixCompressedVocabulary =
      CompressedVocabulary<VocabularyInMemory, PrefixCompressor>;
  using InternalCompressedVocabulary =
      UnicodeVocabulary<PrefixCompressedVocabulary, ComparatorType>;
  using InternalUncompressedVocabulary =
      UnicodeVocabulary<VocabularyInMemory, ComparatorType>;
  using InternalVocabulary =
      std::conditional_t<_isCompressed, InternalCompressedVocabulary,
                         InternalUncompressedVocabulary>;
  InternalVocabulary _internalVocabulary;

  using ExternalVocabulary =
      UnicodeVocabulary<VocabularyOnDisk, ComparatorType>;
  ExternalVocabulary _externalVocabulary;

 public:
  const ExternalVocabulary& getExternalVocab() const {
    return _externalVocabulary;
  }
  auto makeUncompressingWordWriter(const std::string& filename) {
    return VocabularyInMemory::WordWriter{filename};
  }

  template <typename U = StringType, typename = enable_if_compressed<U>>
  auto makeCompressedWordWriter(const std::string& filename) {
    return _internalVocabulary.getUnderlyingVocabulary().makeDiskWriter(
        filename);
  }

  static auto makeUncompressedDiskIterator(const string& filename) {
    return VocabularyInMemory::makeWordDiskIterator(filename);
  }
};

using RdfsVocabulary =
    Vocabulary<CompressedString, TripleComponentComparator, VocabIndex>;
using TextVocabulary =
    Vocabulary<std::string, SimpleStringComparator, WordVocabIndex>;

// _______________________________________________________________
template <typename S, typename C, typename I>
template <typename>
std::optional<string> Vocabulary<S, C, I>::indexToOptionalString(
    IndexType idx) const {
  if (idx.get() < _internalVocabulary.size()) {
    return std::optional<string>(_internalVocabulary[idx.get()]);
  } else {
    // this word must be externalized
    idx.get() -= _internalVocabulary.size();
    AD_CONTRACT_CHECK(idx.get() < _externalVocabulary.size());
    return _externalVocabulary[idx.get()];
  }
}
