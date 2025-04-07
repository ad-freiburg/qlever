// Copyright 2011 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
//
// Authors: Björn Buchhold <buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_H

#include <cassert>
#include <fstream>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "backports/algorithm.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/Pattern.h"
#include "index/CompressedString.h"
#include "index/StringSortComparator.h"
#include "index/VocabularyOnDisk.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/UnicodeVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/Log.h"
#include "util/StringUtils.h"

using std::string;
using std::vector;

template <class StringType>
using AccessReturnType_t =
    std::conditional_t<std::is_same_v<StringType, CompressedString>,
                       std::string, std::string_view>;

template <typename IndexT = WordVocabIndex>
class IdRange {
 public:
  IdRange() = default;
  IdRange(IndexT first, IndexT last) : first_(first), last_(last) {
    AD_CORRECTNESS_CHECK(first <= last);
  }
  IndexT first() const { return first_; }
  IndexT last() const { return last_; }

 private:
  IndexT first_{};
  IndexT last_{};
};

// Stream operator for convenience.
template <typename IndexType>
inline std::ostream& operator<<(std::ostream& stream,
                                const IdRange<IndexType>& idRange) {
  return stream << '[' << idRange.first_ << ", " << idRange.last_ << ']';
}

// A vocabulary. Wraps a vector of strings and provides additional methods for
// retrieval. Template parameters that are supported are:
// std::string -> no compression is applied
// CompressedString -> prefix compression is applied
template <typename StringType, typename ComparatorType, typename IndexT>
class Vocabulary {
 public:
  // The index ranges for a prefix + a function to check whether a given index
  // is contained in one of them.
  //
  // NOTE: There are currently two ranges, one for the internal and one for the
  // external vocabulary. It would be easy to add more ranges.
  struct PrefixRanges {
   public:
    using Ranges = std::array<std::pair<IndexT, IndexT>, 1>;

   private:
    Ranges ranges_{};

   public:
    PrefixRanges() = default;
    explicit PrefixRanges(const Ranges& ranges);
    const Ranges& ranges() const { return ranges_; }
    bool operator==(const PrefixRanges& ranges) const = default;
    bool contain(IndexT index) const;
  };

 private:
  // The different type of data that is stored in the vocabulary
  enum class Datatypes { Literal, Iri, Float, Date };

  template <typename T, typename R = void>
  using enable_if_compressed =
      std::enable_if_t<std::is_same_v<T, CompressedString>>;

  template <typename T, typename R = void>
  using enable_if_uncompressed =
      std::enable_if_t<!std::is_same_v<T, CompressedString>>;

  static constexpr bool isCompressed_ =
      std::is_same_v<StringType, CompressedString>;

  // If a literal uses one of these language tags or starts with one of these
  // prefixes, it will be externalized. By default, everything is externalized.
  // Both of these settings can be overridden using the `settings.json` file.
  //
  // NOTE: Qlever-internal prefixes are currently always internalized, no matter
  // how `internalizedLangs_` and `externalizedPrefixes_` are set.
  vector<std::string> internalizedLangs_;
  vector<std::string> externalizedPrefixes_{""};

  // The 5th highest bit of the vocabulary index is used as a marker to
  // determine whether the word is stored in the normal vocabulary or the
  // geometry vocabulary.
  static constexpr uint64_t geoVocabMarker = 1ull << (ValueId::numDataBits - 1);
  static constexpr uint64_t geoVocabMarkerInvert = ~geoVocabMarker;
  static constexpr uint64_t maxWordIndex = geoVocabMarker - 1;

  using UnderlyingVocabulary =
      std::conditional_t<isCompressed_,
                         CompressedVocabulary<VocabularyInternalExternal>,
                         VocabularyInMemory>;
  using VocabularyWithUnicodeComparator =
      UnicodeVocabulary<UnderlyingVocabulary, ComparatorType>;

  // The vocabulary is split into an underlying vocabulary for normal literals
  // and one for geometry well-known text literals specifically.
  VocabularyWithUnicodeComparator vocabulary_;
  VocabularyWithUnicodeComparator geoVocabulary_;

  // ID ranges for IRIs and literals. Used for the efficient computation of the
  // `isIRI` and `isLiteral` functions.
  PrefixRanges prefixRangesIris_;
  PrefixRanges prefixRangesLiterals_;

 public:
  using SortLevel = typename ComparatorType::Level;
  using IndexType = IndexT;

  template <
      typename = std::enable_if_t<std::is_same_v<StringType, string> ||
                                  std::is_same_v<StringType, CompressedString>>>
  Vocabulary() {}
  Vocabulary& operator=(Vocabulary&&) noexcept = default;
  Vocabulary(Vocabulary&&) noexcept = default;

  virtual ~Vocabulary() = default;

  //! Read the vocabulary from files containing words and geometries
  //! respectively.
  void readFromFile(const string& fileName,
                    const std::optional<string>& geoFileName);

  // Get the word with the given `idx`. Throw if the `idx` is not contained
  // in the vocabulary.
  AccessReturnType_t<StringType> operator[](IndexType idx) const;

  // AccessReturnType_t<StringType> at(IndexType idx) const { return
  // operator[](id); }

  //! Get the number of words in the vocabulary.
  [[nodiscard]] size_t size() const {
    return vocabulary_.size() + geoVocabulary_.size();
  }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(std::string_view word, IndexType* idx) const;

  // Get the index range for the given prefix or `std::nullopt` if no word with
  // the given prefix exists in the vocabulary.
  //
  // TODO<discovered by joka921>: This is only used for the text index, and
  // uses a range, where the last index is still within the range which is
  // against C++ conventions! Consider using the `prefix_range` function.
  //
  // NOTE: Unlike `prefixRanges`, this only looks in the internal vocabulary,
  // which is OK because for the text index, the external vocabulary is always
  // empty.
  std::optional<IdRange<IndexType>> getIdRangeForFullTextPrefix(
      const string& word) const;

  // only used during Index building, not needed for compressed vocabulary
  void createFromSet(const ad_utility::HashSet<std::string>& set,
                     const std::string& filename,
                     const std::optional<std::string>& geoFileName);

  static bool stringIsLiteral(std::string_view s);

  static constexpr std::string_view geoLiteralSuffix =
      ad_utility::constexprStrCat<"\"^^<", GEO_WKT_LITERAL, ">">();
  static bool stringIsGeoLiteral(std::string_view s);

  static uint64_t makeGeoVocabIndex(uint64_t vocabIndex);

  bool isIri(IndexT index) const { return prefixRangesIris_.contain(index); }
  bool isLiteral(IndexT index) const {
    return prefixRangesLiterals_.contain(index);
  }
  bool isGeoLiteral(IndexT index) const {
    return static_cast<bool>(index.get() & geoVocabMarker);
  }

  bool shouldBeExternalized(std::string_view word) const;

  bool shouldEntityBeExternalized(std::string_view word) const;

  bool shouldLiteralBeExternalized(std::string_view word) const;

  static string_view getLanguage(std::string_view literal);

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
    return vocabulary_.getComparator();
  }

  // Get prefix ranges for the given prefix.
  PrefixRanges prefixRanges(std::string_view prefix) const;

  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return getCaseComparator().getLocaleManager();
  }

  // Wraps std::lower_bound and returns an index instead of an iterator
  IndexType lower_bound(std::string_view word,
                        const SortLevel level = SortLevel::QUARTERNARY) const;

  // _______________________________________________________________
  IndexType upper_bound(const string& word,
                        const SortLevel level = SortLevel::QUARTERNARY) const;

  static constexpr std::string_view geoVocabSuffix = ".geometries";

  // This word writer writes words to different vocabularies depending on their
  // content.
  class WordWriter {
   private:
    using WW = UnderlyingVocabulary::WordWriter;
    WW underlyingWordWriter_;
    WW underlyingGeoWordWriter_;

   public:
    WordWriter(const VocabularyWithUnicodeComparator& vocabulary,
               const std::string& filename);

    // Add the next word to the vocabulary and return its index.
    uint64_t operator()(std::string_view word, bool isExternal);

    // Finish the writing on both underlying word writers. After this no more
    // calls to `operator()` are allowed.
    void finish();

    std::string& readableName();
  };

  // Get a writer for each underlying vocab that has an `operator()` method to
  // which the single words + the information whether they shall be cached in
  // the internal vocabulary have to be pushed one by one to add words to the
  // vocabulary. This writer internally splits the words into a generic
  // vocabulary and a geometry vocabulary.
  WordWriter makeWordWriter(const std::string& filename) const {
    return {vocabulary_, filename};
  }
};

using RdfsVocabulary =
    Vocabulary<CompressedString, TripleComponentComparator, VocabIndex>;
using TextVocabulary =
    Vocabulary<std::string, SimpleStringComparator, WordVocabIndex>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_H
