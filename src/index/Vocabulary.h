// Copyright 2011 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
//
// Authors: Björn Buchhold <buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

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
#include "index/StringSortComparator.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/UnicodeVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/Log.h"
#include "util/StringUtils.h"

using std::string;
using std::vector;

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
// retrieval.
template <typename UnderlyingVocabulary, typename ComparatorType,
          typename IndexT>
class Vocabulary {
 public:
  // The type that is returned by the `operator[]` of this vocabulary. Typically
  // either `std::string` or `std::string_view`.
  using AccessReturnType =
      decltype(std::declval<const UnderlyingVocabulary&>()[0]);

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

  // If a literal uses one of these language tags or starts with one of these
  // prefixes, it will be externalized. By default, everything is externalized.
  // Both of these settings can be overridden using the `settings.json` file.
  //
  // NOTE: Qlever-internal prefixes are currently always internalized, no matter
  // how `internalizedLangs_` and `externalizedPrefixes_` are set.
  vector<std::string> internalizedLangs_;
  vector<std::string> externalizedPrefixes_{""};

  using VocabularyWithUnicodeComparator =
      UnicodeVocabulary<UnderlyingVocabulary, ComparatorType>;

  VocabularyWithUnicodeComparator vocabulary_;

  // ID ranges for IRIs and literals. Used for the efficient computation of the
  // `isIRI` and `isLiteral` functions.
  PrefixRanges prefixRangesIris_;
  PrefixRanges prefixRangesLiterals_;

 public:
  using SortLevel = typename ComparatorType::Level;
  using IndexType = IndexT;

  Vocabulary() = default;
  Vocabulary& operator=(Vocabulary&&) noexcept = default;
  Vocabulary(Vocabulary&&) noexcept = default;

  virtual ~Vocabulary() = default;

  //! Read the vocabulary from file.
  void readFromFile(const string& fileName);

  // Get the word with the given `idx`. Throw if the `idx` is not contained
  // in the vocabulary.
  AccessReturnType operator[](IndexType idx) const;

  //! Get the number of words in the vocabulary.
  [[nodiscard]] size_t size() const { return vocabulary_.size(); }

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
                     const std::string& filename);

  static bool stringIsLiteral(std::string_view s);

  bool isIri(IndexT index) const { return prefixRangesIris_.contain(index); }
  bool isLiteral(IndexT index) const {
    return prefixRangesLiterals_.contain(index);
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

  // Get a writer for the vocab that has an `operator()` method to
  // which the single words + the information whether they shall be cached in
  // the internal vocabulary  have to be pushed one by one to add words to the
  // vocabulary.
  UnderlyingVocabulary::WordWriter makeWordWriter(
      const std::string& filename) const {
    // Note: In GCC this triggers a move construction of the created
    // `DiskWriter`, although mandatory copy elision should kick in here
    // according to our understanding (and does in clang). We could investigate
    // whether this is a bug in GCC or whether we are missing something.
    return vocabulary_.getUnderlyingVocabulary().makeDiskWriter(filename);
  }

  // If the `UnderlyingVocabulary` is a `PolymorphicVocabulary`, close the
  // vocabulary and set the type of the vocabulary according to the `type`
  // argument (see the `PolymorphicVocabulary` class for details).
  void resetToType(ad_utility::VocabularyType type) {
    if constexpr (std::is_same_v<UnderlyingVocabulary, PolymorphicVocabulary>) {
      vocabulary_.getUnderlyingVocabulary().resetToType(type);
    }
  }
};

namespace detail {
// Thecompile-time definitions `_QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY` can be
// used to disable the external vocab and the compression of the vocab at
// compile time. NOTE: These change the binary format of QLever's index, so
// changing them requires rebuilding of the indices.

#ifdef _QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY
using UnderlyingVocabRdfsVocabulary = VocabularyInMemory;
#else
using UnderlyingVocabRdfsVocabulary = PolymorphicVocabulary;
#endif

using UnderlyingVocabTextVocabulary = VocabularyInMemory;
}  // namespace detail

using RdfsVocabulary = Vocabulary<detail::UnderlyingVocabRdfsVocabulary,
                                  TripleComponentComparator, VocabIndex>;
using TextVocabulary = Vocabulary<detail::UnderlyingVocabTextVocabulary,
                                  SimpleStringComparator, WordVocabIndex>;
