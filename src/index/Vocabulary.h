// Copyright 2011 - 2025
// University of Freiburg
// Chair of Algorithms and Data Structures
//
// Authors: Björn Buchhold <buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_H

#include <cassert>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "backports/three_way_comparison.h"
#include "index/StringSortComparator.h"
#include "index/vocabulary/UnicodeVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/Exception.h"
#include "util/HashSet.h"

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
// retrieval. It is templated on the type of the underlying vocabulary
// implementation, which can be any of the implementations in the
// `index/vocabulary` directory
template <typename UnderlyingVocabulary, typename ComparatorType,
          typename IndexT>
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
    bool contain(IndexT index) const;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(PrefixRanges, ranges_)
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
  std::vector<std::string> internalizedLangs_;
  std::vector<std::string> externalizedPrefixes_{""};

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
  // The type that is returned by the `operator[]` of this vocabulary. Typically
  // either `std::string` or `std::string_view`.
  using AccessReturnType =
      decltype(std::declval<const UnderlyingVocabulary&>()[0]);

  Vocabulary() = default;
  Vocabulary& operator=(Vocabulary&&) noexcept = default;
  Vocabulary(Vocabulary&&) noexcept = default;

  virtual ~Vocabulary() = default;

  //! Read the vocabulary from file.
  void readFromFile(const std::string& filename);

  // Get the word with the given `idx`. Throw if the `idx` is not contained
  // in the vocabulary.
  AccessReturnType operator[](IndexType idx) const;

  //! Get the number of words in the vocabulary.
  [[nodiscard]] size_t size() const { return vocabulary_.size(); }

  // Efficient iteration over all words of the vocabulary, in order, yielded as
  // single `string_view`s, see `VocabularyOnDisk::scanAll`. Each yielded
  // `string_view` is only valid until the iterator is advanced. The vocabulary
  // must stay alive while the range is used.
  VocabularyScanRange scanAll() const { return vocabulary_.scanAll(); }

  // Get an Id from the vocabulary for some full word (not prefix of a word).
  // Return a boolean value that signals if the word was found. If the word was
  // not found, the lower bound for the word is stored in idx, otherwise the
  // index of the word.
  bool getId(std::string_view word, IndexType* idx) const;

  // Retrieves a precomputed `GeometryInfo` object from the (possibly)
  // underlying `GeoVocabulary`. This function returns a `GeometryInfo` object
  // if and only if a `GeoVocabulary` is used and the given index points to a
  // valid geometry in this `GeoVocabulary`. In all other cases, `std::nullopt`
  // is returned.
  std::optional<ad_utility::GeometryInfo> getGeoInfo(IndexType idx) const;

  // This function determines if precomputed `GeometryInfo` is available for
  // this vocabulary. More specifically, `isGeoInfoAvailable` returns `true` if
  // there is an underlying `GeoVocabulary` such that `getGeoInfo` will return a
  // `GeometryInfo` object for all indices pointing to valid geometries in the
  // `GeoVocabulary`. If this function returns `false`, `getGeoInfo` will return
  // `std::nullopt` for any input, because no precomputed `GeometryInfo` is
  // available.
  bool isGeoInfoAvailable() const;

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
      const std::string& word) const;

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

  static std::string_view getLanguage(std::string_view literal);

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
  IndexType upper_bound(const std::string& word,
                        SortLevel level = SortLevel::QUARTERNARY) const;

  // The position where a word is stored or would be stored if it does not
  // exist. Unlike `lower_bound` and `upper_bound`, this function works with
  // full words, not prefixes. Currently used for `LocalVocabEntry`.
  std::pair<IndexType, IndexType> getPositionOfWord(
      std::string_view word) const;

  // Get a writer for the vocab that has an `operator()` method to
  // which the single words + the information whether they shall be cached in
  // the internal vocabulary  have to be pushed one by one to add words to the
  // vocabulary.
  auto makeWordWriterPtr(const std::string& filename) const {
    return vocabulary_.getUnderlyingVocabulary().makeDiskWriterPtr(filename);
  }

  // If the `UnderlyingVocabulary` is a `PolymorphicVocabulary`, close the
  // vocabulary and set the type of the vocabulary according to the `type`
  // argument (see the `PolymorphicVocabulary` class for details).
  void resetToType(ad_utility::VocabularyType type) {
    if constexpr (std::is_same_v<UnderlyingVocabulary, PolymorphicVocabulary>) {
      vocabulary_.getUnderlyingVocabulary().resetToType(type);
    }
  }

  // Replace the currently held vocabulary with a `VocabularyInMemory` built
  // as a non-owning, zero-copy view directly into `serializer`'s buffer (see
  // `VocabularyInMemory::fromZeroCopyDeserializer`). This switches the active
  // vocabulary implementation to `VocabularyInMemory`, regardless of what was
  // previously loaded. Only possible if `UnderlyingVocabulary` is
  // `PolymorphicVocabulary` or `VocabularyInMemory` itself; for any other
  // vocabulary implementation, this fails to compile (via a `static_assert`).
  // The returned vocabulary is only valid as long as the memory backing
  // `serializer`'s buffer is valid and unchanged.
  CPP_template(typename Serializer)(
      requires ad_utility::serialization::ZeroCopyReadSerializer<
          Serializer>) void loadFromZeroCopyDeserializer(Serializer&
                                                             serializer) {
    if constexpr (std::is_same_v<UnderlyingVocabulary, PolymorphicVocabulary>) {
      // `vocabulary_.getUnderlyingVocabulary()` returns the
      // `PolymorphicVocabulary`, whose own `getUnderlyingVocabulary()` in
      // turn returns a reference to the `std::variant` of the concrete
      // vocabulary implementations that it can hold (see
      // `PolymorphicVocabulary::getUnderlyingVocabulary`). Assigning a
      // `VocabularyInMemory` to that variant switches the active
      // implementation, which requires that `VocabularyInMemory` is one of
      // the variant's alternatives (currently always true, see
      // `PolymorphicVocabulary`'s `Variant` type alias).
      vocabulary_.getUnderlyingVocabulary().getUnderlyingVocabulary() =
          VocabularyInMemory::fromZeroCopyDeserializer(serializer);
    } else {
      static_assert(std::is_same_v<UnderlyingVocabulary, VocabularyInMemory>,
                    "`loadFromZeroCopyDeserializer` requires an in-memory or "
                    "polymorphic vocabulary implementation");
      vocabulary_.getUnderlyingVocabulary() =
          VocabularyInMemory::fromZeroCopyDeserializer(serializer);
    }
  }

  // Serialize the currently held vocabulary to `serializer`, in a format that
  // can later be read back via `loadFromZeroCopyDeserializer`. Only possible
  // if the vocabulary implementation currently in use is `VocabularyInMemory`
  // (which is always the case if `UnderlyingVocabulary` is `VocabularyInMemory`
  // itself, and is the case for a `PolymorphicVocabulary` only after
  // `loadFromZeroCopyDeserializer` or `resetToType` with
  // `VocabularyType::Enum::InMemoryUncompressed` was called); throws
  // otherwise.
  CPP_template(typename Serializer)(
      requires ad_utility::serialization::WriteSerializer<
          Serializer>) void writeAsZeroCopyBlob(Serializer& serializer) const {
    if constexpr (std::is_same_v<UnderlyingVocabulary, PolymorphicVocabulary>) {
      const auto& variant =
          vocabulary_.getUnderlyingVocabulary().getUnderlyingVocabulary();
      AD_CORRECTNESS_CHECK(
          std::holds_alternative<VocabularyInMemory>(variant),
          "Writing a vocabulary to a combined blob currently requires the "
          "in-memory, uncompressed vocabulary implementation");
      serializer << std::get<VocabularyInMemory>(variant);
    } else {
      static_assert(std::is_same_v<UnderlyingVocabulary, VocabularyInMemory>,
                    "`writeAsZeroCopyBlob` requires an in-memory or "
                    "polymorphic vocabulary implementation");
      serializer << vocabulary_.getUnderlyingVocabulary();
    }
  }
};

namespace detail {
// The compile-time definitions `QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY` can be
// used to disable the external vocab and the compression of the vocab at
// compile time. NOTE: These change the binary format of QLever's index, so
// changing them requires rebuilding of the indices.

#ifdef QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY
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

#endif  // QLEVER_SRC_INDEX_VOCABULARY_H
