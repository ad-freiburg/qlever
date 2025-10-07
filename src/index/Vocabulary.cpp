// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchhold@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/Vocabulary.h"

#include <iostream>

#include "backports/StartsWithAndEndsWith.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

using std::string;

// ____________________________________________________________________________
template <typename StringType, typename ComparatorType, typename IndexT>
Vocabulary<StringType, ComparatorType, IndexT>::PrefixRanges::PrefixRanges(
    const Ranges& ranges)
    : ranges_(ranges) {
  for (const auto& range : ranges_) {
    AD_CONTRACT_CHECK(range.first <= range.second);
  }
}

// ____________________________________________________________________________
template <typename StringType, typename ComparatorType, typename IndexT>
bool Vocabulary<StringType, ComparatorType, IndexT>::PrefixRanges::contain(
    IndexT index) const {
  return ql::ranges::any_of(
      ranges_, [index](const std::pair<IndexT, IndexT>& range) {
        return range.first <= index && index < range.second;
      });
}

// _____________________________________________________________________________
template <class S, class C, typename I>
void Vocabulary<S, C, I>::readFromFile(const string& fileName) {
  vocabulary_.close();
  vocabulary_.open(fileName);

  // Precomputing ranges for IRIs, blank nodes, and literals, for faster
  // processing of the `isIrI` and `isLiteral` functions.
  //
  // NOTE: We only need this for the vocabulary of the main index, where
  // `I` is `VocabIndex`. However, since this is a negligible one-time cost,
  // it does not harm to do it for all vocabularies.
  prefixRangesIris_ = prefixRanges("<");
  prefixRangesLiterals_ = prefixRanges("\"");
}

// _____________________________________________________________________________
template <class S, class C, class I>
void Vocabulary<S, C, I>::createFromSet(
    const ad_utility::HashSet<std::string>& set, const std::string& filename) {
  AD_LOG_DEBUG << "BEGIN Vocabulary::createFromSet" << std::endl;
  vocabulary_.close();
  std::vector<std::string> words(set.begin(), set.end());
  auto totalComparison = [this](const auto& a, const auto& b) {
    return getCaseComparator()(a, b, SortLevel::TOTAL);
  };
  std::sort(begin(words), end(words), totalComparison);
  auto writerPtr = makeWordWriterPtr(filename);
  auto writeWords = [&writer = *writerPtr](std::string_view word) {
    // All words are stored in the internal vocab (this is consistent with the
    // previous behavior). NOTE: This function is currently only used for the
    // text index and for few unit tests, where we don't have an external
    // vocabulary anyway.
    writer(word, false);
  };
  ql::ranges::for_each(words, writeWords);
  writerPtr->finish();
  vocabulary_.open(filename);
  AD_LOG_DEBUG << "END Vocabulary::createFromSet" << std::endl;
}

// _____________________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::stringIsLiteral(std::string_view s) {
  return ql::starts_with(s, '"');
}

// _____________________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::shouldBeExternalized(std::string_view s) const {
  // TODO<joka921> We should have a completely separate layer that handles the
  // externalization, not the Vocab.
  if (!stringIsLiteral(s)) {
    return shouldEntityBeExternalized(s);
  } else {
    return shouldLiteralBeExternalized(s);
  }
}

// ___________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::shouldEntityBeExternalized(
    std::string_view word) const {
  // Never externalize the internal IRIs as they are sometimes added before or
  // after the externalization happens, and we thus get inconsistent behavior
  // etc. for `ql:langtag`.
  if (ql::starts_with(word,
                      QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET)) {
    return false;
  }
  // Never externalize the special IRIs starting with `@` (for example,
  // @en@rdfs:label). Otherwise, they will not be found with a setting like
  // `"prefixes-external": ["@"]` or `"prefixes-external": [""]` in the
  // `.settings.json` file.
  //
  // TODO: This points to a bug or inconsistency elsewhere in the code.
  if (ql::starts_with(word, "@")) {
    return false;
  }
  // Otherwise, externalize if and only if there is a prefix match for one of
  // `externalizedPrefixes_`.
  return ql::ranges::any_of(externalizedPrefixes_, [&word](const auto& p) {
    return ql::starts_with(word, p);
  });
}

// ___________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::shouldLiteralBeExternalized(
    std::string_view word) const {
  for (const auto& p : externalizedPrefixes_) {
    if (ql::starts_with(word, p)) {
      return true;
    }
  }

  if (word.size() > MAX_INTERNAL_LITERAL_BYTES) {
    return true;
  }

  const std::string_view lang = getLanguage(word);
  if (lang == "") {
    return false;
  }

  for (const auto& p : internalizedLangs_) {
    if (lang == p) {
      return false;
    }
  }
  return true;
}
// _____________________________________________________________________________
template <class S, class C, class I>
std::string_view Vocabulary<S, C, I>::getLanguage(std::string_view literal) {
  auto lioAt = literal.rfind('@');
  if (lioAt != string::npos) {
    auto lioQ = literal.rfind('\"');
    if (lioQ != string::npos && lioQ < lioAt) {
      return literal.substr(lioAt + 1);
    }
  }
  return "";
}

// ______________________________________________________________________________
template <class S, class C, class I>
template <class StringRange>
void Vocabulary<S, C, I>::initializeExternalizePrefixes(const StringRange& s) {
  externalizedPrefixes_.clear();
  for (const auto& el : s) {
    externalizedPrefixes_.emplace_back(el);
  }
}

// ______________________________________________________________________________
template <class S, class C, class I>
template <class StringRange>
void Vocabulary<S, C, I>::initializeInternalizedLangs(const StringRange& s) {
  internalizedLangs_.clear();
  // `StringRange` might be nlohmann::json, for which we have disabled
  // implicit conversions, so `vector::insert` cannot be used.
  for (const auto& el : s) {
    internalizedLangs_.emplace_back(el);
  }
}

// ___________________________________________________________________________
template <typename S, typename C, typename I>
std::optional<IdRange<I>> Vocabulary<S, C, I>::getIdRangeForFullTextPrefix(
    const string& word) const {
  AD_CONTRACT_CHECK(ql::ends_with(word, PREFIX_CHAR));
  IdRange<I> range;
  if (word.size() == 1) {
    range = IdRange{I::make(0), I::make(size()).decremented()};
    return range;
  }
  auto [begin, end] = vocabulary_.prefix_range(word.substr(0, word.size() - 1));
  bool notEmpty =
      begin.has_value() && (!end.has_value() || end.value() > begin.value());

  if (notEmpty) {
    range = IdRange{I::make(begin.value()),
                    I::make(end.value_or(size())).decremented()};
    AD_CONTRACT_CHECK(range.first().get() < vocabulary_.size());
    AD_CONTRACT_CHECK(range.last().get() < vocabulary_.size());
    return range;
  }
  return std::nullopt;
}

// _______________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::upper_bound(const string& word,
                                      const SortLevel level) const
    -> IndexType {
  auto wordAndIndex = vocabulary_.upper_bound(word, level);
  return IndexType::make(wordAndIndex.indexOrDefault(size()));
}

// _____________________________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::lower_bound(std::string_view word,
                                      const SortLevel level) const
    -> IndexType {
  auto wordAndIndex = vocabulary_.lower_bound(word, level);
  return IndexType::make(wordAndIndex.indexOrDefault(size()));
}

// _____________________________________________________________________________
template <typename S, typename C, typename I>
std::optional<ad_utility::GeometryInfo> Vocabulary<S, C, I>::getGeoInfo(
    IndexType idx) const {
  // For more information on the concepts used here, please see
  // their definitions in `VocabularyConstraints.h`.
  if constexpr (MaybeProvidesGeometryInfo<S>) {
    return vocabulary_.getUnderlyingVocabulary().getGeoInfo(idx.get());
  } else {
    static_assert(NeverProvidesGeometryInfo<S>);
    return std::nullopt;
  }
};

// _____________________________________________________________________________
template <typename S, typename C, typename I>
bool Vocabulary<S, C, I>::isGeoInfoAvailable() const {
  // For more information on the concepts used here, please see
  // their definitions in `VocabularyConstraints.h`.
  if constexpr (MaybeProvidesGeometryInfo<S>) {
    return vocabulary_.getUnderlyingVocabulary().isGeoInfoAvailable();
  } else {
    static_assert(NeverProvidesGeometryInfo<S>);
    return false;
  }
};

// _____________________________________________________________________________
template <typename S, typename ComparatorType, typename I>
void Vocabulary<S, ComparatorType, I>::setLocale(const std::string& language,
                                                 const std::string& country,
                                                 bool ignorePunctuation) {
  vocabulary_.getComparator() =
      ComparatorType(language, country, ignorePunctuation);
  vocabulary_.getComparator() =
      ComparatorType(language, country, ignorePunctuation);
}

// _____________________________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::getPositionOfWord(std::string_view word) const
    -> std::pair<IndexType, IndexType> {
  auto [lower, upper] = vocabulary_.getPositionOfWord(word);
  return {IndexType::make(lower), IndexType::make(upper)};
}

// _____________________________________________________________________________
template <typename S, typename C, typename I>
bool Vocabulary<S, C, I>::getId(std::string_view word, IndexType* idx) const {
  auto [lower, upper] = vocabulary_.getPositionOfWord(word);
  idx->get() = lower;
  return lower != upper;
}

// ___________________________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::prefixRanges(std::string_view prefix) const
    -> Vocabulary<S, C, I>::PrefixRanges {
  auto rangeInternal = vocabulary_.prefix_range(prefix);
  std::pair<I, I> indexRangeInternal{
      I::make(rangeInternal.first.value_or(size())),
      I::make(rangeInternal.second.value_or(size()))};
  return PrefixRanges{{indexRangeInternal}};
}

// _____________________________________________________________________________
template <typename UnderlyingVocabulary, typename C, typename I>
auto Vocabulary<UnderlyingVocabulary, C, I>::operator[](IndexType idx) const
    -> AccessReturnType {
  return vocabulary_[idx.get()];
}

// Explicit template instantiations
template class Vocabulary<detail::UnderlyingVocabRdfsVocabulary,
                          TripleComponentComparator, VocabIndex>;
template class Vocabulary<detail::UnderlyingVocabTextVocabulary,
                          SimpleStringComparator, WordVocabIndex>;

template void RdfsVocabulary::initializeInternalizedLangs<nlohmann::json>(
    const nlohmann::json&);
template void RdfsVocabulary::initializeExternalizePrefixes<nlohmann::json>(
    const nlohmann::json& prefixes);
template void RdfsVocabulary::initializeExternalizePrefixes<
    std::vector<std::string>>(const std::vector<std::string>& prefixes);
