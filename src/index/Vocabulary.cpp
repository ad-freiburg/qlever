// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "./Vocabulary.h"

#include <fstream>
#include <iostream>

#include "../parser/RdfEscaping.h"
#include "../parser/Tokenizer.h"
#include "../util/BatchedPipeline.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "../util/Serializer/FileSerializer.h"
#include "../util/json.h"
#include "./ConstantsIndexBuilding.h"

using std::string;

// _____________________________________________________________________________
template <class S, class C, typename I>
void Vocabulary<S, C, I>::readFromFile(const string& fileName,
                                       const string& extLitsFileName) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << " ..."
            << std::endl;
  internalVocabulary_.close();
  ad_utility::serialization::FileReadSerializer file(fileName);
  internalVocabulary_.open(fileName);
  LOG(INFO) << "Done, number of words: " << internalVocabulary_.size()
            << std::endl;
  if (!extLitsFileName.empty()) {
    if (!isCompressed_) {
      LOG(INFO) << "ERROR: trying to load externalized literals to an "
                   "uncompressed vocabulary. This is not valid and a "
                   "programming error. Terminating"
                << std::endl;
      AD_FAIL();
    }

    LOG(DEBUG) << "Registering external vocabulary" << std::endl;
    externalVocabulary_.open(extLitsFileName);
    LOG(INFO) << "Number of words in external vocabulary: "
              << externalVocabulary_.size() << std::endl;
  }
}

// _____________________________________________________________________________
template <class S, class C, class I>
template <typename, typename>
void Vocabulary<S, C, I>::writeToFile(const string& fileName) const {
  LOG(TRACE) << "BEGIN Vocabulary::writeToFile" << std::endl;
  ad_utility::serialization::FileWriteSerializer file{fileName};
  internalVocabulary_.getUnderlyingVocabulary().writeToFile(fileName);
  LOG(TRACE) << "END Vocabulary::writeToFile" << std::endl;
}

// _____________________________________________________________________________
template <class S, class C, class I>
void Vocabulary<S, C, I>::createFromSet(
    const ad_utility::HashSet<std::string>& set) {
  LOG(DEBUG) << "BEGIN Vocabulary::createFromSet" << std::endl;
  internalVocabulary_.close();
  std::vector<std::string> words(set.begin(), set.end());
  auto totalComparison = [this](const auto& a, const auto& b) {
    return getCaseComparator()(a, b, SortLevel::TOTAL);
  };
  std::sort(begin(words), end(words), totalComparison);
  internalVocabulary_.build(words);
  LOG(DEBUG) << "END Vocabulary::createFromSet" << std::endl;
}

// _____________________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::isLiteral(const string& word) {
  return word.size() > 0 && word[0] == '\"';
}

// _____________________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::shouldBeExternalized(const string& word) const {
  // TODO<joka921> Completely refactor the Vocabulary on the different
  // Types, it is a mess.

  // If the string is not compressed, this means that this is a text vocabulary
  // and thus doesn't support externalization.
  if constexpr (std::is_same_v<S, CompressedString>) {
    if (!isLiteral(word)) {
      return shouldEntityBeExternalized(word);
    } else {
      return shouldLiteralBeExternalized(word);
    }
  } else {
    return false;
  }
}

// ___________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::shouldEntityBeExternalized(const string& word) const {
  // Never externalize the internal URIs as they are sometimes added before or
  // after the externalization happens and we thus get inconsistent behavior
  // etc. for `ql:langtag`.
  if (word.starts_with(INTERNAL_ENTITIES_URI_PREFIX)) {
    return false;
  }
  return std::ranges::any_of(externalizedPrefixes_, [&word](const auto& p) {
    return word.starts_with(p);
  });
}

// ___________________________________________________________________
template <class S, class C, class I>
bool Vocabulary<S, C, I>::shouldLiteralBeExternalized(
    const string& word) const {
  for (const auto& p : externalizedPrefixes_) {
    if (word.starts_with(p)) {
      return true;
    }
  }

  if (word.size() > MAX_INTERNAL_LITERAL_BYTES) {
    return true;
  }

  const string lang = getLanguage(word);
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
string Vocabulary<S, C, I>::getLanguage(const string& literal) {
  auto lioAt = literal.rfind('@');
  if (lioAt != string::npos) {
    auto lioQ = literal.rfind('\"');
    if (lioQ != string::npos && lioQ < lioAt) {
      return literal.substr(lioAt + 1);
    }
  }
  return "";
}

// _____________________________________________________________________________
template <class S, class C, class I>
template <class StringRange, typename, typename>
void Vocabulary<S, C, I>::buildCodebookForPrefixCompression(
    const StringRange& prefixes) {
  internalVocabulary_.getUnderlyingVocabulary().getCompressor().buildCodebook(
      prefixes);
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
  AD_CONTRACT_CHECK(word[word.size() - 1] == PREFIX_CHAR);
  IdRange<I> range;
  auto prefixRange = prefix_range(word.substr(0, word.size() - 1));
  bool success = prefixRange.second > prefixRange.first;

  if (success) {
    range = IdRange{prefixRange.first, prefixRange.second.decremented()};
    AD_CONTRACT_CHECK(range.first().get() < internalVocabulary_.size());
    AD_CONTRACT_CHECK(range.last().get() < internalVocabulary_.size());

    return range;
  }
  return std::nullopt;
}

// _______________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::upper_bound(const string& word,
                                      const SortLevel level) const
    -> IndexType {
  return IndexType::make(internalVocabulary_.upper_bound(word, level)._index);
}

// _____________________________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::lower_bound(const string& word,
                                      const SortLevel level) const
    -> IndexType {
  return IndexType::make(internalVocabulary_.lower_bound(word, level)._index);
}

// _____________________________________________________________________________
template <typename S, typename ComparatorType, typename I>
void Vocabulary<S, ComparatorType, I>::setLocale(const std::string& language,
                                                 const std::string& country,
                                                 bool ignorePunctuation) {
  internalVocabulary_.getComparator() =
      ComparatorType(language, country, ignorePunctuation);
  externalVocabulary_.getComparator() =
      ComparatorType(language, country, ignorePunctuation);
}

template <typename StringType, typename C, typename I>
//! Get the word with the given idx.
//! lvalue for compressedString and const& for string-based vocabulary
AccessReturnType_t<StringType> Vocabulary<StringType, C, I>::at(
    IndexType idx) const {
  return internalVocabulary_[idx.get()];
}

// _____________________________________________________________________________
template <typename S, typename C, typename I>
bool Vocabulary<S, C, I>::getId(const string& word, IndexType* idx) const {
  if (!shouldBeExternalized(word)) {
    // need the TOTAL level because we want the unique word.
    *idx = lower_bound(word, SortLevel::TOTAL);
    // works for the case insensitive version because
    // of the strict ordering.
    return idx->get() < internalVocabulary_.size() && at(*idx) == word;
  }
  auto wordAndIndex = externalVocabulary_.lower_bound(word, SortLevel::TOTAL);
  idx->get() = wordAndIndex._index;
  idx->get() += internalVocabulary_.size();
  return wordAndIndex._word == word;
}

// ___________________________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::prefix_range(const string& prefix) const
    -> std::pair<IndexType, IndexType> {
  auto [begin, end] = internalVocabulary_.prefix_range(prefix);
  return {IndexType::make(begin), IndexType::make(end)};
}

// _____________________________________________________________________________
template <typename S, typename C, typename I>
template <typename, typename>
std::optional<std::string_view> Vocabulary<S, C, I>::operator[](
    IndexType idx) const {
  if (idx.get() < internalVocabulary_.size()) {
    return internalVocabulary_[idx.get()];
  } else {
    return std::nullopt;
  }
}
template std::optional<std::string_view>
TextVocabulary::operator[]<std::string, void>(IndexType idx) const;

// ___________________________________________________________________________
template <typename S, typename C, typename I>
auto Vocabulary<S, C, I>::getRangesForDatatypes() const
    -> ad_utility::HashMap<Datatypes, std::pair<IndexType, IndexType>> {
  ad_utility::HashMap<Datatypes, std::pair<IndexType, IndexType>> result;
  result[Datatypes::Literal] = prefix_range("\"");
  result[Datatypes::Iri] = prefix_range("<");

  return result;
};

template <typename S, typename C, typename I>
template <typename, typename>
void Vocabulary<S, C, I>::printRangesForDatatypes() {
  auto ranges = getRangesForDatatypes();
  auto logRange = [&](const auto& range) {
    LOG(INFO) << range.first << " " << range.second << std::endl;
    if (range.second > range.first) {
      LOG(INFO) << indexToOptionalString(range.first).value() << std::endl;
      LOG(INFO) << indexToOptionalString(range.second.decremented()).value()
                << std::endl;
    }
    if (range.second.get() < internalVocabulary_.size()) {
      LOG(INFO) << indexToOptionalString(range.second).value() << std::endl;
    }

    if (range.first.get() > 0) {
      LOG(INFO) << indexToOptionalString(range.first.decremented()).value()
                << std::endl;
    }
  };

  for (const auto& pair : ranges) {
    logRange(pair.second);
  }
}

template std::optional<string>
RdfsVocabulary::indexToOptionalString<CompressedString>(IndexType idx) const;

// Explicit template instantiations
template class Vocabulary<CompressedString, TripleComponentComparator,
                          VocabIndex>;
template class Vocabulary<std::string, SimpleStringComparator, WordVocabIndex>;

template void RdfsVocabulary::buildCodebookForPrefixCompression<
    std::vector<std::string>, CompressedString, void>(
    const std::vector<std::string>&);
template void RdfsVocabulary::initializeInternalizedLangs<nlohmann::json>(
    const nlohmann::json&);
template void RdfsVocabulary::initializeExternalizePrefixes<nlohmann::json>(
    const nlohmann::json& prefixes);
template void RdfsVocabulary::initializeExternalizePrefixes<
    std::vector<std::string>>(const std::vector<std::string>& prefixes);

template void RdfsVocabulary::printRangesForDatatypes();

template void TextVocabulary::writeToFile<std::string, void>(
    const string& fileName) const;
