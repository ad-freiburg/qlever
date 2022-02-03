// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

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
#include "./SimpleVocabulary.h"

using std::string;

// _____________________________________________________________________________
template <class S, class C>
void SimpleVocabulary<S, C>::readFromFile(const string& fileName
                                    ) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << " ..."
            << std::endl;
  _words.clear();
  ad_utility::serialization::FileReadSerializer file(fileName);
  file >> _words;
  LOG(INFO) << "Done, number of words: " << _words.size() << std::endl;
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
void Vocabulary<S, C>::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer file{fileName};
  file << _words;
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________________
template <typename S, typename C>
Vocabulary<S, C>::SearchResult Vocabulary<S, C>::lower_bound(
    Vocabulary<S, C>::StringView word, const SortLevel level) const {
  SearchResult result;
      // TODO<joka921> do we still need the lower_bound in the _words?
  // Todo<joka921> can be implemented in terms of the other lower_bound.
      result._id = _words.lower_bound(word, getComparatorForSortLevel(level));
  if (result._id < _words.size()) {
    resultInternal._word = _words[result._id];
  }
  return result;
}

// _____________________________________________________________________________
template <typename S, typename C>
template <typename StringType, typename Comparator>
Vocabulary<S, C>::SearchResult Vocabulary<S, C>::lower_bound(
    const StringType& word, Comparator comparator) const {
  SearchResult result;
  // TODO<joka921> do we still need the lower_bound in the _words?
  result._id = _words.lower_bound(word, comparator);
  if (result._id < _words.size()) {
    resultInternal._word = _words[result._id];
  }
  return result;
}

// _____________________________________________________________________________
template <typename S, typename C>
Vocabulary<S, C>::SearchResult Vocabulary<S, C>::upper_bound(
    Vocabulary<S, C>::StringView word, const SortLevel level) const {
  SearchResult result;
  // TODO<joka921> do we still need the lower_bound in the _words?
  // Todo<joka921> can be implemented in terms of the other lower_bound.
  result._id = _words.upper_bound(word, getComparatorForSortLevel(level));
  if (result._id < _words.size()) {
    resultInternal._word = _words[result._id];
  }
  return result;
}

// _____________________________________________________________________________
template <typename S, typename C>
template <typename StringType, typename Comparator>
Vocabulary<S, C>::SearchResult Vocabulary<S, C>::upper_bound(
    const StringType& word, Comparator comparator) const {
  SearchResult result;
  // TODO<joka921> do we still need the lower_bound in the _words?
  result._id = _words.upper_bound(word, comparator);
  if (result._id < _words.size()) {
    resultInternal._word = _words[result._id];
  }
  return result;
}

// _____________________________________________________________________________
template <typename S, typename C>
Vocabulary<S, C>::SearchResult Vocabulary<S, C>::upper_bound(
    const string& word, const SortLevel level) const {
  static_assert(std::random_access_iterator<typename decltype(
                    _words)::StlConformingIterator>);
  auto internalSignedIdFromInternalVocab =
      _words.upper_bound(word, getComparatorForSortLevel(level));
  auto internalIdFromInternalVocab =
      ad_utility::InternalId{internalSignedIdFromInternalVocab.get()};
  SearchResult resultInternal;
  resultInternal._id = IdManager::fromInternalId(internalIdFromInternalVocab);
  if (internalIdFromInternalVocab < _words.size()) {
    resultInternal._word = getInternalWordFromId(internalIdFromInternalVocab);
  }
  if (_externalLiterals.size() == 0) {
    return resultInternal;
  }

  SearchResult resultExternal;
  // TODO<joka921> also support sortLevels for the external vocabulary
  auto [id, wordFromExternal] = _externalLiterals.upper_bound(word, level);
  resultExternal._id = ad_utility::CompleteId{id};
  resultExternal._word = std::move(wordFromExternal);
  return std::min(resultInternal, resultExternal);
}

// _____________________________________________________________________________
template <typename S, typename ComparatorType>
void Vocabulary<S, ComparatorType>::setLocale(const std::string& language,
                                              const std::string& country,
                                              bool ignorePunctuation) {
  _caseComparator = ComparatorType(language, country, ignorePunctuation);
  _externalLiterals.getCaseComparator() =
      ComparatorType(language, country, ignorePunctuation);
}

// TODO<joka921> we need at() and we need to use it in upper_bound and
// lower_bound.
template <typename StringType, typename C>
//! Get the word with the given id.
//! lvalue for compressedString and const& for string-based vocabulary
AccessReturnType_t<StringType> Vocabulary<StringType, C>::getInternalWordFromId(
    ad_utility::InternalId id) const {
  if constexpr (_isCompressed) {
    return expandPrefix(_words[id]);
  } else {
    // TODO<joka921> Make the uncompressed vocabulary use "ordinary" chars
    // again.
    return {reinterpret_cast<const char*>(_words[id].data()),
            _words[id].size()};
  }
}

// _____________________________________________________________________________
// TODO<joka921> return here;
template <typename S, typename C>
bool Vocabulary<S, C>::getId(const string& word, Id* id) const {
  // need the TOTAL level because we want the unique word.
  auto [completeId, actualWord] = lower_bound(word, SortLevel::TOTAL);

  *id = completeId.get();
  return actualWord == word;
}

// ___________________________________________________________________________
template <typename S, typename C>
std::pair<typename Vocabulary<S, C>::SearchResult,
          typename Vocabulary<S, C>::SearchResult>
Vocabulary<S, C>::prefix_range(const string& prefix) const {
  // TODO<joka921> fix this
  if (prefix.empty()) {
    throw std::runtime_error("Empty prefix filters are currently disabled");
    // return {0, _words.size()};
  }

  auto lb = lower_bound(prefix, SortLevel::PRIMARY);
  auto transformed = _caseComparator.transformToFirstPossibleBiggerValue(
      prefix, SortLevel::PRIMARY);

  auto ub = lower_bound(transformed, SortLevel::PRIMARY);

  return {std::move(lb), std::move(ub)};
}

// TODO<joka921> is this needed, if yes, fix it
// _____________________________________________________________________________
template <typename S, typename C>
template <typename, typename>
const std::optional<std::string_view> Vocabulary<S, C>::operator[](
    Id idExternal) const {
  ad_utility::CompleteId idComplete{idExternal};
  AD_CHECK(IdManager::isInternalId(idComplete));
  auto id = IdManager::toInternalId(idComplete);
  if (id < _words.size()) {
    return _words[id];
  } else {
    return std::nullopt;
  }
}
template const std::optional<std::string_view>
TextVocabulary::operator[]<std::string, void>(Id id) const;

template <typename S, typename C>
template <typename, typename>
const std::optional<string> Vocabulary<S, C>::idToOptionalString(Id id) const {
  if (id == ID_NO_VALUE) {
    return std::nullopt;
  }

  auto completeId = ad_utility::CompleteId(id);
  if (IdManager::isInternalId(completeId)) {
    auto internalId = IdManager::toInternalId(completeId);
    AD_CHECK(internalId < _words.size())
    // internal, prefixCompressed word
    // TODO<joka921> use `at` here again.
    return expandPrefix(_words[internalId]);
  } else {
    // this word must be externalized
    auto result = _externalLiterals.idToOptionalString(id);
    AD_CHECK(result.has_value());
    return result;
  }
}

// ___________________________________________________________________________
template <typename S, typename C>
ad_utility::HashMap<typename Vocabulary<S, C>::Datatypes, std::pair<Id, Id>>
Vocabulary<S, C>::getRangesForDatatypes() const {
  ad_utility::HashMap<Datatypes, std::pair<Id, Id>> result;

  auto convert = [](const auto& p) {
    return std::pair{p.first._id.get(), p.second._id.get()};
  };
  result[Datatypes::Float] = convert(prefix_range(VALUE_FLOAT_PREFIX));
  result[Datatypes::Date] = convert(prefix_range(VALUE_DATE_PREFIX));
  result[Datatypes::Literal] = convert(prefix_range("\""));
  result[Datatypes::Iri] = convert(prefix_range("<"));

  return result;
};

template <typename S, typename C>
template <typename, typename>
void Vocabulary<S, C>::printRangesForDatatypes() {
  auto ranges = getRangesForDatatypes();
  auto logRange = [&](const auto& range) {
    LOG(INFO) << range.first << " " << range.second << '\n';
    if (range.second > range.first) {
      LOG(INFO) << idToOptionalString(range.first).value() << '\n';
      LOG(INFO) << idToOptionalString(range.second - 1).value() << '\n';
    }
    auto opt = idToOptionalString(range.second);
    if (opt.has_value()) {
      LOG(INFO) << opt.value() << '\n';
    }

    if (range.first > 0) {
      LOG(INFO) << idToOptionalString(range.first - 1).value() << '\n';
    }
  };

  for (const auto& pair : ranges) {
    logRange(pair.second);
  }
}

template const std::optional<string>
RdfsVocabulary::idToOptionalString<CompressedString, void>(Id id) const;

// Explicit template instantiations
template class Vocabulary<CompressedString, TripleComponentComparator>;
template class Vocabulary<std::string, SimpleStringComparator>;

template void RdfsVocabulary::initializePrefixes<std::vector<std::string>,
                                                 CompressedString, void>(
    const std::vector<std::string>&);
template void RdfsVocabulary::initializeInternalizedLangs<nlohmann::json>(
    const nlohmann::json&);
template void RdfsVocabulary::initializeExternalizePrefixes<nlohmann::json>(
    const nlohmann::json& prefixes);

template CompressedString RdfsVocabulary::compressPrefix(
    const string& word) const;

template void RdfsVocabulary::printRangesForDatatypes();

template void TextVocabulary::writeToFile<std::string, void>(
    const string& fileName) const;
