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
template <class S, class C>
void Vocabulary<S, C>::readFromFile(const string& fileName,
                                    const string& extLitsFileName) {
  LOG(INFO) << "Reading internal vocabulary from file " << fileName << " ..."
            << std::endl;
  _words.clear();
  ad_utility::serialization::FileReadSerializer file(fileName);
  file >> _words;
  LOG(INFO) << "Done, number of words: " << _words.size() << std::endl;
  if (!extLitsFileName.empty()) {
    if (!_isCompressed) {
      LOG(INFO) << "ERROR: trying to load externalized literals to an "
                   "uncompressed vocabulary. This is not valid and a "
                   "programming error. Terminating\n";
      AD_CHECK(false);
    }

    LOG(DEBUG) << "Registering external vocabulary" << std::endl;
    _externalLiterals.initFromFile(extLitsFileName);
    LOG(INFO) << "Number of words in external vocabulary: "
              << _externalLiterals.size() << std::endl;
  }
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
template <class S, class C>
void Vocabulary<S, C>::createFromSet(
    const ad_utility::HashSet<std::string>& set) {
  LOG(INFO) << "Creating vocabulary from set ...\n";
  if constexpr (_isCompressed) {
    // TODO<instate something for testing>
    AD_CHECK(false);
  } else {
    _words.clear();
    std::vector<S> words;
    words.reserve(set.size());
    for (const auto& word : set) {
      words.push_back(word);
    }
    LOG(INFO) << "... sorting ...\n";
    auto totalComparison = [this](const auto& a, const auto& b) {
      return _caseComparator(std::string_view(a.begin(), a.end()),
                             std::string_view(b.begin(), b.end()),
                             SortLevel::TOTAL);
    };
    std::sort(begin(words), end(words), totalComparison);
    _words.build(words);
  }
  LOG(INFO) << "Done creating vocabulary.\n";
}

// _____________________________________________________________________________
template <class S, class C>
bool Vocabulary<S, C>::isLiteral(const string& word) {
  return word.starts_with('"');
}

// TODO<joka921> Can this be removed once we are done with this PR?
// _____________________________________________________________________________
template <class S, class C>
bool Vocabulary<S, C>::isExternalizedLiteral(const string& word) {
  return word.size() > 1 &&
         word.starts_with(EXTERNALIZED_LITERALS_PREFIX + '\"');
}

// _____________________________________________________________________________
template <class S, class C>
bool Vocabulary<S, C>::shouldBeExternalized(const string& word) const {
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
template <class S, class C>
bool Vocabulary<S, C>::shouldEntityBeExternalized(const string& word) const {
  return std::ranges::any_of(_externalizedPrefixes, [&](const auto& prefix) {
    return word.starts_with(prefix);
  });
}

// ___________________________________________________________________
template <class S, class C>
bool Vocabulary<S, C>::shouldLiteralBeExternalized(const string& word) const {
  if (word.size() > MAX_INTERNAL_LITERAL_BYTES) {
    return true;
  }

  const string lang = getLanguage(word);
  if (lang == "") {
    return false;
  }

  for (const auto& p : _internalizedLangs) {
    if (lang == p) {
      return false;
    }
  }
  return true;
}
// _____________________________________________________________________________
template <class S, class C>
string Vocabulary<S, C>::getLanguage(const string& literal) {
  auto lioAt = literal.rfind('@');
  if (lioAt != string::npos) {
    auto lioQ = literal.rfind('\"');
    if (lioQ != string::npos && lioQ < lioAt) {
      return literal.substr(lioAt + 1);
    }
  }
  return "";
}

// ____________________________________________________________________________
template <class S, class C>
template <typename, typename>
string Vocabulary<S, C>::expandPrefix(std::string_view word) const {
  assert(!word.empty());
  auto idx = static_cast<uint8_t>(word[0]) - MIN_COMPRESSION_PREFIX;
  if (idx >= 0 && idx < NUM_COMPRESSION_PREFIXES) {
    return _prefixMap[idx] + word.substr(1);
  } else {
    return string(word.substr(1));
  }
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
CompressedString Vocabulary<S, C>::compressPrefix(const string& word) const {
  for (const auto& p : _prefixVec) {
    if (word.starts_with(p._fulltext)) {
      auto res = CompressedString::fromString(
          p._prefix + std::string_view(word).substr(p._fulltext.size()));
      return res;
    }
  }
  auto res = CompressedString::fromString(NO_PREFIX_CHAR + word);
  return res;
}

// _____________________________________________________________________________
template <class S, class C>
template <class StringRange, typename, typename>
void Vocabulary<S, C>::initializePrefixes(const StringRange& prefixes) {
  for (auto& el : _prefixMap) {
    el = "";
  }
  _prefixVec.clear();
  unsigned char prefixIdx = 0;
  for (const auto& fulltext : prefixes) {
    if (prefixIdx >= NUM_COMPRESSION_PREFIXES) {
      LOG(INFO) << "More than " << NUM_COMPRESSION_PREFIXES
                << " prefixes have been specified. Skipping the rest\n";
      break;
    }
    _prefixMap[prefixIdx] = fulltext;
    _prefixVec.emplace_back(prefixIdx + MIN_COMPRESSION_PREFIX, fulltext);
    prefixIdx++;
  }
  if (prefixIdx != NUM_COMPRESSION_PREFIXES) {
    LOG(WARN) << "less than " << NUM_COMPRESSION_PREFIXES
              << " prefixes specified.";
  }
  // if longest strings come first we correctly handle overlapping prefixes
  auto pred = [](const Prefix& a, const Prefix& b) {
    return a._fulltext.size() > b._fulltext.size();
  };
  std::sort(_prefixVec.begin(), _prefixVec.end(), pred);
}

// ______________________________________________________________________________
template <class S, class C>
template <class StringRange>
void Vocabulary<S, C>::initializeExternalizePrefixes(const StringRange& s) {
  _externalizedPrefixes.clear();
  for (const auto& el : s) {
    _externalizedPrefixes.emplace_back(el);
  }
}

// ______________________________________________________________________________
template <class S, class C>
template <class StringRange>
void Vocabulary<S, C>::initializeInternalizedLangs(const StringRange& s) {
  _internalizedLangs.clear();
  // `StringRange` might be nlohmann::json, for which we have disabled
  // implicit conversions, so `vector::insert` cannot be used.
  for (const auto& el : s) {
    _internalizedLangs.emplace_back(el);
  }
}

// ___________________________________________________________________________
template <typename S, typename C>
bool Vocabulary<S, C>::getIdRangeForFullTextPrefix(const string& word,
                                                   IdRange* range) const {
  AD_CHECK_EQ(word[word.size() - 1], PREFIX_CHAR);
  auto prefixRange = prefix_range(word.substr(0, word.size() - 1));
  bool success = prefixRange.second > prefixRange.first;
  range->_first = prefixRange.first._id.get();
  range->_last = prefixRange.second._id.get() - 1;

  if (success) {
    AD_CHECK_LT(range->_first, internalSize());
    AD_CHECK_LT(range->_last, internalSize());
  }
  return success;
}

// _____________________________________________________________________________
template <typename S, typename C>
template <typename StringType>
Vocabulary<S, C>::SearchResult Vocabulary<S, C>::lower_bound(
    const StringType& word, const SortLevel level) const {
  auto internalSignedIdFromInternalVocab =
      _words.lower_bound(word, getComparatorForSortLevel(level));
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
  auto [id, wordFromExternal] = _externalLiterals.lower_bound(word, level);
  resultExternal._id = ad_utility::CompleteId{id};
  resultExternal._word = std::move(wordFromExternal);
  return std::max(resultInternal, resultExternal);
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
