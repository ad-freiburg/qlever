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
  _internalVocabulary.clear();
  ad_utility::serialization::FileReadSerializer file(fileName);
  _internalVocabulary.readFromFile(fileName);
  LOG(INFO) << "Done, number of words: " << _internalVocabulary.size()
            << std::endl;
  if (extLitsFileName.size() > 0) {
    if (!_isCompressed) {
      LOG(INFO) << "ERROR: trying to load externalized literals to an "
                   "uncompressed vocabulary. This is not valid and a "
                   "programming error. Terminating\n";
      AD_CHECK(false);
    }

    LOG(DEBUG) << "Registering external vocabulary" << std::endl;
    _externalVocabulary.initFromFile(extLitsFileName);
    LOG(INFO) << "Number of words in external vocabulary: "
              << _externalVocabulary.size() << std::endl;
  }
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
void Vocabulary<S, C>::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer file{fileName};
  _internalVocabulary.getUnderlyingVocabulary().writeToFile(fileName);

  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________________
template <class S, class C>
void Vocabulary<S, C>::createFromSet(
    const ad_utility::HashSet<std::string>& set) {
  LOG(INFO) << "Creating vocabulary from set ...\n";
  _internalVocabulary.clear();
  std::vector<std::string> words(set.begin(), set.end());
  LOG(INFO) << "... sorting ...\n";
  auto totalComparison = [this](const auto& a, const auto& b) {
    return getCaseComparator()(a, b, SortLevel::TOTAL);
  };
  std::sort(begin(words), end(words), totalComparison);
  _internalVocabulary.build(words);
  LOG(INFO) << "Done creating vocabulary.\n";
}

// _____________________________________________________________________________
template <class S, class C>
bool Vocabulary<S, C>::isLiteral(const string& word) {
  return word.size() > 0 && word[0] == '\"';
}

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
  for (const auto& p : _externalizedPrefixes) {
    if (word.starts_with(p)) {
      return true;
    }
  }
  return false;
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

// _____________________________________________________________________________
template <class S, class C>
template <class StringRange, typename, typename>
void Vocabulary<S, C>::buildCodebookForPrefixCompression(
    const StringRange& prefixes) {
  _internalVocabulary.getUnderlyingVocabulary().getCompressor().buildCodebook(
      prefixes);
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
  range->_first = prefixRange.first;
  range->_last = prefixRange.second - 1;

  if (success) {
    AD_CHECK_LT(range->_first, _internalVocabulary.size());
    AD_CHECK_LT(range->_last, _internalVocabulary.size());
  }
  return success;
}

// _______________________________________________________________
template <typename S, typename C>
Id Vocabulary<S, C>::upper_bound(const string& word,
                                 const SortLevel level) const {
  return _internalVocabulary.upper_bound(word, level)._index;
}

// _____________________________________________________________________________
template <typename S, typename C>
Id Vocabulary<S, C>::lower_bound(const string& word,
                                 const SortLevel level) const {
  return _internalVocabulary.lower_bound(word, level)._index;
}

// _____________________________________________________________________________
template <typename S, typename ComparatorType>
void Vocabulary<S, ComparatorType>::setLocale(const std::string& language,
                                              const std::string& country,
                                              bool ignorePunctuation) {
  _internalVocabulary.getComparator() =
      ComparatorType(language, country, ignorePunctuation);
  _externalVocabulary.getCaseComparator() =
      ComparatorType(language, country, ignorePunctuation);
}

template <typename StringType, typename C>
//! Get the word with the given id.
//! lvalue for compressedString and const& for string-based vocabulary
AccessReturnType_t<StringType> Vocabulary<StringType, C>::at(Id id) const {
  return _internalVocabulary[static_cast<size_t>(id)];
}

// _____________________________________________________________________________
template <typename S, typename C>
bool Vocabulary<S, C>::getId(const string& word, Id* id) const {
  if (!shouldBeExternalized(word)) {
    // need the TOTAL level because we want the unique word.
    *id = lower_bound(word, SortLevel::TOTAL);
    // works for the case insensitive version because
    // of the strict ordering.
    return *id < _internalVocabulary.size() && at(*id) == word;
  }
  bool success = _externalVocabulary.getId(word, id);
  *id += _internalVocabulary.size();
  return success;
}

// ___________________________________________________________________________
template <typename S, typename C>
std::pair<Id, Id> Vocabulary<S, C>::prefix_range(const string& prefix) const {
  return _internalVocabulary.prefix_range(prefix);
}

// _____________________________________________________________________________
template <typename S, typename C>
template <typename, typename>
const std::optional<std::string_view> Vocabulary<S, C>::operator[](
    Id id) const {
  if (id < _internalVocabulary.size()) {
    return _internalVocabulary[static_cast<size_t>(id)];
  } else {
    return std::nullopt;
  }
}
template const std::optional<std::string_view>
TextVocabulary::operator[]<std::string, void>(Id id) const;

template <typename S, typename C>
template <typename, typename>
const std::optional<string> Vocabulary<S, C>::idToOptionalString(Id id) const {
  if (id < _internalVocabulary.size()) {
    return _internalVocabulary[static_cast<size_t>(id)];
  } else if (id == ID_NO_VALUE) {
    return std::nullopt;
  } else {
    // this word must be externalized
    id -= _internalVocabulary.size();
    AD_CHECK(id < _externalVocabulary.size());
    return _externalVocabulary[id];
  }
}

// ___________________________________________________________________________
template <typename S, typename C>
ad_utility::HashMap<typename Vocabulary<S, C>::Datatypes, std::pair<Id, Id>>
Vocabulary<S, C>::getRangesForDatatypes() const {
  ad_utility::HashMap<Datatypes, std::pair<Id, Id>> result;
  result[Datatypes::Float] = prefix_range(VALUE_FLOAT_PREFIX);
  result[Datatypes::Date] = prefix_range(VALUE_DATE_PREFIX);
  result[Datatypes::Literal] = prefix_range("\"");
  result[Datatypes::Iri] = prefix_range("<");

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
    if (range.second < _internalVocabulary.size()) {
      LOG(INFO) << idToOptionalString(range.second).value() << '\n';
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

template void RdfsVocabulary::buildCodebookForPrefixCompression<
    std::vector<std::string>, CompressedString, void>(
    const std::vector<std::string>&);
template void RdfsVocabulary::initializeInternalizedLangs<nlohmann::json>(
    const nlohmann::json&);
template void RdfsVocabulary::initializeExternalizePrefixes<nlohmann::json>(
    const nlohmann::json& prefixes);

template void RdfsVocabulary::printRangesForDatatypes();

template void TextVocabulary::writeToFile<std::string, void>(
    const string& fileName) const;
