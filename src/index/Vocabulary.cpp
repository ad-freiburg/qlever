// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "./Vocabulary.h"

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

#include "../parser/RdfEscaping.h"
#include "../parser/Tokenizer.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "./ConstantsIndexCreation.h"

using std::string;

// _____________________________________________________________________________
template <class S, class C>
void Vocabulary<S, C>::readFromFile(const string& fileName,
                                    const string& extLitsFileName) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << "\n";
  _words.clear();
  std::fstream in(fileName.c_str(), std::ios_base::in);
  string line;
  [[maybe_unused]] bool first = true;
  std::string lastExpandedString;
  while (std::getline(in, line)) {
    if constexpr (_isCompressed) {
      // when we read from file it means that all preprocessing has been done
      // and the prefixes are already stripped in the file
      auto str = RdfEscaping::unescapeNewlinesAndBackslashes(
          expandPrefix(CompressedString::fromString(line)));

      _words.push_back(compressPrefix(str));
      if (!first) {
        if (!(_caseComparator.compare(lastExpandedString, str,
                                      SortLevel::TOTAL))) {
          LOG(ERROR) << "Vocabulary is not sorted in ascending order for words "
                     << lastExpandedString << " and " << str << std::endl;
          // AD_CHECK(false);
        }
      } else {
        first = false;
      }
      lastExpandedString = std::move(str);
    } else {
      _words.push_back(line);
    }
  }
  in.close();
  LOG(INFO) << "Done reading vocabulary from file.\n";
  LOG(INFO) << "It contains " << _words.size() << " elements\n";
  if (extLitsFileName.size() > 0) {
    if (!_isCompressed) {
      LOG(INFO) << "ERROR: trying to load externalized literals to an "
                   "uncompressed vocabulary. This is not valid and a "
                   "programming error. Terminating\n";
      AD_CHECK(false);
    }

    LOG(INFO) << "Registering external vocabulary for literals.\n";
    _externalLiterals.initFromFile(extLitsFileName);
    LOG(INFO) << "Done registering external vocabulary for literals.\n";
  }
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
void Vocabulary<S, C>::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << "\n";
  std::ofstream out(fileName.c_str(), std::ios_base::out);
  // on disk we save the compressed version, so do not  expand prefixes
  for (size_t i = 0; i + 1 < _words.size(); ++i) {
    out << _words[i] << '\n';
  }
  if (_words.size() > 0) {
    out << _words[_words.size() - 1];
  }
  out.close();
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
void Vocabulary<S, C>::writeToBinaryFileForMerging(
    const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to binary file " << fileName << "\n";
  std::ofstream out(fileName.c_str(),
                    std::ios_base::out | std::ios_base::binary);
  AD_CHECK(out.is_open());
  for (size_t i = 0; i < _words.size(); ++i) {
    // 32 bits should be enough for len of string
    std::string_view word = _words[i];
    uint32_t len = word.size();
    size_t zeros = 0;
    out.write((char*)&len, sizeof(len));
    out.write(word.data(), len);
    out.write((char*)&zeros, sizeof(zeros));
  }
  out.close();
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
void Vocabulary<S, C>::createFromSet(const ad_utility::HashSet<S>& set) {
  LOG(INFO) << "Creating vocabulary from set ...\n";
  _words.clear();
  _words.reserve(set.size());
  _words.insert(begin(_words), begin(set), end(set));
  LOG(INFO) << "... sorting ...\n";
  std::sort(begin(_words), end(_words), _caseComparator);
  LOG(INFO) << "Done creating vocabulary.\n";
}

// _____________________________________________________________________________
// TODO<joka921> is this used? if no, throw out, if yes, transform to
// compressedString as key type
template <class S, class C>
template <typename, typename>
ad_utility::HashMap<string, Id> Vocabulary<S, C>::asMap() {
  ad_utility::HashMap<string, Id> map;
  for (size_t i = 0; i < _words.size(); ++i) {
    map[_words[i]] = i;
  }
  return map;
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
void Vocabulary<S, C>::externalizeLiterals(const string& fileName) {
  LOG(INFO) << "Externalizing literals..." << std::endl;
  auto ext = std::lower_bound(_words.begin(), _words.end(),
                              EXTERNALIZED_LITERALS_PREFIX, _caseComparator);
  size_t nofInternal = ext - _words.begin();
  vector<string> extVocab;
  while (ext != _words.end()) {
    extVocab.push_back((*ext++).substr(1));
  }
  _words.resize(nofInternal);
  _externalLiterals.buildFromVector(extVocab, fileName);
  LOG(INFO) << "Done externalizing literals." << std::endl;
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
         ad_utility::startsWith(word, EXTERNALIZED_LITERALS_PREFIX + '\"');
}

// _____________________________________________________________________________
template <class S, class C>
bool Vocabulary<S, C>::shouldBeExternalized(const string& word) const {
  if (!isLiteral(word)) {
    return shouldEntityBeExternalized(word);
  } else {
    return shouldLiteralBeExternalized(word);
  }
}

// ___________________________________________________________________
template <class S, class C>
bool Vocabulary<S, C>::shouldEntityBeExternalized(const string& word) const {
  for (const auto& p : _externalizedPrefixes) {
    if (ad_utility::startsWith(word, p)) {
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

// ____________________________________________________________________________
template <class S, class C>
template <typename, typename>
string Vocabulary<S, C>::expandPrefix(const CompressedString& word) const {
  assert(!word.empty());
  auto idx = static_cast<uint8_t>(word[0]) - MIN_COMPRESSION_PREFIX;
  if (idx >= 0 && idx < NUM_COMPRESSION_PREFIXES) {
    return _prefixMap[idx] + word.toStringView().substr(1);
  } else {
    return string(word.toStringView().substr(1));
  }
}

// _____________________________________________________________________________
template <class S, class C>
template <typename, typename>
CompressedString Vocabulary<S, C>::compressPrefix(const string& word) const {
  for (const auto& p : _prefixVec) {
    if (ad_utility::startsWith(word, p._fulltext)) {
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
    _externalizedPrefixes.push_back(el);
  }
}

// ______________________________________________________________________________
template <class S, class C>
template <class StringRange>
void Vocabulary<S, C>::initializeInternalizedLangs(const StringRange& s) {
  _internalizedLangs.clear();
  _internalizedLangs.insert(_internalizedLangs.begin(), s.begin(), s.end());
}

// _____________________________________________________
template <class S, class C>
template <typename, typename>
void Vocabulary<S, C>::prefixCompressFile(const string& infile,
                                          const string& outfile,
                                          const vector<string>& prefixes) {
  std::ifstream in(infile);
  std::ofstream out(outfile);
  AD_CHECK(in.is_open() && out.is_open());
  Vocabulary v;
  v.initializePrefixes(prefixes);
  std::string word;
  while (std::getline(in, word)) {
    out << v.compressPrefix(word).toStringView() << '\n';
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
    AD_CHECK_LT(range->_first, _words.size());
    AD_CHECK_LT(range->_last, _words.size());
  }
  return success;
}

// _______________________________________________________________
template <typename S, typename C>
Id Vocabulary<S, C>::upper_bound(const string& word,
                                 const SortLevel level) const {
  return static_cast<Id>(std::upper_bound(_words.begin(), _words.end(), word,
                                          getUpperBoundLambda(level)) -
                         _words.begin());
}

// _____________________________________________________________________________
template <typename S, typename C>
Id Vocabulary<S, C>::lower_bound(const string& word,
                                 const SortLevel level) const {
  return static_cast<Id>(std::lower_bound(_words.begin(), _words.end(), word,
                                          getLowerBoundLambda(level)) -
                         _words.begin());
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

template <typename StringType, typename C>
//! Get the word with the given id.
//! lvalue for compressedString and const& for string-based vocabulary
AccessReturnType_t<StringType> Vocabulary<StringType, C>::at(Id id) const {
  if constexpr (_isCompressed) {
    return expandPrefix(_words[static_cast<size_t>(id)]);
  } else {
    return _words[static_cast<size_t>(id)];
  }
}

// _____________________________________________________________________________
template <typename S, typename C>
bool Vocabulary<S, C>::getId(const string& word, Id* id) const {
  if (!shouldBeExternalized(word)) {
    // need the TOTAL level because we want the unique word.
    *id = lower_bound(word, SortLevel::TOTAL);
    // works for the case insensitive version because
    // of the strict ordering.
    return *id < _words.size() && at(*id) == word;
  }
  bool success = _externalLiterals.getId(word, id);
  *id += _words.size();
  return success;
}

// ___________________________________________________________________________
template <typename S, typename C>
std::pair<Id, Id> Vocabulary<S, C>::prefix_range(const string& prefix) const {
  if (prefix.empty()) {
    return {0, _words.size()};
  }
  Id lb = lower_bound(prefix, SortLevel::PRIMARY);
  auto transformed = _caseComparator.transformToFirstPossibleBiggerValue(
      prefix, SortLevel::PRIMARY);

  auto pred = getLowerBoundLambda<decltype(transformed)>(SortLevel::PRIMARY);
  auto ub = static_cast<Id>(
      std::lower_bound(_words.begin(), _words.end(), transformed, pred) -
      _words.begin());

  return {lb, ub};
}

// _____________________________________________________________________________
template <typename S, typename C>
void Vocabulary<S, C>::push_back(const string& word) {
  if constexpr (_isCompressed) {
    _words.push_back(compressPrefix(word));
  } else {
    _words.push_back(word);
  }
}

// _____________________________________________________________________________
template <typename S, typename C>
template <typename, typename>
const std::optional<std::reference_wrapper<const string>>
Vocabulary<S, C>::operator[](Id id) const {
  if (id < _words.size()) {
    return _words[static_cast<size_t>(id)];
  } else {
    return std::nullopt;
  }
}
template const std::optional<std::reference_wrapper<const string>>
TextVocabulary::operator[]<std::string, void>(Id id) const;

template <typename S, typename C>
template <typename, typename>
const std::optional<string> Vocabulary<S, C>::idToOptionalString(Id id) const {
  if (id < _words.size()) {
    // internal, prefixCompressed word
    return expandPrefix(_words[static_cast<size_t>(id)]);
  } else if (id == ID_NO_VALUE) {
    return std::nullopt;
  } else {
    // this word must be externalized
    id -= _words.size();
    AD_CHECK(id < _externalLiterals.size());
    return _externalLiterals[id];
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
template void RdfsVocabulary::prefixCompressFile<CompressedString, void>(
    const string& infile, const string& outfile,
    const vector<string>& prefixes);

template void TextVocabulary::createFromSet<std::string, void>(
    const ad_utility::HashSet<std::string>& set);
template void TextVocabulary::writeToFile<std::string, void>(
    const string& fileName) const;
