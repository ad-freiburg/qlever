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
#include "../util/Serializer/FileSerializer.h"
#include "../util/Serializer/SerializeString.h"

using std::string;

template<size_t I, typename Tuple>
void fromFileImpl(const string& baseFileName, Tuple& tuple) {
  if constexpr (I - 1 < std::tuple_size_v<Tuple>) {
    auto& vec = std::get<I - 1>(tuple);
    using ValueType = std::decay_t<decltype(vec[0])>;
    string fileName = baseFileName + "." + std::to_string(I);
    ad_utility::serialization::FileReadSerializer in{fileName};
    ValueType v;
    bool isFirst = true;
    ValueType lastWritten;
    while (true) {
      try {
        in& v;
        if (!isFirst) {
          AD_CHECK(v > lastWritten);

        }
        isFirst = false;
        lastWritten = v;

        vec.push_back(std::move(v));
      } catch (const std::exception& e) {
        LOG(INFO) << e.what();
        break;
      }
    }
    LOG(INFO) << "READ " << vec.size() << " values from " << fileName << std::endl;
    fromFileImpl<I + 1>(baseFileName, tuple);
  }
}

// _____________________________________________________________________________
template <class S, class C, typename... A>
void Vocabulary<S, C, A...>::readFromFile(const string& baseFileName,
                                    const string& extLitsFileName) {
  string fileName = baseFileName + ".0";
  LOG(INFO) << "Reading vocabulary from file " << fileName << "\n";
  _words.clear();
  ad_utility::serialization::FileReadSerializer in{fileName};
  string line;
  [[maybe_unused]] bool first = true;
  std::string lastExpandedString;
  while (true) {
    try {
      in & line;
    } catch(...) {
      break;
    }
    if constexpr (_isCompressed) {
      _words.push_back(CompressedString::fromString(line));
      if (!first) {
        /*
        if (!(_caseComparator.compare(lastExpandedString, str,
                                      SortLevel::TOTAL))) {
          LOG(ERROR) << "Vocabulary is not sorted in ascending order for words "
                     << lastExpandedString << " and " << str << std::endl;
          // AD_CHECK(false);
        }
         */
      } else {
        first = false;
      }
      //lastExpandedString = std::move(str);
    } else {
      _words.push_back(line);
    }
  }
  fromFileImpl<1>(baseFileName, _additionalValues.getTuple());
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
//! restore the tmpVocabulary required for the prefix compression
template <class S, class C, typename... A>
void Vocabulary<S,C, A...>::restoreTmpVocabForPrefixCompression(const string& vocFileName, const string& outFileName) {
  std::vector<std::string> words;
  LOG(INFO) << "Reading vocabulary from file " << vocFileName << std::endl;
  ad_utility::serialization::FileReadSerializer in{vocFileName};
  string line;
  while (true) {
    try {
      in & line;
      words.push_back(line);
    } catch(...) {
      break;
    }
  }

  LOG(INFO) << "Sorting the vocabulary according to std::less" << std::endl;

  std::sort(words.begin(), words.end());

  LOG(INFO) << "Done Sorting, writing vocabulary to file " << outFileName << std::endl;

  ad_utility::serialization::FileWriteSerializer out{outFileName};
  for (const auto& el : words) {
    out & el;
  }
  LOG(INFO) << "Done writing vocabulary" << std::endl;
}

// _____________________________________________________________________________
template <class S, class C, typename... A>
template <typename, typename>
void Vocabulary<S, C, A...>::writeToFile(const string& fileName) const {
  LOG(INFO) << "Writing vocabulary to file " << fileName << "\n";
  ad_utility::serialization::FileWriteSerializer out{fileName};
  // on disk we save the compressed version, so do not  expand prefixes
  for (const auto& word : _words) {
    out & word ;
  }
  LOG(INFO) << "Done writing vocabulary to file.\n";
}

// _____________________________________________________________________________
template <class S, class C, typename... A>
template <typename, typename>
void Vocabulary<S, C, A...>::writeToBinaryFileForMerging(
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
template <class S, class C, typename... A>
template <typename, typename>
void Vocabulary<S, C, A...>::createFromSet(const ad_utility::HashSet<S>& set) {
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
template <class S, class C, typename... A>
template <typename, typename>
ad_utility::HashMap<string, Id> Vocabulary<S, C, A...>::asMap() {
  ad_utility::HashMap<string, Id> map;
  for (size_t i = 0; i < _words.size(); ++i) {
    map[_words[i]] = i;
  }
  return map;
}

// _____________________________________________________________________________
template <class S, class C, typename... A>
template <typename, typename>
void Vocabulary<S, C, A...>::externalizeLiterals(const string& fileName) {
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
template <class S, class C, typename... A>
bool Vocabulary<S, C, A...>::isLiteral(const string& word) {
  return word.size() > 0 && word[0] == '\"';
}

// _____________________________________________________________________________
template <class S, class C, typename... A>
bool Vocabulary<S, C, A...>::isExternalizedLiteral(const string& word) {
  return word.size() > 1 &&
         ad_utility::startsWith(word, EXTERNALIZED_LITERALS_PREFIX + '\"');
}

// _____________________________________________________________________________
template <class S, class C, typename... A>
bool Vocabulary<S, C, A...>::shouldBeExternalized(const string& word) const {
  if (!isLiteral(word)) {
    return shouldEntityBeExternalized(word);
  } else {
    return shouldLiteralBeExternalized(word);
  }
}

// ___________________________________________________________________
template <class S, class C, typename... A>
bool Vocabulary<S, C, A...>::shouldEntityBeExternalized(const string& word) const {
  for (const auto& p : _externalizedPrefixes) {
    if (ad_utility::startsWith(word, p)) {
      return true;
    }
  }
  return false;
}

// ___________________________________________________________________
template <class S, class C, typename... A>
bool Vocabulary<S, C, A...>::shouldLiteralBeExternalized(const string& word) const {
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
template <class S, class C, typename... A>
string Vocabulary<S, C, A...>::getLanguage(const string& literal) {
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
template <class S, class C, typename... A>
template <typename, typename>
string Vocabulary<S, C, A...>::expandPrefix(const CompressedString& word) const {
  assert(!word.empty());
  auto idx = static_cast<uint8_t>(word[0]) - MIN_COMPRESSION_PREFIX;
  if (idx >= 0 && idx < NUM_COMPRESSION_PREFIXES) {
    return _prefixMap[idx] + word.toStringView().substr(1);
  } else {
    return string(word.toStringView().substr(1));
  }
}

// _____________________________________________________________________________
template <class S, class C, typename... A>
template <typename, typename>
CompressedString Vocabulary<S, C, A...>::compressPrefix(const string& word) const {
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
template <class S, class C, typename... A>
template <class StringRange, typename, typename>
void Vocabulary<S, C, A...>::initializePrefixes(const StringRange& prefixes) {
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
template <class S, class C, typename... A>
template <class StringRange>
void Vocabulary<S, C, A...>::initializeExternalizePrefixes(const StringRange& s) {
  _externalizedPrefixes.clear();
  for (const auto& el : s) {
    _externalizedPrefixes.push_back(el);
  }
}

// ______________________________________________________________________________
template <class S, class C, typename... A>
template <class StringRange>
void Vocabulary<S, C, A...>::initializeInternalizedLangs(const StringRange& s) {
  _internalizedLangs.clear();
  _internalizedLangs.insert(_internalizedLangs.begin(), s.begin(), s.end());
}

// _____________________________________________________
template <class S, class C, typename... A>
template <typename, typename>
void Vocabulary<S, C, A...>::prefixCompressFile(const string& infile,
                                          const string& outfile,
                                          const vector<string>& prefixes) {
  ad_utility::serialization::FileReadSerializer in(infile);
  ad_utility::serialization::FileWriteSerializer out(outfile);
  Vocabulary v;
  v.initializePrefixes(prefixes);
  std::string word;
  size_t numWords = 0;
  while (true) {
    try {
      in & word;
      numWords++;
    } catch(...) {
      break;
    }
    out & v.compressPrefix(word).toString();
  }
}

// ___________________________________________________________________________
template <typename S, typename C, typename... A>
bool Vocabulary<S, C, A...>::getIdRangeForFullTextPrefix(const string& word,
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
template <typename S, typename C, typename... A>
Id Vocabulary<S, C, A...>::upper_bound(const string& word,
                                 const SortLevel level) const {
  return static_cast<Id>(std::upper_bound(_words.begin(), _words.end(), word,
                                          getUpperBoundLambda(level)) -
                         _words.begin());
}

// _____________________________________________________________________________
template <typename S, typename C, typename... A>
Id Vocabulary<S, C, A...>::lower_bound(const string& word,
                                 const SortLevel level) const {
  return static_cast<Id>(std::lower_bound(_words.begin(), _words.end(), word,
                                          getLowerBoundLambda(level)) -
                         _words.begin());
}

// _____________________________________________________________________________
template <typename S, typename ComparatorType, typename... A>
void Vocabulary<S, ComparatorType, A...>::setLocale(const std::string& language,
                                              const std::string& country,
                                              bool ignorePunctuation) {
  _caseComparator = ComparatorType(language, country, ignorePunctuation);
  _externalLiterals.getCaseComparator() =
      ComparatorType(language, country, ignorePunctuation);
}

template <typename StringType, typename C, typename... A>
//! Get the word with the given id.
//! lvalue for compressedString and const& for string-based vocabulary
AccessReturnType_t<StringType> Vocabulary<StringType, C, A...>::at(Id id) const {
  if constexpr (_isCompressed) {
    return idToOptionalString(id).value();
  } else {
    return _words[static_cast<size_t>(id)];
  }
}

// _____________________________________________________________________________
template <typename S, typename C, typename... A>
bool Vocabulary<S, C, A...>::getId(const string& word, Id* id) const {
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
template <typename S, typename C, typename... A>
std::pair<Id, Id> Vocabulary<S, C, A...>::prefix_range(const string& prefix) const {
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
template <typename S, typename C, typename... A>
void Vocabulary<S, C, A...>::push_back(const string& word) {
  if constexpr (_isCompressed) {
    _words.push_back(compressPrefix(word));
  } else {
    _words.push_back(word);
  }
}

// _____________________________________________________________________________
template <typename S, typename C, typename... A>
template <typename, typename>
const std::optional<std::reference_wrapper<const string>> Vocabulary<S, C, A...>::
operator[](Id id) const {
  if (id < _words.size()) {
    return _words[static_cast<size_t>(id)];
  } else {
    return std::nullopt;
  }
}
template const std::optional<std::reference_wrapper<const string>>
    TextVocabulary::operator[]<std::string, void>(Id id) const;

template <typename S, typename C, typename... A>
template <typename, typename>
const std::optional<string> Vocabulary<S, C, A...>::idToOptionalString(Id id) const {
  if (id < _words.size()) {
    // internal, prefixCompressed word
    return expandPrefix(_words[static_cast<size_t>(id)]);
  } else if (id == ID_NO_VALUE) {
  return std::nullopt;
  }

  // this word is either externalized, or a special value
  id -= _words.size();
  if (id < _externalLiterals.size()) {
    return _externalLiterals[id];
  }

  id -= _externalLiterals.size();
  id -= RuntimeParameters().get<"bounding_box_hack_correction_bias">();
  if (id >= _additionalValues.size()) {
    throw std::runtime_error("Tried to retrieve id " + std::to_string(id) + "from additional values, which has only" + std::to_string(_additionalValues.size()) + "elements");
  }
  AD_CHECK(id < _additionalValues.size());
  if constexpr (!decltype(_additionalValues)::isEmptyType) {
    auto toString = [](const auto& x) { using std::to_string; return to_string(x); };
    return _additionalValues.template at(id, toString);
  }
}

template <typename S, typename C, typename... A>
template <typename, typename>
float Vocabulary<S, C, A...>::idToFloat(Id id) const {
  const auto nan = std::numeric_limits<float>::quiet_NaN();
  if (id < _words.size() + _externalLiterals.size()) {
    return nan;
  } else if (id == ID_NO_VALUE) {
    return nan;
  }
  // this word is either externalized, or a special value
  id -= (_words.size() + _externalLiterals.size());
  if constexpr (!decltype(_additionalValues)::isEmptyType) {
    if (id < _additionalValues.size()) {
      auto toString = [](const auto& x) -> float {
        if constexpr (std::is_same_v<ad_geo::Rectangle, std::decay_t<decltype(x)>>) {
          return std::numeric_limits<float>::quiet_NaN();
        } else {
          return x;
        }
      };
      return _additionalValues.template at(id, toString);
    }
  }
  return nan;
}

template <typename S, typename C, typename... A>
template <typename, typename>
std::optional<ad_geo::Rectangle> Vocabulary<S, C, A...>::idToRectangle(Id id) const {
  const auto empty = std::nullopt;
  if (id < _words.size() + _externalLiterals.size()) {
    return empty;
  } else if (id == ID_NO_VALUE) {
    return empty;
  }
  // this word is either externalized, or a special value
  id -= (_words.size() + _externalLiterals.size());
  id -= RuntimeParameters().get<"bounding_box_hack_correction_bias">();

  if (id >= _additionalValues.size()) {
    throw std::runtime_error("Tried to retrieve id " + std::to_string(id) + "from additional values, which has only" + std::to_string(_additionalValues.size()) + "elements");
  }
  if constexpr (!decltype(_additionalValues)::isEmptyType) {
    if (id < _additionalValues.size()) {
      auto toString = [](const auto& x) -> std::optional<ad_geo::Rectangle> {
        if constexpr (!std::is_same_v<ad_geo::Rectangle, std::decay_t<decltype(x)>>) {
          return std::nullopt;
        } else {
          return x;
        }
      };
      return _additionalValues.template at(id, toString);
    }
  }
  return empty;
}
template const std::optional<string>
RdfsVocabulary::idToOptionalString<CompressedString, void>(Id id) const;

template float
RdfsVocabulary::idToFloat<CompressedString, void>(Id id) const;

template std::optional<ad_geo::Rectangle>
RdfsVocabulary::idToRectangle<CompressedString, void>(Id id) const;

// Explicit template instantiations
template class Vocabulary<CompressedString, TripleComponentComparator, AdditionalFloatType, AdditionalRectangleType>;
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
