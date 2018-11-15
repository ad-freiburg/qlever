// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>,
//          Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <fstream>
#include <iostream>

#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"
#include "./ConstantsIndexCreation.h"
#include "./Vocabulary.h"

using std::string;

// _____________________________________________________________________________
template <class S>
void Vocabulary<S>::readFromFile(const string& fileName,
                                 const string& extLitsFileName) {
  LOG(INFO) << "Reading vocabulary from file " << fileName << "\n";
  _words.clear();
  std::fstream in(fileName.c_str(), std::ios_base::in);
  string line;
  while (std::getline(in, line)) {
    if constexpr (_isCompressed) {
      // when we read from file it means that all preprocessing has been done
      // and the prefixes are already stripped in the file
      _words.push_back(CompressedString::fromString(line));
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
template <class S>
template <typename>
void Vocabulary<S>::writeToFile(const string& fileName) const {
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
template <class S>
template <typename>
void Vocabulary<S>::writeToBinaryFileForMerging(const string& fileName) const {
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
template <class S>
template <typename>
void Vocabulary<S>::createFromSet(const ad_utility::HashSet<S>& set) {
  LOG(INFO) << "Creating vocabulary from set ...\n";
  _words.clear();
  _words.reserve(set.size());
  _words.insert(begin(_words), begin(set), end(set));
  LOG(INFO) << "... sorting ...\n";
  std::sort(begin(_words), end(_words));
  LOG(INFO) << "Done creating vocabulary.\n";
}

// _____________________________________________________________________________
// TODO<joka921> is this used? if no, throw out, if yes, transform to
// compressedString as key type
template <class S>
template <typename>
google::sparse_hash_map<string, Id> Vocabulary<S>::asMap() {
  google::sparse_hash_map<string, Id> map;
  for (size_t i = 0; i < _words.size(); ++i) {
    map[_words[i]] = i;
  }
  return map;
}

// _____________________________________________________________________________
template <class S>
template <typename>
void Vocabulary<S>::externalizeLiterals(const string& fileName) {
  LOG(INFO) << "Externalizing literals..." << std::endl;
  auto ext = std::lower_bound(_words.begin(), _words.end(),
                              string({EXTERNALIZED_LITERALS_PREFIX}));
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
template <class S>
bool Vocabulary<S>::isLiteral(const string& word) {
  return word.size() > 0 && word[0] == '\"';
}

// _____________________________________________________________________________
template <class S>
bool Vocabulary<S>::isExternalizedLiteral(const string& word) {
  return word.size() > 1 && word[0] == EXTERNALIZED_LITERALS_PREFIX &&
         word[1] == '\"';
}

// _____________________________________________________________________________
template <class S>
template <bool isEntity>
bool Vocabulary<S>::shouldBeExternalized(const string& word) const {
  if constexpr (isEntity) {
    return shouldEntityBeExternalized(word);
  }

  if (!isLiteral(word)) {
    return shouldEntityBeExternalized(word);
  } else {
    if (word.size() > 100) {
      return true;
    }
    string lang = getLanguage(word);
    if (lang != "") {
      return (lang != "en");  // && lang != "en_gb" && lang != "en_us" &&
      // lang != "de" && lang != "es" && lang != "fr");
    }
  }
  return false;
}

// ___________________________________________________________________
template <class S>
bool Vocabulary<S>::shouldEntityBeExternalized(const string& word) const {
  for (const auto& p : _externalizedPrefixes) {
    if (ad_utility::startsWith(word, p)) {
      return true;
    }
  }
  return false;
}

// _____________________________________________________________________________
template <class S>
string Vocabulary<S>::getLanguage(const string& literal) {
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
template <class S>
template <typename>
string Vocabulary<S>::expandPrefix(const CompressedString& word) const {
  assert(!word.empty());
  auto idx = static_cast<uint8_t>(word[0]) - MIN_COMPRESSION_PREFIX;
  if (idx < NUM_COMPRESSION_PREFIXES) {
    return _prefixMap[idx] + word.toStringView().substr(1);
  } else {
    return string(word.toStringView().substr(1));
  }
}

// _____________________________________________________________________________
template <class S>
template <typename>
CompressedString Vocabulary<S>::compressPrefix(const string& word) const {
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
template <class S>
template <class StringRange, typename>
void Vocabulary<S>::initializePrefixes(const StringRange& prefixes) {
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
template <class S>
template <class StringRange>
void Vocabulary<S>::initializeExternalizePrefixes(const StringRange& s) {
  _externalizedPrefixes.clear();
  for (const auto& el : s) {
    _externalizedPrefixes.push_back(el);
  }
}

// __________________________________________________________________________
template <class S>
bool PrefixComparator<S>::operator()(const CompressedString& lhsComp,
                                     const string& rhs) const {
  string lhs = _vocab->expandPrefix(lhsComp);
  return this->operator()(lhs, rhs);
}

// __________________________________________________________________________
template <class S>
bool PrefixComparator<S>::operator()(const string& lhs,
                                     const CompressedString& rhsComp) const {
  string rhs = _vocab->expandPrefix(rhsComp);
  return this->operator()(lhs, rhs);
}

// __________________________________________________________________________
template <class S>
bool PrefixComparator<S>::operator()(const string& lhs,
                                     const string& rhs) const {
  // TODO<joka921> use string_view for the substrings
  return (lhs.size() > _prefixLength ? lhs.substr(0, _prefixLength) : lhs) <
         (rhs.size() > _prefixLength ? rhs.substr(0, _prefixLength) : rhs);
}

// _____________________________________________________
template <class S>
template <typename>
void Vocabulary<S>::prefixCompressFile(const string& infile,
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
