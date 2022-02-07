//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PREFIXCOMPRESSOR_H
#define QLEVER_PREFIXCOMPRESSOR_H

#include <array>
#include <string>
#include <vector>

#include "../../global/Constants.h"
#include "../../util/Exception.h"
#include "../../util/Log.h"

// TODO<joka921> Include the relevant constants directly here.

class PrefixCompressor {
 private:
  // simple class for members of a prefix compression codebook
  struct Prefix {
    Prefix() = default;
    Prefix(char prefix, std::string fulltext)
        : _prefix(1, prefix), _fulltext(std::move(fulltext)) {}

    std::string _prefix;
    std::string _fulltext;
  };

  // List of all prefixes, sorted descending by the length
  // of the prefixes. Used for lookup when compressing.
  std::vector<Prefix> _prefixVec{};

  // maps (numeric) keys to the prefix they encode.
  // currently only 128 prefixes are supported.
  std::array<std::string, NUM_COMPRESSION_PREFIXES> _prefixMap{""};

 public:
  // Compress a word.
  [[nodiscard]] std::string compress(std::string_view word) const {
    for (const auto& p : _prefixVec) {
      if (word.starts_with(p._fulltext)) {
        return p._prefix + std::string_view(word).substr(p._fulltext.size());
      }
    }
    return static_cast<char>(NO_PREFIX_CHAR) + word;
  }

  // Decompress a word which was previously compressed by the `compress` method.
  [[nodiscard]] std::string decompress(std::string_view word) const {
    AD_CHECK(!word.empty());
    auto idx = static_cast<uint8_t>(word[0]) - MIN_COMPRESSION_PREFIX;
    if (idx >= 0 && idx < NUM_COMPRESSION_PREFIXES) {
      return _prefixMap[idx] + word.substr(1);
    } else {
      return string(word.substr(1));
    }
  }

  // Initialize compression from list of prefixes
  // The prefixes do not have to be in any specific order.
  //
  // `prefixes` can be of any type where
  // for (const string& el : prefixes {}
  // works.
  // TODO<joka921> Make this a part of the constructor, as soon as we
  // have integrated this code into qlever.
  template <typename StringRange>
  void initializePrefixes(const StringRange& prefixes) {
    for (auto& el : _prefixMap) {
      el = "";
    }

    _prefixVec.clear();
    unsigned char prefixIdx = 0;
    for (const auto& fulltext : prefixes) {
      if (prefixIdx >= NUM_COMPRESSION_PREFIXES) {
        LOG(ERROR)
            << "More than " << NUM_COMPRESSION_PREFIXES
            << " prefixes have been specified. This should never happen\n";
        AD_CHECK(false);
      }
      _prefixMap[prefixIdx] = fulltext;
      _prefixVec.emplace_back(prefixIdx + MIN_COMPRESSION_PREFIX, fulltext);
      prefixIdx++;
    }

    // if longest strings come first we correctly handle overlapping prefixes
    auto pred = [](const Prefix& a, const Prefix& b) {
      return a._fulltext.size() > b._fulltext.size();
    };
    std::sort(_prefixVec.begin(), _prefixVec.end(), pred);
  }
};

#endif  // QLEVER_PREFIXCOMPRESSOR_H
