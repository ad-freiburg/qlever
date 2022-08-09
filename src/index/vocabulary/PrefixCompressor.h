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

/// Compression and decompression of words given a codebook of common prefixes.
/// The maximum number of prefixes is `NUM_COMPRESSION_PREFIXES` (currently
/// 126).
class PrefixCompressor {
 private:
  // Simple class for a prefix and its code as members of the codebook.
  struct PrefixCode {
    PrefixCode() = default;
    PrefixCode(char code, std::string prefix)
        : _code(1, code), _prefix(std::move(prefix)) {}

    std::string _code;
    std::string _prefix;
  };

  // List of all prefixes, sorted descending by the length
  // of the prefixes. Used for lookup when compressing.
  std::vector<PrefixCode> _codeToPrefix{};

  // maps (numeric) keys to the prefix they encode.
  // currently only 128 prefixes are supported.
  std::array<std::string, NUM_COMPRESSION_PREFIXES> _prefixToCode{""};

 public:
  // Compress the given `word`. Note: This iterates over all prefixes in the
  // codebook, and it is currently not a bottleneck in the IndexBuilder.
  [[nodiscard]] std::string compress(std::string_view word) const {
    for (const auto& p : _codeToPrefix) {
      if (word.starts_with(p._prefix)) {
        return p._code + std::string_view(word).substr(p._prefix.size());
      }
    }
    return static_cast<char>(NO_PREFIX_CHAR) + word;
  }

  // Decompress the given `compressedWord`.
  [[nodiscard]] std::string decompress(std::string_view compressedWord) const {
    AD_CHECK(!compressedWord.empty());
    auto idx = static_cast<uint8_t>(compressedWord[0]) - MIN_COMPRESSION_PREFIX;
    if (idx >= 0 && idx < NUM_COMPRESSION_PREFIXES) {
      return _prefixToCode[idx] + compressedWord.substr(1);
    } else {
      return string(compressedWord.substr(1));
    }
  }

  // From the given list of prefixes, build the internal data structure for
  // efficient lookup. The prefixes do not have to be in any specific order. The
  // type of `prefixes` can be any type for which `for (const string& el :
  // prefixes) {...}` works.
  // TODO<joka921> Make this a part of the constructor, as soon as we have
  // integrated this code into qlever.
  template <typename StringRange>
  void buildCodebook(const StringRange& prefixes) {
    for (auto& el : _prefixToCode) {
      el = "";
    }

    _codeToPrefix.clear();
    unsigned char prefixIdx = 0;
    for (const auto& fulltext : prefixes) {
      if (prefixIdx >= NUM_COMPRESSION_PREFIXES) {
        LOG(ERROR)
            << "More than " << NUM_COMPRESSION_PREFIXES
            << " prefixes have been specified. This should never happen\n";
        AD_FAIL();
      }
      _prefixToCode[prefixIdx] = fulltext;
      _codeToPrefix.emplace_back(prefixIdx + MIN_COMPRESSION_PREFIX, fulltext);
      prefixIdx++;
    }

    // if longest strings come first we correctly handle overlapping prefixes
    auto pred = [](const PrefixCode& a, const PrefixCode& b) {
      return a._prefix.size() > b._prefix.size();
    };
    std::sort(_codeToPrefix.begin(), _codeToPrefix.end(), pred);
  }
};

#endif  // QLEVER_PREFIXCOMPRESSOR_H
