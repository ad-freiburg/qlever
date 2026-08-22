// Copyright 2022, 2026, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//         Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

#ifndef QLEVER_PREFIXCOMPRESSOR_H
#define QLEVER_PREFIXCOMPRESSOR_H

#include <array>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeVector.h"
#include "util/StringUtils.h"

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
        : code_(1, code), prefix_(std::move(prefix)) {}

    std::string code_;
    std::string prefix_;
    AD_SERIALIZE_FRIEND_FUNCTION(PrefixCode) {
      serializer | arg.code_;
      serializer | arg.prefix_;
    }
  };

  // List of all prefixes, sorted descending by the length
  // of the prefixes. Used for lookup when compressing.
  std::vector<PrefixCode> codeToPrefix_{};

  // maps (numeric) keys to the prefix they encode.
  // currently only 128 prefixes are supported.
  std::array<std::string, NUM_COMPRESSION_PREFIXES> prefixToCode_{""};

  AD_SERIALIZE_FRIEND_FUNCTION(PrefixCompressor) {
    serializer | arg.codeToPrefix_;
    serializer | arg.prefixToCode_;
  }

 public:
  // Compress the given `word`. Note: This iterates over all prefixes in the
  // codebook, and it is currently not a bottleneck in the IndexBuilder.
  [[nodiscard]] std::string compress(std::string_view word) const {
    for (const auto& p : codeToPrefix_) {
      if (ql::starts_with(word, p.prefix_)) {
        return p.code_ + std::string_view(word).substr(p.prefix_.size());
      }
    }
    return static_cast<char>(NO_PREFIX_CHAR) + word;
  }

  // Return the index in `prefixToCode_` if the word was stored with a valid
  // compression prefix, or `std::nullopt` if it was stored uncompressed.
  [[nodiscard]] static std::optional<size_t> prefixIndex(
      std::string_view compressedWord) {
    AD_CONTRACT_CHECK(!compressedWord.empty());
    const auto leadingByte = static_cast<uint8_t>(compressedWord.front());

    if (leadingByte >= MIN_COMPRESSION_PREFIX &&
        leadingByte < MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES) {
      return leadingByte - MIN_COMPRESSION_PREFIX;
    }

    return std::nullopt;
  }

  // Return an upper bound on the decompressed size of `compressedWord`.
  [[nodiscard]] size_t maxDecompressedSize(
      std::string_view compressedWord) const {
    const auto idx = prefixIndex(compressedWord);
    const size_t rest = compressedWord.size() - 1;

    if (idx.has_value()) {
      return prefixToCode_[idx.value()].size() + rest;
    }
    return rest;
  }

  // Decompress `compressedWord` into `out`. `out.size()` must be at least
  // `maxDecompressedSize(compressedWord)`. Return the number of bytes written.
  [[nodiscard]] size_t decompressInto(std::string_view compressedWord,
                                      ql::span<char> out) const {
    AD_CONTRACT_CHECK(out.size() >= maxDecompressedSize(compressedWord));

    const auto idx = prefixIndex(compressedWord);
    const std::string_view rest = compressedWord.substr(1);

    size_t numBytesWritten = 0;
    if (idx.has_value()) {
      const std::string& prefix = prefixToCode_[idx.value()];
      if (!prefix.empty()) {
        std::memcpy(out.data(), prefix.data(), prefix.size());
        numBytesWritten = prefix.size();
      }
    }
    if (!rest.empty()) {
      std::memcpy(out.data() + numBytesWritten, rest.data(), rest.size());
      numBytesWritten += rest.size();
    }
    return numBytesWritten;
  }

  // Decompress the given `compressedWord`.
  [[nodiscard]] std::string decompress(std::string_view compressedWord) const {
    const auto idx = prefixIndex(compressedWord);
    if (idx.has_value()) {
      return prefixToCode_[idx.value()] + compressedWord.substr(1);
    }
    return std::string(compressedWord.substr(1));
  }

  // From the given list of prefixes, build the internal data structure for
  // efficient lookup. The prefixes do not have to be in any specific order. The
  // type of `prefixes` can be any type for which `for (const string& el :
  // prefixes) {...}` works.
  // TODO<joka921> Make this a part of the constructor, as soon as we have
  // integrated this code into qlever.
  template <typename StringRange>
  void buildCodebook(const StringRange& prefixes) {
    for (auto& el : prefixToCode_) {
      el = "";
    }

    codeToPrefix_.clear();
    unsigned char prefixIdx = 0;
    for (const auto& fulltext : prefixes) {
      if (prefixIdx >= NUM_COMPRESSION_PREFIXES) {
        AD_THROW(absl::StrCat(
            "More than ", NUM_COMPRESSION_PREFIXES,
            " prefixes have been specified. This should never happen"));
      }
      prefixToCode_[prefixIdx] = fulltext;
      codeToPrefix_.emplace_back(prefixIdx + MIN_COMPRESSION_PREFIX, fulltext);
      prefixIdx++;
    }

    // if longest strings come first we correctly handle overlapping prefixes
    auto pred = [](const PrefixCode& a, const PrefixCode& b) {
      return a.prefix_.size() > b.prefix_.size();
    };
    std::sort(codeToPrefix_.begin(), codeToPrefix_.end(), pred);
  }

  const auto& prefixToCode() const { return prefixToCode_; }
};

#endif  // QLEVER_PREFIXCOMPRESSOR_H
