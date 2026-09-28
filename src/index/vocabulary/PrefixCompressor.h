// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_PREFIXCOMPRESSOR_H
#define QLEVER_PREFIXCOMPRESSOR_H

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "backports/string.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeVector.h"
#include "util/StringUtils.h"

// TODO<joka921> Include the relevant constants directly here.

// ____________________________________________________________________________
/// Compression and decompression of words given a codebook of common prefixes.
/// The maximum number of prefixes is `NUM_COMPRESSION_PREFIXES` (currently
/// 126).
class PrefixCompressor {
 private:
  // ___________________________________________________________________________
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

  // ___________________________________________________________________________
  // List of all prefixes, sorted descending by the length
  // of the prefixes. Used for lookup when compressing.
  std::vector<PrefixCode> codeToPrefix_{};

  // ___________________________________________________________________________
  // maps (numeric) keys to the prefix they encode.
  // currently NUM_COMPRESSION_PREFIXES prefixes are supported.
  std::array<std::string, NUM_COMPRESSION_PREFIXES> prefixToCode_{""};

  // ___________________________________________________________________________
  AD_SERIALIZE_FRIEND_FUNCTION(PrefixCompressor) {
    serializer | arg.codeToPrefix_;
    serializer | arg.prefixToCode_;
  }

  // ___________________________________________________________________________
  // Compute the decompressed size from the already-determined `prefixIdx`,
  // avoiding a second scan of the leading byte. `restSize` is the size of
  // `compressedWord` without its leading code byte.
  [[nodiscard]] size_t decompressedSizeWithIndex(
      size_t restSize, std::optional<size_t> prefixIdx) const {
    if (prefixIdx.has_value()) {
      AD_CORRECTNESS_CHECK(*prefixIdx < prefixToCode_.size());
      const size_t prefixSize = prefixToCode_[*prefixIdx].size();
      AD_CORRECTNESS_CHECK(prefixSize <=
                           std::numeric_limits<size_t>::max() - restSize);
      return prefixSize + restSize;
    }
    return restSize;
  }

  // ___________________________________________________________________________
  // Shared memcpy implementation for `decompressInto` and `decompress`.
  // The caller must guarantee `out.size()` is at least
  // `decompressedSizeWithIndex` for the given `prefixIdx`, and
  // `compressedWord` must not be empty.
  size_t decompressIntoWithIndex(std::string_view compressedWord,
                                 std::optional<size_t> prefixIdx,
                                 ql::span<char> out) const {
    const std::string_view rest = compressedWord.substr(1);
    size_t outputSize = 0;
    if (prefixIdx.has_value()) {
      const std::string& prefix = prefixToCode_[*prefixIdx];
      AD_CORRECTNESS_CHECK(prefix.size() <= out.size());
      if (!prefix.empty()) {
        std::memcpy(out.data(), prefix.data(), prefix.size());
      }
      outputSize = prefix.size();
    }
    AD_CORRECTNESS_CHECK(rest.size() <= out.size() - outputSize);
    if (!rest.empty()) {
      std::memcpy(out.data() + outputSize, rest.data(), rest.size());
    }
    return outputSize + rest.size();
  }

 public:
  // ___________________________________________________________________________
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

  // ___________________________________________________________________________
  // Return the `prefixToCode_` index when the first byte is in the range
  // [MIN_COMPRESSION_PREFIX, MIN_COMPRESSION_PREFIX +
  // NUM_COMPRESSION_PREFIXES); otherwise return `std::nullopt`. Exposed so
  // that callers can inspect the prefix code without decompressing; used by
  // `maxDecompressedSize` and `decompressInto` to scan the leading byte once.
  [[nodiscard]] static std::optional<size_t> prefixIndex(
      std::string_view compressedWord) {
    if (compressedWord.empty()) {
      return std::nullopt;
    }
    const auto leadingByte = static_cast<uint8_t>(compressedWord.front());

    if (leadingByte >= MIN_COMPRESSION_PREFIX &&
        leadingByte < MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES) {
      const size_t index = leadingByte - MIN_COMPRESSION_PREFIX;
      // The surrounding range check establishes that `index` is a valid prefix
      // code.
      return index;
    }

    return std::nullopt;
  }

  // ___________________________________________________________________________
  // Return the exact decompressed size of `compressedWord`. Use this to size
  // an output buffer before calling `decompressInto` for allocation-free
  // decompression.
  [[nodiscard]] size_t maxDecompressedSize(
      std::string_view compressedWord) const {
    AD_CONTRACT_CHECK(!compressedWord.empty());
    return decompressedSizeWithIndex(compressedWord.size() - 1,
                                     prefixIndex(compressedWord));
  }

  // ___________________________________________________________________________
  // Decompress `compressedWord` into `out`. `out.size()` must be at least
  // `maxDecompressedSize(compressedWord)`. Return the number of bytes
  // written. Zero-copy decompression into a pre-allocated buffer; avoids a
  // heap allocation per word when the caller reuses `out`.
  [[nodiscard]] size_t decompressInto(std::string_view compressedWord,
                                      ql::span<char> out) const {
    AD_CONTRACT_CHECK(!compressedWord.empty());
    const auto idx = prefixIndex(compressedWord);
    AD_CONTRACT_CHECK(out.size() >= decompressedSizeWithIndex(
                                        compressedWord.size() - 1, idx));
    return decompressIntoWithIndex(compressedWord, idx, out);
  }

  // ___________________________________________________________________________
  // Decompress the given `compressedWord`.
  [[nodiscard]] std::string decompress(std::string_view compressedWord) const {
    AD_CONTRACT_CHECK(!compressedWord.empty());
    const auto idx = prefixIndex(compressedWord);
    // `decompressIntoWithIndex` always writes exactly
    // `decompressedSizeWithIndex` bytes, so decode directly into
    // uninitialized storage instead of zero-filling it first.
    std::string result;
    ql::resize_and_overwrite(
        result, decompressedSizeWithIndex(compressedWord.size() - 1, idx),
        [&](char* buf, size_t count) {
          return decompressIntoWithIndex(compressedWord, idx,
                                         ql::span<char>{buf, count});
        });
    return result;
  }

  // ___________________________________________________________________________
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

  // ___________________________________________________________________________
  const auto& prefixToCode() const { return prefixToCode_; }
};

#endif  // QLEVER_PREFIXCOMPRESSOR_H
